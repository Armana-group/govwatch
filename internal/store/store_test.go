package store

import (
	"bytes"
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/koinos/koinos-proto-golang/v2/koinos/contracts/governance"
	"github.com/koinos/koinos-proto-golang/v2/koinos/protocol"
	"govwatch/internal/decode"
	"govwatch/internal/testfix"
)

var (
	net   = decode.Mainnet
	gov   = net.Governance
	koin  = net.Koin
	alice = testfix.Addr(0xA1)
	ctx   = context.Background()
)

func open(t *testing.T) *Store {
	t.Helper()
	s, err := Open(filepath.Join(t.TempDir(), "t.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func factsOf(t *testing.T, b *protocol.Block, r *protocol.BlockReceipt) []decode.Fact {
	t.Helper()
	f, err := decode.Decode(net, b, r)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func apply(t *testing.T, s *Store, irreversible bool, b *protocol.Block, r *protocol.BlockReceipt) {
	t.Helper()
	if err := s.Apply(ctx, b.Header.Height, b.Id, irreversible, factsOf(t, b, r)); err != nil {
		t.Fatalf("Apply@%d: %v", b.Header.Height, err)
	}
}

func applyStatus(t *testing.T, s *Store, irreversible bool, h uint64, id []byte, st governance.ProposalStatus) {
	t.Helper()
	b := testfix.Block(h)
	apply(t, s, irreversible, b, testfix.Receipt(b, []*protocol.EventData{testfix.StatusEvent(gov, id, st)}))
}

func submission(h uint64) (*protocol.Block, *protocol.BlockReceipt, []byte) {
	tx, root := testfix.SubmitProposalTx(testfix.TxID(h), alice, gov, []*protocol.Operation{testfix.UploadOp(koin, []byte("v2"))}, 5000)
	b := testfix.Block(h, tx)
	return b, testfix.Receipt(b, nil, testfix.TxReceipt(tx.Id, alice, false, testfix.StatusEvent(gov, root, governance.ProposalStatus_pending))), root
}

func counts(t *testing.T, s *Store) map[string]int64 {
	t.Helper()
	c, err := s.Counts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestOpen_CreatesSchemaAndEmptyCursor(t *testing.T) {
	s := open(t)
	c, err := s.Cursor(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if c.Height != 0 || c.BlockID != nil {
		t.Fatalf("cursor = %+v, want zero", c)
	}
	got := counts(t, s)
	for _, table := range []string{"proposals", "proposal_events", "system_changes", "system_contracts"} {
		if n, ok := got[table]; !ok || n != 0 {
			t.Fatalf("counts[%s] = %d, %v", table, n, ok)
		}
	}
}

func TestApply_ProposalLifecycle(t *testing.T) {
	s := open(t)
	b, r, id := submission(100)
	apply(t, s, true, b, r)
	applyStatus(t, s, true, 201700, id, governance.ProposalStatus_active)
	applyStatus(t, s, true, 604900, id, governance.ProposalStatus_approved)
	b4 := testfix.Block(806500)
	r4 := testfix.Receipt(b4, []*protocol.EventData{testfix.StatusEvent(gov, id, governance.ProposalStatus_applied)}, testfix.TxReceipt(testfix.TxID(999), gov, false, testfix.SetSystemContractEvent(koin, true)))
	apply(t, s, true, b4, r4)

	rows, err := s.Proposals(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("proposals = %d, want 1", len(rows))
	}
	p := rows[0]
	if !bytes.Equal(p.ID, id) || p.SubmittedHeight != 100 || !bytes.Equal(p.Payer, alice) || p.Fee != 5000 || p.Status != "applied" || p.UpdatesGovernance || p.VoteStartHeight != 100+201600 || p.LastHeight != 806500 {
		t.Fatalf("row = %+v", p)
	}
	c := counts(t, s)
	if c["proposal_events"] != 5 { // pending, active, approved, applied, proposal_applied
		t.Fatalf("proposal_events = %d, want 5", c["proposal_events"])
	}
	if c["system_changes"] != 2 { // the kernel set_system_contract event plus the proposal's own upload_contract, expanded on application
		t.Fatalf("system_changes = %d, want 2", c["system_changes"])
	}
	cur, _ := s.Cursor(ctx)
	if cur.Height != 806500 || !bytes.Equal(cur.BlockID, b4.Id) {
		t.Fatalf("cursor = %+v", cur)
	}
}

func TestApply_Idempotent(t *testing.T) {
	s := open(t)
	b, r, id := submission(100)
	apply(t, s, true, b, r)
	applyStatus(t, s, true, 201700, id, governance.ProposalStatus_active)
	before := counts(t, s)
	apply(t, s, true, b, r)
	applyStatus(t, s, true, 201700, id, governance.ProposalStatus_active)
	after := counts(t, s)
	for k, v := range before {
		if after[k] != v {
			t.Fatalf("counts[%s] %d → %d after re-apply", k, v, after[k])
		}
	}
	cur, _ := s.Cursor(ctx)
	if cur.Height != 201700 {
		t.Fatalf("cursor = %+v", cur)
	}
}

func TestApply_SystemContractSet(t *testing.T) {
	s := open(t)
	b1 := testfix.Block(10, testfix.Tx(testfix.TxID(10), gov, testfix.SetSystemContractOp(koin, true)))
	apply(t, s, true, b1, testfix.Receipt(b1, nil, testfix.ReceiptsFor(b1)...))
	b2 := testfix.Block(20, testfix.Tx(testfix.TxID(20), gov, testfix.SetSystemContractOp(koin, false)))
	apply(t, s, true, b2, testfix.Receipt(b2, nil, testfix.ReceiptsFor(b2)...))
	rows, err := s.SystemContracts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || !bytes.Equal(rows[0].ContractID, koin) || rows[0].System || rows[0].SinceHeight != 20 {
		t.Fatalf("rows = %+v", rows)
	}
}

func TestRollback_RemovesProvisional(t *testing.T) {
	s := open(t)
	b, r, id := submission(40)
	apply(t, s, true, b, r)
	applyStatus(t, s, false, 50, id, governance.ProposalStatus_active)
	if err := s.Rollback(ctx, 50); err != nil {
		t.Fatal(err)
	}
	c := counts(t, s)
	if c["proposal_events"] != 1 || c["proposals"] != 1 {
		t.Fatalf("counts after rollback = %v", c)
	}
	rows, _ := s.Proposals(ctx)
	if rows[0].Status != "pending" || rows[0].LastHeight != 40 {
		t.Fatalf("proposal after rollback = %+v", rows[0])
	}
	cur, _ := s.Cursor(ctx)
	if cur.Height != 49 {
		t.Fatalf("cursor = %+v, want 49", cur)
	}
}

func TestMarkIrreversible(t *testing.T) {
	s := open(t)
	b, r, _ := submission(40)
	apply(t, s, false, b, r)
	if err := s.MarkIrreversible(ctx, 40); err != nil {
		t.Fatal(err)
	}
	// Now a rollback at 40 must not remove the irreversible rows.
	if err := s.Rollback(ctx, 40); err != nil {
		t.Fatal(err)
	}
	if c := counts(t, s); c["proposals"] != 1 || c["proposal_events"] != 1 {
		t.Fatalf("irreversible rows removed: %v", c)
	}
}

func TestApply_ResubmittedProposalGetsOwnRow(t *testing.T) {
	s := open(t)
	b1, r1, id := submission(100)
	apply(t, s, true, b1, r1)
	applyStatus(t, s, true, 300, id, governance.ProposalStatus_reverted)
	// Same operations, same merkle root, same id: a brand-new submission.
	b2, r2, id2 := submission(400)
	if !bytes.Equal(id, id2) {
		t.Fatal("fixture should produce the same proposal id")
	}
	apply(t, s, true, b2, r2)
	applyStatus(t, s, true, 600, id, governance.ProposalStatus_active)
	rows, err := s.Proposals(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("rows = %d, want one per submission: %+v", len(rows), rows)
	}
	if rows[0].SubmittedHeight != 100 || rows[0].Status != "reverted" || rows[0].LastHeight != 300 {
		t.Fatalf("first submission = %+v", rows[0])
	}
	if rows[1].SubmittedHeight != 400 || rows[1].Status != "active" || rows[1].LastHeight != 600 || !bytes.Equal(rows[1].SubmittedTx, b2.Transactions[0].Id) {
		t.Fatalf("second submission = %+v", rows[1])
	}
}

func TestApply_CursorNeverMovesBackward(t *testing.T) {
	s := open(t)
	if err := s.Apply(ctx, 100, testfix.BlockID(100), true, nil); err != nil {
		t.Fatal(err)
	}
	b, r, _ := submission(50)
	apply(t, s, true, b, r)
	cur, _ := s.Cursor(ctx)
	if cur.Height != 100 || !bytes.Equal(cur.BlockID, testfix.BlockID(100)) {
		t.Fatalf("cursor moved backwards: %+v", cur)
	}
	if c := counts(t, s); c["proposals"] != 1 {
		t.Fatalf("re-applied block not recorded: %v", c)
	}
}

func TestOpen_MigratesV1Proposals(t *testing.T) {
	path := filepath.Join(t.TempDir(), "v1.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{
		`CREATE TABLE proposals (id BLOB PRIMARY KEY, submitted_height INTEGER NOT NULL, submitted_tx BLOB NOT NULL, payer BLOB NOT NULL, fee INTEGER NOT NULL, operations BLOB NOT NULL, updates_governance INTEGER NOT NULL, vote_start_height INTEGER NOT NULL, status TEXT NOT NULL, last_height INTEGER NOT NULL, irreversible INTEGER NOT NULL)`,
		`INSERT INTO proposals VALUES (x'1220aa', 100, x'1220bb', x'00', 7, x'', 0, 201700, 'reverted', 300, 1)`,
		`CREATE TABLE proposal_events (proposal_id BLOB NOT NULL, height INTEGER NOT NULL, block_id BLOB NOT NULL, tx_id BLOB, kind TEXT NOT NULL, status TEXT, tally INTEGER, threshold INTEGER, irreversible INTEGER NOT NULL, UNIQUE (proposal_id, height, kind, status))`,
		`INSERT INTO proposal_events VALUES (x'1220aa', 100, x'01', x'1220bb', 'proposal_status', 'pending', NULL, NULL, 1)`,
		`INSERT INTO proposal_events VALUES (x'1220aa', 300, x'02', NULL, 'proposal_status', 'reverted', NULL, NULL, 1)`,
		`PRAGMA user_version = 1`,
	} {
		if _, err := db.Exec(q); err != nil {
			t.Fatal(q, err)
		}
	}
	db.Close()
	s, err := Open(path)
	if err != nil {
		t.Fatalf("Open v1 db: %v", err)
	}
	defer s.Close()
	rows, err := s.Proposals(ctx)
	if err != nil || len(rows) != 1 || rows[0].SubmittedHeight != 100 || rows[0].Status != "reverted" {
		t.Fatalf("rows=%+v err=%v", rows, err)
	}
	var v int
	if err := s.db.QueryRow("PRAGMA user_version").Scan(&v); err != nil || v != schemaVersion {
		t.Fatalf("user_version = %d (err %v), want %d", v, err, schemaVersion)
	}
	// The new key allows a second submission of the same id.
	if _, err := s.db.Exec(`INSERT INTO proposals (id, submitted_height, submitted_tx, payer, fee, operations, updates_governance, vote_start_height, status, last_height, irreversible) VALUES (x'1220aa', 400, x'1220cc', x'00', 7, x'', 0, 201700, 'pending', 400, 1)`); err != nil {
		t.Fatalf("second submission rejected after migration: %v", err)
	}
}

// --- fix pass after final review ---

func TestApply_MultipleKernelEventsInOneReceiptSurviveRollback(t *testing.T) {
	s := open(t)
	b, r, id := submission(100)
	apply(t, s, true, b, r)
	vhp := testfix.Addr(0xC3)
	b2 := testfix.Block(806500)
	orphan := testfix.TxReceipt(testfix.TxID(900), gov, false, testfix.SetSystemContractEvent(koin, true), testfix.SetSystemContractEvent(vhp, true), testfix.SetSystemCallEvent(201, koin, 1), testfix.SetSystemCallEvent(202, vhp, 2))
	apply(t, s, false, b2, testfix.Receipt(b2, []*protocol.EventData{testfix.StatusEvent(gov, id, governance.ProposalStatus_applied)}, orphan))
	if c := counts(t, s); c["system_changes"] != 5 || c["system_contracts"] != 2 { // 4 kernel events + the expanded upload
		t.Fatalf("counts after apply = %v, want 5 changes and 2 contracts", c)
	}
	b3 := testfix.Block(806501)
	apply(t, s, false, b3, testfix.Receipt(b3, nil))
	if err := s.Rollback(ctx, 806501); err != nil {
		t.Fatal(err)
	}
	rows, _ := s.SystemContracts(ctx)
	if len(rows) != 2 {
		t.Fatalf("system_contracts after rollback = %+v, want both", rows)
	}
}

func TestApply_IdempotentWithVotes(t *testing.T) {
	s := open(t)
	id := testfix.TxID(5)
	b := testfix.Block(300000)
	r := testfix.Receipt(b, []*protocol.EventData{testfix.VoteEvent(gov, id, 1, 241920)})
	apply(t, s, true, b, r)
	apply(t, s, true, b, r)
	if c := counts(t, s); c["proposal_events"] != 1 {
		t.Fatalf("vote rows = %d after re-apply, want 1", c["proposal_events"])
	}
}

func TestApply_WrappedSubmissionCreatesRow(t *testing.T) {
	s := open(t)
	id := testfix.TxID(72)
	tx := testfix.Tx(testfix.TxID(73), alice, testfix.CallOp(testfix.Addr(0xB2), 0x1234, nil))
	b := testfix.Block(72, tx)
	apply(t, s, true, b, testfix.Receipt(b, nil, testfix.TxReceipt(tx.Id, alice, false, testfix.StatusEvent(gov, id, governance.ProposalStatus_pending))))
	rows, err := s.Proposals(ctx)
	if err != nil || len(rows) != 1 {
		t.Fatalf("rows=%+v err=%v", rows, err)
	}
	p := rows[0]
	if !bytes.Equal(p.ID, id) || p.OperationsKnown || p.Status != "pending" || !bytes.Equal(p.Payer, alice) || p.SubmittedHeight != 72 {
		t.Fatalf("row = %+v", p)
	}
}

func TestApply_AppliedProposalExpandsUploads(t *testing.T) {
	s := open(t)
	b, r, id := submission(100) // proposal uploads new code to koin
	apply(t, s, true, b, r)
	b2 := testfix.Block(806500)
	orphan := testfix.TxReceipt(testfix.TxID(901), gov, false)
	apply(t, s, true, b2, testfix.Receipt(b2, []*protocol.EventData{testfix.StatusEvent(gov, id, governance.ProposalStatus_applied)}, orphan))
	var n int64
	var payer, txid []byte
	err := s.db.QueryRow(`SELECT COUNT(*), MAX(payer), MAX(tx_id) FROM system_changes WHERE height = 806500 AND kind = 'upload_contract' AND contract_id = ?`, koin).Scan(&n, &payer, &txid)
	if err != nil || n != 1 || !bytes.Equal(payer, gov) || !bytes.Equal(txid, orphan.Id) {
		t.Fatalf("upload rows=%d payer=%x tx=%x err=%v", n, payer, txid, err)
	}
	// A reverted application must not be expanded.
	b3, r3, id3 := submission(200)
	_ = r3
	apply(t, s, true, b3, testfix.Receipt(b3, nil, testfix.TxReceipt(b3.Transactions[0].Id, alice, false, testfix.StatusEvent(gov, id3, governance.ProposalStatus_pending))))
	b4 := testfix.Block(806600)
	apply(t, s, true, b4, testfix.Receipt(b4, []*protocol.EventData{testfix.StatusEvent(gov, id3, governance.ProposalStatus_reverted)}, testfix.TxReceipt(testfix.TxID(902), gov, false)))
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM system_changes WHERE height = 806600`).Scan(&n); err != nil || n != 0 {
		t.Fatalf("reverted application expanded: %d rows", n)
	}
}

func TestApply_VoteEventRecordsThreshold(t *testing.T) {
	s := open(t)
	b, r, id := submission(100)
	apply(t, s, true, b, r)
	applyStatus(t, s, true, 201700, id, governance.ProposalStatus_active)
	b2 := testfix.Block(201701)
	apply(t, s, true, b2, testfix.Receipt(b2, []*protocol.EventData{testfix.VoteEvent(gov, id, 0, 241920)}))
	rows, _ := s.Proposals(ctx)
	if rows[0].VoteThreshold != 241920 || rows[0].VoteTally != 0 {
		t.Fatalf("row = %+v", rows[0])
	}
}

func TestOpen_MigratesV2(t *testing.T) {
	path := filepath.Join(t.TempDir(), "v2.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{
		`CREATE TABLE proposals (id BLOB NOT NULL, submitted_height INTEGER NOT NULL, submitted_tx BLOB NOT NULL, payer BLOB NOT NULL, fee INTEGER NOT NULL, operations BLOB NOT NULL, updates_governance INTEGER NOT NULL, vote_start_height INTEGER NOT NULL, status TEXT NOT NULL, last_height INTEGER NOT NULL, irreversible INTEGER NOT NULL, PRIMARY KEY (id, submitted_height))`,
		`INSERT INTO proposals VALUES (x'1220aa', 100, x'1220bb', x'00', 7, x'', 0, 201700, 'active', 300, 1)`,
		`CREATE TABLE proposal_events (proposal_id BLOB NOT NULL, height INTEGER NOT NULL, block_id BLOB NOT NULL, tx_id BLOB, kind TEXT NOT NULL, status TEXT, tally INTEGER, threshold INTEGER, irreversible INTEGER NOT NULL, UNIQUE (proposal_id, height, kind, status))`,
		`INSERT INTO proposal_events VALUES (x'1220aa', 300, x'02', NULL, 'proposal_vote', NULL, 1, 241920, 1)`,
		`INSERT INTO proposal_events VALUES (x'1220aa', 300, x'02', NULL, 'proposal_vote', NULL, 1, 241920, 1)`,
		`CREATE TABLE system_changes (height INTEGER NOT NULL, block_id BLOB NOT NULL, tx_id BLOB NOT NULL, op_index INTEGER NOT NULL, payer BLOB, kind TEXT NOT NULL, contract_id BLOB, call_id INTEGER, target BLOB, system_flag INTEGER, bytecode_hash BLOB, irreversible INTEGER NOT NULL, UNIQUE (height, tx_id, op_index, kind))`,
		`INSERT INTO system_changes VALUES (11, x'01', x'1220cc', 0, x'00', 'set_system_contract', x'0001', NULL, NULL, 1, NULL, 1)`,
		`PRAGMA user_version = 2`,
	} {
		if _, err := db.Exec(q); err != nil {
			t.Fatal(q, err)
		}
	}
	db.Close()
	s, err := Open(path)
	if err != nil {
		t.Fatalf("Open v2 db: %v", err)
	}
	defer s.Close()
	c := counts(t, s)
	if c["proposal_events"] != 1 || c["system_changes"] != 1 || c["proposals"] != 1 {
		t.Fatalf("counts after migration = %v (duplicate vote should be gone)", c)
	}
	var v int
	if err := s.db.QueryRow("PRAGMA user_version").Scan(&v); err != nil || v != schemaVersion {
		t.Fatalf("user_version = %d, want %d", v, schemaVersion)
	}
	var ei int64
	if err := s.db.QueryRow("SELECT event_index FROM system_changes").Scan(&ei); err != nil || ei != -1 {
		t.Fatalf("event_index = %d err %v", ei, err)
	}
	rows, _ := s.Proposals(ctx)
	if len(rows) != 1 || !rows[0].OperationsKnown || rows[0].VoteThreshold != 241920 {
		t.Fatalf("rows = %+v (operations_known should default true, threshold recomputed from votes)", rows)
	}
}

func TestIrreversible_Watermark(t *testing.T) {
	s := open(t)
	if w, _ := s.Irreversible(ctx); w != 0 {
		t.Fatalf("empty watermark = %d", w)
	}
	if err := s.Apply(ctx, 40, testfix.BlockID(40), true, nil); err != nil {
		t.Fatal(err)
	}
	if w, _ := s.Irreversible(ctx); w != 40 {
		t.Fatalf("watermark after irreversible apply = %d, want 40", w)
	}
	if err := s.Apply(ctx, 45, testfix.BlockID(45), false, nil); err != nil {
		t.Fatal(err)
	}
	if w, _ := s.Irreversible(ctx); w != 40 {
		t.Fatalf("provisional apply moved watermark to %d", w)
	}
	if err := s.MarkIrreversible(ctx, 44); err != nil {
		t.Fatal(err)
	}
	if w, _ := s.Irreversible(ctx); w != 44 {
		t.Fatalf("watermark after mark = %d, want 44", w)
	}
}

func TestRollbackProvisional_FromLowestProvisionalRow(t *testing.T) {
	s := open(t)
	b, r, id := submission(10)
	apply(t, s, true, b, r)
	applyStatus(t, s, false, 20, id, governance.ProposalStatus_active)
	if err := s.Apply(ctx, 21, testfix.BlockID(21), false, nil); err != nil {
		t.Fatal(err)
	}
	if err := s.Apply(ctx, 22, testfix.BlockID(22), false, nil); err != nil {
		t.Fatal(err)
	}
	from, err := s.RollbackProvisional(ctx)
	if err != nil || from != 20 {
		t.Fatalf("from=%d err=%v, want 20 (lowest provisional row)", from, err)
	}
	cur, _ := s.Cursor(ctx)
	if cur.Height != 19 {
		t.Fatalf("cursor = %+v, want 19", cur)
	}
	// Nothing provisional left: the cursor block itself is re-applied.
	if err := s.Apply(ctx, 25, testfix.BlockID(25), true, nil); err != nil {
		t.Fatal(err)
	}
	from, err = s.RollbackProvisional(ctx)
	if err != nil || from != 25 {
		t.Fatalf("from=%d err=%v, want 25 (cursor height)", from, err)
	}
}

func TestOpen_MigratesV3LegacyEventRows(t *testing.T) {
	// A v3 database that was migrated from v2 keeps event-derived rows with
	// event_index -1; once the block is re-applied they duplicate the indexed rows.
	path := filepath.Join(t.TempDir(), "v3.db")
	s0, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{
		`INSERT INTO system_changes (height, block_id, tx_id, op_index, event_index, payer, kind, contract_id, system_flag, irreversible) VALUES (24804033, x'01', x'1220aa', -1, -1, x'00', 'set_system_contract', x'0001', 1, 1)`,
		`INSERT INTO system_changes (height, block_id, tx_id, op_index, event_index, payer, kind, contract_id, system_flag, irreversible) VALUES (24804033, x'01', x'1220aa', -1, 0, x'00', 'set_system_contract', x'0001', 1, 1)`,
		`INSERT INTO system_changes (height, block_id, tx_id, op_index, event_index, payer, kind, contract_id, system_flag, irreversible) VALUES (30038128, x'02', x'1220bb', -1, -1, x'00', 'set_system_contract', x'0002', 1, 1)`, // not yet re-applied: keep
		`PRAGMA user_version = 3`,
	} {
		if _, err := s0.db.Exec(q); err != nil {
			t.Fatal(q, err)
		}
	}
	s0.Close()
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	var n int64
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM system_changes`).Scan(&n); err != nil || n != 2 {
		t.Fatalf("rows = %d, want 2 (duplicate legacy row removed, lone legacy row kept)", n)
	}
	var v int
	if err := s.db.QueryRow("PRAGMA user_version").Scan(&v); err != nil || v != schemaVersion {
		t.Fatalf("user_version = %d, want %d", v, schemaVersion)
	}
}

// --- Codex review ---

func TestRollback_RecomputesVoteTally(t *testing.T) {
	s := open(t)
	b, r, id := submission(100)
	apply(t, s, true, b, r)
	applyStatus(t, s, true, 201700, id, governance.ProposalStatus_active)
	for i, tally := range []uint64{0, 1, 2} {
		vb := testfix.Block(201701 + uint64(i))
		apply(t, s, false, vb, testfix.Receipt(vb, []*protocol.EventData{testfix.VoteEvent(gov, id, tally, 241920)}))
	}
	rows, _ := s.Proposals(ctx)
	if rows[0].VoteTally != 2 {
		t.Fatalf("tally before rollback = %d", rows[0].VoteTally)
	}
	if err := s.Rollback(ctx, 201701); err != nil {
		t.Fatal(err)
	}
	rows, _ = s.Proposals(ctx)
	if rows[0].VoteTally != 0 || rows[0].Status != "active" {
		t.Fatalf("after rollback of all votes: tally=%d status=%s, want 0/active", rows[0].VoteTally, rows[0].Status)
	}
}

func TestRollback_RebuildKeepsSameBlockOrder(t *testing.T) {
	s := open(t)
	// Two transactions in one block flip koin on, then off. Execution order must win.
	tx1 := testfix.Tx(testfix.TxID(1), gov, testfix.SetSystemContractOp(koin, true))
	tx2 := testfix.Tx(testfix.TxID(2), gov, testfix.SetSystemContractOp(koin, false))
	b := testfix.Block(50, tx1, tx2)
	apply(t, s, false, b, testfix.Receipt(b, nil, testfix.ReceiptsFor(b)...))
	rows, _ := s.SystemContracts(ctx)
	if len(rows) != 1 || rows[0].System {
		t.Fatalf("live upsert = %+v, want koin off", rows)
	}
	b2 := testfix.Block(51)
	apply(t, s, false, b2, testfix.Receipt(b2, nil))
	if err := s.Rollback(ctx, 51); err != nil {
		t.Fatal(err)
	}
	rows, _ = s.SystemContracts(ctx)
	if len(rows) != 1 || rows[0].System {
		t.Fatalf("rebuild after rollback = %+v, want koin off (last change in the block)", rows)
	}
}
