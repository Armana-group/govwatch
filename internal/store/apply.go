package store

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/koinos/koinos-proto-golang/v2/koinos/contracts/governance"
	"github.com/koinos/koinos-proto-golang/v2/koinos/protocol"
	"google.golang.org/protobuf/proto"
	"govwatch/internal/decode"
)

// BlocksPerWeek is the governance review period (Governance.ts:5-6).
const BlocksPerWeek uint64 = 201600

// Apply records one block's facts and advances the cursor, in one SQL
// transaction. Re-applying the same block is a no-op.
func (s *Store) Apply(ctx context.Context, height uint64, blockID []byte, irreversible bool, facts []decode.Fact) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	// Governance status per proposal in this block, so an application can
	// tell "applied" from "reverted" (its own receipt flag is not enough).
	status := map[string]governance.ProposalStatus{}
	for _, f := range facts {
		if f.Kind == decode.ProposalStatus {
			status[string(f.ProposalID)] = f.Status
		}
	}
	for _, f := range facts {
		if err := applyFact(ctx, tx, irreversible, f, status); err != nil {
			return fmt.Errorf("store: apply %s at %d: %w", f.Kind, height, err)
		}
	}
	if err := setCursor(ctx, tx, height, blockID); err != nil {
		return err
	}
	if irreversible {
		if err := raiseWatermark(ctx, tx, height); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func applyFact(ctx context.Context, tx *sql.Tx, irreversible bool, f decode.Fact, status map[string]governance.ProposalStatus) error {
	irr := boolInt(irreversible)
	switch f.Kind {
	case decode.ProposalSubmitted:
		var ops []byte
		var fee int64
		known, updates := int64(0), int64(0)
		if args, ok := f.Payload.(*governance.SubmitProposalArguments); ok {
			b, err := proto.Marshal(args)
			if err != nil {
				return err
			}
			ops, fee, known = b, int64(args.Fee), 1
			updates = boolInt(decode.UpdatesGovernance(args.Operations, f.Contract))
		} else if f.Payload != nil {
			return fmt.Errorf("payload %T", f.Payload)
		}
		res, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO proposals
			(id, submitted_height, submitted_tx, payer, fee, operations, updates_governance, vote_start_height, status, last_height, irreversible, operations_known)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?, ?)`,
			f.ProposalID, f.Height, f.TxID, f.Payer, fee, nz(ops), updates, f.Height+BlocksPerWeek, f.Height, irr, known)
		if err != nil {
			return err
		}
		// A new submission narrows the event window of an earlier one with the
		// same id (re-applied history); re-derive both from their events.
		if n, _ := res.RowsAffected(); n > 0 {
			return recomputeProposals(ctx, tx, 0)
		}
		return nil
	case decode.ProposalStatus:
		st := f.Status.String()
		if _, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO proposal_events (proposal_id, height, block_id, tx_id, kind, status, irreversible) VALUES (?, ?, ?, ?, ?, ?, ?)`,
			f.ProposalID, f.Height, f.BlockID, f.TxID, string(f.Kind), st, irr); err != nil {
			return err
		}
		// The event names only the id; it belongs to the latest submission at or before it.
		_, err := tx.ExecContext(ctx, `UPDATE proposals SET status = ?1, last_height = ?2 WHERE id = ?3 AND last_height <= ?2
			AND submitted_height = (SELECT MAX(p2.submitted_height) FROM proposals p2 WHERE p2.id = ?3 AND p2.submitted_height <= ?2)`, st, f.Height, f.ProposalID)
		return err
	case decode.ProposalVote:
		ev, ok := f.Payload.(*governance.ProposalVoteEvent)
		if !ok {
			return fmt.Errorf("payload %T", f.Payload)
		}
		if _, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO proposal_events (proposal_id, height, block_id, tx_id, kind, status, tally, threshold, irreversible) VALUES (?, ?, ?, ?, ?, '', ?, ?, ?)`,
			f.ProposalID, f.Height, f.BlockID, f.TxID, string(f.Kind), int64(ev.VoteTally), int64(ev.VoteThreshold), irr); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `UPDATE proposals SET vote_tally = MAX(vote_tally, ?1), vote_threshold = ?2 WHERE id = ?3
			AND submitted_height = (SELECT MAX(p2.submitted_height) FROM proposals p2 WHERE p2.id = ?3 AND p2.submitted_height <= ?4)`, int64(ev.VoteTally), int64(ev.VoteThreshold), f.ProposalID, f.Height)
		return err
	case decode.ProposalApplied:
		if _, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO proposal_events (proposal_id, height, block_id, tx_id, kind, status, irreversible) VALUES (?, ?, ?, ?, ?, ?, ?)`,
			nz(f.ProposalID), f.Height, f.BlockID, f.TxID, string(f.Kind), appliedStatus(f.Reverted), irr); err != nil {
			return err
		}
		if status[string(f.ProposalID)] == governance.ProposalStatus_applied && len(f.ProposalID) > 0 {
			return expandUploads(ctx, tx, irr, f)
		}
		return nil
	case decode.UploadContract:
		op, ok := f.Payload.(*protocol.UploadContractOperation)
		if !ok {
			return fmt.Errorf("payload %T", f.Payload)
		}
		_, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO system_changes (height, block_id, tx_id, tx_index, op_index, event_index, payer, kind, contract_id, bytecode_hash, irreversible) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			f.Height, f.BlockID, nz(f.TxID), f.TxIndex, f.OpIndex, f.EventIndex, f.Payer, string(f.Kind), f.Contract, decode.BytecodeHash(op.Bytecode), irr)
		return err
	case decode.SetSystemCall:
		op, ok := f.Payload.(*protocol.SetSystemCallOperation)
		if !ok {
			return fmt.Errorf("payload %T", f.Payload)
		}
		target, err := proto.Marshal(op.GetTarget())
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `INSERT OR IGNORE INTO system_changes (height, block_id, tx_id, tx_index, op_index, event_index, payer, kind, contract_id, call_id, target, irreversible) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			f.Height, f.BlockID, nz(f.TxID), f.TxIndex, f.OpIndex, f.EventIndex, f.Payer, string(f.Kind), f.Contract, int64(op.CallId), target, irr)
		return err
	case decode.SetSystemContract:
		op, ok := f.Payload.(*protocol.SetSystemContractOperation)
		if !ok {
			return fmt.Errorf("payload %T", f.Payload)
		}
		if _, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO system_changes (height, block_id, tx_id, tx_index, op_index, event_index, payer, kind, contract_id, system_flag, irreversible) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			f.Height, f.BlockID, nz(f.TxID), f.TxIndex, f.OpIndex, f.EventIndex, f.Payer, string(f.Kind), f.Contract, boolInt(op.SystemContract), irr); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `INSERT INTO system_contracts (contract_id, system, since_height) VALUES (?, ?, ?)
			ON CONFLICT (contract_id) DO UPDATE SET system = excluded.system, since_height = excluded.since_height WHERE excluded.since_height >= system_contracts.since_height`,
			f.Contract, boolInt(op.SystemContract), f.Height)
		return err
	}
	return fmt.Errorf("unknown fact kind %q", f.Kind)
}

// expandUploads records the upload_contract operations of an applied proposal
// as system changes. upload_contract emits no event, and the applied
// transaction is not in the block body, so the stored submission is the only
// source. Nothing is recorded when the operations are unknown.
func expandUploads(ctx context.Context, tx *sql.Tx, irr int64, f decode.Fact) error {
	var ops []byte
	var known int64
	err := tx.QueryRowContext(ctx, `SELECT operations, operations_known FROM proposals WHERE id = ?1 AND submitted_height = (SELECT MAX(submitted_height) FROM proposals WHERE id = ?1 AND submitted_height <= ?2)`, f.ProposalID, f.Height).Scan(&ops, &known)
	if err == sql.ErrNoRows || known == 0 {
		return nil
	}
	if err != nil {
		return err
	}
	var args governance.SubmitProposalArguments
	if err := proto.Unmarshal(ops, &args); err != nil {
		return fmt.Errorf("stored operations of %x: %w", f.ProposalID, err)
	}
	for i, op := range args.Operations {
		up := op.GetUploadContract()
		if up == nil {
			continue
		}
		if _, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO system_changes (height, block_id, tx_id, tx_index, op_index, event_index, payer, kind, contract_id, bytecode_hash, irreversible) VALUES (?, ?, ?, ?, ?, -1, ?, ?, ?, ?, ?)`,
			f.Height, f.BlockID, nz(f.TxID), f.TxIndex, i, f.Payer, string(decode.UploadContract), up.ContractId, decode.BytecodeHash(up.Bytecode), irr); err != nil {
			return err
		}
	}
	return nil
}

// Rollback removes provisional rows at or above fromHeight, recomputes the
// derived proposal fields and the system-contract set, and moves the cursor
// to fromHeight-1.
func (s *Store) Rollback(ctx context.Context, fromHeight uint64) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := rollbackTx(ctx, tx, fromHeight); err != nil {
		return err
	}
	return tx.Commit()
}

// RollbackProvisional drops every provisional row, from the lowest one, and
// returns the height to re-apply from. With no provisional rows the cursor
// block itself is re-applied (our copy of it may be from a fork that lost).
// Returns 0 when the store is empty.
func (s *Store) RollbackProvisional(ctx context.Context) (uint64, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	var from sql.NullInt64
	if err := tx.QueryRowContext(ctx, `SELECT MIN(h) FROM (
		SELECT MIN(height) AS h FROM proposal_events WHERE irreversible = 0
		UNION ALL SELECT MIN(height) FROM system_changes WHERE irreversible = 0
		UNION ALL SELECT MIN(submitted_height) FROM proposals WHERE irreversible = 0)`).Scan(&from); err != nil {
		return 0, err
	}
	if !from.Valid {
		var cur Cursor
		if err := tx.QueryRowContext(ctx, "SELECT height, block_id FROM cursor WHERE id = 1").Scan(&cur.Height, &cur.BlockID); err == sql.ErrNoRows || cur.Height == 0 {
			return 0, nil
		} else if err != nil {
			return 0, err
		}
		from = sql.NullInt64{Int64: int64(cur.Height), Valid: true}
	}
	if err := rollbackTx(ctx, tx, uint64(from.Int64)); err != nil {
		return 0, err
	}
	return uint64(from.Int64), tx.Commit()
}

func rollbackTx(ctx context.Context, tx *sql.Tx, fromHeight uint64) error {
	for _, q := range []string{
		`DELETE FROM proposal_events WHERE height >= ?1 AND irreversible = 0`,
		`DELETE FROM system_changes WHERE height >= ?1 AND irreversible = 0`,
		`DELETE FROM proposals WHERE submitted_height >= ?1 AND irreversible = 0`,
		`DELETE FROM system_contracts`,
		`INSERT INTO system_contracts (contract_id, system, since_height)
			SELECT contract_id, system_flag, height FROM (
				SELECT contract_id, system_flag, height,
					ROW_NUMBER() OVER (PARTITION BY contract_id ORDER BY height DESC, tx_index DESC, event_index DESC, op_index DESC) AS rn
				FROM system_changes WHERE kind = 'set_system_contract')
			WHERE rn = 1`,
	} {
		if _, err := tx.ExecContext(ctx, q, fromHeight); err != nil {
			return fmt.Errorf("store: rollback: %w", err)
		}
	}
	if err := recomputeProposals(ctx, tx, fromHeight); err != nil {
		return fmt.Errorf("store: rollback: %w", err)
	}
	var blockID []byte
	if fromHeight > 1 {
		_ = tx.QueryRowContext(ctx, `SELECT block_id FROM proposal_events WHERE height = ?1 UNION SELECT block_id FROM system_changes WHERE height = ?1 LIMIT 1`, fromHeight-1).Scan(&blockID)
	}
	if blockID == nil {
		blockID = []byte{}
	}
	return forceCursor(ctx, tx, fromHeight-1, blockID)
}

// recomputeProposals re-derives status, last_height, vote tally and threshold
// of every submission from its own events: those at or after its submission
// and before the next submission of the same id. All rows are recomputed (a
// rollback can remove votes of a proposal whose last status event is far
// below it); the proposal_id index keeps this cheap.
func recomputeProposals(ctx context.Context, tx *sql.Tx, _ uint64) error {
	const window = `e.proposal_id = proposals.id AND e.height >= proposals.submitted_height
		AND e.height < COALESCE((SELECT MIN(p2.submitted_height) FROM proposals p2 WHERE p2.id = proposals.id AND p2.submitted_height > proposals.submitted_height), 9223372036854775807)`
	_, err := tx.ExecContext(ctx, `UPDATE proposals SET
		status = COALESCE((SELECT e.status FROM proposal_events e WHERE `+window+` AND e.kind = 'proposal_status' ORDER BY e.height DESC LIMIT 1), 'pending'),
		last_height = COALESCE((SELECT MAX(e.height) FROM proposal_events e WHERE `+window+` AND e.kind = 'proposal_status'), submitted_height),
		vote_tally = COALESCE((SELECT MAX(e.tally) FROM proposal_events e WHERE `+window+` AND e.kind = 'proposal_vote'), 0),
		vote_threshold = COALESCE((SELECT MAX(e.threshold) FROM proposal_events e WHERE `+window+` AND e.kind = 'proposal_vote'), 0)`)
	return err
}

// MarkIrreversible flags every row at or below upTo as irreversible.
func (s *Store) MarkIrreversible(ctx context.Context, upTo uint64) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, q := range []string{
		`UPDATE proposal_events SET irreversible = 1 WHERE irreversible = 0 AND height <= ?`,
		`UPDATE system_changes SET irreversible = 1 WHERE irreversible = 0 AND height <= ?`,
		`UPDATE proposals SET irreversible = 1 WHERE irreversible = 0 AND submitted_height <= ?`,
	} {
		if _, err := tx.ExecContext(ctx, q, upTo); err != nil {
			return fmt.Errorf("store: mark irreversible: %w", err)
		}
	}
	if err := raiseWatermark(ctx, tx, upTo); err != nil {
		return err
	}
	return tx.Commit()
}

func appliedStatus(reverted bool) string {
	if reverted {
		return "reverted"
	}
	return "applied"
}

func boolInt(b bool) int64 {
	if b {
		return 1
	}
	return 0
}

// nz turns a nil byte slice into an empty one so NOT NULL columns accept it.
func nz(b []byte) []byte {
	if b == nil {
		return []byte{}
	}
	return b
}
