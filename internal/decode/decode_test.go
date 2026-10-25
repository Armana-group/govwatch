package decode

import (
	"bytes"
	"testing"

	"github.com/koinos/koinos-proto-golang/v2/koinos/contracts/governance"
	"github.com/koinos/koinos-proto-golang/v2/koinos/protocol"
	"govwatch/internal/testfix"
)

var (
	gov   = Mainnet.Governance
	koin  = Mainnet.Koin
	alice = testfix.Addr(0xA1)
	other = testfix.Addr(0xB2)
)

func decodeOrFail(t *testing.T, b *protocol.Block, r *protocol.BlockReceipt) []Fact {
	t.Helper()
	facts, err := Decode(Mainnet, b, r)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	return facts
}

func TestDecode_ProposalSubmitted(t *testing.T) {
	ops := []*protocol.Operation{testfix.UploadOp(koin, []byte("new koin"))}
	tx, root := testfix.SubmitProposalTx(testfix.TxID(1), alice, gov, ops, 12345)
	b := testfix.Block(100, tx)
	facts := decodeOrFail(t, b, testfix.Receipt(b, nil, testfix.ReceiptsFor(b)...))
	if len(facts) != 1 {
		t.Fatalf("got %d facts, want 1: %+v", len(facts), facts)
	}
	f := facts[0]
	if f.Kind != ProposalSubmitted || !bytes.Equal(f.ProposalID, root) || !bytes.Equal(f.TxID, tx.Id) || !bytes.Equal(f.Payer, alice) || f.OpIndex != 0 || f.Height != 100 {
		t.Fatalf("unexpected fact %+v", f)
	}
	args, ok := f.Payload.(*governance.SubmitProposalArguments)
	if !ok || len(args.Operations) != 1 || args.Fee != 12345 {
		t.Fatalf("payload = %T %+v", f.Payload, f.Payload)
	}
}

func TestDecode_BlockLevelStatusEvent(t *testing.T) {
	id := testfix.TxID(7)
	b := testfix.Block(201700)
	facts := decodeOrFail(t, b, testfix.Receipt(b, []*protocol.EventData{testfix.StatusEvent(gov, id, governance.ProposalStatus_active)}))
	if len(facts) != 1 {
		t.Fatalf("got %d facts, want 1", len(facts))
	}
	f := facts[0]
	if f.Kind != ProposalStatus || f.TxID != nil || f.Status != governance.ProposalStatus_active || !bytes.Equal(f.ProposalID, id) || !bytes.Equal(f.Contract, gov) {
		t.Fatalf("unexpected fact %+v", f)
	}
}

func TestDecode_IgnoresSpoofedGovernanceEvent(t *testing.T) {
	b := testfix.Block(5)
	facts := decodeOrFail(t, b, testfix.Receipt(b, []*protocol.EventData{testfix.StatusEvent(other, testfix.TxID(7), governance.ProposalStatus_applied)}))
	if len(facts) != 0 {
		t.Fatalf("spoofed event produced facts: %+v", facts)
	}
}

func TestDecode_SkipsRevertedTransaction(t *testing.T) {
	tx, root := testfix.SubmitProposalTx(testfix.TxID(2), alice, gov, []*protocol.Operation{testfix.UploadOp(koin, []byte("x"))}, 1)
	b := testfix.Block(6, tx)
	r := testfix.Receipt(b, nil, testfix.TxReceipt(tx.Id, alice, true, testfix.StatusEvent(gov, root, governance.ProposalStatus_pending)))
	if facts := decodeOrFail(t, b, r); len(facts) != 0 {
		t.Fatalf("reverted tx produced facts: %+v", facts)
	}
}

func TestDecode_VoteEvent(t *testing.T) {
	id := testfix.TxID(9)
	b := testfix.Block(300000)
	facts := decodeOrFail(t, b, testfix.Receipt(b, []*protocol.EventData{testfix.VoteEvent(gov, id, 5, 241920)}))
	if len(facts) != 1 || facts[0].Kind != ProposalVote || !bytes.Equal(facts[0].ProposalID, id) {
		t.Fatalf("unexpected facts %+v", facts)
	}
	ev := facts[0].Payload.(*governance.ProposalVoteEvent)
	if ev.VoteTally != 5 || ev.VoteThreshold != 241920 {
		t.Fatalf("payload %+v", ev)
	}
}

func TestDecode_ProposalApplied(t *testing.T) {
	id := testfix.TxID(11)
	applied := testfix.TxID(12)
	b := testfix.Block(806500)
	r := testfix.Receipt(b, []*protocol.EventData{testfix.StatusEvent(gov, id, governance.ProposalStatus_applied)}, testfix.TxReceipt(applied, gov, false))
	facts := decodeOrFail(t, b, r)
	if len(facts) != 2 {
		t.Fatalf("got %d facts, want 2: %+v", len(facts), facts)
	}
	var found bool
	for _, f := range facts {
		if f.Kind == ProposalApplied {
			found = true
			if !bytes.Equal(f.TxID, applied) || !bytes.Equal(f.ProposalID, id) || f.Reverted || !bytes.Equal(f.Payer, gov) {
				t.Fatalf("unexpected applied fact %+v", f)
			}
		}
	}
	if !found {
		t.Fatalf("no ProposalApplied fact in %+v", facts)
	}
}

func TestDecode_SystemOperations(t *testing.T) {
	tx := testfix.Tx(testfix.TxID(3), gov, testfix.UploadOp(koin, []byte("code")), testfix.SetSystemCallOp(110, gov, 0x1234), testfix.SetSystemContractOp(koin, true))
	b := testfix.Block(50, tx)
	facts := decodeOrFail(t, b, testfix.Receipt(b, nil, testfix.ReceiptsFor(b)...))
	if len(facts) != 3 {
		t.Fatalf("got %d facts, want 3: %+v", len(facts), facts)
	}
	want := []struct {
		kind     Kind
		contract []byte
	}{{UploadContract, koin}, {SetSystemCall, gov}, {SetSystemContract, koin}}
	for i, w := range want {
		f := facts[i]
		if f.Kind != w.kind || !bytes.Equal(f.Contract, w.contract) || f.OpIndex != i || !bytes.Equal(f.TxID, tx.Id) || !bytes.Equal(f.Payer, gov) {
			t.Fatalf("fact %d = %+v, want kind %s contract %x", i, f, w.kind, w.contract)
		}
	}
	if _, ok := facts[0].Payload.(*protocol.UploadContractOperation); !ok {
		t.Fatalf("upload payload %T", facts[0].Payload)
	}
}

func TestUpdatesGovernance(t *testing.T) {
	cases := []struct {
		name string
		op   *protocol.Operation
		want bool
	}{
		{"upload governance", testfix.UploadOp(gov, nil), true},
		{"upload koin", testfix.UploadOp(koin, nil), false},
		{"set_system_call pre_block_callback", testfix.SetSystemCallOp(8, koin, 1), true},
		{"set_system_call check_system_authority", testfix.SetSystemCallOp(110, koin, 1), true},
		{"set_system_call apply_set_system_call", testfix.SetSystemCallThunkOp(6, 6), true},
		{"set_system_call apply_set_system_contract", testfix.SetSystemCallThunkOp(7, 7), true},
		{"set_system_call event", testfix.SetSystemCallOp(402, koin, 1), false},
		{"set_system_contract governance", testfix.SetSystemContractOp(gov, false), true},
		{"set_system_contract koin", testfix.SetSystemContractOp(koin, true), false},
		{"call governance", testfix.CallOp(gov, 1, nil), false},
	}
	for _, c := range cases {
		if got := UpdatesGovernance([]*protocol.Operation{c.op}, gov); got != c.want {
			t.Errorf("%s: got %v want %v", c.name, got, c.want)
		}
	}
}

func TestBytecodeHash(t *testing.T) {
	h := BytecodeHash([]byte("wasm"))
	if len(h) != 34 || h[0] != 0x12 || h[1] != 0x20 {
		t.Fatalf("hash = %x", h)
	}
}

func TestDecode_AppliedProposalReceiptEventsAreFacts(t *testing.T) {
	id := testfix.TxID(21)
	applied := testfix.TxID(22)
	b := testfix.Block(806500)
	orphan := testfix.TxReceipt(applied, gov, false, testfix.SetSystemContractEvent(koin, true), testfix.SetSystemCallEvent(110, gov, 0x55))
	r := testfix.Receipt(b, []*protocol.EventData{testfix.StatusEvent(gov, id, governance.ProposalStatus_applied)}, orphan)
	facts := decodeOrFail(t, b, r)
	var kinds []Kind
	for _, f := range facts {
		kinds = append(kinds, f.Kind)
		if (f.Kind == SetSystemContract || f.Kind == SetSystemCall) && !bytes.Equal(f.TxID, applied) {
			t.Fatalf("system fact not attributed to the applied tx: %+v", f)
		}
	}
	if len(facts) != 4 {
		t.Fatalf("got kinds %v, want status, applied, set_system_contract, set_system_call", kinds)
	}
}

func TestDecode_BodyChainEventNotDuplicated(t *testing.T) {
	tx := testfix.Tx(testfix.TxID(31), gov, testfix.SetSystemContractOp(koin, true))
	b := testfix.Block(60, tx)
	r := testfix.Receipt(b, nil, testfix.TxReceipt(tx.Id, gov, false, testfix.SetSystemContractEvent(koin, true)))
	facts := decodeOrFail(t, b, r)
	if len(facts) != 1 || facts[0].Kind != SetSystemContract || facts[0].OpIndex != 0 {
		t.Fatalf("got %+v, want exactly one operation-derived fact", facts)
	}
}

// --- fix pass after final review ---

func TestDecode_IgnoresChainEventFromContract(t *testing.T) {
	// Kernel events carry no source. A contract emitting the same name must be ignored, not trusted.
	ev := testfix.SetSystemContractEvent(koin, true)
	ev.Source = other
	b := testfix.Block(70)
	facts := decodeOrFail(t, b, testfix.Receipt(b, []*protocol.EventData{ev}))
	if len(facts) != 0 {
		t.Fatalf("contract-sourced chain event produced facts: %+v", facts)
	}
}

func TestDecode_JunkEventFromOtherContractDoesNotError(t *testing.T) {
	// A hostile contract emits well-known names with garbage bytes; Decode must neither fail nor record.
	junk := []*protocol.EventData{
		{Source: other, Name: "koinos.chain.set_system_call_event", Data: []byte{0xff, 0xff, 0x01}},
		{Source: other, Name: "koinos.contracts.governance.proposal_status_event", Data: []byte{0xff, 0xff, 0x01}},
	}
	tx := testfix.Tx(testfix.TxID(71), alice, testfix.CallOp(other, 1, nil))
	b := testfix.Block(71, tx)
	facts := decodeOrFail(t, b, testfix.Receipt(b, junk, testfix.TxReceipt(tx.Id, alice, false, junk...)))
	if len(facts) != 0 {
		t.Fatalf("junk events produced facts: %+v", facts)
	}
}

func TestDecode_PendingEventWithoutCallIsSubmission(t *testing.T) {
	// submit_proposal called through a wrapper contract: no top-level call to governance,
	// but governance itself emitted the pending event inside this transaction.
	id := testfix.TxID(72)
	tx := testfix.Tx(testfix.TxID(73), alice, testfix.CallOp(other, 0x1234, nil))
	b := testfix.Block(72, tx)
	facts := decodeOrFail(t, b, testfix.Receipt(b, nil, testfix.TxReceipt(tx.Id, alice, false, testfix.StatusEvent(gov, id, governance.ProposalStatus_pending))))
	var sub *Fact
	for i := range facts {
		if facts[i].Kind == ProposalSubmitted {
			sub = &facts[i]
		}
	}
	if sub == nil {
		t.Fatalf("no ProposalSubmitted fact for a wrapped submission: %+v", facts)
	}
	if !bytes.Equal(sub.ProposalID, id) || !bytes.Equal(sub.TxID, tx.Id) || !bytes.Equal(sub.Payer, alice) || sub.Payload != nil || sub.OpIndex != -1 {
		t.Fatalf("unexpected submission fact %+v", sub)
	}
}

func TestDecode_TwoApplicationsInOneBlockKeepTheirIDs(t *testing.T) {
	idA, idB := testfix.TxID(81), testfix.TxID(82)
	rcA, rcB := testfix.TxID(83), testfix.TxID(84)
	b := testfix.Block(806500)
	events := []*protocol.EventData{testfix.StatusEvent(gov, idA, governance.ProposalStatus_applied), testfix.StatusEvent(gov, idB, governance.ProposalStatus_reverted)}
	r := testfix.Receipt(b, events, testfix.TxReceipt(rcA, gov, false), testfix.TxReceipt(rcB, gov, false))
	facts := decodeOrFail(t, b, r)
	got := map[string][]byte{}
	for _, f := range facts {
		if f.Kind == ProposalApplied {
			got[string(f.TxID)] = f.ProposalID
		}
	}
	if len(got) != 2 || !bytes.Equal(got[string(rcA)], idA) || !bytes.Equal(got[string(rcB)], idB) {
		t.Fatalf("applied facts = %v", got)
	}
}
