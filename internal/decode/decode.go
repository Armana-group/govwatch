package decode

import (
	"bytes"
	"fmt"

	"github.com/koinos/koinos-proto-golang/v2/koinos/chain"
	"github.com/koinos/koinos-proto-golang/v2/koinos/contracts/governance"
	"github.com/koinos/koinos-proto-golang/v2/koinos/protocol"
	"google.golang.org/protobuf/proto"
)

// SubmitProposalEntryPoint is governance.submit_proposal (governance.abi).
const SubmitProposalEntryPoint uint32 = 0xe74b785c

const (
	evProposalStatus    = "koinos.contracts.governance.proposal_status_event"
	evProposalVote      = "koinos.contracts.governance.proposal_vote_event"
	evSetSystemCall     = "koinos.chain.set_system_call_event"
	evSetSystemContract = "koinos.chain.set_system_contract_event"
)

// Decode extracts every governance and system-contract fact from a block and
// its receipt. Transactions whose receipt is reverted contribute nothing.
// Governance events are trusted only from n.Governance; kernel events
// (set_system_call_event, set_system_contract_event) only when they carry no
// source, which is how the chain emits them. Events from any other source are
// ignored before their bytes are parsed, so no contract can make Decode fail.
func Decode(n Network, block *protocol.Block, receipt *protocol.BlockReceipt) ([]Fact, error) {
	if block == nil || receipt == nil || block.GetHeader() == nil {
		return nil, fmt.Errorf("decode: block and receipt are required")
	}
	base := Fact{Height: block.Header.Height, BlockID: block.Id, TxIndex: -1, OpIndex: -1, EventIndex: -1}
	var facts []Fact

	// Block-level events: governance block callback and kernel events.
	var applications [][]byte // ids of applied/reverted status events, in order
	for i, ev := range receipt.GetEvents() {
		f, ok, err := eventFact(n, base, i, ev)
		if err != nil {
			return nil, err
		}
		if ok {
			facts = append(facts, f)
			if f.Kind == ProposalStatus && (f.Status == governance.ProposalStatus_applied || f.Status == governance.ProposalStatus_reverted) {
				applications = append(applications, f.ProposalID)
			}
		}
	}

	bodyIDs := make(map[string]*protocol.Transaction, len(block.Transactions))
	for _, tx := range block.Transactions {
		bodyIDs[string(tx.GetId())] = tx
	}
	reverted := make(map[string]bool, len(receipt.TransactionReceipts))
	for _, r := range receipt.GetTransactionReceipts() {
		reverted[string(r.GetId())] = r.GetReverted()
	}

	// Operations of every non-reverted transaction in the body.
	submitted := map[string]bool{} // txID+proposalID seen as a direct submit_proposal call
	for ti, tx := range block.Transactions {
		if reverted[string(tx.GetId())] {
			continue
		}
		txBase := base
		txBase.TxID = tx.Id
		txBase.TxIndex = ti
		txBase.Payer = tx.GetHeader().GetPayer()
		for i, op := range tx.Operations {
			f, ok, err := operationFact(n, txBase, i, op)
			if err != nil {
				return nil, fmt.Errorf("decode: tx %x op %d: %w", tx.Id, i, err)
			}
			if ok {
				facts = append(facts, f)
				if f.Kind == ProposalSubmitted {
					submitted[string(tx.Id)+string(f.ProposalID)] = true
				}
			}
		}
	}

	// Events inside transaction receipts. For body transactions only
	// governance events are taken (their system changes were already read
	// from the operations). A governance "pending" event with no matching
	// submit_proposal call in the same transaction means the proposal was
	// submitted through another contract; it is recorded as a submission
	// with unknown operations. Orphan receipts, written when governance
	// applies a proposal, have no body, so their kernel events are the only
	// record of what the proposal changed.
	var orphans []*protocol.TransactionReceipt
	var orphanIndex []int
	for ri, r := range receipt.GetTransactionReceipts() {
		if r.GetReverted() {
			continue
		}
		txBase := base
		txBase.TxID = r.Id
		txBase.TxIndex = ri
		txBase.Payer = r.GetPayer()
		tx, inBody := bodyIDs[string(r.GetId())]
		if inBody {
			txBase.Payer = tx.GetHeader().GetPayer()
		} else if bytes.Equal(r.GetPayer(), n.Governance) {
			orphans = append(orphans, r)
			orphanIndex = append(orphanIndex, ri)
		}
		for i, ev := range r.GetEvents() {
			f, ok, err := eventFact(n, txBase, i, ev)
			if err != nil {
				return nil, err
			}
			if !ok || (inBody && (f.Kind == SetSystemCall || f.Kind == SetSystemContract)) {
				continue
			}
			facts = append(facts, f)
			if inBody && f.Kind == ProposalStatus && f.Status == governance.ProposalStatus_pending && !submitted[string(r.Id)+string(f.ProposalID)] {
				sub := txBase
				sub.Kind, sub.Contract, sub.ProposalID, sub.EventIndex = ProposalSubmitted, n.Governance, f.ProposalID, i
				facts = append(facts, sub)
				submitted[string(r.Id)+string(f.ProposalID)] = true
			}
		}
	}

	// Receipts with no body transaction and payer == governance are proposal
	// applications. The governance block callback applies proposals in the
	// order it emits their status events, so the k-th orphan receipt belongs
	// to the k-th applied/reverted event.
	for k, r := range orphans {
		f := base
		f.Kind = ProposalApplied
		f.TxID = r.Id
		f.TxIndex = orphanIndex[k]
		f.Payer = r.Payer
		f.Contract = n.Governance
		if k < len(applications) {
			f.ProposalID = applications[k]
		}
		f.Reverted = r.GetReverted()
		facts = append(facts, f)
	}
	return facts, nil
}

func eventFact(n Network, base Fact, index int, ev *protocol.EventData) (Fact, bool, error) {
	f := base
	f.EventIndex = index
	switch ev.GetName() {
	case evProposalStatus, evProposalVote:
		if !bytes.Equal(ev.GetSource(), n.Governance) {
			return Fact{}, false, nil
		}
		f.Contract = n.Governance
		if ev.Name == evProposalStatus {
			var e governance.ProposalStatusEvent
			if err := proto.Unmarshal(ev.Data, &e); err != nil {
				return Fact{}, false, fmt.Errorf("decode %s from governance at height %d: %w", ev.Name, base.Height, err)
			}
			f.Kind, f.ProposalID, f.Status, f.Payload = ProposalStatus, e.Id, e.Status, &e
			return f, true, nil
		}
		var e governance.ProposalVoteEvent
		if err := proto.Unmarshal(ev.Data, &e); err != nil {
			return Fact{}, false, fmt.Errorf("decode %s from governance at height %d: %w", ev.Name, base.Height, err)
		}
		f.Kind, f.ProposalID, f.Payload = ProposalVote, e.Id, &e
		return f, true, nil
	case evSetSystemCall, evSetSystemContract:
		if len(ev.GetSource()) != 0 {
			return Fact{}, false, nil // only the kernel emits these without a source
		}
		if ev.Name == evSetSystemCall {
			var e chain.SetSystemCallEvent
			if err := proto.Unmarshal(ev.Data, &e); err != nil {
				return Fact{}, false, fmt.Errorf("decode kernel %s at height %d: %w", ev.Name, base.Height, err)
			}
			f.Kind = SetSystemCall
			f.Contract = e.GetTarget().GetSystemCallBundle().GetContractId()
			f.Payload = &protocol.SetSystemCallOperation{CallId: e.CallId, Target: e.Target}
			return f, true, nil
		}
		var e chain.SetSystemContractEvent
		if err := proto.Unmarshal(ev.Data, &e); err != nil {
			return Fact{}, false, fmt.Errorf("decode kernel %s at height %d: %w", ev.Name, base.Height, err)
		}
		f.Kind = SetSystemContract
		f.Contract = e.ContractId
		f.Payload = &protocol.SetSystemContractOperation{ContractId: e.ContractId, SystemContract: e.SystemContract}
		return f, true, nil
	}
	return Fact{}, false, nil
}

func operationFact(n Network, base Fact, index int, op *protocol.Operation) (Fact, bool, error) {
	f := base
	f.OpIndex = index
	switch o := op.GetOp().(type) {
	case *protocol.Operation_CallContract:
		c := o.CallContract
		if !bytes.Equal(c.GetContractId(), n.Governance) || c.GetEntryPoint() != SubmitProposalEntryPoint {
			return Fact{}, false, nil
		}
		var args governance.SubmitProposalArguments
		if err := proto.Unmarshal(c.Args, &args); err != nil {
			return Fact{}, false, fmt.Errorf("submit_proposal args: %w", err)
		}
		f.Kind, f.Contract, f.ProposalID, f.Payload = ProposalSubmitted, n.Governance, args.OperationMerkleRoot, &args
		return f, true, nil
	case *protocol.Operation_UploadContract:
		f.Kind, f.Contract, f.Payload = UploadContract, o.UploadContract.GetContractId(), o.UploadContract
		return f, true, nil
	case *protocol.Operation_SetSystemCall:
		f.Kind, f.Contract, f.Payload = SetSystemCall, o.SetSystemCall.GetTarget().GetSystemCallBundle().GetContractId(), o.SetSystemCall
		return f, true, nil
	case *protocol.Operation_SetSystemContract:
		f.Kind, f.Contract, f.Payload = SetSystemContract, o.SetSystemContract.GetContractId(), o.SetSystemContract
		return f, true, nil
	}
	return Fact{}, false, nil
}
