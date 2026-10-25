// Package decode turns a Koinos block and its receipt into typed facts about
// governance proposals and system-contract changes. It performs no IO.
package decode

import (
	"github.com/koinos/koinos-proto-golang/v2/koinos/contracts/governance"
	"google.golang.org/protobuf/proto"
)

// Kind names what a Fact records.
type Kind string

const (
	ProposalSubmitted Kind = "proposal_submitted"
	ProposalStatus    Kind = "proposal_status"
	ProposalVote      Kind = "proposal_vote"
	ProposalApplied   Kind = "proposal_applied" // receipt with no body transaction, payer == governance
	UploadContract    Kind = "upload_contract"
	SetSystemCall     Kind = "set_system_call"
	SetSystemContract Kind = "set_system_contract"
)

// Fact is one decoded observation. Payload holds the decoded protobuf:
// *governance.SubmitProposalArguments, *governance.ProposalStatusEvent,
// *governance.ProposalVoteEvent, *protocol.UploadContractOperation,
// *protocol.SetSystemCallOperation or *protocol.SetSystemContractOperation.
// ProposalApplied facts carry a nil Payload, as do ProposalSubmitted facts for
// submissions made through another contract (the operations are then unknown).
type Fact struct {
	Kind       Kind
	Height     uint64
	BlockID    []byte
	TxID       []byte // nil for block-level events
	TxIndex    int    // position of the transaction (receipt) in the block, -1 for block-level
	OpIndex    int    // -1 when not from an operation
	EventIndex int    // position in the receipt's event list, -1 when from an operation
	Payer      []byte // transaction payer, nil for block-level
	Contract   []byte // governance for proposal kinds; target contract for system kinds
	ProposalID []byte // operation merkle root, nil when unknown
	Status     governance.ProposalStatus
	Reverted   bool // ProposalApplied only: the applied transaction's receipt.reverted
	Payload    proto.Message
}
