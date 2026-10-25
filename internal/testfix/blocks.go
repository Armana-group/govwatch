// Package testfix builds synthetic Koinos blocks and receipts for tests.
// It is a normal package (not _test) so every package's tests can import it.
package testfix

import (
	"crypto/sha256"
	"encoding/binary"

	"github.com/koinos/koinos-proto-golang/v2/koinos/canonical"
	"github.com/koinos/koinos-proto-golang/v2/koinos/chain"
	"github.com/koinos/koinos-proto-golang/v2/koinos/contracts/governance"
	"github.com/koinos/koinos-proto-golang/v2/koinos/protocol"
	"google.golang.org/protobuf/proto"
)

// SubmitProposalEntryPoint is governance.submit_proposal (governance.abi).
const SubmitProposalEntryPoint uint32 = 0xe74b785c

// Multihash wraps a sha256 digest as Koinos does: 0x12 0x20 + digest.
func Multihash(digest [32]byte) []byte {
	return append([]byte{0x12, 0x20}, digest[:]...)
}

// Sha256 returns the multihash of b.
func Sha256(b []byte) []byte { return Multihash(sha256.Sum256(b)) }

// MerkleRoot mirrors koinos-util-golang CalculateMerkleRoot: leaves are
// multihashes, pairs are hashed digest||digest, an odd node is carried up.
func MerkleRoot(leaves [][]byte) []byte {
	if len(leaves) == 0 {
		return Sha256(nil)
	}
	nodes := append([][]byte(nil), leaves...)
	for len(nodes) > 1 {
		for i := 0; i < len(nodes); i += 2 {
			if i+1 < len(nodes) {
				h := sha256.New()
				h.Write(nodes[i][2:])
				h.Write(nodes[i+1][2:])
				var d [32]byte
				copy(d[:], h.Sum(nil))
				nodes[i/2] = Multihash(d)
			} else {
				nodes[i/2] = nodes[i]
			}
		}
		nodes = nodes[:(len(nodes)+1)/2]
	}
	return nodes[0]
}

// OperationMerkleRoot hashes each operation's canonical bytes, as
// Governance.ts submit_proposal does before verifying the root.
func OperationMerkleRoot(ops []*protocol.Operation) []byte {
	leaves := make([][]byte, len(ops))
	for i, op := range ops {
		b, err := canonical.Marshal(op)
		if err != nil {
			panic(err)
		}
		leaves[i] = Sha256(b)
	}
	return MerkleRoot(leaves)
}

// Addr returns a deterministic 25-byte fake address seeded by tag.
func Addr(tag byte) []byte {
	a := make([]byte, 25)
	a[0] = tag
	for i := 1; i < len(a); i++ {
		a[i] = tag ^ byte(i*7)
	}
	return a
}

// TxID returns a deterministic multihash transaction id for n.
func TxID(n uint64) []byte {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], n)
	return Sha256(b[:])
}

func Must(b []byte, err error) []byte {
	if err != nil {
		panic(err)
	}
	return b
}

// UploadOp, CallOp, SetSystemCallOp, SetSystemContractOp build operations.
func UploadOp(contract, bytecode []byte) *protocol.Operation {
	return &protocol.Operation{Op: &protocol.Operation_UploadContract{UploadContract: &protocol.UploadContractOperation{ContractId: contract, Bytecode: bytecode}}}
}
func CallOp(contract []byte, entry uint32, args []byte) *protocol.Operation {
	return &protocol.Operation{Op: &protocol.Operation_CallContract{CallContract: &protocol.CallContractOperation{ContractId: contract, EntryPoint: entry, Args: args}}}
}
func SetSystemCallOp(callID uint32, contract []byte, entry uint32) *protocol.Operation {
	return &protocol.Operation{Op: &protocol.Operation_SetSystemCall{SetSystemCall: &protocol.SetSystemCallOperation{CallId: callID, Target: &protocol.SystemCallTarget{Target: &protocol.SystemCallTarget_SystemCallBundle{SystemCallBundle: &protocol.ContractCallBundle{ContractId: contract, EntryPoint: entry}}}}}}
}
func SetSystemCallThunkOp(callID, thunk uint32) *protocol.Operation {
	return &protocol.Operation{Op: &protocol.Operation_SetSystemCall{SetSystemCall: &protocol.SetSystemCallOperation{CallId: callID, Target: &protocol.SystemCallTarget{Target: &protocol.SystemCallTarget_ThunkId{ThunkId: thunk}}}}}
}
func SetSystemContractOp(contract []byte, system bool) *protocol.Operation {
	return &protocol.Operation{Op: &protocol.Operation_SetSystemContract{SetSystemContract: &protocol.SetSystemContractOperation{ContractId: contract, SystemContract: system}}}
}

// Tx builds a transaction with the given id, payer and operations.
func Tx(id, payer []byte, ops ...*protocol.Operation) *protocol.Transaction {
	return &protocol.Transaction{Id: id, Header: &protocol.TransactionHeader{Payer: payer, OperationMerkleRoot: OperationMerkleRoot(ops)}, Operations: ops}
}

// SubmitProposalTx wraps proposal ops in a submit_proposal call to governance.
// It returns the transaction and the proposal id (the ops' merkle root).
func SubmitProposalTx(id, payer, governance []byte, proposalOps []*protocol.Operation, fee uint64) (*protocol.Transaction, []byte) {
	root := OperationMerkleRoot(proposalOps)
	args := Must(proto.Marshal(&governance_SubmitProposalArguments{Operations: proposalOps, OperationMerkleRoot: root, Fee: fee}))
	return Tx(id, payer, CallOp(governance, SubmitProposalEntryPoint, args)), root
}

type governance_SubmitProposalArguments = governance.SubmitProposalArguments

// Events.
func StatusEvent(source, proposalID []byte, s governance.ProposalStatus) *protocol.EventData {
	return &protocol.EventData{Source: source, Name: "koinos.contracts.governance.proposal_status_event", Data: Must(proto.Marshal(&governance.ProposalStatusEvent{Id: proposalID, Status: s}))}
}
func VoteEvent(source, proposalID []byte, tally, threshold uint64) *protocol.EventData {
	return &protocol.EventData{Source: source, Name: "koinos.contracts.governance.proposal_vote_event", Data: Must(proto.Marshal(&governance.ProposalVoteEvent{Id: proposalID, VoteTally: tally, VoteThreshold: threshold}))}
}
func SetSystemCallEvent(callID uint32, contract []byte, entry uint32) *protocol.EventData {
	return &protocol.EventData{Name: "koinos.chain.set_system_call_event", Data: Must(proto.Marshal(&chain.SetSystemCallEvent{CallId: callID, Target: &protocol.SystemCallTarget{Target: &protocol.SystemCallTarget_SystemCallBundle{SystemCallBundle: &protocol.ContractCallBundle{ContractId: contract, EntryPoint: entry}}}}))}
}
func SetSystemContractEvent(contract []byte, system bool) *protocol.EventData {
	return &protocol.EventData{Name: "koinos.chain.set_system_contract_event", Data: Must(proto.Marshal(&chain.SetSystemContractEvent{ContractId: contract, SystemContract: system}))}
}

// TxReceipt builds a transaction receipt.
func TxReceipt(id, payer []byte, reverted bool, events ...*protocol.EventData) *protocol.TransactionReceipt {
	return &protocol.TransactionReceipt{Id: id, Payer: payer, Reverted: reverted, Events: events}
}

// Block builds a block at height with the given transactions. Its id is
// deterministic from the height.
func Block(height uint64, txs ...*protocol.Transaction) *protocol.Block {
	var prev []byte
	if height > 1 {
		prev = BlockID(height - 1)
	}
	return &protocol.Block{Id: BlockID(height), Header: &protocol.BlockHeader{Height: height, Previous: prev}, Transactions: txs}
}

// BlockID is the deterministic id Block gives a height.
func BlockID(height uint64) []byte {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], ^height)
	return Sha256(b[:])
}

// ForkBlock is like Block but on a different fork: distinct id, same height.
func ForkBlock(height uint64, fork byte, txs ...*protocol.Transaction) *protocol.Block {
	b := Block(height, txs...)
	b.Id = Sha256(append([]byte{fork}, BlockID(height)...))
	return b
}

// Receipt builds a block receipt for block with block-level events and
// transaction receipts.
func Receipt(block *protocol.Block, events []*protocol.EventData, txReceipts ...*protocol.TransactionReceipt) *protocol.BlockReceipt {
	return &protocol.BlockReceipt{Id: block.Id, Height: block.Header.Height, Events: events, TransactionReceipts: txReceipts}
}

// ReceiptsFor builds one non-reverted empty receipt per transaction in block.
func ReceiptsFor(block *protocol.Block) []*protocol.TransactionReceipt {
	out := make([]*protocol.TransactionReceipt, len(block.Transactions))
	for i, tx := range block.Transactions {
		out[i] = TxReceipt(tx.Id, tx.Header.Payer, false)
	}
	return out
}
