package decode

import (
	"bytes"
	"crypto/sha256"

	"github.com/koinos/koinos-proto-golang/v2/koinos/protocol"
)

// System call ids whose override counts as a governance update
// (Governance.ts:106-119; ids from system_call_ids.proto).
const (
	sysPreBlockCallback                uint32 = 8
	sysApplySetSystemCallOperation     uint32 = 6
	sysApplySetSystemContractOperation uint32 = 7
	sysCheckSystemAuthority            uint32 = 110
)

// UpdatesGovernance ports Governance.proposal_updates_governance
// (Governance.ts:97-127): true when any operation uploads to the governance
// contract, flips its system flag, or overrides one of the four guarded
// system calls.
func UpdatesGovernance(ops []*protocol.Operation, governance []byte) bool {
	for _, op := range ops {
		switch o := op.GetOp().(type) {
		case *protocol.Operation_UploadContract:
			if bytes.Equal(o.UploadContract.GetContractId(), governance) {
				return true
			}
		case *protocol.Operation_SetSystemCall:
			switch o.SetSystemCall.GetCallId() {
			case sysPreBlockCallback, sysCheckSystemAuthority, sysApplySetSystemCallOperation, sysApplySetSystemContractOperation:
				return true
			}
		case *protocol.Operation_SetSystemContract:
			if bytes.Equal(o.SetSystemContract.GetContractId(), governance) {
				return true
			}
		}
	}
	return false
}

// BytecodeHash returns the sha256 multihash (0x12 0x20 + digest) the chain
// stores in contract metadata for uploaded bytecode.
func BytecodeHash(bytecode []byte) []byte {
	d := sha256.Sum256(bytecode)
	return append([]byte{0x12, 0x20}, d[:]...)
}
