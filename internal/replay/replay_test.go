package replay

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/koinos/koinos-proto-golang/v2/koinos/protocol"
	bs "github.com/koinos/koinos-proto-golang/v2/koinos/rpc/block_store"
	"govwatch/internal/decode"
	"govwatch/internal/node"
	"govwatch/internal/store"
	"govwatch/internal/testfix"
)

var ctx = context.Background()

// fakeSource serves synthetic blocks; height 50 carries a proposal submission.
type fakeSource struct {
	fin   node.Finality
	short bool // return one block fewer than asked
	calls []struct {
		start uint64
		count uint32
	}
}

func (f *fakeSource) Finality(context.Context) (node.Finality, error) { return f.fin, nil }

func (f *fakeSource) Blocks(_ context.Context, _ []byte, start uint64, count uint32) ([]*bs.BlockItem, error) {
	f.calls = append(f.calls, struct {
		start uint64
		count uint32
	}{start, count})
	n := count
	if f.short {
		n--
	}
	items := make([]*bs.BlockItem, 0, n)
	for h := start; h < start+uint64(n); h++ {
		var b *protocol.Block
		var r *protocol.BlockReceipt
		if h == 50 {
			tx, _ := testfix.SubmitProposalTx(testfix.TxID(h), testfix.Addr(1), decode.Mainnet.Governance, []*protocol.Operation{testfix.UploadOp(decode.Mainnet.Koin, []byte("x"))}, 7)
			b = testfix.Block(h, tx)
			r = testfix.Receipt(b, nil, testfix.ReceiptsFor(b)...)
		} else {
			b = testfix.Block(h)
			r = testfix.Receipt(b, nil)
		}
		items = append(items, &bs.BlockItem{BlockId: b.Id, BlockHeight: h, Block: b, Receipt: r})
	}
	return items, nil
}

func newStore(t *testing.T) *store.Store {
	t.Helper()
	s, err := store.Open(filepath.Join(t.TempDir(), "r.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func src(lib uint64) *fakeSource {
	return &fakeSource{fin: node.Finality{HeadHeight: lib + 10, HeadID: []byte{1}, Irreversible: lib}}
}

func TestReplay_WalksRangeAndAdvancesCursor(t *testing.T) {
	s := newStore(t)
	f := src(10000)
	last, err := Run(ctx, decode.Mainnet, f, s, Options{From: 1, To: 2500, Batch: 1000})
	if err != nil {
		t.Fatal(err)
	}
	if last != 2500 || len(f.calls) != 3 || f.calls[2].start != 2001 || f.calls[2].count != 500 {
		t.Fatalf("last=%d calls=%+v", last, f.calls)
	}
	cur, _ := s.Cursor(ctx)
	if cur.Height != 2500 {
		t.Fatalf("cursor = %+v", cur)
	}
	if c, _ := s.Counts(ctx); c["proposals"] != 1 {
		t.Fatalf("counts = %v, want the height-50 proposal", c)
	}
}

func TestReplay_ResumesFromCursor(t *testing.T) {
	s := newStore(t)
	f := src(10000)
	if _, err := Run(ctx, decode.Mainnet, f, s, Options{From: 1, To: 1000, Batch: 1000}); err != nil {
		t.Fatal(err)
	}
	before, _ := s.Counts(ctx)
	f.calls = nil
	last, err := Run(ctx, decode.Mainnet, f, s, Options{To: 2000, Batch: 1000})
	if err != nil {
		t.Fatal(err)
	}
	if last != 2000 || len(f.calls) != 1 || f.calls[0].start != 1001 {
		t.Fatalf("last=%d calls=%+v, want one call from 1001", last, f.calls)
	}
	after, _ := s.Counts(ctx)
	if after["proposals"] != before["proposals"] || after["proposal_events"] != before["proposal_events"] {
		t.Fatalf("counts changed on resume: %v → %v", before, after)
	}
}

func TestReplay_ShortBatchIsError(t *testing.T) {
	s := newStore(t)
	f := src(10000)
	f.short = true
	last, err := Run(ctx, decode.Mainnet, f, s, Options{From: 1, To: 1000, Batch: 1000})
	if !errors.Is(err, ErrShortBatch) {
		t.Fatalf("err = %v, want ErrShortBatch", err)
	}
	cur, _ := s.Cursor(ctx)
	if last != cur.Height || cur.Height > 999 {
		t.Fatalf("last=%d cursor=%+v", last, cur)
	}
}

func TestReplay_ClampsToIrreversible(t *testing.T) {
	s := newStore(t)
	f := src(1500)
	last, err := Run(ctx, decode.Mainnet, f, s, Options{From: 1, To: 3000, Batch: 1000})
	if err != nil {
		t.Fatal(err)
	}
	cur, _ := s.Cursor(ctx)
	if last != 1500 || cur.Height != 1500 {
		t.Fatalf("last=%d cursor=%+v, want 1500", last, cur)
	}
	// To == 0 means "up to the last irreversible block".
	last, err = Run(ctx, decode.Mainnet, f, s, Options{Batch: 1000})
	if err != nil || last != 1500 {
		t.Fatalf("last=%d err=%v", last, err)
	}
}

func TestReplay_RollsBackForkedCursor(t *testing.T) {
	// The watcher stopped on a provisional block that later lost its fork. On
	// restart the node's block at cursor+1 does not descend from our cursor block.
	s := newStore(t)
	f := src(30)
	fork := testfix.ForkBlock(20, 0xF0)
	tx, _ := testfix.SubmitProposalTx(testfix.TxID(20), testfix.Addr(2), decode.Mainnet.Governance, []*protocol.Operation{testfix.UploadOp(decode.Mainnet.Koin, []byte("fork"))}, 1)
	fork.Transactions = []*protocol.Transaction{tx}
	facts, _ := decode.Decode(decode.Mainnet, fork, testfix.Receipt(fork, nil, testfix.ReceiptsFor(fork)...))
	if err := s.Apply(ctx, 20, fork.Id, false, facts); err != nil {
		t.Fatal(err)
	}
	last, err := Run(ctx, decode.Mainnet, f, s, Options{Batch: 1000})
	if err != nil || last != 30 {
		t.Fatalf("last=%d err=%v", last, err)
	}
	if c, _ := s.Counts(ctx); c["proposals"] != 0 {
		t.Fatalf("dead-fork proposal survived restart: %v", c)
	}
	if f.calls[len(f.calls)-1].start > 20 {
		t.Fatalf("block 20 was never refetched: calls %+v", f.calls)
	}
}
