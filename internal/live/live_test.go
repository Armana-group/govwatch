package live

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/koinos/koinos-proto-golang/v2/koinos"
	"github.com/koinos/koinos-proto-golang/v2/koinos/broadcast"
	"github.com/koinos/koinos-proto-golang/v2/koinos/contracts/governance"
	"github.com/koinos/koinos-proto-golang/v2/koinos/protocol"
	bs "github.com/koinos/koinos-proto-golang/v2/koinos/rpc/block_store"
	"google.golang.org/protobuf/proto"
	"govwatch/internal/decode"
	"govwatch/internal/node"
	"govwatch/internal/store"
	"govwatch/internal/testfix"
)

var (
	ctx = context.Background()
	gov = decode.Mainnet.Governance
)

type fakeFeed struct{ ch chan Delivery }

func (f *fakeFeed) Deliveries(context.Context) (<-chan Delivery, error) { return f.ch, nil }

type fakeSource struct {
	fin   node.Finality
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
	items := make([]*bs.BlockItem, 0, count)
	for h := start; h < start+uint64(count); h++ {
		b := testfix.Block(h)
		items = append(items, &bs.BlockItem{BlockId: b.Id, BlockHeight: h, Block: b, Receipt: testfix.Receipt(b, nil)})
	}
	return items, nil
}

func marshal(t *testing.T, m proto.Message) []byte {
	t.Helper()
	b, err := proto.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func accept(t *testing.T, b *protocol.Block, r *protocol.BlockReceipt, head bool, acked *int) Delivery {
	return Delivery{RoutingKey: "koinos.block.accept", Body: marshal(t, &broadcast.BlockAccepted{Block: b, Receipt: r, Head: head, Live: true}), Ack: func() error { *acked++; return nil }}
}

func irreversible(t *testing.T, h uint64, acked *int) Delivery {
	return Delivery{RoutingKey: "koinos.block.irreversible", Body: marshal(t, &broadcast.BlockIrreversible{Topology: &koinos.BlockTopology{Height: h, Id: testfix.BlockID(h)}}), Ack: func() error { *acked++; return nil }}
}

func submissionBlock(h uint64, fork byte, seed uint64) (*protocol.Block, *protocol.BlockReceipt, []byte) {
	tx, root := testfix.SubmitProposalTx(testfix.TxID(seed), testfix.Addr(1), gov, []*protocol.Operation{testfix.UploadOp(decode.Mainnet.Koin, []byte{byte(seed)})}, 9)
	var b *protocol.Block
	if fork == 0 {
		b = testfix.Block(h, tx)
	} else {
		b = testfix.ForkBlock(h, fork, tx)
	}
	return b, testfix.Receipt(b, nil, testfix.TxReceipt(tx.Id, testfix.Addr(1), false, testfix.StatusEvent(gov, root, governance.ProposalStatus_pending))), root
}

// run feeds the deliveries and returns when the processor has drained them.
func run(t *testing.T, st *store.Store, src *fakeSource, deliveries ...Delivery) {
	t.Helper()
	ch := make(chan Delivery, len(deliveries))
	for _, d := range deliveries {
		ch <- d
	}
	close(ch)
	p := &Processor{Net: decode.Mainnet, Src: src, St: st}
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx, &fakeFeed{ch: ch}) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after the feed closed")
	}
}

func newStore(t *testing.T) *store.Store {
	t.Helper()
	s, err := store.Open(filepath.Join(t.TempDir(), "l.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestLive_AppliesHeadBlock(t *testing.T) {
	st := newStore(t)
	acked := 0
	b, r, _ := submissionBlock(1, 0, 1)
	run(t, st, &fakeSource{}, accept(t, b, r, true, &acked))
	c, _ := st.Counts(ctx)
	cur, _ := st.Cursor(ctx)
	if c["proposals"] != 1 || cur.Height != 1 || !bytes.Equal(cur.BlockID, b.Id) || acked != 1 {
		t.Fatalf("counts=%v cursor=%+v acked=%d", c, cur, acked)
	}
}

func TestLive_IgnoresNonHeadBlocks(t *testing.T) {
	st := newStore(t)
	acked := 0
	b, r, _ := submissionBlock(1, 0, 1)
	run(t, st, &fakeSource{}, accept(t, b, r, false, &acked))
	c, _ := st.Counts(ctx)
	cur, _ := st.Cursor(ctx)
	if c["proposals"] != 0 || cur.Height != 0 || acked != 1 {
		t.Fatalf("side-fork block was stored: counts=%v cursor=%+v acked=%d", c, cur, acked)
	}
}

func TestLive_BackfillsGap(t *testing.T) {
	st := newStore(t)
	if err := st.Apply(ctx, 10, testfix.BlockID(10), true, nil); err != nil {
		t.Fatal(err)
	}
	src := &fakeSource{fin: node.Finality{HeadHeight: 13, HeadID: testfix.BlockID(13), Irreversible: 11}}
	acked := 0
	b := testfix.Block(13)
	run(t, st, src, accept(t, b, testfix.Receipt(b, nil), true, &acked))
	if len(src.calls) != 1 || src.calls[0].start != 11 || src.calls[0].count != 2 {
		t.Fatalf("backfill calls = %+v, want one call 11..12", src.calls)
	}
	cur, _ := st.Cursor(ctx)
	if cur.Height != 13 || acked != 1 {
		t.Fatalf("cursor=%+v acked=%d", cur, acked)
	}
}

func TestLive_ReorgRollsBack(t *testing.T) {
	st := newStore(t)
	if err := st.Apply(ctx, 19, testfix.BlockID(19), true, nil); err != nil {
		t.Fatal(err)
	}
	src := &fakeSource{fin: node.Finality{HeadHeight: 20, HeadID: testfix.BlockID(20), Irreversible: 19}}
	acked := 0
	a, ra, idA := submissionBlock(20, 0, 20)
	bb, rb, idB := submissionBlock(20, 0xF0, 21)
	run(t, st, src, accept(t, a, ra, true, &acked), accept(t, bb, rb, true, &acked))
	rows, _ := st.Proposals(ctx)
	if len(rows) != 1 || !bytes.Equal(rows[0].ID, idB) || bytes.Equal(rows[0].ID, idA) {
		t.Fatalf("proposals after reorg = %+v, want only the fork-B proposal", rows)
	}
	cur, _ := st.Cursor(ctx)
	if cur.Height != 20 || !bytes.Equal(cur.BlockID, bb.Id) {
		t.Fatalf("cursor = %+v", cur)
	}
}

func TestLive_MarksIrreversible(t *testing.T) {
	st := newStore(t)
	acked := 0
	b, r, _ := submissionBlock(1, 0, 1)
	run(t, st, &fakeSource{}, accept(t, b, r, true, &acked), irreversible(t, 1, &acked))
	if err := st.Rollback(ctx, 1); err != nil {
		t.Fatal(err)
	}
	if c, _ := st.Counts(ctx); c["proposals"] != 1 {
		t.Fatalf("irreversible row removed by rollback: %v", c)
	}
	if acked != 2 {
		t.Fatalf("acked = %d", acked)
	}
}

func TestLive_IgnoresStaleBelowIrreversible(t *testing.T) {
	st := newStore(t)
	if err := st.Apply(ctx, 30, testfix.BlockID(30), true, nil); err != nil {
		t.Fatal(err)
	}
	acked := 0
	b, r, _ := submissionBlock(25, 0xF0, 25) // a head message from a fork that already lost
	run(t, st, &fakeSource{fin: node.Finality{HeadHeight: 31, HeadID: testfix.BlockID(31), Irreversible: 30}}, accept(t, b, r, true, &acked))
	c, _ := st.Counts(ctx)
	cur, _ := st.Cursor(ctx)
	if c["proposals"] != 0 || cur.Height != 30 || acked != 1 {
		t.Fatalf("stale block applied: counts=%v cursor=%+v acked=%d", c, cur, acked)
	}
}

func TestLive_BackfillDetectsForkedCursor(t *testing.T) {
	st := newStore(t)
	if err := st.Apply(ctx, 19, testfix.BlockID(19), true, nil); err != nil {
		t.Fatal(err)
	}
	forkB, forkR, idFork := submissionBlock(20, 0xF0, 20)
	facts, _ := decode.Decode(decode.Mainnet, forkB, forkR)
	if err := st.Apply(ctx, 20, forkB.Id, false, facts); err != nil {
		t.Fatal(err)
	}
	src := &fakeSource{fin: node.Finality{HeadHeight: 23, HeadID: testfix.BlockID(23), Irreversible: 19}}
	acked := 0
	b := testfix.Block(23)
	run(t, st, src, accept(t, b, testfix.Receipt(b, nil), true, &acked))
	rows, _ := st.Proposals(ctx)
	for _, p := range rows {
		if bytes.Equal(p.ID, idFork) {
			t.Fatalf("dead-fork proposal survived backfill: %+v", rows)
		}
	}
	cur, _ := st.Cursor(ctx)
	if cur.Height != 23 || !bytes.Equal(cur.BlockID, b.Id) {
		t.Fatalf("cursor = %+v", cur)
	}
	if src.calls[len(src.calls)-1].start != 20 {
		t.Fatalf("expected a refetch from 20 after the fork check, calls %+v", src.calls)
	}
}

// --- Codex review ---

type ctxFeed struct {
	fakeFeed
	ctx context.Context
}

func (f *ctxFeed) Deliveries(ctx context.Context) (<-chan Delivery, error) {
	f.ctx = ctx
	return f.ch, nil
}

func TestLive_RunCancelsFeedOnError(t *testing.T) {
	// A processing error must release the feed (and its exclusive consumer),
	// otherwise the next connection attempt is refused forever.
	st := newStore(t)
	ch := make(chan Delivery, 1)
	ch <- Delivery{RoutingKey: "koinos.block.accept", Body: []byte{0xff, 0xff}, Ack: func() error { return nil }}
	f := &ctxFeed{fakeFeed: fakeFeed{ch: ch}}
	p := &Processor{Net: decode.Mainnet, Src: &fakeSource{}, St: st}
	if err := p.Run(ctx, f); err == nil {
		t.Fatal("Run should fail on an undecodable broadcast")
	}
	select {
	case <-f.ctx.Done():
	default:
		t.Fatal("feed context not cancelled after Run returned an error")
	}
}
