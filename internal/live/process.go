package live

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"

	"github.com/koinos/koinos-proto-golang/v2/koinos/broadcast"
	"google.golang.org/protobuf/proto"
	"govwatch/internal/decode"
	"govwatch/internal/node"
	"govwatch/internal/store"
)

const (
	keyAccept       = "koinos.block.accept"
	keyIrreversible = "koinos.block.irreversible"
)

type Processor struct {
	Net decode.Network
	Src node.Source
	St  *store.Store
	Log *slog.Logger
}

// Run consumes the feed until it closes or ctx ends. Any store or node error
// stops processing without acking the message, so a restart redelivers it.
func (p *Processor) Run(ctx context.Context, f Feed) error {
	if p.Log == nil {
		p.Log = slog.Default()
	}
	// The feed lives only as long as this run: cancelling releases its
	// connection and exclusive consumer so the next run can bind the queue.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	ch, err := f.Deliveries(ctx)
	if err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case d, ok := <-ch:
			if !ok {
				return nil
			}
			if err := p.handle(ctx, d); err != nil {
				return err
			}
			if err := d.Ack(); err != nil {
				return fmt.Errorf("live: ack: %w", err)
			}
		}
	}
}

func (p *Processor) handle(ctx context.Context, d Delivery) error {
	switch d.RoutingKey {
	case keyIrreversible:
		var m broadcast.BlockIrreversible
		if err := proto.Unmarshal(d.Body, &m); err != nil {
			return fmt.Errorf("live: block_irreversible: %w", err)
		}
		return p.St.MarkIrreversible(ctx, m.GetTopology().GetHeight())
	case keyAccept:
		var m broadcast.BlockAccepted
		if err := proto.Unmarshal(d.Body, &m); err != nil {
			return fmt.Errorf("live: block_accepted: %w", err)
		}
		return p.accept(ctx, &m)
	}
	return nil
}

func (p *Processor) accept(ctx context.Context, m *broadcast.BlockAccepted) error {
	if !m.Head || m.Block == nil || m.Receipt == nil || m.Block.Header == nil {
		return nil
	}
	block := m.Block
	height := block.Header.Height
	if w, err := p.St.Irreversible(ctx); err != nil {
		return err
	} else if height <= w {
		return nil // a stale head from a fork that already lost; the canonical block is in
	}
	cur, err := p.St.Cursor(ctx)
	if err != nil {
		return err
	}
	switch {
	case height == cur.Height && bytes.Equal(block.Id, cur.BlockID):
		return nil // already applied
	case height <= cur.Height,
		height == cur.Height+1 && len(cur.BlockID) > 0 && len(block.Header.Previous) > 0 && !bytes.Equal(block.Header.Previous, cur.BlockID):
		// A different block where we already have one, or a head whose
		// parent is not our cursor: the chain forked. Drop every provisional
		// row and rebuild from the last irreversible block on this fork.
		restart, err := p.St.RollbackProvisional(ctx)
		if err != nil {
			return err
		}
		p.Log.Warn("reorg", "height", height, "cursor", cur.Height, "restart", restart)
		if cur, err = p.St.Cursor(ctx); err != nil {
			return err
		}
	}
	if height > cur.Height+1 {
		if err := p.fill(ctx, block.Id, cur, height-1, true); err != nil {
			return err
		}
	}
	facts, err := decode.Decode(p.Net, block, m.Receipt)
	if err != nil {
		return err
	}
	if len(facts) > 0 {
		p.Log.Info("facts", "height", height, "count", len(facts))
	}
	return p.St.Apply(ctx, height, block.Id, false, facts)
}

// fill fetches (cur.Height, to] on the fork that headID belongs to and applies
// each block, irreversible when at or below the node's last irreversible
// block. The first fetched block must descend from the cursor block; if it
// does not, every provisional row is dropped and the fill restarts once from
// the new cursor.
func (p *Processor) fill(ctx context.Context, headID []byte, cur store.Cursor, to uint64, mayRestart bool) error {
	fin, err := p.Src.Finality(ctx)
	if err != nil {
		return err
	}
	from := cur.Height + 1
	p.Log.Info("backfill", "from", from, "to", to, "irreversible", fin.Irreversible)
	for h := from; h <= to; {
		count := uint32(to - h + 1)
		if count > node.MaxBatch {
			count = node.MaxBatch
		}
		items, err := p.Src.Blocks(ctx, headID, h, count)
		if err != nil {
			return err
		}
		if len(items) != int(count) {
			return fmt.Errorf("live: backfill asked %d from %d, got %d", count, h, len(items))
		}
		if h == from && len(cur.BlockID) > 0 && items[0].Block != nil && !bytes.Equal(items[0].Block.GetHeader().GetPrevious(), cur.BlockID) {
			if !mayRestart {
				return fmt.Errorf("live: block %d does not descend from the cursor block after a rollback", h)
			}
			restart, err := p.St.RollbackProvisional(ctx)
			if err != nil {
				return err
			}
			p.Log.Warn("cursor block is not on the accepted fork; rolled back", "cursor", cur.Height, "restart", restart)
			newCur, err := p.St.Cursor(ctx)
			if err != nil {
				return err
			}
			return p.fill(ctx, headID, newCur, to, false)
		}
		for i, it := range items {
			want := h + uint64(i)
			if it.BlockHeight != want || it.Block == nil || it.Receipt == nil {
				return fmt.Errorf("live: backfill item %d has height %d, want %d", i, it.BlockHeight, want)
			}
			facts, err := decode.Decode(p.Net, it.Block, it.Receipt)
			if err != nil {
				return err
			}
			if err := p.St.Apply(ctx, want, it.BlockId, want <= fin.Irreversible, facts); err != nil {
				return err
			}
		}
		h += uint64(count)
	}
	return nil
}
