// Package replay walks irreversible blocks from the node's block store and
// records their facts. It is used for historical catch-up and for filling
// gaps in the live feed.
package replay

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"

	"govwatch/internal/decode"
	"govwatch/internal/node"
	"govwatch/internal/store"
)

// ErrShortBatch means block_store returned fewer or non-contiguous blocks;
// replay stops rather than skipping heights.
var ErrShortBatch = errors.New("replay: block_store returned fewer blocks than requested")

type Options struct {
	From  uint64 // 0: cursor + 1
	To    uint64 // 0 or above the last irreversible block: clamped to it
	Batch uint32 // 0: node.MaxBatch
	Log   *slog.Logger
}

// Run replays [From, To] and returns the last height applied. Everything it
// writes is marked irreversible.
func Run(ctx context.Context, n decode.Network, src node.Source, st *store.Store, o Options) (uint64, error) {
	log := o.Log
	if log == nil {
		log = slog.Default()
	}
	batch := o.Batch
	if batch == 0 || batch > node.MaxBatch {
		batch = node.MaxBatch
	}
	fin, err := src.Finality(ctx)
	if err != nil {
		return 0, err
	}
	cur, err := st.Cursor(ctx)
	if err != nil {
		return 0, err
	}
	from := o.From
	if from == 0 {
		from = cur.Height + 1
	}
	to := o.To
	if to == 0 || to > fin.Irreversible {
		to = fin.Irreversible
	}
	last := cur.Height
	if from > to {
		return last, nil
	}
	log.Info("replay start", "network", n.Name, "from", from, "to", to, "irreversible", fin.Irreversible)
	checkedLink := false
	for h := from; h <= to; {
		count := batch
		if remaining := to - h + 1; remaining < uint64(count) {
			count = uint32(remaining)
		}
		items, err := src.Blocks(ctx, fin.HeadID, h, count)
		if err != nil {
			return last, err
		}
		if len(items) != int(count) {
			return last, fmt.Errorf("%w: asked %d from %d, got %d", ErrShortBatch, count, h, len(items))
		}
		// The first block must descend from the block our cursor points at;
		// otherwise our last blocks were on a fork that lost while we were
		// away. Drop every provisional row and start again from there.
		if !checkedLink && h == cur.Height+1 && len(cur.BlockID) > 0 && items[0].Block != nil && !bytes.Equal(items[0].Block.GetHeader().GetPrevious(), cur.BlockID) {
			restart, err := st.RollbackProvisional(ctx)
			if err != nil {
				return last, err
			}
			log.Warn("cursor block is not an ancestor of the node's chain; rolled back", "cursor", cur.Height, "restart", restart)
			if cur, err = st.Cursor(ctx); err != nil {
				return last, err
			}
			h, last, checkedLink = restart, cur.Height, true
			continue
		}
		checkedLink = true
		for i, it := range items {
			want := h + uint64(i)
			if it.BlockHeight != want || it.Block == nil || it.Receipt == nil {
				return last, fmt.Errorf("%w: item %d has height %d (want %d) block=%v receipt=%v", ErrShortBatch, i, it.BlockHeight, want, it.Block != nil, it.Receipt != nil)
			}
			facts, err := decode.Decode(n, it.Block, it.Receipt)
			if err != nil {
				return last, err
			}
			if err := st.Apply(ctx, want, it.BlockId, true, facts); err != nil {
				return last, err
			}
			last = want
			if len(facts) > 0 {
				log.Info("facts", "height", want, "count", len(facts), "kinds", kinds(facts))
			}
			if want%10000 == 0 {
				log.Info("replay progress", "height", want, "to", to)
			}
		}
		h += uint64(count)
	}
	log.Info("replay done", "last", last)
	return last, nil
}

func kinds(facts []decode.Fact) []string {
	out := make([]string, len(facts))
	for i, f := range facts {
		out[i] = string(f.Kind)
	}
	return out
}
