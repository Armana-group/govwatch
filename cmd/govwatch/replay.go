package main

import (
	"context"
	"flag"
	"log/slog"

	"govwatch/internal/replay"
)

func cmdReplay(ctx context.Context, args []string) error {
	var from, to uint64
	var batch uint
	c, err := parseCommon("replay", args, func(fs *flag.FlagSet) {
		fs.Uint64Var(&from, "from", 0, "first height (default: cursor + 1)")
		fs.Uint64Var(&to, "to", 0, "last height (default: last irreversible block)")
		fs.UintVar(&batch, "batch", 1000, "blocks per block_store request (max 1000)")
	})
	if err != nil {
		return badFlags{err}
	}
	n, err := c.net()
	if err != nil {
		return badFlags{err}
	}
	st, err := c.openStore()
	if err != nil {
		return err
	}
	defer st.Close()
	src := c.openSource(n)
	defer src.Close()
	last, err := replay.Run(ctx, n, src, st, replay.Options{From: from, To: to, Batch: uint32(batch), Log: slog.Default()})
	slog.Info("replay stopped", "last", last)
	return err
}
