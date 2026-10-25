package main

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"govwatch/internal/live"
	"govwatch/internal/replay"
)

const reconnectDelay = 5 * time.Second

// cmdRun catches up to the last irreversible block over RPC, then follows
// the broadcast feed. If the feed drops it reconnects; the catch-up on each
// reconnect covers whatever was missed.
func cmdRun(ctx context.Context, args []string) error {
	c, err := parseCommon("run", args)
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
	log := slog.Default()
	p := &live.Processor{Net: n, Src: src, St: st, Log: log}
	for {
		if _, err := replay.Run(ctx, n, src, st, replay.Options{Log: log}); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			log.Error("catch-up failed, retrying", "err", err, "in", reconnectDelay)
		} else if err := p.Run(ctx, live.NewAMQPFeed(c.amqp, n.Name)); err != nil && !errors.Is(err, context.Canceled) {
			log.Error("live feed stopped, reconnecting", "err", err, "in", reconnectDelay)
		} else {
			log.Info("live feed closed, reconnecting", "in", reconnectDelay)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(reconnectDelay):
		}
	}
}
