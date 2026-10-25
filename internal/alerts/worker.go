// Package alerts delivers the durable public notification outbox independently
// of block collection. It never retries an ambiguous delivery automatically.
package alerts

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"govwatch/internal/store"
	"govwatch/internal/telegram"
)

type Sender interface {
	Send(context.Context, string, string) telegram.Result
}

type Worker struct {
	Store                *store.Store
	Sender               Sender
	Network, Destination string
}

// Step collects a bounded range of confirmed history and attempts at most one
// due notification. Claiming is durable before HTTP; a crash leaves 'sending'
// for operator reconciliation instead of permitting a duplicate send.
func (w *Worker) Step(ctx context.Context, now time.Time) (time.Duration, error) {
	const interval = 2 * time.Second
	if _, err := w.Store.QueueNotifications(ctx, w.Network, w.Destination, 1000); err != nil {
		return interval, err
	}
	n, err := w.Store.ClaimNotification(ctx, w.Network, w.Destination, now)
	if err != nil || n == nil {
		return interval, err
	}
	result := w.Sender.Send(ctx, n.Destination, n.Text)
	state := result.State
	wait := interval
	retryAt := time.Time{}
	switch state {
	case "sent", "failed", "uncertain":
	case "retry":
		state = "pending"
		if result.RetryAfter > wait {
			wait = result.RetryAfter
		}
		// Telegram's delay starts when its response arrives, not when the
		// request began (which may have spent seconds in flight).
		retryAt = time.Now().Add(wait)
	default:
		return interval, fmt.Errorf("notification %d: invalid delivery state; claim retained for reconciliation", n.ID)
	}
	// Persist an observed provider outcome even if shutdown cancelled the HTTP
	// context. Failure to persist leaves the claim intact and must not resend.
	finishCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if err := w.Store.FinishNotification(finishCtx, n.ID, state, result.MessageID, retryAt); err != nil {
		return wait, fmt.Errorf("notification %d: persist result: %w", n.ID, err)
	}
	if state == "failed" || state == "uncertain" {
		slog.Warn("notification needs attention", "id", n.ID, "state", state)
	} else {
		slog.Info("notification delivery", "id", n.ID, "state", state)
	}
	return wait, nil
}

func (w *Worker) Run(ctx context.Context) error {
	for {
		if ctx.Err() != nil {
			return nil
		}
		wait, err := w.Step(ctx, time.Now())
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}
	}
}
