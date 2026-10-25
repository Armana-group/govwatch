package alerts

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/koinos/koinos-proto-golang/v2/koinos/contracts/governance"
	"govwatch/internal/decode"
	"govwatch/internal/store"
	"govwatch/internal/telegram"
)

type fakeSender struct {
	result            telegram.Result
	calls             int
	destination, text string
}

func (f *fakeSender) Send(_ context.Context, dest, text string) telegram.Result {
	f.calls++
	f.destination = dest
	f.text = text
	return f.result
}

func queuedStore(t *testing.T) *store.Store {
	t.Helper()
	s, err := store.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	ctx := context.Background()
	if err := s.InitNotifications(ctx, "mainnet", "-100123", 10); err != nil {
		t.Fatal(err)
	}
	f := decode.Fact{Kind: decode.ProposalStatus, Height: 10, BlockID: []byte{10}, ProposalID: []byte{1}, Status: governance.ProposalStatus_active}
	if err := s.Apply(ctx, 10, []byte{10}, true, []decode.Fact{f}); err != nil {
		t.Fatal(err)
	}
	return s
}

func TestWorkerRecordsDeliveryAndDoesNotResend(t *testing.T) {
	s := queuedStore(t)
	sender := &fakeSender{result: telegram.Result{State: "sent", MessageID: 42}}
	w := Worker{Store: s, Sender: sender, Network: "mainnet", Destination: "-100123"}
	for i := 0; i < 2; i++ {
		if _, err := w.Step(context.Background(), time.Now()); err != nil {
			t.Fatal(err)
		}
	}
	counts, err := s.NotificationCounts(context.Background(), "mainnet", "-100123")
	if err != nil {
		t.Fatal(err)
	}
	if sender.calls != 1 || sender.destination != "-100123" || sender.text == "" || counts["sent"] != 1 {
		t.Fatalf("calls=%d destination=%q counts=%v", sender.calls, sender.destination, counts)
	}
}
func TestWorkerDoesNotRetryUncertainDelivery(t *testing.T) {
	s := queuedStore(t)
	sender := &fakeSender{result: telegram.Result{State: "uncertain"}}
	w := Worker{Store: s, Sender: sender, Network: "mainnet", Destination: "-100123"}
	for i := 0; i < 2; i++ {
		if _, err := w.Step(context.Background(), time.Now()); err != nil {
			t.Fatal(err)
		}
	}
	counts, _ := s.NotificationCounts(context.Background(), "mainnet", "-100123")
	if sender.calls != 1 || counts["uncertain"] != 1 {
		t.Fatalf("calls=%d counts=%v", sender.calls, counts)
	}
}
func TestWorkerHonorsExplicitRetryDelay(t *testing.T) {
	s := queuedStore(t)
	sender := &fakeSender{result: telegram.Result{State: "retry", RetryAfter: time.Minute}}
	w := Worker{Store: s, Sender: sender, Network: "mainnet", Destination: "-100123"}
	now := time.Now()
	delay, err := w.Step(context.Background(), now)
	if err != nil {
		t.Fatal(err)
	}
	if delay < time.Minute {
		t.Fatalf("delay=%v", delay)
	}
	if _, err = w.Step(context.Background(), now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if sender.calls != 1 {
		t.Fatalf("retried early: %d", sender.calls)
	}
	sender.result = telegram.Result{State: "sent", MessageID: 43}
	if _, err = w.Step(context.Background(), now.Add(2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if sender.calls != 2 {
		t.Fatalf("did not retry after delay: %d", sender.calls)
	}
}

// A long request must not consume the cooldown Telegram specifies in its reply.
func TestWorkerRetryDeadlineStartsWhenResponseArrives(t *testing.T) {
	s := queuedStore(t)
	sender := &fakeSender{result: telegram.Result{State: "retry", RetryAfter: time.Minute}}
	w := Worker{Store: s, Sender: sender, Network: "mainnet", Destination: "-100123"}
	if _, err := w.Step(context.Background(), time.Now().Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Step(context.Background(), time.Now()); err != nil {
		t.Fatal(err)
	}
	if sender.calls != 1 {
		t.Fatalf("retry delay expired before response: %d sends", sender.calls)
	}
}
