package store

import (
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/koinos/koinos-proto-golang/v2/koinos/contracts/governance"
	"github.com/koinos/koinos-proto-golang/v2/koinos/protocol"
	"govwatch/internal/b58"
	"govwatch/internal/testfix"
)

func initNotices(t *testing.T, s *Store, from uint64) {
	t.Helper()
	if err := s.InitNotifications(ctx, "mainnet", "-100", from); err != nil {
		t.Fatal(err)
	}
}
func queueNotices(t *testing.T, s *Store, limit uint64) int {
	t.Helper()
	n, err := s.QueueNotifications(ctx, "mainnet", "-100", limit)
	if err != nil {
		t.Fatal(err)
	}
	return n
}
func notices(t *testing.T, s *Store) []Notification {
	t.Helper()
	rows, err := s.ListNotifications(ctx, "mainnet", "-100")
	if err != nil {
		t.Fatal(err)
	}
	return rows
}

func TestNotificationsActivationFinalityAndResubmission(t *testing.T) {
	s := open(t)
	b, r, id := submission(10)
	apply(t, s, true, b, r)
	b, r, _ = submission(20)
	apply(t, s, false, b, r)
	initNotices(t, s, 20)
	if n := queueNotices(t, s, 100); n != 0 {
		t.Fatal(n)
	}
	// Watermark can precede row flagging: do not advance past provisional rows.
	if _, err := s.db.Exec(`UPDATE watermark SET irreversible=20`); err != nil {
		t.Fatal(err)
	}
	if n := queueNotices(t, s, 100); n != 0 {
		t.Fatal(n)
	}
	if err := s.MarkIrreversible(ctx, 20); err != nil {
		t.Fatal(err)
	}
	if n := queueNotices(t, s, 100); n != 1 {
		t.Fatal(n)
	}
	apply(t, s, true, b, r)
	if n := queueNotices(t, s, 100); n != 0 {
		t.Fatal(n)
	}
	applyStatus(t, s, true, 21, id, governance.ProposalStatus_reverted)
	b, r, _ = submission(22)
	apply(t, s, true, b, r)
	if n := queueNotices(t, s, 100); n != 2 {
		t.Fatal(n)
	}
	rows := notices(t, s)
	if len(rows) != 3 {
		t.Fatal(rows)
	}
	if !strings.Contains(rows[2].Text, b58.Encode(id)) || !strings.Contains(rows[2].Text, "upload_contract") {
		t.Fatal(rows[2])
	}
	if err := s.InitNotifications(ctx, "mainnet", "-200", 10); err != nil {
		t.Fatal(err)
	}
	if n, err := s.QueueNotifications(ctx, "mainnet", "-200", 100); err != nil || n != 4 {
		t.Fatal(n, err)
	}
	if err := s.InitNotifications(ctx, "mainnet", "-100", 20); err != nil {
		t.Fatal(err)
	}
	for _, v := range []struct {
		net, dest string
		h         uint64
	}{{"mainnet", "-100", 19}, {"harbinger", "-100", 20}, {"wrong", "-300", 20}, {"mainnet", "@channel", 20}, {"mainnet", "-0100", 20}, {"mainnet", "-300", 0}} {
		if err := s.InitNotifications(ctx, v.net, v.dest, v.h); err == nil {
			t.Fatalf("accepted %+v", v)
		}
	}
}

func TestNotificationsRangeAtomicityAndReorg(t *testing.T) {
	s := open(t)
	initNotices(t, s, 10)
	b, r, _ := submission(10)
	apply(t, s, false, b, r)
	if err := s.Rollback(ctx, 10); err != nil {
		t.Fatal(err)
	}
	if err := s.Apply(ctx, 11, testfix.BlockID(11), true, nil); err != nil {
		t.Fatal(err)
	}
	if n := queueNotices(t, s, 1); n != 0 {
		t.Fatal(n)
	}
	var next int
	if err := s.db.QueryRow(`SELECT next_height FROM notification_routes`).Scan(&next); err != nil || next != 11 {
		t.Fatal(next, err)
	}
	b, r, _ = submission(12)
	apply(t, s, true, b, r)
	b, r, _ = submission(13)
	apply(t, s, true, b, r)
	if _, err := s.db.Exec(`CREATE TRIGGER fail_queue BEFORE INSERT ON notification_outbox WHEN NEW.height=13 BEGIN SELECT RAISE(ABORT,'injected failure'); END`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.QueueNotifications(ctx, "mainnet", "-100", 100); err == nil {
		t.Fatal("expected injected failure")
	}
	if err := s.db.QueryRow(`SELECT next_height FROM notification_routes`).Scan(&next); err != nil || next != 11 {
		t.Fatal(next, err)
	}
	if len(notices(t, s)) != 0 {
		t.Fatal("partial queue insert survived rollback")
	}
	if _, err := s.db.Exec(`DROP TRIGGER fail_queue`); err != nil {
		t.Fatal(err)
	}
	if n := queueNotices(t, s, 100); n != 2 {
		t.Fatal(n)
	}
	if len(notices(t, s)) != 2 {
		t.Fatal("fork notice survived")
	}
}

func TestNotificationsClaimAndFinish(t *testing.T) {
	path := filepath.Join(t.TempDir(), "queue.db")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	initNotices(t, s, 10)
	b, r, _ := submission(10)
	apply(t, s, true, b, r)
	queueNotices(t, s, 100)
	now := time.Now().UTC()
	n, err := s.ClaimNotification(ctx, "mainnet", "-100", now)
	if err != nil || n == nil || n.State != "sending" {
		t.Fatal(n, err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if second, err := s.ClaimNotification(ctx, "mainnet", "-100", now.Add(time.Hour)); err != nil || second != nil {
		t.Fatal(second, err)
	}
	if err := s.FinishNotification(ctx, n.ID, "sent", 0, time.Time{}); err == nil {
		t.Fatal("invalid sent")
	}
	if err := s.FinishNotification(ctx, n.ID, "pending", 0, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if second, err := s.ClaimNotification(ctx, "mainnet", "-100", now); err != nil || second != nil {
		t.Fatal(second, err)
	}
	n, err = s.ClaimNotification(ctx, "mainnet", "-100", now.Add(time.Minute))
	if err != nil || n == nil {
		t.Fatal(n, err)
	}
	if err := s.FinishNotification(ctx, n.ID, "sent", 42, time.Time{}); err != nil {
		t.Fatal(err)
	}
	// A deliberate cursor rescan cannot duplicate or reset a completed send.
	if _, err := s.db.Exec(`UPDATE notification_routes SET next_height=from_height`); err != nil {
		t.Fatal(err)
	}
	if queued := queueNotices(t, s, 100); queued != 0 {
		t.Fatal(queued)
	}
	if rows := notices(t, s); len(rows) != 1 || rows[0].State != "sent" || rows[0].MessageID != 42 {
		t.Fatal(rows)
	}
	if err := s.FinishNotification(ctx, n.ID, "pending", 0, time.Time{}); err == nil {
		t.Fatal("sent must be terminal")
	}
	c, err := s.NotificationCounts(ctx, "mainnet", "-100")
	if err != nil || c["sent"] != 1 || c["sending"] != 0 {
		t.Fatal(c, err)
	}
}

func TestNotificationsSystemHistoricalMembershipAndSuppression(t *testing.T) {
	s := open(t)
	block := func(h uint64, ops ...*protocol.Operation) {
		b := testfix.Block(h, testfix.Tx(testfix.TxID(h), gov, ops...))
		apply(t, s, true, b, testfix.Receipt(b, nil, testfix.ReceiptsFor(b)...))
	}
	block(1, testfix.SetSystemContractOp(koin, true))
	block(10, testfix.UploadOp(koin, []byte("system")), testfix.UploadOp(alice, []byte("ordinary")))
	block(11, testfix.SetSystemContractOp(koin, false))
	block(12, testfix.UploadOp(koin, []byte("ordinary now")))
	block(13, testfix.UploadOp(alice, []byte("during")), testfix.SetSystemContractOp(alice, true), testfix.SetSystemContractOp(alice, false), testfix.SetSystemCallThunkOp(7, 3))
	initNotices(t, s, 10)
	if n := queueNotices(t, s, 100); n != 3 {
		t.Fatal(n)
	}
	rows := notices(t, s)
	if !strings.Contains(rows[2].Text, b58.Encode(koin)) || strings.Contains(rows[2].Text, b58.Encode(alice)) {
		t.Fatal(rows[2].Text)
	}
	if !strings.Contains(rows[0].Text, "upload_contract") || !strings.Contains(rows[0].Text, "set_system_call") {
		t.Fatal(rows[0].Text)
	}
	// Receipt-only application and votes do not create proposal notices.
	b, r, id := submission(20)
	apply(t, s, true, b, r)
	queueNotices(t, s, 100)
	if _, err := s.db.Exec(`INSERT INTO proposal_events (proposal_id,height,block_id,kind,status,irreversible) VALUES (?,21,?,'proposal_vote','',1),(?,21,?,'proposal_applied','applied',1)`, id, testfix.BlockID(21), id, testfix.BlockID(21)); err != nil {
		t.Fatal(err)
	}
	if err := s.Apply(ctx, 21, testfix.BlockID(21), true, nil); err != nil {
		t.Fatal(err)
	}
	if n := queueNotices(t, s, 100); n != 0 {
		t.Fatal(n)
	}
}

func TestNotificationsV5MigrationDoesNotRecompute(t *testing.T) {
	path := filepath.Join(t.TempDir(), "v5.db")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	b, r, _ := submission(10)
	apply(t, s, true, b, r)
	if _, err := s.db.Exec(`PRAGMA user_version=5; CREATE TRIGGER no_recompute BEFORE UPDATE ON proposals BEGIN SELECT RAISE(ABORT,'must not recompute v5'); END;`); err != nil {
		t.Fatal(err)
	}
	s.Close()
	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	s.Close()
}

func TestNotificationsConcurrentConnections(t *testing.T) {
	path := filepath.Join(t.TempDir(), "concurrent.db")
	a, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	initNotices(t, a, 10)
	block, receipt, _ := submission(10)
	apply(t, a, true, block, receipt)
	var wg sync.WaitGroup
	errors := make(chan error, 2)
	queued := make(chan int, 2)
	for _, s := range []*Store{a, b} {
		wg.Add(1)
		go func(s *Store) {
			defer wg.Done()
			n, e := s.QueueNotifications(ctx, "mainnet", "-100", 100)
			queued <- n
			errors <- e
		}(s)
	}
	wg.Wait()
	if e := <-errors; e != nil {
		t.Fatal(e)
	}
	if e := <-errors; e != nil {
		t.Fatal(e)
	}
	if total := <-queued + <-queued; total != 1 {
		t.Fatal(total)
	}
	claimed := make(chan *Notification, 2)
	for _, s := range []*Store{a, b} {
		wg.Add(1)
		go func(s *Store) {
			defer wg.Done()
			n, e := s.ClaimNotification(ctx, "mainnet", "-100", time.Now())
			claimed <- n
			errors <- e
		}(s)
	}
	wg.Wait()
	if e := <-errors; e != nil {
		t.Fatal(e)
	}
	if e := <-errors; e != nil {
		t.Fatal(e)
	}
	first, second := <-claimed, <-claimed
	if (first == nil) == (second == nil) {
		t.Fatalf("claims: %+v %+v", first, second)
	}
}

func TestNotificationsUnknownOperationsAndTextBounds(t *testing.T) {
	s := open(t)
	initNotices(t, s, 10)
	// An indirect submission emits pending without the proposal operations.
	id := testfix.TxID(99)
	b := testfix.Block(10)
	apply(t, s, true, b, testfix.Receipt(b, []*protocol.EventData{testfix.StatusEvent(gov, id, governance.ProposalStatus_pending)}))
	if n := queueNotices(t, s, 100); n != 1 {
		t.Fatal(n)
	}
	if text := notices(t, s)[0].Text; !strings.Contains(text, "Operations: unknown") {
		t.Fatal(text)
	}
	var ops []*protocol.Operation
	for i := 0; i < 100; i++ {
		ops = append(ops, testfix.SetSystemCallOp(uint32(i), koin, uint32(i)))
	}
	tx, root := testfix.SubmitProposalTx(testfix.TxID(11), alice, gov, ops, 1)
	b = testfix.Block(11, tx)
	apply(t, s, true, b, testfix.Receipt(b, nil, testfix.TxReceipt(tx.Id, alice, false, testfix.StatusEvent(gov, root, governance.ProposalStatus_pending))))
	b = testfix.Block(12, testfix.Tx(testfix.TxID(12), gov, ops...))
	apply(t, s, true, b, testfix.Receipt(b, nil, testfix.ReceiptsFor(b)...))
	if n := queueNotices(t, s, 100); n != 2 {
		t.Fatal(n)
	}
	rows := notices(t, s)
	for _, n := range rows[:2] {
		if utf8.RuneCountInString(n.Text) > 3500 || !strings.Contains(n.Text, "omitted") {
			t.Fatal(n.Text)
		}
	}
	var evidence int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM system_changes WHERE height=12`).Scan(&evidence); err != nil || evidence != 100 {
		t.Fatal(evidence, err)
	}
}

func TestNotificationsFinishFailureRollbackAndTerminalStates(t *testing.T) {
	s := open(t)
	initNotices(t, s, 10)
	b, r, _ := submission(10)
	apply(t, s, true, b, r)
	queueNotices(t, s, 100)
	n, err := s.ClaimNotification(ctx, "mainnet", "-100", time.Now())
	if err != nil || n == nil {
		t.Fatal(n, err)
	}
	if _, err = s.db.Exec(`CREATE TRIGGER fail_finish BEFORE UPDATE ON notification_outbox WHEN NEW.state='sent' BEGIN SELECT RAISE(ABORT,'injected finish failure'); END`); err != nil {
		t.Fatal(err)
	}
	if err = s.FinishNotification(ctx, n.ID, "sent", 9, time.Time{}); err == nil {
		t.Fatal("expected finish failure")
	}
	if rows := notices(t, s); rows[0].State != "sending" || rows[0].MessageID != 0 {
		t.Fatal(rows)
	}
	if err = s.FinishNotification(ctx, n.ID, "uncertain", 0, time.Time{}); err != nil {
		t.Fatal(err)
	}
	if n, err = s.ClaimNotification(ctx, "mainnet", "-100", time.Now().Add(time.Hour)); err != nil || n != nil {
		t.Fatal(n, err)
	}
}

func TestNotificationsRouteCooldownSurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cooldown.db")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	initNotices(t, s, 10)
	for _, h := range []uint64{10, 11} {
		b, r, _ := submission(h)
		apply(t, s, true, b, r)
	}
	queueNotices(t, s, 100)
	now := time.Now().UTC()
	until := now.Add(time.Minute)
	first, err := s.ClaimNotification(ctx, "mainnet", "-100", now)
	if err != nil || first == nil {
		t.Fatal(first, err)
	}
	if err = s.FinishNotification(ctx, first.ID, "pending", 0, until); err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	// Repeating activation must preserve the cooldown too.
	initNotices(t, s, 10)
	other, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	for _, connection := range []*Store{s, other} {
		if n, err := connection.ClaimNotification(ctx, "mainnet", "-100", now.Add(time.Second)); err != nil || n != nil {
			t.Fatalf("cooldown bypass: %+v %v", n, err)
		}
	}
	if n, err := other.ClaimNotification(ctx, "mainnet", "-100", until); err != nil || n == nil || n.ID != first.ID {
		t.Fatal(n, err)
	}
}

func TestNotificationsRouteCooldownFinishAtomicity(t *testing.T) {
	s := open(t)
	initNotices(t, s, 10)
	for _, h := range []uint64{10, 11} {
		b, r, _ := submission(h)
		apply(t, s, true, b, r)
	}
	queueNotices(t, s, 100)
	now := time.Now().UTC()
	until := now.Add(time.Minute)
	first, err := s.ClaimNotification(ctx, "mainnet", "-100", now)
	if err != nil || first == nil {
		t.Fatal(first, err)
	}
	second, err := s.ClaimNotification(ctx, "mainnet", "-100", now)
	if err != nil || second == nil {
		t.Fatal(second, err)
	}
	if _, err = s.db.Exec(`CREATE TRIGGER fail_cooldown BEFORE UPDATE ON notification_routes BEGIN SELECT RAISE(ABORT,'injected cooldown failure'); END`); err != nil {
		t.Fatal(err)
	}
	if err = s.FinishNotification(ctx, first.ID, "pending", 0, until); err == nil {
		t.Fatal("expected finish failure")
	}
	for _, n := range notices(t, s) {
		if n.State != "sending" {
			t.Fatal(n)
		}
	}
	if _, err = s.db.Exec(`DROP TRIGGER fail_cooldown`); err != nil {
		t.Fatal(err)
	}
	if err = s.FinishNotification(ctx, first.ID, "pending", 0, until); err != nil {
		t.Fatal(err)
	}
	// An independently finishing earlier deadline must not shorten cooldown.
	if err = s.FinishNotification(ctx, second.ID, "pending", 0, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if n, err := s.ClaimNotification(ctx, "mainnet", "-100", now.Add(2*time.Second)); err != nil || n != nil {
		t.Fatal(n, err)
	}
}
