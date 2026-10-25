package main

import (
	"context"
	"github.com/koinos/koinos-proto-golang/v2/koinos/contracts/governance"
	"govwatch/internal/decode"
	"govwatch/internal/store"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestNotifyRejectsInvitationOrAlias(t *testing.T) {
	for _, chat := range []string{"@somechannel", "https://t.me/+invite", "12345", "-0", "-001"} {
		if err := validateNotifyChat(chat); err == nil {
			t.Fatalf("accepted noncanonical channel ID %q", chat)
		}
	}
	if err := validateNotifyChat("-1001234567890"); err != nil {
		t.Fatal(err)
	}
}
func TestNotifyRunRequiresExplicitSendBeforeDBAccess(t *testing.T) {
	path := filepath.Join(t.TempDir(), "absent.db")
	err := cmdNotify(context.Background(), []string{"run", "-chat-id=-100123", "-db", path})
	if err == nil || !strings.Contains(err.Error(), "-send") {
		t.Fatalf("err=%v", err)
	}
	if _, err = os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("unexpected DB creation: %v", err)
	}
}
func TestNotifyInitRequiresExplicitBoundary(t *testing.T) {
	path := filepath.Join(t.TempDir(), "absent.db")
	err := cmdNotify(context.Background(), []string{"init", "-chat-id=-100123", "-db", path})
	if err == nil || !strings.Contains(err.Error(), "from-height") {
		t.Fatalf("err=%v", err)
	}
	if _, err = os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("unexpected DB creation: %v", err)
	}
}
func TestReadTelegramTokenRequiresPrivateRegularFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "token")
	const secret = "12345:Example_test_token"
	if err := os.WriteFile(path, []byte(secret+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	got, err := readTelegramToken(path)
	if err != nil || got != secret {
		t.Fatalf("private token read failed")
	}
	if err = os.Chmod(path, 0644); err != nil {
		t.Fatal(err)
	}
	_, err = readTelegramToken(path)
	if err == nil {
		t.Fatal("accepted world-readable token")
	}
	if strings.Contains(err.Error(), secret) {
		t.Fatal("token leaked in error")
	}
	if err = os.Chmod(path, 0600); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(path, []byte(strings.Repeat("x", 4097)), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = readTelegramToken(path); err == nil {
		t.Fatal("accepted oversized token file")
	}
}

func TestNotifyPreviewQueuesWithoutLoadingToken(t *testing.T) {
	db := filepath.Join(t.TempDir(), "collector.db")
	st, err := store.Open(db)
	if err != nil {
		t.Fatal(err)
	}
	f := decode.Fact{Kind: decode.ProposalStatus, Height: 10, BlockID: []byte{10}, ProposalID: []byte{1}, Status: governance.ProposalStatus_active}
	if err = st.Apply(context.Background(), 10, []byte{10}, true, []decode.Fact{f}); err != nil {
		t.Fatal(err)
	}
	st.Close()
	for _, args := range [][]string{
		{"init", "-db", db, "-chat-id=-100123", "-from-height=10"},
		{"preview", "-db", db, "-chat-id=-100123", "-token-file", filepath.Join(t.TempDir(), "missing-token")},
	} {
		if err = cmdNotify(context.Background(), args); err != nil {
			t.Fatal(err)
		}
	}
	st, err = store.Open(db)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	counts, err := st.NotificationCounts(context.Background(), "mainnet", "-100123")
	if err != nil {
		t.Fatal(err)
	}
	if counts["pending"] != 1 || counts["sending"] != 0 || counts["sent"] != 0 {
		t.Fatalf("preview attempted delivery: %v", counts)
	}
}
