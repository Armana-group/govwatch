package main

import (
	"bytes"
	"context"
	"path/filepath"
	"strings"
	"testing"

	"govwatch/internal/store"
)

func TestStatus_EmptyDB(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "s.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	var out bytes.Buffer
	if err := writeStatus(context.Background(), st, &out); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "cursor: 0") {
		t.Fatalf("status output:\n%s", out.String())
	}
}

func TestCommonFlags_EnvFallback(t *testing.T) {
	t.Setenv("GOVWATCH_NETWORK", "harbinger")
	t.Setenv("GOVWATCH_DB", "x.db")
	c, err := parseCommon("status", nil)
	if err != nil {
		t.Fatal(err)
	}
	if c.network != "harbinger" || c.db != "x.db" || c.amqp != "amqp://guest:guest@127.0.0.1:5672/" {
		t.Fatalf("common = %+v", c)
	}
}

func TestVersion_PrintsBuildCommit(t *testing.T) {
	old := buildCommit
	buildCommit = "abc123"
	defer func() { buildCommit = old }()
	var out bytes.Buffer
	writeVersion(&out)
	if strings.TrimSpace(out.String()) != "abc123" {
		t.Fatalf("version output %q", out.String())
	}
}
