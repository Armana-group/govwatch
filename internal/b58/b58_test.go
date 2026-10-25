package b58

import (
	"bytes"
	"testing"
)

func TestRoundTrip(t *testing.T) {
	const addr = "19qj51eTbSFJYU7ZagudkpxPgNSzPMfdPX"
	raw, err := Decode(addr)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(raw) != 25 {
		t.Fatalf("decoded length = %d, want 25", len(raw))
	}
	if got := Encode(raw); got != addr {
		t.Fatalf("Encode(Decode(x)) = %q, want %q", got, addr)
	}
}

func TestLeadingZeros(t *testing.T) {
	if got := Encode([]byte{0, 0, 1}); got != "112" {
		t.Fatalf("Encode = %q, want %q", got, "112")
	}
	raw, err := Decode("112")
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if !bytes.Equal(raw, []byte{0, 0, 1}) {
		t.Fatalf("Decode = %v, want [0 0 1]", raw)
	}
}

func TestInvalidChar(t *testing.T) {
	if _, err := Decode("0OIl"); err == nil {
		t.Fatal("Decode accepted characters outside the alphabet")
	}
}
