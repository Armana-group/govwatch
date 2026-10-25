package telegram

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

const testToken = "123456:abc_DEF-123"

func testClient(t *testing.T, h http.Handler) *Client {
	t.Helper()
	s := httptest.NewServer(h)
	t.Cleanup(s.Close)
	c, err := New(testToken)
	if err != nil {
		t.Fatal(err)
	}
	c.endpoint = s.URL
	c.httpClient = &http.Client{
		Transport: s.Client().Transport,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	return c
}

func TestSend_SendsPlainTextWithoutPreviewsOrPaidBroadcasts(t *testing.T) {
	c := testClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/bot"+testToken+"/sendMessage" {
			t.Fatalf("request = %s %s", r.Method, r.URL.Path)
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatal(err)
		}
		got := string(body)
		for _, want := range []string{`"chat_id":"-1001234567890"`, `"text":"hello *world*"`, `"link_preview_options":{"is_disabled":true}`} {
			if !strings.Contains(got, want) {
				t.Fatalf("body %q lacks %q", got, want)
			}
		}
		for _, forbidden := range []string{"parse_mode", "allow_paid_broadcast"} {
			if strings.Contains(got, forbidden) {
				t.Fatalf("body unexpectedly contains %q: %s", forbidden, got)
			}
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"ok":true,"result":{"message_id":42}}`)
	}))

	got := c.Send(context.Background(), "-1001234567890", "hello *world*")
	if got != (Result{State: "sent", MessageID: 42}) {
		t.Fatalf("result = %+v", got)
	}
}

func TestSend_RateLimitReturnsBoundedRetryAfter(t *testing.T) {
	c := testClient(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = io.WriteString(w, `{"ok":false,"error_code":429,"parameters":{"retry_after":12}}`)
	}))

	got := c.Send(context.Background(), "-1001234567890", "hello")
	if got != (Result{State: "retry", RetryAfter: 12 * time.Second}) {
		t.Fatalf("result = %+v", got)
	}
}

func TestSend_RateLimitWithoutRetryAfterUsesDefault(t *testing.T) {
	c := testClient(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = io.WriteString(w, `{"ok":false,"error_code":429}`)
	}))

	got := c.Send(context.Background(), "-1001234567890", "hello")
	if got != (Result{State: "retry", RetryAfter: defaultRetryAfter}) {
		t.Fatalf("result = %+v", got)
	}
}

func TestSend_RateLimitDoesNotRequireOptionalErrorCode(t *testing.T) {
	c := testClient(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = io.WriteString(w, `{"ok":false,"parameters":{"retry_after":12}}`)
	}))

	got := c.Send(context.Background(), "-1001234567890", "hello")
	if got != (Result{State: "retry", RetryAfter: 12 * time.Second}) {
		t.Fatalf("result = %+v", got)
	}
}

func TestSend_DefiniteRejectionIsFailed(t *testing.T) {
	for _, status := range []int{http.StatusBadRequest, http.StatusUnauthorized, http.StatusForbidden} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			c := testClient(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(status)
				_, _ = io.WriteString(w, `{"ok":false}`)
			}))
			if got := c.Send(context.Background(), "-1001234567890", "hello"); got.State != "failed" {
				t.Fatalf("result = %+v", got)
			}
		})
	}
}

func TestSend_UncertainResponsesStayUncertain(t *testing.T) {
	for name, response := range map[string]struct {
		status int
		body   string
	}{
		"malformed":            {http.StatusOK, `{"ok":true`},
		"server error":         {http.StatusInternalServerError, `{"ok":false}`},
		"contradictory status": {http.StatusInternalServerError, `{"ok":true,"result":{"message_id":42}}`},
		"missing message id":   {http.StatusOK, `{"ok":true,"result":{}}`},
		"unbounded retry":      {http.StatusTooManyRequests, `{"ok":false,"error_code":429,"parameters":{"retry_after":999999}}`},
	} {
		t.Run(name, func(t *testing.T) {
			c := testClient(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(response.status)
				_, _ = io.WriteString(w, response.body)
			}))
			if got := c.Send(context.Background(), "-1001234567890", "hello"); got.State != "uncertain" {
				t.Fatalf("result = %+v", got)
			}
		})
	}
}

func TestSend_TransportErrorAndRedirectAreUncertainWithoutFollowing(t *testing.T) {
	c, err := New(testToken)
	if err != nil {
		t.Fatal(err)
	}
	c.endpoint = "http://127.0.0.1:1"
	c.httpClient = &http.Client{Timeout: time.Second}
	if got := c.Send(context.Background(), "-1001234567890", "hello"); got.State != "uncertain" {
		t.Fatalf("transport result = %+v", got)
	}

	var destinationCalls atomic.Int32
	destination := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		destinationCalls.Add(1)
	}))
	t.Cleanup(destination.Close)
	c = testClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, destination.URL, http.StatusFound)
	}))
	if got := c.Send(context.Background(), "-1001234567890", "hello"); got.State != "uncertain" {
		t.Fatalf("redirect result = %+v", got)
	}
	if destinationCalls.Load() != 0 {
		t.Fatal("followed redirect")
	}
}

func TestSend_InvalidInputFailsWithoutRequest(t *testing.T) {
	var calls atomic.Int32
	c := testClient(t, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		calls.Add(1)
	}))

	for name, in := range map[string]struct{ chatID, text string }{
		"alias":       {"@govwatch", "hello"},
		"positive id": {"123", "hello"},
		"zero":        {"-0", "hello"},
		"empty text":  {"-1001234567890", ""},
		"too long":    {"-1001234567890", strings.Repeat("😀", 2049)},
	} {
		t.Run(name, func(t *testing.T) {
			if got := c.Send(context.Background(), in.chatID, in.text); got.State != "failed" {
				t.Fatalf("result = %+v", got)
			}
		})
	}
	if calls.Load() != 0 {
		t.Fatalf("made %d requests for invalid input", calls.Load())
	}
}

func TestNew_RejectsUnsafeTokenWithoutLeakingIt(t *testing.T) {
	const secret = "not a telegram token"
	_, err := New(secret)
	if err == nil {
		t.Fatal("accepted unsafe token")
	}
	if strings.Contains(err.Error(), secret) {
		t.Fatalf("error leaked token: %q", err)
	}
}
