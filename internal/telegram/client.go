// Package telegram sends plain-text governance alerts to numeric Telegram chats.
package telegram

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"time"
	"unicode/utf16"
)

const (
	productionEndpoint = "https://api.telegram.org"
	requestTimeout     = 15 * time.Second
	maxResponseBytes   = 1 << 20
	maxTextUTF16       = 4096
	defaultRetryAfter  = 60 * time.Second
	maxRetryAfter      = time.Hour
)

var (
	tokenPattern  = regexp.MustCompile(`^[0-9]+:[A-Za-z0-9_-]+$`)
	chatIDPattern = regexp.MustCompile(`^-[1-9][0-9]*$`)
)

// Result describes whether Telegram definitely accepted, rejected, or may have
// processed a request. Retry is returned only for an explicit, bounded 429.
type Result struct {
	State      string
	MessageID  int64
	RetryAfter time.Duration
}

// Client sends alerts to the fixed Telegram Bot API endpoint.
type Client struct {
	token      string
	endpoint   string // test-only override; production is always productionEndpoint.
	httpClient *http.Client
}

// New validates token syntax without including the token in errors.
func New(token string) (*Client, error) {
	if !tokenPattern.MatchString(token) {
		return nil, errors.New("telegram: invalid token")
	}

	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	return &Client{
		token:    token,
		endpoint: productionEndpoint,
		httpClient: &http.Client{
			Timeout:   requestTimeout,
			Transport: transport,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}, nil
}

// Send makes one request and deliberately exposes no response content or
// transport errors, because the request URL contains the bot token.
func (c *Client) Send(ctx context.Context, chatID, text string) Result {
	if !validChatID(chatID) || !validText(text) {
		return Result{State: "failed"}
	}

	body, err := json.Marshal(struct {
		ChatID             string `json:"chat_id"`
		Text               string `json:"text"`
		LinkPreviewOptions struct {
			IsDisabled bool `json:"is_disabled"`
		} `json:"link_preview_options"`
	}{
		ChatID: chatID,
		Text:   text,
		LinkPreviewOptions: struct {
			IsDisabled bool `json:"is_disabled"`
		}{IsDisabled: true},
	})
	if err != nil {
		return Result{State: "failed"}
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint+"/bot"+c.token+"/sendMessage", bytes.NewReader(body))
	if err != nil {
		return Result{State: "uncertain"}
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return Result{State: "uncertain"}
	}
	defer resp.Body.Close()

	contents, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes+1))
	if err != nil || len(contents) > maxResponseBytes {
		return Result{State: "uncertain"}
	}

	var reply apiResponse
	if json.Unmarshal(contents, &reply) != nil || reply.OK == nil {
		return Result{State: "uncertain"}
	}
	return classify(resp.StatusCode, reply)
}

type apiResponse struct {
	OK        *bool `json:"ok"`
	ErrorCode int   `json:"error_code"`
	Result    *struct {
		MessageID int64 `json:"message_id"`
	} `json:"result"`
	Parameters *struct {
		RetryAfter *int64 `json:"retry_after"`
	} `json:"parameters"`
}

func classify(status int, reply apiResponse) Result {
	if *reply.OK && status >= http.StatusOK && status < http.StatusMultipleChoices && reply.Result != nil && reply.Result.MessageID > 0 {
		return Result{State: "sent", MessageID: reply.Result.MessageID}
	}
	if !*reply.OK && status == http.StatusTooManyRequests && (reply.ErrorCode == 0 || reply.ErrorCode == http.StatusTooManyRequests) {
		delay := defaultRetryAfter
		if reply.Parameters != nil && reply.Parameters.RetryAfter != nil {
			seconds := *reply.Parameters.RetryAfter
			if seconds <= 0 || seconds > int64(maxRetryAfter/time.Second) {
				return Result{State: "uncertain"}
			}
			delay = time.Duration(seconds) * time.Second
		}
		return Result{State: "retry", RetryAfter: delay}
	}
	if !*reply.OK && (status == http.StatusBadRequest || status == http.StatusUnauthorized || status == http.StatusForbidden) {
		return Result{State: "failed"}
	}
	return Result{State: "uncertain"}
}

func validChatID(chatID string) bool {
	if !chatIDPattern.MatchString(chatID) {
		return false
	}
	_, err := strconv.ParseInt(chatID, 10, 64)
	return err == nil
}

func validText(text string) bool {
	if text == "" {
		return false
	}
	units := 0
	for _, r := range text {
		units += utf16.RuneLen(r)
		if units > maxTextUTF16 {
			return false
		}
	}
	return true
}
