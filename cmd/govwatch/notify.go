package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"govwatch/internal/alerts"
	"govwatch/internal/telegram"
)

// Notification commands are separate from collection: a provider outage must
// not halt block ingestion. Only 'run -send' can make Telegram HTTP requests.
func cmdNotify(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return badFlags{errors.New("usage: govwatch notify <init|preview|status|run> -chat-id=-100... [flags]")}
	}
	action := args[0]
	switch action {
	case "init", "preview", "status", "run":
	default:
		return badFlags{errors.New("notify action must be init, preview, status, or run")}
	}
	var chat, tokenFile string
	var from uint64
	var send bool
	c, err := parseCommon("notify "+action, args[1:], func(fs *flag.FlagSet) {
		fs.StringVar(&chat, "chat-id", os.Getenv("GOVWATCH_TELEGRAM_CHAT_ID"), "verified numeric channel/group ID (negative)")
		fs.StringVar(&tokenFile, "token-file", os.Getenv("GOVWATCH_TELEGRAM_TOKEN_FILE"), "private file containing the bot token; run only")
		fs.Uint64Var(&from, "from-height", 0, "first block to notify about; required for init")
		fs.BoolVar(&send, "send", false, "explicitly enable Telegram delivery; run only")
	})
	if err != nil {
		return badFlags{err}
	}
	if c.fs.NArg() != 0 {
		return badFlags{errors.New("unexpected positional notification arguments")}
	}
	if _, err = c.net(); err != nil {
		return badFlags{err}
	}
	if err = validateNotifyChat(chat); err != nil {
		return badFlags{err}
	}
	if action == "init" && (from == 0 || from > uint64(1<<63-1)) {
		return badFlags{errors.New("init requires a positive -from-height within SQLite integer range")}
	}
	if action != "init" && from != 0 {
		return badFlags{errors.New("-from-height is only accepted by init; an existing route cannot be silently reset")}
	}
	if action == "run" && !send {
		return badFlags{errors.New("run requires -send; use preview to inspect messages without Telegram requests")}
	}
	if action != "run" && send {
		return badFlags{errors.New("-send is only accepted by run")}
	}
	var sender *telegram.Client
	if action == "run" {
		token, err := readTelegramToken(tokenFile)
		if err != nil {
			return err
		}
		sender, err = telegram.New(token)
		if err != nil {
			return errors.New("invalid Telegram token file contents")
		}
	}
	// Never accidentally create an empty collector DB because a path was wrong.
	if info, err := os.Stat(c.db); err != nil || !info.Mode().IsRegular() {
		return errors.New("notifications require an existing collector database")
	}
	st, err := c.openStore()
	if err != nil {
		return err
	}
	defer st.Close()
	switch action {
	case "init":
		if err := st.InitNotifications(ctx, c.network, chat, from); err != nil {
			return err
		}
		fmt.Fprintf(os.Stdout, "Notification route initialized for %s at block %d. No messages sent.\n", chat, from)
	case "preview":
		count, err := st.QueueNotifications(ctx, c.network, chat, 1000)
		if err != nil {
			return err
		}
		fmt.Fprintf(os.Stdout, "Queued %d notices from up to 1000 block heights. No messages sent.\n", count)
		rows, err := st.ListNotifications(ctx, c.network, chat)
		if err != nil {
			return err
		}
		for _, n := range rows {
			fmt.Fprintf(os.Stdout, "\n[id=%d state=%s height=%d message_id=%d]\n%s\n", n.ID, n.State, n.Height, n.MessageID, n.Text)
		}
	case "status":
		counts, err := st.NotificationCounts(ctx, c.network, chat)
		if err != nil {
			return err
		}
		for _, state := range []string{"pending", "sending", "sent", "failed", "uncertain"} {
			fmt.Fprintf(os.Stdout, "%s: %d\n", state, counts[state])
		}
		if counts["sending"]+counts["uncertain"] > 0 {
			fmt.Fprintln(os.Stdout, "sending/uncertain require reconciliation; they are not automatically retried.")
		}
	case "run":
		w := alerts.Worker{Store: st, Sender: sender, Network: c.network, Destination: chat}
		return w.Run(ctx)
	}
	return nil
}

func validateNotifyChat(chat string) error {
	id, err := strconv.ParseInt(chat, 10, 64)
	if err != nil || id >= 0 || strconv.FormatInt(id, 10) != chat {
		return errors.New("use a verified negative numeric Telegram chat ID; invite links and @usernames are not delivery destinations")
	}
	return nil
}

func readTelegramToken(path string) (string, error) {
	if path == "" {
		return "", errors.New("-token-file or GOVWATCH_TELEGRAM_TOKEN_FILE is required")
	}
	f, err := os.Open(path)
	if err != nil {
		return "", errors.New("cannot open Telegram token file")
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
		return "", errors.New("Telegram token must be in a regular file with no group/other permissions (chmod 600)")
	}
	data, err := io.ReadAll(io.LimitReader(f, 4097))
	if err != nil || len(data) > 4096 {
		return "", errors.New("cannot read Telegram token file or file is too large")
	}
	token := strings.TrimSpace(string(data))
	if token == "" {
		return "", errors.New("Telegram token file is empty")
	}
	return token, nil
}
