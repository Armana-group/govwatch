// Command govwatch watches Koinos governance proposals and system-contract
// changes from a node's message broker. Read-only: it holds no keys and
// never submits anything.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"govwatch/internal/amqprpc"
	"govwatch/internal/decode"
	"govwatch/internal/node"
	"govwatch/internal/store"
)

const usage = `usage: govwatch <command> [flags]

commands:
  replay   walk irreversible blocks from the node's block store into the database
  run      follow the node's block broadcasts (catches up first)
  status   print cursor, counts, proposals and system contracts
  version  print the build commit

common flags (env fallback in parentheses):
  -amqp     broker URL      (GOVWATCH_AMQP, default amqp://guest:guest@127.0.0.1:5672/)
  -network  mainnet|harbinger (GOVWATCH_NETWORK, default mainnet)
  -db       sqlite path     (GOVWATCH_DB, default govwatch.db)
`

const rpcTimeout = 30 * time.Second

// buildCommit is set at build time: -ldflags "-X main.buildCommit=<git sha>".
var buildCommit = "dev"

func writeVersion(w io.Writer) { fmt.Fprintln(w, buildCommit) }

type common struct {
	fs      *flag.FlagSet
	amqp    string
	network string
	db      string
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// parseCommon builds the flag set for cmd, registers the shared flags with
// environment fallbacks, and parses args. Callers add their own flags through
// extra before parsing.
func parseCommon(cmd string, args []string, extra ...func(*flag.FlagSet)) (*common, error) {
	c := &common{fs: flag.NewFlagSet("govwatch "+cmd, flag.ContinueOnError)}
	c.fs.StringVar(&c.amqp, "amqp", envOr("GOVWATCH_AMQP", "amqp://guest:guest@127.0.0.1:5672/"), "AMQP broker URL")
	c.fs.StringVar(&c.network, "network", envOr("GOVWATCH_NETWORK", "mainnet"), "mainnet or harbinger")
	c.fs.StringVar(&c.db, "db", envOr("GOVWATCH_DB", "govwatch.db"), "SQLite database path")
	for _, e := range extra {
		e(c.fs)
	}
	if err := c.fs.Parse(args); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *common) net() (decode.Network, error) { return decode.NetworkByName(c.network) }

func (c *common) openStore() (*store.Store, error) { return store.Open(c.db) }

// openSource connects to the node over the broker's RPC exchange and checks
// the chain id matches the chosen network.
func (c *common) openSource(n decode.Network) *node.Client {
	return node.New(amqprpc.New(c.amqp, rpcTimeout), n.ChainID)
}

func main() {
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, nil)))
	if len(args) == 0 {
		fmt.Fprint(os.Stderr, usage)
		return 2
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	var err error
	switch args[0] {
	case "replay":
		err = cmdReplay(ctx, args[1:])
	case "run":
		err = cmdRun(ctx, args[1:])
	case "status":
		err = cmdStatus(ctx, args[1:])
	case "version":
		writeVersion(os.Stdout)
		return 0
	case "help", "-h", "--help":
		fmt.Fprint(os.Stdout, usage)
		return 0
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n%s", args[0], usage)
		return 2
	}
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		var bad badFlags
		if errors.As(err, &bad) {
			fmt.Fprintln(os.Stderr, err)
			return 2
		}
		slog.Error("govwatch failed", "err", err)
		return 1
	}
	return 0
}

type badFlags struct{ err error }

func (b badFlags) Error() string { return b.err.Error() }
func (b badFlags) Unwrap() error { return b.err }
