# Govwatch

Govwatch watches Koinos governance proposals and system-contract changes. It is the observation and history layer of Blockchain Overwatch: a read-only Go service that runs beside a Koinos node, decodes governance activity, and records it in SQLite.

The purpose is to make changes to the chain's own contracts visible and auditable. Governance review periods are useful only when people can see and examine the proposed changes.

## Current capabilities

- Replay historical irreversible blocks with their receipts through the node's AMQP RPC interface.
- Follow live block broadcasts, recover gaps, and roll back provisional observations after reorganizations.
- Record proposal submissions, status changes, votes, and application receipts.
- Track contract uploads, system-call mappings, and system-contract flags.
- Keep separate records when identical proposal operations are resubmitted.
- Report proposals and system contracts through the `status` command.

Risk scoring, Slack/email notifications, heartbeat monitoring, and a web dashboard are not implemented yet. The watcher holds no signing keys and does not submit transactions.

## Build and run

Use the Go version pinned in `.mise.toml` (1.25.14). The SQLite driver is pure Go and does not require cgo.

```sh
mise install
make test vet build
./bin/govwatch replay -network mainnet -db mainnet.db
./bin/govwatch run -network mainnet -db mainnet.db
./bin/govwatch status -db mainnet.db
```

The replay and live commands need access to a Koinos node's RabbitMQ broker. Configure access for your own node; do not expose its broker publicly. The default endpoint is local (`127.0.0.1:5672`).

| Setting | Flag | Environment | Default |
|---|---|---|---|
| Broker URL | `-amqp` | `GOVWATCH_AMQP` | local RabbitMQ guest account |
| Network | `-network` | `GOVWATCH_NETWORK` | `mainnet` |
| SQLite path | `-db` | `GOVWATCH_DB` | `govwatch.db` |

`harbinger` is also a supported network selection. Use a separate database per network. Replay starts at the saved cursor plus one and stops at the node's last irreversible block. Initial mainnet replay can take hours. Avoid `-from` unless deliberately rebuilding a historical range: starting beyond the cursor can leave gaps.

To embed a build identity:

```sh
GOTOOLCHAIN=local mise exec -- go build -trimpath \
  -ldflags "-X main.buildCommit=$(git rev-parse HEAD)" \
  -o bin/govwatch ./cmd/govwatch
./bin/govwatch version
```

## Architecture

`node / amqprpc → replay or live → decode → store → status`

Decoding is independent of network and database access. SQLite stores facts and derived proposal/system-contract state. Writes and cursor advancement are transactional. Live observations remain provisional until marked irreversible. One exclusive consumer is allowed per network queue on a broker.

## Verified research findings

The decoder uses the official Koinos protobuf definitions. Mainnet governance is seeded as `19qj51eTbSFJYU7ZagudkpxPgNSzPMfdPX`; Harbinger governance is `17MjUXDCuTX1p9Kyqy48SQkkPfKScoggo`. Network constants are in `internal/decode/network.go`.

Proposal IDs identify the operation merkle root and can recur across submissions. Status and vote events can occur at block level. Governance application receipts can exist without a matching transaction in the block body. These cases are represented explicitly in the decoder and tests.

## Validation and limitations

An earlier mainnet replay covered blocks 1 through 40,487,182. Its recorded acceptance evidence included interruption/resumption, selected replay comparisons, account-history and contract-metadata cross-checks, and a 30-minute live soak with a connection interruption. Those are historical observations, not proof that a current deployment is healthy.

Tests cover decoding, source validation, migrations, replay, reorgs, feed cancellation, and rollback reconstruction. Run `make test vet` for the current checkout. There is no CI workflow in this initial snapshot.

Submissions made through another contract can have unknown operations. Code-upload history does not yet provide old-bytecode diffs. Alerts and an external heartbeat are required before this can serve as an unattended warning service. Keep local databases, credentials, captures, and operator-specific configuration outside version control.
