# Public Telegram notifications

Govwatch can deliver factual notices to a verified Telegram channel or group. The notifier is a separate process from `govwatch run`: collection continues independently of Telegram availability.

The first version sends proposal status events (including new submissions), system-contract flag changes, system-call mappings, and uploads to contracts that were system contracts before or during the block. It waits for irreversible stored facts, omits per-block votes, and explicitly labels unavailable proposal operations. It does not provide vulnerability assessments, confidential security findings, email/Slack delivery, or external heartbeat monitoring.

## Set up the bot and destination

1. Create a bot with Telegram's BotFather and add it to the intended chat. For broadcast channels, grant permission to post. For groups, allow sending messages.
2. Save its token in a regular private file with no group/other permissions (`chmod 600`). Never put the token on a command line, in a commit, or in logs. `GOVWATCH_TELEGRAM_TOKEN_FILE` contains the file path, not the token.
3. Verify the bot identity and the numeric chat ID using Telegram's read-only APIs. An invite link is not a chat ID. Use the negative numeric ID rather than an alias so future username changes cannot redirect alerts. If the community has separate discussion groups and broadcast channels, verify each destination explicitly. Forum-topic routing is not implemented in this version.
4. Verify the database belongs to the intended network, is caught up, and has no intentional replay gaps. Choose an explicit first block for notifications, normally the observed chain head plus one. There is no automatic historical announcement sweep.

The commands below are examples. Replace the chat ID, height, and paths with verified values. They do not configure or deploy a service automatically.

```sh
# Collector remains running independently.
# Initialize this exact network/destination once, with a reviewed starting height.
govwatch notify init -network mainnet -db /path/to/mainnet.db \
  -chat-id=-1001234567890 -from-height=12345678

# Build a bounded batch of messages in SQLite and inspect their text; no HTTP.
govwatch notify preview -network mainnet -db /path/to/mainnet.db \
  -chat-id=-1001234567890

# Inspect local delivery state; no HTTP.
govwatch notify status -network mainnet -db /path/to/mainnet.db \
  -chat-id=-1001234567890

# Only this explicit command enables delivery to the configured destination.
govwatch notify run -send -network mainnet -db /path/to/mainnet.db \
  -chat-id=-1001234567890 -token-file=/private/path/telegram-token
```

Every destination has an independent activation height, cursor, and delivery state. Initialize and run a separate notifier for each destination. Changing an existing route's activation height is rejected; use its saved cursor when restarting. The network selection is pinned for notification routes in that database. That pin prevents later relabeling; the operator must still verify the original collector database's network.

`preview` queues at most 1,000 block heights per invocation and shows the latest 100 outbox records with states. It changes local outbox state but never sends. It may show nothing when finality has not reached the activation height. `run` continues queueing and sends at most one message every two seconds per process. Do not run multiple workers for one destination; atomic claims prevent duplicate claims but do not coordinate aggregate rate limits across processes.

## Failure and recovery

- Facts must be stored and irreversible before the notification cursor advances. Queue insertion and that cursor advance are one transaction. Reapplying history does not enqueue duplicate messages; repeated proposal submissions at different heights remain separate.
- A durable `sending` claim is written before HTTP. Successful delivery records Telegram's message ID. Explicit rate limiting saves a destination-wide cooldown using Telegram's retry delay, measured from response arrival; it survives process restarts.
- A timeout, malformed response, server failure, or interrupted request can leave delivery `uncertain`. A crash can leave `sending`. Neither is automatically retried because Telegram might already have posted it.
- A definite rejection becomes `failed`. Inspect local status and check the bot's permissions/configuration. There is currently no automatic requeue or operator resolution command: reconcile against the destination history and preserve evidence before a reviewed database repair. Do not delete/reinitialize routes to try again; that can create duplicates.
- Startup replay and notification polling rely on the collector's existing contiguous-history assumption. Starting `replay -from` beyond its cursor can create gaps; do not use that to initialize a live notification source.
- Notifications cannot report the failure of their own process. A separate heartbeat monitor remains a later slice.

The schema upgrade adds notification tables and indexes without rebuilding the existing vote history. No token is stored in SQLite. Transport errors deliberately omit raw request URLs and provider response bodies because Bot API URLs contain the credential.

## Validation boundary

Local tests use disposable SQLite databases and an HTTP test server. They cover finality gating, historical suppression, multiple routes, replay deduplication, resubmissions, reorgs, atomic writes, concurrent claims, rate limiting, ambiguous outcomes, credential-file permissions, and explicit send opt-in. These checks do not prove live bot permissions, destination identity, deployment, or human acceptance.
