CREATE TABLE IF NOT EXISTS cursor (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  height INTEGER NOT NULL,
  block_id BLOB NOT NULL
);

CREATE TABLE IF NOT EXISTS proposals (
  id BLOB NOT NULL,                 -- operation merkle root; recurs when identical operations are resubmitted
  submitted_height INTEGER NOT NULL,
  submitted_tx BLOB NOT NULL,
  payer BLOB NOT NULL,
  fee INTEGER NOT NULL,
  operations BLOB NOT NULL,         -- marshalled SubmitProposalArguments; empty when operations_known = 0
  updates_governance INTEGER NOT NULL,
  vote_start_height INTEGER NOT NULL,
  status TEXT NOT NULL,
  last_height INTEGER NOT NULL,
  irreversible INTEGER NOT NULL,
  operations_known INTEGER NOT NULL DEFAULT 1,   -- 0 when submitted through another contract
  vote_tally INTEGER NOT NULL DEFAULT 0,         -- highest tally seen in vote events
  vote_threshold INTEGER NOT NULL DEFAULT 0,     -- threshold reported by vote events
  PRIMARY KEY (id, submitted_height)
);
CREATE INDEX IF NOT EXISTS proposals_provisional ON proposals (submitted_height) WHERE irreversible = 0;

CREATE TABLE IF NOT EXISTS proposal_events (
  proposal_id BLOB NOT NULL,
  height INTEGER NOT NULL,
  block_id BLOB NOT NULL,
  tx_id BLOB,
  kind TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT '',  -- '' for votes; NULL would defeat the unique key
  tally INTEGER,
  threshold INTEGER,
  irreversible INTEGER NOT NULL,
  UNIQUE (proposal_id, height, kind, status)
);
CREATE INDEX IF NOT EXISTS proposal_events_height ON proposal_events (height);
CREATE INDEX IF NOT EXISTS proposal_events_proposal ON proposal_events (proposal_id, height);
CREATE INDEX IF NOT EXISTS proposal_events_provisional ON proposal_events (height) WHERE irreversible = 0;

CREATE TABLE IF NOT EXISTS system_changes (
  height INTEGER NOT NULL,
  block_id BLOB NOT NULL,
  tx_id BLOB NOT NULL,              -- empty for block-level events
  tx_index INTEGER NOT NULL DEFAULT -1, -- position of the transaction (receipt) in the block, -1 for block-level
  op_index INTEGER NOT NULL,        -- -1 when derived from an event
  event_index INTEGER NOT NULL DEFAULT -1, -- -1 when derived from an operation
  payer BLOB,
  kind TEXT NOT NULL,
  contract_id BLOB,
  call_id INTEGER,
  target BLOB,
  system_flag INTEGER,
  bytecode_hash BLOB,
  irreversible INTEGER NOT NULL,
  UNIQUE (height, tx_id, op_index, event_index, kind)
);
CREATE INDEX IF NOT EXISTS system_changes_height ON system_changes (height);
CREATE INDEX IF NOT EXISTS system_changes_provisional ON system_changes (height) WHERE irreversible = 0;

CREATE TABLE IF NOT EXISTS system_contracts (
  contract_id BLOB PRIMARY KEY,
  system INTEGER NOT NULL,
  since_height INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS watermark (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  irreversible INTEGER NOT NULL      -- highest height known to be irreversible
);
