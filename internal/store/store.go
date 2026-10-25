// Package store persists decoded facts in SQLite: idempotent per block,
// with a cursor, a provisional/irreversible flag for reorg handling, and
// the live set of system contracts.
package store

import (
	"context"
	"database/sql"
	_ "embed"
	"fmt"
	"strings"

	_ "modernc.org/sqlite"
)

//go:embed schema.sql
var schema string

// schemaVersion is stored in PRAGMA user_version. 1 (or 0 with tables) keyed
// proposals by id alone; 2 keys them by (id, submitted_height); 3 adds
// event_index to system_changes, makes vote rows unique, and adds
// operations_known / vote_tally / vote_threshold to proposals; 4 removes
// event-derived rows left without an event index by the v3 migration once
// their block has been re-applied; 5 adds tx_index to system_changes;
// 6 adds the independent public notification routes and durable outbox.
const schemaVersion = 6

type Store struct{ db *sql.DB }

// Open opens or creates the database at path and applies the schema.
func Open(path string) (*Store, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	for _, pragma := range []string{"PRAGMA journal_mode = WAL", "PRAGMA foreign_keys = ON", "PRAGMA busy_timeout = 5000"} {
		if _, err := db.Exec(pragma); err != nil {
			db.Close()
			return nil, fmt.Errorf("store: %s: %w", pragma, err)
		}
	}
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("store: schema: %w", err)
	}
	s := &Store{db: db}
	if err := s.migrate(context.Background()); err != nil {
		db.Close()
		return nil, fmt.Errorf("store: migrate: %w", err)
	}
	return s, nil
}

// migrate upgrades older databases in place.
func (s *Store) migrate(ctx context.Context) error {
	var version int
	if err := s.db.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
		return err
	}
	if version >= schemaVersion {
		return nil
	}
	var pkCols int
	if err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM pragma_table_info('proposals') WHERE pk > 0").Scan(&pkCols); err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	run := func(qs ...string) error {
		for _, q := range qs {
			if _, err := tx.ExecContext(ctx, q); err != nil {
				return fmt.Errorf("%s: %w", q[:min(len(q), 60)], err)
			}
		}
		return nil
	}
	if pkCols == 1 { // v1: proposals keyed by id alone
		if err := run(`ALTER TABLE proposals RENAME TO proposals_v1`, schemaTable(schema, "proposals"),
			`INSERT INTO proposals (id, submitted_height, submitted_tx, payer, fee, operations, updates_governance, vote_start_height, status, last_height, irreversible) SELECT * FROM proposals_v1`,
			`DROP TABLE proposals_v1`); err != nil {
			return err
		}
	}
	if version < 3 {
		var hasEventIndex, hasKnown int
		_ = tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM pragma_table_info('system_changes') WHERE name = 'event_index'").Scan(&hasEventIndex)
		_ = tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM pragma_table_info('proposals') WHERE name = 'operations_known'").Scan(&hasKnown)
		if hasEventIndex == 0 {
			if err := run(`ALTER TABLE system_changes RENAME TO system_changes_v2`, schemaTable(schema, "system_changes"),
				`INSERT OR IGNORE INTO system_changes (height, block_id, tx_id, op_index, payer, kind, contract_id, call_id, target, system_flag, bytecode_hash, irreversible)
					SELECT height, block_id, tx_id, op_index, payer, kind, contract_id, call_id, target, system_flag, bytecode_hash, irreversible FROM system_changes_v2`,
				`DROP TABLE system_changes_v2`); err != nil {
				return err
			}
		}
		// Vote rows had a NULL status, which the unique key did not catch; rebuild deduplicated.
		if err := run(`ALTER TABLE proposal_events RENAME TO proposal_events_v2`, schemaTable(schema, "proposal_events"),
			`INSERT OR IGNORE INTO proposal_events (proposal_id, height, block_id, tx_id, kind, status, tally, threshold, irreversible)
				SELECT proposal_id, height, block_id, tx_id, kind, COALESCE(status, ''), tally, threshold, irreversible FROM proposal_events_v2`,
			`DROP TABLE proposal_events_v2`); err != nil {
			return err
		}
		if hasKnown == 0 {
			if err := run(`ALTER TABLE proposals ADD COLUMN operations_known INTEGER NOT NULL DEFAULT 1`,
				`ALTER TABLE proposals ADD COLUMN vote_tally INTEGER NOT NULL DEFAULT 0`,
				`ALTER TABLE proposals ADD COLUMN vote_threshold INTEGER NOT NULL DEFAULT 0`); err != nil {
				return err
			}
		}
	}
	if version < 4 {
		// Legacy event-derived rows (event_index -1) duplicate the indexed rows
		// written when their block was re-applied under v3; drop those, keep
		// the ones whose block has not been re-applied yet.
		if err := run(`DELETE FROM system_changes WHERE op_index = -1 AND event_index = -1 AND EXISTS (
			SELECT 1 FROM system_changes d WHERE d.height = system_changes.height AND d.tx_id = system_changes.tx_id
			AND d.kind = system_changes.kind AND d.contract_id IS system_changes.contract_id AND d.event_index >= 0)`); err != nil {
			return err
		}
	}
	if version < 5 {
		var hasTxIndex int
		_ = tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM pragma_table_info('system_changes') WHERE name = 'tx_index'").Scan(&hasTxIndex)
		if hasTxIndex == 0 {
			if err := run(`ALTER TABLE system_changes ADD COLUMN tx_index INTEGER NOT NULL DEFAULT -1`); err != nil {
				return err
			}
		}
	}
	if version < 5 {
		if err := recomputeProposals(ctx, tx, 0); err != nil {
			return err
		}
	}
	if err := run(schema, fmt.Sprintf("PRAGMA user_version = %d", schemaVersion)); err != nil { // re-create indexes on rebuilt tables
		return err
	}
	return tx.Commit()
}

// schemaTable returns the CREATE TABLE statement for name from the embedded schema.
func schemaTable(schema, name string) string {
	marker := "CREATE TABLE IF NOT EXISTS " + name + " ("
	start := strings.Index(schema, marker)
	end := strings.Index(schema[start:], ");")
	return schema[start : start+end+2]
}

func (s *Store) Close() error { return s.db.Close() }

type Cursor struct {
	Height  uint64
	BlockID []byte
}

// Cursor returns the last applied block, or the zero value when empty.
func (s *Store) Cursor(ctx context.Context) (Cursor, error) {
	var c Cursor
	err := s.db.QueryRowContext(ctx, "SELECT height, block_id FROM cursor WHERE id = 1").Scan(&c.Height, &c.BlockID)
	if err == sql.ErrNoRows {
		return Cursor{}, nil
	}
	return c, err
}

// Irreversible returns the highest height known to be irreversible (0 when none).
func (s *Store) Irreversible(ctx context.Context) (uint64, error) {
	var w uint64
	err := s.db.QueryRowContext(ctx, "SELECT irreversible FROM watermark WHERE id = 1").Scan(&w)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	return w, err
}

func raiseWatermark(ctx context.Context, tx *sql.Tx, height uint64) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO watermark (id, irreversible) VALUES (1, ?) ON CONFLICT (id) DO UPDATE SET irreversible = MAX(irreversible, excluded.irreversible)`, height)
	return err
}

// setCursor records the last applied block; Apply never moves it backwards,
// so an older block can be re-applied without disturbing the live position.
func setCursor(ctx context.Context, tx *sql.Tx, height uint64, blockID []byte) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO cursor (id, height, block_id) VALUES (1, ?, ?)
		ON CONFLICT (id) DO UPDATE SET height = excluded.height, block_id = excluded.block_id WHERE excluded.height >= cursor.height`, height, blockID)
	return err
}

// forceCursor moves the cursor anywhere (used by Rollback).
func forceCursor(ctx context.Context, tx *sql.Tx, height uint64, blockID []byte) error {
	_, err := tx.ExecContext(ctx, "INSERT INTO cursor (id, height, block_id) VALUES (1, ?, ?) ON CONFLICT (id) DO UPDATE SET height = excluded.height, block_id = excluded.block_id", height, blockID)
	return err
}
