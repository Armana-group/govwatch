package store

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"strconv"
	"time"
	"unicode/utf8"

	"github.com/koinos/koinos-proto-golang/v2/koinos/contracts/governance"
	"github.com/koinos/koinos-proto-golang/v2/koinos/protocol"
	"google.golang.org/protobuf/proto"
	"govwatch/internal/b58"
)

// Notification is a durable public notice. A sending row is deliberately never
// reclaimed automatically: a crash may have happened after Telegram accepted it.
type Notification struct {
	ID                                int64
	Network, Destination, Text, State string
	Height                            uint64
	MessageID                         int64
}

func validateNotificationRoute(network, destination string) error {
	if network != "mainnet" && network != "harbinger" {
		return fmt.Errorf("notifications: unsupported network")
	}
	id, err := strconv.ParseInt(destination, 10, 64)
	if err != nil || id >= 0 || strconv.FormatInt(id, 10) != destination {
		return fmt.Errorf("notifications: destination must be a canonical negative numeric chat ID")
	}
	return nil
}

// InitNotifications explicitly activates a route without resetting an existing
// cursor. The network pin prevents relabeling one collector history as another.
func (s *Store) InitNotifications(ctx context.Context, network, destination string, fromHeight uint64) error {
	if err := validateNotificationRoute(network, destination); err != nil {
		return err
	}
	if fromHeight == 0 || fromHeight >= math.MaxInt64 {
		return fmt.Errorf("notifications: from height must be positive and below MaxInt64")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `INSERT INTO notification_network(id,network) VALUES(1,?) ON CONFLICT(id) DO NOTHING`, network); err != nil {
		return err
	}
	var saved string
	if err = tx.QueryRowContext(ctx, `SELECT network FROM notification_network WHERE id=1`).Scan(&saved); err != nil {
		return err
	}
	if saved != network {
		return fmt.Errorf("notifications: database notification network differs from requested network")
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO notification_routes(network,destination,from_height,next_height) VALUES(?,?,?,?) ON CONFLICT(network,destination) DO NOTHING`, network, destination, fromHeight, fromHeight); err != nil {
		return err
	}
	var from uint64
	if err = tx.QueryRowContext(ctx, `SELECT from_height FROM notification_routes WHERE network=? AND destination=?`, network, destination).Scan(&from); err != nil {
		return err
	}
	if from != fromHeight {
		return fmt.Errorf("notifications: route already initialized with a different from height")
	}
	return tx.Commit()
}

// QueueNotifications atomically snapshots confirmed facts, inserts notices and
// advances the separate route cursor. A watermark alone is not finality proof:
// provisional rows in the range hold the entire range until flagging completes.
func (s *Store) QueueNotifications(ctx context.Context, network, destination string, maxHeights uint64) (int, error) {
	if err := validateNotificationRoute(network, destination); err != nil {
		return 0, err
	}
	if maxHeights == 0 {
		return 0, fmt.Errorf("notifications: max heights must be positive")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	// Acquire the write reservation before reading, also serializing independent
	// notifier connections without a deferred-transaction lock upgrade race.
	result, err := tx.ExecContext(ctx, `UPDATE notification_routes SET next_height=next_height WHERE network=? AND destination=?`, network, destination)
	if err != nil {
		return 0, err
	}
	if n, _ := result.RowsAffected(); n != 1 {
		return 0, fmt.Errorf("notifications: route is not initialized")
	}
	var next, end uint64
	if err = tx.QueryRowContext(ctx, `SELECT next_height FROM notification_routes WHERE network=? AND destination=?`, network, destination).Scan(&next); err != nil {
		return 0, err
	}
	if err = tx.QueryRowContext(ctx, `SELECT MIN(COALESCE((SELECT height FROM cursor WHERE id=1),0),COALESCE((SELECT irreversible FROM watermark WHERE id=1),0))`).Scan(&end); err != nil {
		return 0, err
	}
	if next > end {
		return 0, nil
	}
	if end-next >= maxHeights {
		end = next + maxHeights - 1
	}
	if end >= math.MaxInt64 {
		return 0, fmt.Errorf("notifications: height cannot advance beyond SQLite integer range")
	}
	var provisional bool
	if err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM proposal_events WHERE irreversible=0 AND height BETWEEN ?1 AND ?2 UNION ALL SELECT 1 FROM system_changes WHERE irreversible=0 AND height BETWEEN ?1 AND ?2 UNION ALL SELECT 1 FROM proposals WHERE irreversible=0 AND submitted_height BETWEEN ?1 AND ?2)`, next, end).Scan(&provisional); err != nil {
		return 0, err
	}
	if provisional {
		return 0, nil
	}
	notices, err := proposalNotices(ctx, tx, network, next, end)
	if err != nil {
		return 0, err
	}
	system, err := systemNotices(ctx, tx, network, next, end)
	if err != nil {
		return 0, err
	}
	notices = append(notices, system...)
	count := 0
	for _, n := range notices {
		result, err = tx.ExecContext(ctx, `INSERT INTO notification_outbox(network,destination,height,block_id,event_identity,text) VALUES(?,?,?,?,?,?) ON CONFLICT(network,destination,block_id,event_identity) DO NOTHING`, network, destination, n.height, n.block, n.identity, n.text)
		if err != nil {
			return 0, err
		}
		added, err := result.RowsAffected()
		if err != nil {
			return 0, err
		}
		count += int(added)
	}
	if _, err = tx.ExecContext(ctx, `UPDATE notification_routes SET next_height=? WHERE network=? AND destination=?`, end+1, network, destination); err != nil {
		return 0, err
	}
	if err = tx.Commit(); err != nil {
		return 0, err
	}
	return count, nil
}

type noticeDraft struct {
	height         uint64
	block          []byte
	identity, text string
}

func noticeHeader(network string, height uint64, block []byte) string {
	return fmt.Sprintf("Koinos %s | irreversible block %d\nBlock ID: %s", network, height, b58.Encode(block))
}

func proposalNotices(ctx context.Context, tx *sql.Tx, network string, start, end uint64) ([]noticeDraft, error) {
	rows, err := tx.QueryContext(ctx, `SELECT e.proposal_id,e.height,e.block_id,e.status,COALESCE(p.operations_known,0),p.operations
 FROM proposal_events e LEFT JOIN proposals p ON p.id=e.proposal_id AND p.submitted_height=(SELECT MAX(p2.submitted_height) FROM proposals p2 WHERE p2.id=e.proposal_id AND p2.submitted_height<=e.height) AND p.irreversible=1
 WHERE e.kind='proposal_status' AND e.irreversible=1 AND e.height BETWEEN ? AND ? ORDER BY e.height,e.proposal_id,e.status`, start, end)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []noticeDraft
	for rows.Next() {
		var id, block, operations []byte
		var height uint64
		var status string
		var known bool
		if err = rows.Scan(&id, &height, &block, &status, &known, &operations); err != nil {
			return nil, err
		}
		header := noticeHeader(network, height, block) + "\nProposal: " + b58.Encode(id) + "\nStatus: " + status
		lines := []string{"Operations: unknown (submission operations unavailable)"}
		if known {
			lines = []string{"Operations: known"}
			if status == "pending" {
				var args governance.SubmitProposalArguments
				if err = proto.Unmarshal(operations, &args); err != nil {
					return nil, fmt.Errorf("notifications: decode stored proposal operations: %w", err)
				}
				lines[0] = fmt.Sprintf("Operations: known (%d)", len(args.Operations))
				for _, op := range args.Operations {
					lines = append(lines, operationNotice(op))
				}
			}
		}
		out = append(out, noticeDraft{height, block, fmt.Sprintf("proposal_status:%x:%d:%s", id, height, status), boundedNotice(header, lines, "details")})
	}
	return out, rows.Err()
}
func operationNotice(op *protocol.Operation) string {
	switch {
	case op.GetUploadContract() != nil:
		return "upload_contract: " + b58.Encode(op.GetUploadContract().ContractId)
	case op.GetSetSystemContract() != nil:
		v := op.GetSetSystemContract()
		return fmt.Sprintf("set_system_contract: %s = %t", b58.Encode(v.ContractId), v.SystemContract)
	case op.GetSetSystemCall() != nil:
		v := op.GetSetSystemCall()
		return fmt.Sprintf("set_system_call: %d -> %s", v.CallId, targetNotice(v.Target))
	case op.GetCallContract() != nil:
		v := op.GetCallContract()
		return fmt.Sprintf("call_contract: %s entry 0x%08x", b58.Encode(v.ContractId), v.EntryPoint)
	default:
		return "operation: unrecognized type"
	}
}
func targetNotice(target *protocol.SystemCallTarget) string {
	if target == nil {
		return "unknown target"
	}
	switch v := target.Target.(type) {
	case *protocol.SystemCallTarget_ThunkId:
		return fmt.Sprintf("thunk %d", v.ThunkId)
	case *protocol.SystemCallTarget_SystemCallBundle:
		if v.SystemCallBundle != nil {
			return fmt.Sprintf("%s entry 0x%08x", b58.Encode(v.SystemCallBundle.ContractId), v.SystemCallBundle.EntryPoint)
		}
	}
	return "unknown target"
}

func systemNotices(ctx context.Context, tx *sql.Tx, network string, start, end uint64) ([]noticeDraft, error) {
	// Before-block membership is the last historical flag, not the current
	// system_contracts table. Any true flag during this block also qualifies.
	rows, err := tx.QueryContext(ctx, `SELECT c.height,c.block_id,c.kind,c.contract_id,c.call_id,c.target,c.system_flag FROM system_changes c
 WHERE c.irreversible=1 AND c.height BETWEEN ?1 AND ?2 AND (c.kind IN ('set_system_contract','set_system_call') OR (c.kind='upload_contract' AND (
 COALESCE((SELECT h.system_flag FROM system_changes h WHERE h.kind='set_system_contract' AND h.irreversible=1 AND h.contract_id=c.contract_id AND h.height<c.height ORDER BY h.height DESC,h.tx_index DESC,h.event_index DESC,h.op_index DESC LIMIT 1),0)=1
 OR EXISTS(SELECT 1 FROM system_changes h WHERE h.kind='set_system_contract' AND h.irreversible=1 AND h.contract_id=c.contract_id AND h.height=c.height AND h.block_id=c.block_id AND h.system_flag=1))))
 ORDER BY c.height,c.block_id,c.tx_index,c.event_index,c.op_index,c.kind`, start, end)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []noticeDraft
	var lines []string
	var current noticeDraft
	flush := func() {
		if len(lines) > 0 {
			current.text = boundedNotice(noticeHeader(network, current.height, current.block)+"\nSystem changes", lines, "changes")
			out = append(out, current)
			lines = nil
		}
	}
	for rows.Next() {
		var height uint64
		var block, contract, target []byte
		var kind string
		var call, flag sql.NullInt64
		if err = rows.Scan(&height, &block, &kind, &contract, &call, &target, &flag); err != nil {
			return nil, err
		}
		if height != current.height || string(block) != string(current.block) {
			flush()
			current = noticeDraft{height: height, block: block, identity: "system_changes"}
		}
		switch kind {
		case "set_system_contract":
			lines = append(lines, fmt.Sprintf("set_system_contract: %s = %t", b58.Encode(contract), flag.Int64 != 0))
		case "set_system_call":
			var decoded protocol.SystemCallTarget
			if err = proto.Unmarshal(target, &decoded); err != nil {
				return nil, fmt.Errorf("notifications: decode system call target: %w", err)
			}
			lines = append(lines, fmt.Sprintf("set_system_call: %d -> %s", call.Int64, targetNotice(&decoded)))
		case "upload_contract":
			lines = append(lines, "upload_contract: "+b58.Encode(contract)+" (system before or during block)")
		}
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	flush()
	return out, nil
}

// Keep evidence identifiers and complete lines, reserving space for an explicit
// omitted count; the complete decoded facts remain in the source tables.
func boundedNotice(header string, lines []string, noun string) string {
	out := header
	for i, line := range lines {
		if utf8.RuneCountInString(out)+utf8.RuneCountInString(line)+1 > 3400 {
			return out + fmt.Sprintf("\n%d more %s omitted; complete evidence retained in collector.", len(lines)-i, noun)
		}
		out += "\n" + line
	}
	return out
}

// ClaimNotification is a single atomic state change, including across separate
// Store connections. sending rows survive restart and are never eligible again.
func (s *Store) ClaimNotification(ctx context.Context, network, destination string, now time.Time) (*Notification, error) {
	if err := validateNotificationRoute(network, destination); err != nil {
		return nil, err
	}
	n := new(Notification)
	err := s.db.QueryRowContext(ctx, `UPDATE notification_outbox SET state='sending' WHERE id=(SELECT id FROM notification_outbox WHERE network=?1 AND destination=?2 AND state='pending' AND retry_at<=?3 AND EXISTS (SELECT 1 FROM notification_routes r WHERE r.network=?1 AND r.destination=?2 AND r.cooldown_until<=?3) ORDER BY height,id LIMIT 1) AND state='pending' RETURNING id,network,destination,text,state,height,message_id`, network, destination, now.UnixMilli()).Scan(&n.ID, &n.Network, &n.Destination, &n.Text, &n.State, &n.Height, &n.MessageID)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return n, nil
}

// FinishNotification only completes a current claim. pending is reserved for
// explicitly safe retries and extends the durable cooldown for the whole route;
// uncertain and failed are terminal operator evidence.
func (s *Store) FinishNotification(ctx context.Context, id int64, state string, messageID int64, retryAt time.Time) error {
	switch state {
	case "sent":
		if messageID <= 0 {
			return fmt.Errorf("notifications: sent requires a positive message ID")
		}
	case "pending", "failed", "uncertain":
		if messageID != 0 {
			return fmt.Errorf("notifications: non-sent result must not have a message ID")
		}
	default:
		return fmt.Errorf("notifications: invalid finish state")
	}
	retry := int64(0)
	if !retryAt.IsZero() {
		retry = retryAt.UnixMilli()
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `UPDATE notification_outbox SET state=?,message_id=?,retry_at=? WHERE id=? AND state='sending'`, state, messageID, retry, id)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return fmt.Errorf("notifications: row is not currently sending")
	}
	if state == "pending" {
		if _, err = tx.ExecContext(ctx, `UPDATE notification_routes SET cooldown_until=MAX(cooldown_until,?1) WHERE (network,destination)=(SELECT network,destination FROM notification_outbox WHERE id=?2)`, retry, id); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) NotificationCounts(ctx context.Context, network, destination string) (map[string]int64, error) {
	if err := validateNotificationRoute(network, destination); err != nil {
		return nil, err
	}
	out := map[string]int64{"pending": 0, "sending": 0, "sent": 0, "failed": 0, "uncertain": 0}
	rows, err := s.db.QueryContext(ctx, `SELECT state,COUNT(*) FROM notification_outbox WHERE network=? AND destination=? GROUP BY state`, network, destination)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var state string
		var count int64
		if err = rows.Scan(&state, &count); err != nil {
			return nil, err
		}
		out[state] = count
	}
	return out, rows.Err()
}

// ListNotifications returns at most the latest 100 notices, newest first.
func (s *Store) ListNotifications(ctx context.Context, network, destination string) ([]Notification, error) {
	if err := validateNotificationRoute(network, destination); err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT id,network,destination,text,state,height,message_id FROM notification_outbox WHERE network=? AND destination=? ORDER BY id DESC LIMIT 100`, network, destination)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Notification
	for rows.Next() {
		var n Notification
		if err = rows.Scan(&n.ID, &n.Network, &n.Destination, &n.Text, &n.State, &n.Height, &n.MessageID); err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, rows.Err()
}
