package store

import "context"

type ProposalRow struct {
	ID                []byte
	SubmittedHeight   uint64
	SubmittedTx       []byte
	Payer             []byte
	Fee               uint64
	Status            string
	UpdatesGovernance bool
	VoteStartHeight   uint64
	LastHeight        uint64
	OperationsKnown   bool   // false when submitted through another contract
	VoteTally         uint64 // highest tally reported by vote events
	VoteThreshold     uint64 // threshold reported by vote events, 0 before voting
}

// Proposals lists every known proposal, oldest first.
func (s *Store) Proposals(ctx context.Context) ([]ProposalRow, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id, submitted_height, submitted_tx, payer, fee, status, updates_governance, vote_start_height, last_height, operations_known, vote_tally, vote_threshold FROM proposals ORDER BY submitted_height`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ProposalRow
	for rows.Next() {
		var r ProposalRow
		var fee, ug, known, tally, threshold int64
		if err := rows.Scan(&r.ID, &r.SubmittedHeight, &r.SubmittedTx, &r.Payer, &fee, &r.Status, &ug, &r.VoteStartHeight, &r.LastHeight, &known, &tally, &threshold); err != nil {
			return nil, err
		}
		r.Fee = uint64(fee)
		r.UpdatesGovernance = ug != 0
		r.OperationsKnown = known != 0
		r.VoteTally = uint64(tally)
		r.VoteThreshold = uint64(threshold)
		out = append(out, r)
	}
	return out, rows.Err()
}

type SystemContractRow struct {
	ContractID  []byte
	System      bool
	SinceHeight uint64
}

// SystemContracts lists the current system flag of every contract that
// ever had one set.
func (s *Store) SystemContracts(ctx context.Context) ([]SystemContractRow, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT contract_id, system, since_height FROM system_contracts ORDER BY since_height`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []SystemContractRow
	for rows.Next() {
		var r SystemContractRow
		var sys int64
		if err := rows.Scan(&r.ContractID, &sys, &r.SinceHeight); err != nil {
			return nil, err
		}
		r.System = sys != 0
		out = append(out, r)
	}
	return out, rows.Err()
}

var countedTables = []string{"proposals", "proposal_events", "system_changes", "system_contracts"}

// Counts returns row counts per table.
func (s *Store) Counts(ctx context.Context) (map[string]int64, error) {
	out := make(map[string]int64, len(countedTables))
	for _, t := range countedTables {
		var n int64
		if err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+t).Scan(&n); err != nil {
			return nil, err
		}
		out[t] = n
	}
	return out, nil
}
