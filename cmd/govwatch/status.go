package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"sort"
	"text/tabwriter"

	"govwatch/internal/b58"
	"govwatch/internal/store"
)

func cmdStatus(ctx context.Context, args []string) error {
	c, err := parseCommon("status", args)
	if err != nil {
		return badFlags{err}
	}
	st, err := c.openStore()
	if err != nil {
		return err
	}
	defer st.Close()
	return writeStatus(ctx, st, os.Stdout)
}

func writeStatus(ctx context.Context, st *store.Store, w io.Writer) error {
	cur, err := st.Cursor(ctx)
	if err != nil {
		return err
	}
	fmt.Fprintf(w, "cursor: %d", cur.Height)
	if len(cur.BlockID) > 0 {
		fmt.Fprintf(w, " (%x)", cur.BlockID)
	}
	fmt.Fprintln(w)
	counts, err := st.Counts(ctx)
	if err != nil {
		return err
	}
	keys := make([]string, 0, len(counts))
	for k := range counts {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Fprintf(w, "%s: %d\n", k, counts[k])
	}
	props, err := st.Proposals(ctx)
	if err != nil {
		return err
	}
	if len(props) > 0 {
		fmt.Fprintln(w, "\nproposals:")
		tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
		fmt.Fprintln(tw, "id\tstatus\tsubmitted\tvote_start\tvotes\tthreshold\tupdates_governance\tops\tpayer\tfee")
		for _, p := range props {
			ops := "known"
			if !p.OperationsKnown {
				ops = "UNKNOWN (via contract)"
			}
			fmt.Fprintf(tw, "%s\t%s\t%d\t%d\t%d\t%d\t%v\t%s\t%s\t%d\n", b58.Encode(p.ID), p.Status, p.SubmittedHeight, p.VoteStartHeight, p.VoteTally, p.VoteThreshold, p.UpdatesGovernance, ops, b58.Encode(p.Payer), p.Fee)
		}
		tw.Flush()
	}
	sys, err := st.SystemContracts(ctx)
	if err != nil {
		return err
	}
	if len(sys) > 0 {
		fmt.Fprintln(w, "\nsystem contracts:")
		tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
		fmt.Fprintln(tw, "contract\tsystem\tsince")
		for _, s := range sys {
			fmt.Fprintf(tw, "%s\t%v\t%d\n", b58.Encode(s.ContractID), s.System, s.SinceHeight)
		}
		tw.Flush()
	}
	return nil
}
