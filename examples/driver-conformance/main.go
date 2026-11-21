// Command driver-conformance plans how a driver's conformance suite is
// split into shares, one test binary each, and checks the plan against
// the suite: every group a share names must exist (workflowtest.GroupNames)
// and no group may be named twice, and the last share runs every group
// the others do not name, so a group added to the suite later still runs.
//
// main_test.go runs the plan's first share of workflowtest.Run on the
// memory driver, and every core-helper case (workflowtest.RunChildFlows,
// RunSignalFlows, RunPromiseStore, RunObjectFlows, RunResetFlows,
// RunRedoFlows, RunForkFlows, RunSlotFlows, RunMachineFlows,
// RunReplayFlows) with its harness: the tests a driver author copies
// with their own client in place of memory.New.
//
// It is the program behind the conformance sections of
// docs/extending/writing-a-driver.md: the guide copies its regions
// (marked "snippet:") and task docs:snippets fails when they drift.
package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"

	"github.com/ubgo/workflow/workflowtest"
)

// main runs the example and exits non-zero on error.
func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "driver-conformance:", err)
		os.Exit(1)
	}
}

// snippet:shares
// shares names the groups of every share but the last. Put the slowest
// groups of your driver in shares of their own; the last share is every
// group none of these names, so it needs no list.
var shares = [][]string{
	{"lifecycle", "run", "retry", "timeout", "onfailure"},
	{"cron", "misfire", "schedule", "overlap", "trigger"},
}

// shareGroups is the Harness.Groups filter of share i (0-based): the
// groups shares[i] names, or, for the last share (i == len(shares)), every
// group no other share names.
func shareGroups(i int) func(group string) bool {
	if i < len(shares) {
		return func(g string) bool { return slices.Contains(shares[i], g) }
	}
	return func(g string) bool {
		for _, s := range shares {
			if slices.Contains(s, g) {
				return false
			}
		}
		return true
	}
}

// checkShares refuses a plan that names a group the suite does not have
// (a typo would silently run nothing) or names one group twice (it would
// run in two binaries).
func checkShares(suite []string) error {
	seen := map[string]bool{}
	for i, s := range shares {
		for _, g := range s {
			if !slices.Contains(suite, g) {
				return fmt.Errorf("share %d names %q, which is not a conformance group", i+1, g)
			}
			if seen[g] {
				return fmt.Errorf("share %d names %q again", i+1, g)
			}
			seen[g] = true
		}
	}
	return nil
}

// snippet:end

// run prints the plan: the groups of each share, in suite order.
func run(_ context.Context, w io.Writer) error {
	suite := workflowtest.GroupNames()
	if err := checkShares(suite); err != nil {
		return err
	}
	for i := 0; i <= len(shares); i++ {
		var in []string
		for _, g := range suite {
			if shareGroups(i)(g) {
				in = append(in, g)
			}
		}
		if i == len(shares) {
			fmt.Fprintf(w, "share %d: every other group (%d of %d)\n", i+1, len(in), len(suite))
			continue
		}
		fmt.Fprintf(w, "share %d: %s\n", i+1, strings.Join(in, " "))
	}
	return nil
}
