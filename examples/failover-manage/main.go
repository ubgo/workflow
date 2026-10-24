// Command failover-manage manages runs through a failover client while
// they sit on two backends: some jobs went to the primary, some to the
// secondary during an outage. Runs merges both backends' runs into one
// list, newest first, with every Ref prefixed by the backend that holds
// the run; RunQuery.Ref picks one run; Retry and Delete go to the backend
// the ref names; CancelAll and Pause act on both. Both backends are the
// in-memory driver on one fake clock.
//
// It is the program behind "Query, retry, cancel and pause runs" in
// docs/reliability/failover.md: the guide copies its regions (marked
// "snippet:") and task docs:snippets fails when they drift.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/failover"
	"github.com/ubgo/workflow/memory"
	"github.com/ubgo/workflow/middleware"
)

// start is the fake clock's first instant; any fixed time works.
var start = time.Date(2026, 1, 5, 9, 0, 0, 0, time.UTC)

const (
	// statusPoll is how often waitTerminal re-reads a run's status.
	statusPoll = time.Millisecond
	// waitTimeout bounds each wait for a run to finish; the memory driver
	// takes well under a millisecond.
	waitTimeout = 5 * time.Second
	// keyLabel carries each job's idempotency key, so the output can name
	// the job behind a listed run.
	keyLabel = "order"
)

func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "failover-manage:", err)
		os.Exit(1)
	}
}

// side is the backend a RunRef names: the part before the first colon.
func side(ref workflow.RunRef) string {
	s, _, _ := strings.Cut(string(ref.RunID), ":")
	return s
}

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context, out io.Writer) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	clk := clock.NewFake(start)

	// The invoice step fails until the billing API is fixed.
	var fixed atomic.Bool
	invoice := workflow.Workflow{Name: "invoice", Steps: []workflow.Step{{
		Name:  "bill",
		Retry: workflow.RetriesNone(),
		Run: func(_ context.Context, in []byte) ([]byte, error) {
			if string(in) == "fail" && !fixed.Load() {
				return nil, errors.New("billing API: 500")
			}
			return in, nil
		},
	}}}

	// The primary refuses enqueues while down is set: an engine outage.
	var down atomic.Bool
	primaryMem := memory.New(memory.WithClock(clk))
	primary := middleware.WrapClient(primaryMem, func(next workflow.EnqueueFunc) workflow.EnqueueFunc {
		return func(ctx context.Context, job workflow.Job) (workflow.RunRef, error) {
			if down.Load() {
				return workflow.RunRef{}, workflow.Unavailable(errors.New("connection refused"))
			}
			return next(ctx, job)
		}
	})
	secondary := memory.New(memory.WithClock(clk))
	defer primaryMem.Stop()
	defer secondary.Stop()

	c := failover.New(primary, secondary, failover.WithClock(clk))
	if err := c.Register(invoice); err != nil {
		return err
	}
	if err := c.Start(ctx); err != nil {
		return err
	}

	enqueue := func(key, payload string, at time.Time) (workflow.RunRef, error) {
		clk.Advance(time.Second) // a distinct enqueue time per job, so the order is fixed
		return c.Enqueue(ctx, workflow.Job{
			Workflow: "invoice", IdempotencyKey: key, Payload: []byte(payload), RunAt: at,
			Labels: map[string]string{keyLabel: key},
		})
	}
	var refs []workflow.RunRef
	for _, j := range []struct {
		key, payload string
		down         bool
	}{
		{"o-1", "ok", false},
		{"o-2", "fail", false},
		{"o-3", "fail", true}, // the primary is down: to the secondary
		{"o-4", "ok", true},
	} {
		down.Store(j.down)
		ref, err := enqueue(j.key, j.payload, time.Time{})
		if err != nil {
			return err
		}
		refs = append(refs, ref)
	}
	down.Store(false)
	for _, ref := range refs {
		if _, err := waitTerminal(ctx, c, ref); err != nil {
			return err
		}
	}

	fmt.Fprintln(out, "-- every run, from both backends, newest first")
	if err := list(ctx, c, workflow.RunQuery{Workflow: "invoice"}, out); err != nil {
		return err
	}

	fmt.Fprintln(out, "-- retry the failed runs, each on its own backend")
	fixed.Store(true)
	// snippet:retry
	page, err := c.Runs(ctx, workflow.RunQuery{Workflow: "invoice", Statuses: []workflow.Status{workflow.StatusFailed}})
	if err != nil {
		return err
	}
	retried := make([]workflow.RunRef, 0, len(page.Runs))
	for _, r := range page.Runs {
		// r.Ref is "primary:..." or "secondary:...", so the retry reaches the backend holding the run.
		again, err := c.Retry(ctx, r.Ref)
		if err != nil {
			return err
		}
		retried = append(retried, again)
	}
	// snippet:end
	for i, again := range retried {
		st, err := waitTerminal(ctx, c, again)
		if err != nil {
			return err
		}
		fmt.Fprintf(out, "  %s on %s: retried, %s\n", page.Runs[i].Labels[keyLabel], side(again), st)
	}

	fmt.Fprintln(out, "-- one run by its ref")
	// snippet:one
	// A prefixed Ref asks only the backend it names, and returns zero or one run.
	one, err := c.Runs(ctx, workflow.RunQuery{Ref: refs[2]})
	if err != nil {
		return err
	}
	// snippet:end
	for _, r := range one.Runs {
		fmt.Fprintf(out, "  %s on %s: %s\n", r.Labels[keyLabel], side(r.Ref), r.Status)
	}

	fmt.Fprintln(out, "-- forget a finished run")
	// snippet:delete
	if err := c.Delete(ctx, refs[0]); err != nil { // goes to the primary only
		return err
	}
	// snippet:end
	if err := list(ctx, c, workflow.RunQuery{Workflow: "invoice"}, out); err != nil {
		return err
	}

	fmt.Fprintln(out, "-- cancel every waiting run on both backends")
	later := clk.Now().Add(time.Hour)
	if _, err := enqueue("o-5", "ok", later); err != nil {
		return err
	}
	down.Store(true)
	if _, err := enqueue("o-6", "ok", later); err != nil {
		return err
	}
	down.Store(false)
	// snippet:cancel-all
	n, err := c.CancelAll(ctx, workflow.RunQuery{Workflow: "invoice"}) // scheduled and queued runs, on both
	if err != nil {
		return err
	}
	// snippet:end
	fmt.Fprintln(out, "  cancelled:", n)

	fmt.Fprintln(out, "-- pause on both backends")
	// snippet:pause
	if err := c.Pause(ctx, workflow.PauseWorkflow("invoice")); err != nil { // primary first, then the secondary
		return err
	}
	paused, err := c.Paused(ctx) // what either backend holds, without repeats
	if err != nil {
		return err
	}
	// snippet:end
	for _, p := range paused {
		fmt.Fprintln(out, "  paused:", p.Workflow)
	}
	return c.Resume(ctx, workflow.PauseWorkflow("invoice"))
}

// list prints the runs q matches through the failover client.
func list(ctx context.Context, c workflow.RunQuerier, q workflow.RunQuery, out io.Writer) error {
	// snippet:list
	page, err := c.Runs(ctx, q)
	if err != nil {
		return err
	}
	for _, r := range page.Runs {
		line := fmt.Sprintf("  %s on %-9s %s %s", r.Labels[keyLabel], side(r.Ref), r.Status, r.Error)
		fmt.Fprintln(out, strings.TrimRight(line, " "))
	}
	// snippet:end
	return nil
}

// waitTerminal polls a run's status until it is terminal.
func waitTerminal(ctx context.Context, c workflow.Client, ref workflow.RunRef) (workflow.Status, error) {
	deadline := time.Now().Add(waitTimeout)
	for {
		st, err := c.Status(ctx, ref)
		if err != nil || st.IsTerminal() {
			return st, err
		}
		if time.Now().After(deadline) {
			return st, fmt.Errorf("run is %s after %s", st, waitTimeout)
		}
		time.Sleep(statusPoll)
	}
}
