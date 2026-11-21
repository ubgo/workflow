package main

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/cron"
)

// helperTour calls the core helpers a driver uses for the capabilities
// Tiny does not declare, and prints what each one decides. A driver that
// adds the capability calls the same helper at the same point, so two
// drivers cannot disagree on the answer. Each region below is one place
// in a driver; the guide copies them.
func helperTour(ctx context.Context, w io.Writer) error {
	// snippet:reset-fork
	// ResetRun and ForkRun: which runs may be reset or forked, whether a
	// fork redoes succeeded work, and the call's own checks, before the
	// driver looks the run up.
	fmt.Fprintln(w, "reset failed:", workflow.ResetStatusOK(workflow.StatusFailed, workflow.ResetOptions{}),
		"succeeded:", workflow.ResetStatusOK(workflow.StatusSucceeded, workflow.ResetOptions{}),
		"succeeded with Succeeded:", workflow.ResetStatusOK(workflow.StatusSucceeded, workflow.ResetOptions{Succeeded: true}))
	fmt.Fprintln(w, "fork running:", workflow.ForkStatusOK(workflow.StatusRunning),
		"cancelled:", workflow.ForkStatusOK(workflow.StatusCancelled),
		"redo of succeeded:", workflow.ForkRedo(workflow.StatusSucceeded, nil))
	fmt.Fprintln(w, "fork without a step:", workflow.CheckFork("", workflow.ForkOptions{}))
	// snippet:end

	// snippet:chain-edits
	// A step attempt edits its run's chain through a ChainEdits made from
	// the driver's capabilities and the stored chain; the driver applies
	// the edits only when the attempt succeeds.
	stored := []workflow.Job{{Workflow: "email"}}
	edits := workflow.NewChainEdits(tinyCaps, stored)
	if err := edits.Append(workflow.Job{Workflow: "audit"}); err != nil {
		return err
	}
	refused := edits.Append(workflow.Job{Workflow: "later", RunAt: time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)})
	var chain []string
	for _, j := range edits.Apply(stored) {
		chain = append(chain, j.Workflow)
	}
	fmt.Fprintln(w, "chain after the attempt:", chain, "changed:", edits.Changed())
	fmt.Fprintln(w, "delayed chain job:", refused)
	// snippet:end

	// snippet:ticks
	// Schedules: the tick a misfired fire stands for, and the labels of a
	// run a manual trigger starts (CapTickTime adds the tick time).
	hourly, err := cron.Parse("0 * * * *")
	if err != nil {
		return err
	}
	down := time.Date(2026, 1, 5, 9, 10, 0, 0, time.UTC)
	back := time.Date(2026, 1, 5, 12, 30, 0, 0, time.UTC)
	fmt.Fprintln(w, "last missed tick:", workflow.LastTick(hourly, down, back, time.UTC).Format(time.TimeOnly))
	labels := workflow.TriggeredLabelsAt(map[string]string{"team": "core"}, "sched-1", "hourly-sync", back)
	fmt.Fprintln(w, "trigger labels:", len(labels), "tick at", labels[workflow.LabelTickAt], "trigger", labels[workflow.LabelTrigger])
	// snippet:end

	// snippet:fair
	// Round-robin concurrency: a key's weight, then the virtual time one
	// of its turns costs. A heavier key's turns cost less, so it gets more.
	conc := &workflow.Concurrency{Weight: workflow.KeyWeights(map[string]int{"enterprise": 4}, 1)} // enterprise weighs 4, every other key 1
	for _, key := range []string{"enterprise", "free"} {
		weight := workflow.FairWeight(conc, key)
		fmt.Fprintf(w, "key %s weight %d stride %d\n", key, weight, workflow.FairStride(weight))
	}
	// snippet:end

	// snippet:cancel-pause
	// CancelAll's default statuses (never a run that has started) and the
	// capability a pause target needs.
	fmt.Fprintln(w, "CancelAll cancels:", workflow.CancelStatuses(workflow.RunQuery{}))
	fmt.Fprintln(w, "pause a queue needs:", workflow.RequiresPause(workflow.PauseQueue("bulk")),
		"a workflow needs:", workflow.RequiresPause(workflow.PauseWorkflow("report")))
	// snippet:end

	// snippet:slots
	// Step.Slots: an attempt takes its slots from the client's pool
	// between claiming and running, and gives them back after. Waiters
	// are served first come, first served.
	pool := workflow.NewSlotPool(4)                  // the client runs at most 4 slots' worth of work at once
	heavy := workflow.Step{Name: "render", Slots: 3} // a heavy step takes 3 of them
	if err := pool.Acquire(ctx, workflow.StepSlots(heavy)); err != nil {
		return err
	}
	fmt.Fprintf(w, "slots in use %d of %d\n", pool.InUse(), pool.Size())
	pool.Release(workflow.StepSlots(heavy))
	fmt.Fprintln(w, "too many slots:", pool.Acquire(ctx, 5)) // more than the pool holds: refused, never waited for
	// snippet:end

	return reportersTour(ctx, w)
}

// snippet:reporters
// stepContext is what a driver with CapHeartbeat and CapProgress puts on
// each attempt's ctx before the middleware chain runs: a Heartbeater that
// renews the attempt's lease and a ProgressReporter that fills Step and
// At and stores the report with the run. Here both write to a log.
func stepContext(ctx context.Context, step string, now func() time.Time, log *[]string) context.Context {
	ctx = workflow.WithHeartbeater(ctx, workflow.HeartbeatFunc(func(_ context.Context, details []byte) error {
		*log = append(*log, fmt.Sprintf("heartbeat %s %s", step, details))
		return nil
	}))
	return workflow.WithProgressReporter(ctx, workflow.ProgressFunc(func(_ context.Context, p workflow.Progress) error {
		p.Step, p.At = step, now()
		*log = append(*log, fmt.Sprintf("progress %s %.0f%% %q at %s", p.Step, p.Percent, p.Message, p.At.Format(time.TimeOnly)))
		return nil
	}))
}

// snippet:end

// reportersTour runs a handler that beats and reports progress, once on
// a step ctx the driver built and once on a bare ctx.
func reportersTour(ctx context.Context, w io.Writer) error {
	at := time.Date(2026, 1, 5, 9, 0, 0, 0, time.UTC)
	// handler is a step body that beats once, then reports how far it got.
	handler := func(ctx context.Context, _ []byte) ([]byte, error) {
		if err := workflow.Heartbeat(ctx, []byte("row 4000")); err != nil {
			return nil, err
		}
		return nil, workflow.ReportProgress(ctx, workflow.Progress{Percent: 40, Message: "4,000 of 10,000 rows"}) // 40% is the share the message names
	}
	var log []string
	_, err := handler(stepContext(ctx, "export", func() time.Time { return at }, &log), nil)
	if err != nil {
		return err
	}
	for _, l := range log {
		fmt.Fprintln(w, l)
	}
	// Without them Heartbeat is a silent no-op, but ReportProgress refuses:
	// the caller asked for data to be kept.
	_, err = handler(ctx, nil)
	fmt.Fprintln(w, "without a reporter:", err)
	return nil
}
