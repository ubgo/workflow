// Command admin-inspect reads what an operator's screen shows, from Go
// through the admin module, on the in-memory driver with a fake clock:
// runs page by page, failed and discarded runs, a count with numbered
// pages, each queue's depth, oldest wait and workers (and what a bounded
// scan reports), schedules with their last fire, the online nodes and
// the client's health, and the output each attempt captured.
//
// It is the program behind steps 2 and 4 of docs/admin/operations.md: the
// guide copies its regions (marked "snippet:") and task docs:snippets
// fails when they drift.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/admin"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/memory"
	"github.com/ubgo/workflow/middleware"
)

// main runs the example and exits non-zero on error.
func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "admin-inspect:", err)
		os.Exit(1)
	}
}

// start is the fake clock's first instant, so every time printed is fixed.
var start = time.Date(2026, 10, 14, 9, 0, 0, 0, time.UTC)

// The workflow and queue names the example uses.
const (
	wfImport    = "import"
	wfEmail     = "send-email"
	wfCleanup   = "cleanup"
	wfReport    = "nightly-report"
	queueEmails = "emails"
)

// The example's sizes: how many imports run (some fail), how many emails
// wait in the paused queue, how long they wait, and the page size.
const (
	imports   = 5
	failEvery = 2 // every second import fails
	emails    = 4
	waited    = 3 * time.Minute
	pageSize  = 2
	// smallScan is the max scan of the second Admin, below the emails
	// waiting, to show a truncated report.
	smallScan = 3
	// outputBytes bounds each attempt's captured text, to show a cut.
	outputBytes = 140
	// noteWords is how often the import step repeats "long " in its log
	// line, enough to push the captured text past outputBytes.
	noteWords = 10
)

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context, w io.Writer) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	clk := clock.NewFake(start)

	// snippet:outputs
	outs := admin.NewMemoryOutputs(admin.MemoryOutputLimits{
		Runs:   100,         // the runs whose output is kept (default 1,000)
		PerRun: 5,           // the newest attempts per run (default 20)
		Bytes:  outputBytes, // per attempt; longer text is cut and flagged (default 64 KiB)
	})
	c := memory.New(memory.WithClock(clk),
		memory.WithMiddleware(middleware.CaptureOutput(outs, middleware.CaptureClock(clk))), // outs is the sink
		memory.WithNodeID("worker-1"), memory.WithWorkers(4), // four step attempts at once on this node
		memory.WithQueues(workflow.QueueConfig{Name: workflow.DefaultQueue}, workflow.QueueConfig{Name: queueEmails, Workers: 2}), // two emails at a time
	)
	adm := admin.New(c, admin.WithClock(clk), admin.WithOutputs(outs)) // and the source
	// snippet:end
	defer c.Stop()

	// pass is a step that hands its input on unchanged.
	pass := func(_ context.Context, in []byte) ([]byte, error) { return in, nil }
	for _, wf := range []workflow.Workflow{
		{Name: wfImport, Steps: []workflow.Step{{Name: "load", Run: func(ctx context.Context, in []byte) ([]byte, error) {
			log := middleware.OutputLogger(ctx, nil)
			log.Info("loading file", "name", string(in), "note", strings.Repeat("long ", noteWords))
			if strings.HasSuffix(string(in), "-bad.csv") {
				return nil, workflow.Permanent(errors.New("load: header row missing"))
			}
			return nil, nil
		}}}},
		{Name: wfEmail, Queue: queueEmails, Steps: []workflow.Step{{Name: "send", Run: pass}}},
		{Name: wfCleanup, Steps: []workflow.Step{{Name: "sweep", Run: func(context.Context, []byte) ([]byte, error) {
			return nil, workflow.Discard(errors.New("nothing to clean"))
		}}}},
		{Name: wfReport, Steps: []workflow.Step{{Name: "build", Run: pass}}},
	} {
		if err := c.Register(wf); err != nil {
			return err
		}
	}
	if err := c.Start(ctx); err != nil {
		return err
	}
	// Every run is enqueued under a pause of its queue, so the IDs the
	// driver hands out do not depend on which runs its workers pick up
	// first. The emails queue stays paused, so its runs wait.
	if err := c.Pause(ctx, workflow.PauseQueue(workflow.DefaultQueue)); err != nil {
		return err
	}
	for i := range imports {
		name := fmt.Sprintf("orders-%d.csv", i+1)
		if i%failEvery == 1 {
			name = fmt.Sprintf("orders-%d-bad.csv", i+1)
		}
		if _, err := c.Enqueue(ctx, workflow.Job{Workflow: wfImport, Payload: []byte(name), Labels: map[string]string{"source": "sftp"}}); err != nil {
			return err
		}
	}
	if _, err := c.Enqueue(ctx, workflow.Job{Workflow: wfCleanup}); err != nil {
		return err
	}
	if err := c.Resume(ctx, workflow.PauseQueue(workflow.DefaultQueue)); err != nil {
		return err
	}
	if _, err := c.RunAll(ctx); err != nil {
		return err
	}
	if err := c.Pause(ctx, workflow.PauseQueue(queueEmails)); err != nil {
		return err
	}
	for range emails {
		if _, err := c.Enqueue(ctx, workflow.Job{Workflow: wfEmail}); err != nil {
			return err
		}
	}
	clk.Advance(waited)
	sched, err := c.Schedule(ctx, workflow.Schedule{Name: "nightly", Workflow: wfReport, Cron: "0 2 * * *", Location: time.UTC})
	if err != nil {
		return err
	}

	fmt.Fprintln(w, "-- runs, page by page")
	// snippet:runs
	q := workflow.RunQuery{Workflow: wfImport, Labels: map[string]string{"source": "sftp"}, Limit: pageSize}
	for page := 1; ; page++ {
		p, err := adm.Runs(ctx, q)
		if err != nil {
			return err
		}
		for _, r := range p.Runs {
			fmt.Fprintf(w, "page %d: %s %s %s\n", page, r.Ref.RunID, r.Status, r.Payload)
		}
		if p.Next == "" {
			break
		}
		q.Cursor = p.Next // the driver's own opaque cursor
	}
	// snippet:end

	fmt.Fprintln(w, "-- failed and discarded")
	// snippet:failures
	failed, err := adm.Failures(ctx, workflow.RunQuery{Workflow: wfImport})
	if err != nil {
		return err
	}
	for _, r := range failed.Runs {
		fmt.Fprintf(w, "failed %s at step %s, %d attempt(s): %s\n", r.Ref.RunID, r.FailedStep, r.Attempts, r.Errors[0].Error)
	}
	discarded, err := adm.Discarded(ctx, workflow.RunQuery{}) // dropped on purpose: nobody has to act
	if err != nil {
		return err
	}
	fmt.Fprintf(w, "discarded: %d run of %s\n", len(discarded.Runs), discarded.Runs[0].Workflow)
	// snippet:end
	_, err = adm.Failures(ctx, workflow.RunQuery{Statuses: []workflow.Status{workflow.StatusSucceeded}})
	fmt.Fprintln(w, "Failures with statuses set:", errors.Is(err, workflow.ErrInvalid))

	fmt.Fprintln(w, "-- count for numbered pages")
	// snippet:count
	n, err := adm.CountRuns(ctx, workflow.RunQuery{Workflow: wfImport, Limit: pageSize})
	if err != nil {
		return err
	}
	fmt.Fprintf(w, "%d runs, truncated %v, %d pages\n", n.Total, n.Truncated, len(n.Cursors))
	// n.Cursors[i] is the Cursor that opens page i+1 ("" for the first).
	// snippet:end

	fmt.Fprintln(w, "-- queues")
	// snippet:queues
	rep, err := adm.QueueStats(ctx)
	if err != nil {
		return err
	}
	for _, s := range rep.Queues {
		fmt.Fprintf(w, "%-8s depth %d, oldest wait %s, workers %d, running %d\n", s.Queue, s.Depth, s.OldestWait, s.Workers, s.Running)
	}
	fmt.Fprintf(w, "truncated %v, worker counts %v, at %s\n", rep.Truncated, rep.Workers, rep.At.Format(time.TimeOnly))

	small := admin.New(c, admin.WithClock(clk), admin.WithMaxScan(smallScan))
	depths, err := small.QueueDepths(ctx) // map[queue]depth, the OpenTelemetry gauge's signature
	if err != nil {
		return err
	}
	fmt.Fprintf(w, "with WithMaxScan(%d): emails depth %d (a lower bound)\n", smallScan, depths[queueEmails])
	// snippet:end

	fmt.Fprintln(w, "-- schedules")
	// snippet:schedules
	if err := adm.PauseSchedule(ctx, sched); err != nil {
		return err
	}
	all, err := adm.Schedules(ctx)
	if err != nil {
		return err
	}
	fmt.Fprintf(w, "%s %q paused %v, next fire zero %v\n", all[0].ID, all[0].Cron, all[0].Paused, all[0].Next.IsZero())
	if err := adm.ResumeSchedule(ctx, sched); err != nil {
		return err
	}
	ref, err := adm.TriggerSchedule(ctx, sched) // runs now with the schedule's input and labels
	if err != nil {
		return err
	}
	if err := awaitEnd(ctx, c, ref); err != nil {
		return err
	}
	one, err := adm.Schedule(ctx, sched)
	if err != nil {
		return err
	}
	fmt.Fprintf(w, "%s next fire %s\n", one.ID, one.Next.Format(time.DateTime))
	if !one.Last.Fire.IsZero() { // zero before the first fire, or without CapScheduleHistory
		fmt.Fprintf(w, "last fire %s started %s, now %s (trigger returned %s)\n", one.Last.Fire.Format(time.TimeOnly), one.Last.Run.RunID, one.Last.Status, ref.RunID)
	}
	// snippet:end

	fmt.Fprintln(w, "-- nodes and health")
	// snippet:nodes
	nodes, err := adm.Nodes(ctx)
	if err != nil {
		return err
	}
	for _, nd := range nodes {
		fmt.Fprintf(w, "node %s (%s) queues %v, %d workers, %d running\n", nd.ID, nd.Mode, nd.Queues, nd.Workers, nd.Running)
	}
	h := adm.Health(ctx) // never fails: an unreachable backend is a failed check
	fmt.Fprintf(w, "live %v, ready %v\n", h.Live, h.Ready)
	for _, chk := range h.Checks {
		fmt.Fprintf(w, "  check %s ok=%v %s\n", chk.Name, chk.OK, chk.Detail)
	}
	// snippet:end

	fmt.Fprintln(w, "-- captured output")
	// snippet:output
	out, err := adm.RunOutput(ctx, failed.Runs[0].Ref)
	if err != nil {
		return err
	}
	for _, a := range out {
		fmt.Fprintf(w, "step %s attempt %d, error %q, truncated %v\n", a.Step, a.Attempt, a.Error, a.Truncated)
		fmt.Fprintf(w, "%s\n", strings.TrimSpace(a.Text))
	}
	// snippet:end
	_, err = admin.New(c).RunOutput(ctx, failed.Runs[0].Ref)
	fmt.Fprintln(w, "without WithOutputs:", errors.Is(err, workflow.ErrUnsupported))
	return nil
}

// endPoll is how often awaitEnd looks at a run, and endTimeout how long it
// waits for one to end before giving up.
const (
	endPoll    = time.Millisecond
	endTimeout = 10 * time.Second
)

// awaitEnd waits until the run ref has ended. The memory driver runs
// steps on its own goroutines; RunAll cannot be used here, since it
// reports the paused emails queue as stalled only after a grace period.
func awaitEnd(ctx context.Context, c *memory.Client, ref workflow.RunRef) error {
	deadline := time.Now().Add(endTimeout)
	for {
		page, err := c.Runs(ctx, workflow.RunQuery{Ref: ref})
		if err != nil {
			return err
		}
		if len(page.Runs) == 1 && page.Runs[0].Status.IsTerminal() {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("run %s did not end within %s", ref.RunID, endTimeout)
		}
		time.Sleep(endPoll)
	}
}
