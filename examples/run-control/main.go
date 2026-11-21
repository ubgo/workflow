// Command run-control shows operator tools on the in-memory driver:
// finding runs by a field of their payload, suspending one run and
// resuming it, a detour (hold, resume, then reset from the step that
// failed), a schedule whose new tick replaces the running run, and a
// duplicate enqueue that replaces a scheduled job's arguments.
//
// It is the program behind sections of docs/jobs/manage-runs.md,
// docs/schedules/overlap.md and docs/reliability/unique-jobs.md: the
// guides copy its regions (marked "snippet:") and task docs:snippets
// fails when they drift.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/memory"
	"github.com/ubgo/workflow/middleware"
)

// main runs the example and exits non-zero on error.
func main() {
	if err := run(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, "run-control:", err)
		os.Exit(1)
	}
}

// Timing tunables: in-memory runs move in microseconds, so short polls
// keep the program fast without spinning.
const (
	// pollEvery is how often the program, and a step parked by a
	// suspension, looks again.
	pollEvery = time.Millisecond
	// parkedWatch is how long the program watches a suspended run to see
	// that its next step does not start.
	parkedWatch = 30 * time.Millisecond
	// statusTimeout bounds waitStatus.
	statusTimeout = 3 * time.Second
	// loadedBuffer gives the load step room to report without blocking.
	loadedBuffer = 2
	// digestDelay is how far ahead the digests are scheduled: far enough
	// that none runs before the duplicates replace its arguments.
	digestDelay = time.Hour
)

// await is how the program waits for a run.
var await = workflow.AwaitOptions{Poll: pollEvery}

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	for _, section := range []func(context.Context) error{search, suspend, detour, replace, uniqueReplace} {
		if err := section(ctx); err != nil {
			return err
		}
	}
	return nil
}

// started registers wfs on c and starts it.
func started(ctx context.Context, c workflow.Client, wfs ...workflow.Workflow) error {
	for _, wf := range wfs {
		if err := c.Register(wf); err != nil {
			return err
		}
	}
	return c.Start(ctx)
}

// echo is a one-step workflow named name that returns its input.
func echo(name string) workflow.Workflow {
	return workflow.Workflow{Name: name, Steps: []workflow.Step{{Name: "s", Run: func(_ context.Context, in []byte) ([]byte, error) { return in, nil }}}}
}

// search finds the runs that handled one order.
func search(ctx context.Context) error {
	c := memory.New()
	if err := started(ctx, c, echo("fulfil")); err != nil {
		return err
	}
	for _, p := range []string{`{"order":{"id":"o-1"},"rush":true}`, `{"order":{"id":"o-2"},"rush":true}`, `{"order":{"id":"o-1"},"rush":false}`} {
		ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "fulfil", Payload: []byte(p)})
		if err != nil {
			return err
		}
		if _, err := workflow.Await(ctx, c, ref, await); err != nil {
			return err
		}
	}
	// snippet:search
	page, err := c.Runs(ctx, workflow.RunQuery{
		Workflow: "fulfil",
		Payload:  map[string]string{"order.id": "o-1", "rush": "true"}, // needs CapPayloadSearch
	})
	if err != nil {
		return err
	}
	// snippet:end
	fmt.Println("search:")
	fmt.Printf("  %d run(s) for order o-1 with rush: %s\n", len(page.Runs), page.Runs[0].Payload)
	return nil
}

// suspend holds one import between its steps and resumes it.
func suspend(ctx context.Context) error {
	gate := make(chan struct{})
	loaded := make(chan string, loadedBuffer)
	importWF := workflow.Workflow{Name: "import", Steps: []workflow.Step{
		{Name: "fetch", Run: func(_ context.Context, in []byte) ([]byte, error) {
			<-gate
			return in, nil
		}},
		{Name: "load", After: []string{"fetch"}, Run: func(_ context.Context, in []byte) ([]byte, error) {
			loaded <- string(in)
			return in, nil
		}},
	}}
	// snippet:suspend
	journal := workflow.NewMemoryJournal() // a durable JournalStore makes the flag fleet-wide
	// The second argument of Suspension is how often a parked step looks
	// at its flag again.
	c, err := workflow.Use(memory.New(), workflow.Journal(journal), workflow.Suspension(journal, time.Millisecond))
	if err != nil {
		return err
	}
	// snippet:end
	if err := started(ctx, c, importWF); err != nil {
		return err
	}
	ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "import", Payload: []byte(`"feed.csv"`)})
	if err != nil {
		return err
	}
	if err := waitStatus(ctx, c, ref, workflow.StatusRunning); err != nil {
		return err
	}
	// snippet:suspend-call
	if err := workflow.SuspendRun(ctx, journal, ref); err != nil { // holds it at its next step
		return err
	}
	// snippet:end
	close(gate)
	fmt.Println("suspend:")
	select {
	case <-loaded:
		return fmt.Errorf("a suspended run ran its next step")
	case <-time.After(parkedWatch):
	}
	st, err := c.Status(ctx, ref)
	if err != nil {
		return err
	}
	// Between looks the parked step snoozes (waiting) or is looking
	// (running); either way it has not ended and load has not run.
	fmt.Printf("  after fetch, while suspended: load not run, run finished: %v\n", st.IsTerminal())
	if err := workflow.ResumeRun(ctx, journal, ref); err != nil {
		return err
	}
	fmt.Printf("  resumed, load got %s\n", <-loaded)
	_, err = workflow.Await(ctx, c, ref, await)
	return err
}

// detour holds an import, lets it go on, and once its transform fails
// because of a mapping bug, redoes it from the transform after the fix:
// the fetch that succeeded is not repeated.
func detour(ctx context.Context) error {
	var mappingFixed atomic.Bool
	var fetches atomic.Int64
	gate := make(chan struct{})
	importWF := workflow.Workflow{Name: "import-feed", Retry: workflow.RetryPolicy{Max: 1}, Steps: []workflow.Step{
		{Name: "fetch", Run: func(_ context.Context, in []byte) ([]byte, error) {
			fetches.Add(1)
			<-gate // still fetching when the operator holds the run
			return in, nil
		}},
		{Name: "transform", After: []string{"fetch"}, Run: func(_ context.Context, in []byte) ([]byte, error) {
			if !mappingFixed.Load() {
				return nil, errors.New("unknown column \"sku_v2\"")
			}
			return in, nil
		}},
	}}
	journal := workflow.NewMemoryJournal()
	c, err := workflow.Use(memory.New(), workflow.Journal(journal), workflow.Suspension(journal, pollEvery))
	if err != nil {
		return err
	}
	if err := started(ctx, c, importWF); err != nil {
		return err
	}
	ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "import-feed", Payload: []byte(`"feed.csv"`)})
	if err != nil {
		return err
	}
	if err := waitStatus(ctx, c, ref, workflow.StatusRunning); err != nil {
		return err
	}
	fmt.Println("detour:")
	// snippet:detour
	if err := workflow.SuspendRun(ctx, journal, ref); err != nil { // hold
		return err
	}
	close(gate) // fetch ends; transform parks instead of running
	// The source checks out: let the run go on.
	if err := workflow.ResumeRun(ctx, journal, ref); err != nil {
		return err
	}
	if err := waitStatus(ctx, c, ref, workflow.StatusFailed); err != nil { // the mapping bug
		return err
	}
	mappingFixed.Store(true) // the fix is deployed
	if _, err := c.(workflow.RunResetter).ResetRun(ctx, ref, "transform"); err != nil {
		return err
	}
	// snippet:end
	st, err := workflow.Await(ctx, c, ref, await)
	if err != nil {
		return err
	}
	fmt.Printf("  held, resumed, failed in transform, reset from transform: %s; fetch ran %d time(s)\n", st, fetches.Load())
	return nil
}

// replace fires a schedule twice; the second fire cancels the first run.
func replace(ctx context.Context) error {
	c := memory.New()
	prices := workflow.Workflow{Name: "prices", Steps: []workflow.Step{{Name: "pull", Run: func(ctx context.Context, _ []byte) ([]byte, error) {
		<-ctx.Done() // a long pull; only cancellation ends it here
		return nil, ctx.Err()
	}}}}
	if err := c.Register(prices); err != nil {
		return err
	}
	// snippet:guard
	if err := workflow.GuardOverlap(c, nil, workflow.OverlapRule{Name: "prices", Mode: workflow.OverlapReplace}); err != nil {
		return err // needs CapTickEvents, CapRunQuery and CapCancel; call before Start
	}
	// snippet:end
	if err := c.Start(ctx); err != nil {
		return err
	}
	id, err := c.Schedule(ctx, workflow.Schedule{Name: "prices", Workflow: "prices", Cron: "*/5 * * * *", Location: time.UTC})
	if err != nil {
		return err
	}
	first, err := c.TriggerSchedule(ctx, id)
	if err != nil {
		return err
	}
	if err := waitStatus(ctx, c, first, workflow.StatusRunning); err != nil {
		return err
	}
	second, err := c.TriggerSchedule(ctx, id)
	if err != nil {
		return err
	}
	if err := waitStatus(ctx, c, first, workflow.StatusCancelled); err != nil {
		return err
	}
	if err := waitStatus(ctx, c, second, workflow.StatusRunning); err != nil {
		return err
	}
	fmt.Println("replace:")
	fmt.Println("  first run cancelled, second running")
	return nil
}

// uniqueReplace keeps one digest per user and lets the latest list win.
func uniqueReplace(ctx context.Context) error {
	mc := memory.New()
	sent := make(chan string, 1)
	digest := workflow.Workflow{Name: "digest", Steps: []workflow.Step{{Name: "send", Run: func(_ context.Context, in []byte) ([]byte, error) {
		sent <- string(in)
		return nil, nil
	}}}}
	if err := mc.Register(digest); err != nil {
		return err
	}
	// snippet:unique-replace
	mw, err := middleware.UniqueWith(middleware.UniqueConfig{
		By:      func(j workflow.Job) string { return "digest:" + j.Labels["user"] },
		Store:   middleware.NewMemoryUnique(clock.Real{}),
		TTL:     24 * time.Hour, // one digest per user per day
		Client:  mc,             // implements workflow.RunEditor
		Replace: true,
	})
	if err != nil {
		return err
	}
	c := middleware.WrapClient(mc, mw)
	// snippet:end
	if err := c.Start(ctx); err != nil {
		return err
	}
	user := map[string]string{"user": "u1"}
	tonight := time.Now().Add(digestDelay)
	for _, comments := range []string{`["c1"]`, `["c1","c2"]`, `["c1","c2","c3"]`} {
		if _, err := c.Enqueue(ctx, workflow.Job{Workflow: "digest", Payload: []byte(comments), Labels: user, RunAt: tonight}); err != nil {
			return err
		}
	}
	page, err := mc.Runs(ctx, workflow.RunQuery{Workflow: "digest"})
	if err != nil {
		return err
	}
	now := time.Now()
	if _, err := mc.Reschedule(ctx, page.Runs[0].Ref, now); err != nil { // send it now for the demo
		return err
	}
	fmt.Println("unique replace:")
	fmt.Printf("  %d scheduled digest, sent with %s\n", len(page.Runs), <-sent)
	return nil
}

// waitStatus polls ref until it reaches want.
func waitStatus(ctx context.Context, c workflow.Client, ref workflow.RunRef, want workflow.Status) error {
	deadline := time.Now().Add(statusTimeout)
	for time.Now().Before(deadline) {
		st, err := c.Status(ctx, ref)
		if err != nil {
			return err
		}
		if st == want {
			return nil
		}
		time.Sleep(pollEvery)
	}
	st, _ := c.Status(ctx, ref)
	return fmt.Errorf("run %v is %s, want %s", ref, st, want)
}
