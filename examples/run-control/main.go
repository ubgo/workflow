// Command run-control shows four operator tools on the in-memory driver:
// finding runs by a field of their payload, suspending one run and
// resuming it, a schedule whose new tick replaces the running run, and a
// duplicate enqueue that replaces a scheduled job's arguments.
//
// It is the program behind sections of docs/jobs/manage-runs.md,
// docs/schedules/overlap.md and docs/reliability/unique-jobs.md: the
// guides copy its regions (marked "snippet:") and task docs:snippets
// fails when they drift.
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/memory"
	"github.com/ubgo/workflow/middleware"
)

func main() {
	if err := run(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, "run-control:", err)
		os.Exit(1)
	}
}

var await = workflow.AwaitOptions{Poll: time.Millisecond}

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	for _, section := range []func(context.Context) error{search, suspend, replace, uniqueReplace} {
		if err := section(ctx); err != nil {
			return err
		}
	}
	return nil
}

func started(ctx context.Context, c workflow.Client, wfs ...workflow.Workflow) error {
	for _, wf := range wfs {
		if err := c.Register(wf); err != nil {
			return err
		}
	}
	return c.Start(ctx)
}

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
	loaded := make(chan string, 2)
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
	case <-time.After(30 * time.Millisecond):
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
		TTL:     24 * time.Hour,
		Client:  mc, // implements workflow.RunEditor
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
	tonight := time.Now().Add(time.Hour)
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
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		st, err := c.Status(ctx, ref)
		if err != nil {
			return err
		}
		if st == want {
			return nil
		}
		time.Sleep(time.Millisecond)
	}
	st, _ := c.Status(ctx, ref)
	return fmt.Errorf("run %v is %s, want %s", ref, st, want)
}
