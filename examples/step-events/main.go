// Command step-events records a step's timeline and its run's outcome.
// The step calls workflow.RecordEvent and workflow.SetOutcome the same way
// everywhere; what happens to them depends on what is bound on the step's
// context. First, on a plain memory client: a sink of the app's own (step
// middleware calling workflow.WithStepEventSink) prints each event as it
// is recorded, middleware.CaptureOutput keeps the events with the step's
// captured log, and SetOutcome reports that nothing keeps an outcome.
// Then, with a run recorder applied: the events are stored with the step,
// the set outcome with the run, and a run that sets none gets the default
// outcome of its status.
//
// It is the program behind docs/concepts/step-events.md,
// docs/concepts/run-outcome.md and the "Capture step events" section of
// docs/middleware/step-middleware.md: the guides copy its regions (marked
// "snippet:") and task docs:snippets fails when they drift.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/memory"
	"github.com/ubgo/workflow/middleware"
	"github.com/ubgo/workflow/recorder"
)

// runTimeout bounds each wait for a run end, generous for a slow CI host.
const runTimeout = 10 * time.Second

func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "step-events:", err)
		os.Exit(1)
	}
}

// syncStep is the step both clients run. It records two events and sets
// a partial outcome, and says whether the outcome was kept.
func syncStep(out io.Writer) workflow.Handler {
	// snippet:step
	return func(ctx context.Context, _ []byte) ([]byte, error) {
		workflow.RecordEvent(ctx, "fetched changes", slog.Int("rows", 100))
		workflow.RecordEventLevel(ctx, slog.LevelWarn, "rows rejected", slog.Int("bad", 3), slog.String("first", "row-17"))
		err := workflow.SetOutcome(ctx, workflow.OutcomePartial, "3 of 100 rows rejected")
		fmt.Fprintln(out, "outcome kept:", err == nil, "unsupported:", errors.Is(err, workflow.ErrUnsupported))
		return nil, nil
	}
	// snippet:end
}

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context, out io.Writer) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// snippet:sink
	// Step middleware that binds a sink of its own: every event the step
	// records reaches it, next to any other sink bound on the context.
	printEvents := func(next workflow.Handler) workflow.Handler {
		return func(ctx context.Context, in []byte) ([]byte, error) {
			sink := workflow.StepEventFunc(func(_ context.Context, e workflow.StepEvent) {
				fmt.Fprintf(out, "event %s/%s %s %q %v\n", e.Step.Workflow, e.Step.Step, e.Level, e.Message, e.Attrs)
			})
			return next(workflow.WithStepEventSink(ctx, sink), in)
		}
	}
	// snippet:end

	// snippet:capture
	// CaptureOutput is a sink too: events join the step's captured log.
	// The fake clock only makes the printed times fixed.
	stamp := clock.NewFake(time.Date(2026, 1, 5, 2, 0, 0, 0, time.UTC))
	capture := middleware.CaptureOutput(middleware.WriterSink(out), middleware.CaptureClock(stamp))
	plain := memory.New(memory.WithMiddleware(printEvents, capture))
	// snippet:end
	defer plain.Stop()
	fmt.Fprintln(out, "-- without a recorder")
	if err := runOnce(ctx, plain, syncStep(out)); err != nil {
		return err
	}

	fmt.Fprintln(out, "-- with a recorder")
	// snippet:recorder
	store, err := recorder.NewMemoryStore() // in process; sqlrecorder for a database
	if err != nil {
		return err
	}
	rec, err := recorder.New(store)
	if err != nil {
		return err
	}
	base := memory.New()
	defer base.Stop()
	c, err := workflow.Use(base, rec.Plugin())
	if err != nil {
		return err
	}
	// snippet:end
	if err := c.Register(workflow.Workflow{Name: "cleanup", Steps: []workflow.Step{{Name: "purge", Run: func(context.Context, []byte) ([]byte, error) {
		return nil, nil // sets no outcome
	}}}}); err != nil {
		return err
	}
	if err := runOnce(ctx, c, syncStep(out)); err != nil {
		return err
	}
	return readBack(ctx, c, store, out)
}

// readBack prints the sync run's record and the cleanup run's outcome.
func readBack(ctx context.Context, c workflow.Client, store *recorder.MemoryStore, out io.Writer) error {
	page, err := store.Records(ctx, recorder.Query{RunQuery: workflow.RunQuery{Workflow: "sync"}})
	if err != nil {
		return err
	}
	if len(page.Runs) != 1 {
		return fmt.Errorf("%d sync records, want 1", len(page.Runs))
	}
	syncRef := page.Runs[0].Ref
	if err := waitRecorded(ctx, store, syncRef); err != nil {
		return err
	}
	// snippet:read
	r, err := store.Run(ctx, syncRef)
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "run %s outcome=%s (%s)\n", r.Status, r.Outcome, r.OutcomeNote)
	for _, s := range r.Steps {
		for _, e := range s.Events {
			fmt.Fprintf(out, "  %s %s %q %v\n", s.Step, e.Level, e.Message, e.Attrs)
		}
	}
	// snippet:end

	ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "cleanup"})
	if err != nil {
		return err
	}
	if err := waitRecorded(ctx, store, ref); err != nil {
		return err
	}
	clean, err := store.Run(ctx, ref)
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "cleanup %s outcome=%s (set by a step: %t)\n", clean.Status, clean.Outcome, clean.OutcomeSet)
	return nil
}

// runOnce registers the sync workflow on c, starts c, runs one job and
// waits for it to end.
func runOnce(ctx context.Context, c workflow.Client, step workflow.Handler) error {
	if err := c.Register(workflow.Workflow{Name: "sync", Steps: []workflow.Step{{Name: "fetch", Run: step}}}); err != nil {
		return err
	}
	if err := c.Start(ctx); err != nil {
		return err
	}
	ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "sync"})
	if err != nil {
		return err
	}
	deadline := time.Now().Add(runTimeout)
	for {
		st, err := c.Status(ctx, ref)
		if err != nil {
			return err
		}
		if st.IsTerminal() {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("sync did not finish within %s", runTimeout)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// waitRecorded waits until the run's record is finished: the run-finished
// observer writes it on the driver's goroutine, a moment after the run ends.
func waitRecorded(ctx context.Context, store recorder.Store, ref workflow.RunRef) error {
	deadline := time.Now().Add(runTimeout)
	for {
		r, err := store.Run(ctx, ref)
		if err == nil && r.Status.IsTerminal() {
			return nil
		}
		if err != nil && !errors.Is(err, workflow.ErrNotFound) {
			return err
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("no finished record within %s", runTimeout)
		}
		time.Sleep(5 * time.Millisecond)
	}
}
