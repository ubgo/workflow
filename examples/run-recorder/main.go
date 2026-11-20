// Command run-recorder keeps a run history the app owns: a recorder on the
// memory driver writes every run into a SQLite table through sqlrecorder,
// with the run's trigger source, its steps and their timeline events, the
// outcome a step set, and the input with a card number redacted. It then
// reads the history back three ways: one run with its steps, a listing
// filtered by source, and the admin API reading the recorder instead of
// the driver. A Reconciler pass closes the loop.
//
// It is the program behind docs/observability/run-recorder.md: the guide
// copies its regions (marked "snippet:") and task docs:snippets fails when
// they drift.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	driversqlite "github.com/ubgo/sqlb/contrib/driver-sqlite"
	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/admin"
	"github.com/ubgo/workflow/memory"
	"github.com/ubgo/workflow/middleware"
	"github.com/ubgo/workflow/recorder"
	"github.com/ubgo/workflow/recorder/sqlrecorder"
)

func main() {
	dir, err := os.MkdirTemp("", "run-recorder")
	if err != nil {
		fmt.Fprintln(os.Stderr, "run-recorder:", err)
		os.Exit(1)
	}
	err = errors.Join(run(context.Background(), filepath.Join(dir, "runs.db")), os.RemoveAll(dir))
	if err != nil {
		fmt.Fprintln(os.Stderr, "run-recorder:", err)
		os.Exit(1)
	}
}

// requireSource is a one-line plugin that adds middleware.RequireSource
// to every enqueue, so no run reaches the history without its source.
type requireSource struct{}

func (requireSource) Name() string { return "require-source" }

func (requireSource) EnqueueMiddleware() []workflow.EnqueueMiddleware {
	return []workflow.EnqueueMiddleware{middleware.RequireSource()}
}

// importOrders is the workflow the program runs: fetch records a timeline
// event, save sets a partial outcome.
func importOrders() workflow.Workflow {
	// snippet:steps
	fetch := func(ctx context.Context, in []byte) ([]byte, error) {
		workflow.RecordEvent(ctx, "fetched orders", slog.Int("rows", 120), slog.String("shop", "s-1"))
		return []byte(`{"rows":120}`), nil
	}
	save := func(ctx context.Context, in []byte) ([]byte, error) {
		// The run succeeds either way; the outcome says how well.
		_ = workflow.SetOutcome(ctx, workflow.OutcomePartial, "3 of 120 rows rejected")
		return []byte(`{"saved":117}`), nil
	}
	// snippet:end
	return workflow.Workflow{Name: "import-orders", Steps: []workflow.Step{
		{Name: "fetch", Run: fetch},
		{Name: "save", After: []string{"fetch"}, Run: save},
	}}
}

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context, path string) (err error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	db, err := driversqlite.Open(path)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, db.Close()) }()

	// snippet:wire
	store, err := sqlrecorder.New(db, sqlrecorder.SQLite) // the app's own *sql.DB
	if err != nil {
		return err
	}
	if err := store.Migrate(ctx); err != nil { // idempotent, on every start
		return err
	}
	redact, err := recorder.JSONPaths("card.number")
	if err != nil {
		return err
	}
	rec, err := recorder.New(store, recorder.WithRedactor(redact))
	if err != nil {
		return err
	}
	c, err := workflow.Use(memory.New(), rec.Plugin(), requireSource{})
	if err != nil {
		return err
	}
	// snippet:end

	if err := c.Register(importOrders()); err != nil {
		return err
	}
	if err := c.Start(ctx); err != nil {
		return err
	}

	// snippet:enqueue
	payload := []byte(`{"shop":"s-1","card":{"number":"4111111111111111"}}`)
	_, err = c.Enqueue(ctx, workflow.Job{Workflow: "import-orders", Payload: payload})
	fmt.Println("no source refused:", errors.Is(err, workflow.ErrInvalid))

	job := workflow.WithSource(workflow.Job{Workflow: "import-orders", Payload: payload}, "webhook", "delivery-42")
	ref, err := c.Enqueue(ctx, job)
	if err != nil {
		return err
	}
	// snippet:end

	if err := waitRecorded(ctx, store, ref); err != nil {
		return err
	}

	// snippet:read
	r, err := store.Run(ctx, ref)
	if err != nil {
		return err
	}
	fmt.Printf("run %s source=%s/%s outcome=%s (%s)\n", r.Status, r.Source, r.SourceRef, r.Outcome, r.OutcomeNote)
	fmt.Printf("input as stored: %s\n", r.Payload)
	for _, s := range r.Steps {
		fmt.Printf("  step %s attempt %d %s output=%s\n", s.Step, s.Attempt, s.Status, s.Output)
		for _, e := range s.Events {
			fmt.Printf("    event %q %v\n", e.Message, e.Attrs)
		}
	}
	page, err := store.Records(ctx, recorder.Query{Sources: []workflow.Source{"webhook"}, Outcomes: []workflow.Outcome{workflow.OutcomePartial}})
	if err != nil {
		return err
	}
	fmt.Println("webhook runs with a partial outcome:", len(page.Runs))
	// snippet:end

	// snippet:admin
	a := admin.New(c, admin.WithRunQuerier(store, recorder.QueryCaps))
	info, err := a.Run(ctx, ref)
	if err != nil {
		return err
	}
	fmt.Println("admin reads the recorder:", info.Status)
	// snippet:end

	// snippet:reconcile
	rc, err := recorder.NewReconciler(c, store, recorder.WithStaleAfter(10*time.Minute))
	if err != nil {
		return err
	}
	rep, err := rc.Pass(ctx) // or go rc.Run(ctx) next to the client
	if err != nil {
		return err
	}
	fmt.Printf("reconcile: checked=%d finished=%d stale=%d\n", rep.Checked, rep.Finished, rep.Stale)
	// snippet:end
	return nil
}

// waitRecorded waits until the run's record is finished: the run-finished
// observer writes it on the driver's goroutine, a moment after the run ends.
func waitRecorded(ctx context.Context, store *sqlrecorder.Store, ref workflow.RunRef) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	for {
		r, err := store.Run(ctx, ref)
		if err == nil && r.Status.IsTerminal() {
			return nil
		}
		if err != nil && !errors.Is(err, workflow.ErrNotFound) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// Interface check: the SQL store is a workflow.RunQuerier, which is what
// admin.WithRunQuerier takes.
var _ workflow.RunQuerier = (*sqlrecorder.Store)(nil)
