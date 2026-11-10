// Command recorder-split records runs across a web node that only
// enqueues and a worker node that runs them, on the SQL driver over one
// SQLite file. The web node is built with sqlstore.WithEnqueueOnly, runs no
// run-end delivery, and so gets the recorder's EnqueuePlugin: it records
// each accepted run with its source and input. The worker gets the full
// Plugin over the same store and records the steps, their events and the
// end into the same record.
//
// It is the program behind the "Record runs from web nodes" sections of
// docs/drivers/hatchet.md and docs/observability/run-recorder.md: the
// guides copy its regions (marked "snippet:") and task docs:snippets fails
// when they drift. On Hatchet only the clients change.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/ubgo/sqlb"
	driversqlite "github.com/ubgo/sqlb/contrib/driver-sqlite"
	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/recorder"
	"github.com/ubgo/workflow/recorder/sqlrecorder"
	"github.com/ubgo/workflow/sqlstore"
)

// pollInterval is short so the demo is quick; the default is one second.
const pollInterval = 20 * time.Millisecond

// runTimeout bounds the wait for the run's end, generous for a slow host.
const runTimeout = 10 * time.Second

func main() {
	dir, err := os.MkdirTemp("", "recorder-split-")
	if err != nil {
		fmt.Fprintln(os.Stderr, "recorder-split:", err)
		os.Exit(1)
	}
	err = errors.Join(run(context.Background(), filepath.Join(dir, "app.db")), os.RemoveAll(dir))
	if err != nil {
		fmt.Fprintln(os.Stderr, "recorder-split:", err)
		os.Exit(1)
	}
}

// thumbnail is the workflow the worker runs; it records one event.
var thumbnail = workflow.Workflow{Name: "thumbnail", Steps: []workflow.Step{{Name: "resize", Run: func(ctx context.Context, _ []byte) ([]byte, error) {
	workflow.RecordEvent(ctx, "resized", slog.Int("width", 320))
	return []byte(`"thumb-1.png"`), nil
}}}}

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context, path string) (err error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	db, err := driversqlite.Open(path)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, db.Close()) }()

	// One recorder store for every node: here the jobs database itself.
	store, err := sqlrecorder.New(db, sqlrecorder.SQLite)
	if err != nil {
		return err
	}
	if err := store.Migrate(ctx); err != nil {
		return err
	}
	rec, err := recorder.New(store)
	if err != nil {
		return err
	}

	// snippet:web
	// The web node enqueues and never runs a step, so it has no run-end
	// hook: it takes the recorder's enqueue half only.
	webDriver, err := sqlstore.New(db, sqlb.SQLiteDialect{}, sqlstore.WithEnqueueOnly())
	if err != nil {
		return err
	}
	web, err := workflow.Use(webDriver, rec.EnqueuePlugin())
	if err != nil {
		return err
	}
	// snippet:end
	if err := webDriver.Migrate(ctx); err != nil { // usually the deploy step
		return err
	}

	// snippet:worker
	// A worker node takes the full plugin over the same store, and
	// registers and starts through the client Use returned.
	workerDriver, err := sqlstore.New(db, sqlb.SQLiteDialect{}, sqlstore.WithPollInterval(pollInterval))
	if err != nil {
		return err
	}
	worker, err := workflow.Use(workerDriver, rec.Plugin())
	if err != nil {
		return err
	}
	if err := worker.Register(thumbnail); err != nil {
		return err
	}
	if err := worker.Start(ctx); err != nil {
		return err
	}
	// snippet:end
	defer workerDriver.Stop()

	// snippet:enqueue
	job := workflow.WithSource(workflow.Job{Workflow: "thumbnail", Payload: []byte(`{"image":"img-1"}`)}, "api", "req-7")
	ref, err := web.Enqueue(ctx, job)
	if err != nil {
		return err
	}
	// snippet:end
	if r, err := store.Run(ctx, ref); err == nil {
		fmt.Printf("web node recorded: source=%s/%s input=%s\n", r.Source, r.SourceRef, r.Payload)
	} else {
		return err
	}

	r, err := waitRecorded(ctx, store, ref)
	if err != nil {
		return err
	}
	fmt.Printf("worker finished it: %s source=%s/%s outcome=%s\n", r.Status, r.Source, r.SourceRef, r.Outcome)
	for _, s := range r.Steps {
		fmt.Printf("  step %s attempt %d %s output=%s events=%d\n", s.Step, s.Attempt, s.Status, s.Output, len(s.Events))
	}
	page, err := store.Records(ctx, recorder.Query{RunQuery: workflow.RunQuery{Workflow: "thumbnail"}})
	if err != nil {
		return err
	}
	fmt.Println("records for the run:", len(page.Runs))
	return nil
}

// waitRecorded waits until the run's record is finished: the worker's
// run-finished observer writes it a moment after the run ends.
func waitRecorded(ctx context.Context, store recorder.Store, ref workflow.RunRef) (recorder.RunRecord, error) {
	deadline := time.Now().Add(runTimeout)
	for {
		r, err := store.Run(ctx, ref)
		if err == nil && r.Status.IsTerminal() {
			return r, nil
		}
		if err != nil && !errors.Is(err, workflow.ErrNotFound) {
			return r, err
		}
		if time.Now().After(deadline) {
			return r, fmt.Errorf("no finished record within %s", runTimeout)
		}
		time.Sleep(pollInterval)
	}
}
