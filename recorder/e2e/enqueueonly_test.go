package e2e_test

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/hatchet"
	"github.com/ubgo/workflow/recorder"
	"github.com/ubgo/workflow/sqlstore"
)

// splitNodes is one deployment split in two: a web node that only
// enqueues and a worker node that runs, over one backend and one recorder
// store.
type splitNodes struct {
	web    workflow.Client // enqueue-only, never started
	worker workflow.Client // registered and started
}

// recordAcrossNodes wraps the web node with EnqueuePlugin and the worker
// with Plugin over store, runs one job enqueued on the web node, and checks
// the one record both nodes wrote: the source from the web node, the steps,
// events and end from the worker.
func recordAcrossNodes(t *testing.T, nodes splitNodes, store recorder.Store, wfName string, wait time.Duration) {
	t.Helper()
	ctx := context.Background()
	rec, err := recorder.New(store, recorder.WithOnError(func(_ context.Context, op recorder.WriteOp, _ workflow.RunRef, err error) {
		t.Errorf("recorder %s: %v", op, err)
	}))
	if err != nil {
		t.Fatal(err)
	}
	web, err := workflow.Use(nodes.web, rec.EnqueuePlugin())
	if err != nil {
		t.Fatalf("EnqueuePlugin on the enqueue-only node: %v", err)
	}
	worker, err := workflow.Use(nodes.worker, rec.Plugin())
	if err != nil {
		t.Fatalf("Plugin on the worker: %v", err)
	}
	if err := worker.Register(workflow.Workflow{Name: wfName, Steps: []workflow.Step{{Name: "work", Run: func(ctx context.Context, in []byte) ([]byte, error) {
		workflow.RecordEvent(ctx, "worked", slog.String("node", "worker"))
		return []byte(`"done"`), nil
	}}}}); err != nil {
		t.Fatal(err)
	}
	runCtx, cancel := context.WithCancel(ctx)
	t.Cleanup(cancel)
	if err := worker.Start(runCtx); err != nil {
		t.Fatal(err)
	}
	ref, err := web.Enqueue(ctx, workflow.WithSource(workflow.Job{Workflow: wfName, Payload: []byte(`{"from":"web"}`)}, "api", "req-7"))
	if err != nil {
		t.Fatalf("Enqueue on the web node: %v", err)
	}
	deadline := time.Now().Add(wait)
	var r recorder.RunRecord
	for {
		r, err = store.Run(ctx, ref)
		if err == nil && r.Status.IsTerminal() {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("no finished record after %s: %+v, %v", wait, r, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if r.Status != workflow.StatusSucceeded || r.Source != "api" || r.SourceRef != "req-7" || string(r.Payload) != `{"from":"web"}` {
		t.Fatalf("record = status %q source %q/%q payload %s", r.Status, r.Source, r.SourceRef, r.Payload)
	}
	if len(r.Steps) != 1 || r.Steps[0].Status != recorder.StepSucceeded || len(r.Steps[0].Events) != 1 {
		t.Fatalf("steps = %+v", r.Steps)
	}
	p, err := store.Records(ctx, recorder.Query{RunQuery: workflow.RunQuery{Workflow: wfName}})
	if err != nil || len(p.Runs) != 1 {
		t.Fatalf("the two nodes left %d records, want one: %v", len(p.Runs), err)
	}
}

// TestEnqueueOnlyAndWorkerOnSQLStore splits the SQL driver into an
// enqueue-only web node and a worker on one database, recording into
// sqlrecorder in that database, on every engine.
func TestEnqueueOnlyAndWorkerOnSQLStore(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			db, prefix := e.open(t)
			newClient := func(opts ...sqlstore.Option) *sqlstore.Client {
				c, err := sqlstore.New(db, sqlstoreDialect(e.name), append([]sqlstore.Option{sqlstore.WithTablePrefix(prefix),
					sqlstore.WithPollInterval(testPoll), sqlstore.WithLease(testLease)}, opts...)...)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(c.Stop)
				return c
			}
			worker := newClient()
			if err := worker.Migrate(context.Background()); err != nil {
				t.Fatal(err)
			}
			web := newClient(sqlstore.WithEnqueueOnly())
			recordAcrossNodes(t, splitNodes{web: web, worker: worker}, newSQLStore(t, e, db, prefix), "split-"+randHex(3), 30*time.Second)
		})
	}
}

// TestEnqueueOnlyAndWorkerOnHatchet does the same against a live Hatchet
// engine (Config.EnqueueOnly for the web node), recording into SQLite.
func TestEnqueueOnlyAndWorkerOnHatchet(t *testing.T) {
	if os.Getenv(envHatchet) == "" {
		t.Skipf("%s unset: the Hatchet cases need a live engine (task recorder:e2e:hatchet)", envHatchet)
	}
	e := engines(t)[0]
	db, prefix := e.open(t)
	nodes := splitNodes{
		web:    hatchet.New(hatchet.Config{EnqueueOnly: true}),
		worker: hatchet.New(hatchet.Config{WorkerName: "rectest-split"}),
	}
	// The full Plugin needs a run-finished hook, which Hatchet's
	// enqueue-only client refuses: the reason EnqueuePlugin exists.
	probe, err := recorder.New(newSQLStore(t, e, db, prefix+"p"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := workflow.Use(hatchet.New(hatchet.Config{EnqueueOnly: true}), probe.Plugin()); !errors.Is(err, workflow.ErrUnsupported) {
		t.Fatalf("Plugin on Hatchet's enqueue-only client: %v, want ErrUnsupported", err)
	}
	recordAcrossNodes(t, nodes, newSQLStore(t, e, db, prefix), "rectest-split-"+randHex(3), 90*time.Second)
}
