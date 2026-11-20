package enginetest

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/sqlstore"
)

// versionLog records which client ran which step of which run version.
type versionLog struct {
	mu  sync.Mutex
	ran []string // "<client> <step> v<version>"
}

func (l *versionLog) add(s string) {
	l.mu.Lock()
	l.ran = append(l.ran, s)
	l.mu.Unlock()
}

func (l *versionLog) all() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.ran...)
}

// versioned is one definition of workflow "w": step a, a 50 ms sleep, then
// step b, each logging who ran it.
func versioned(who string, version, min int, log *versionLog) workflow.Workflow {
	step := func(name string) workflow.Handler {
		return func(ctx context.Context, in []byte) ([]byte, error) {
			info, _ := workflow.StepInfoFrom(ctx)
			log.add(fmt.Sprintf("%s %s v%d", who, name, info.Version))
			return in, nil
		}
	}
	return workflow.Workflow{Name: "w", Version: version, MinVersion: min, Steps: []workflow.Step{
		{Name: "a", Run: step("a")},
		{Name: "nap", After: []string{"a"}, Sleep: 50 * time.Millisecond},
		{Name: "b", After: []string{"nap"}, Run: step("b")},
	}}
}

func startVersioned(t *testing.T, e engine, db *sql.DB, prefix string, wf workflow.Workflow, opts ...sqlstore.Option) *sqlstore.Client {
	t.Helper()
	c := newClient(t, e, db, prefix, opts...)
	if err := c.Register(wf); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	return c
}

// TestWorkflowVersions (LO-11): while an old worker (v1) and a new one
// (v2) share the database, a run enqueued by v2 code is never admitted,
// run or advanced by the v1 worker, and waits until a v2 worker runs it;
// a v2 worker with MinVersion 0 drains a v1 run; a pinned worker
// (MinVersion 2) leaves the v1 run alone. RunInfo and StepInfo report the
// run's version.
func TestWorkflowVersions(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			ctx := context.Background()
			db, prefix := e.open(t)
			log := &versionLog{}
			old := startVersioned(t, e, db, prefix, versioned("old", 1, 0, log))
			web := startVersioned(t, e, db, prefix, versioned("web", 2, 0, log), sqlstore.WithEnqueueOnly())
			ref2, err := web.Enqueue(ctx, workflow.Job{Workflow: "w", Payload: []byte(`"v2"`)})
			if err != nil {
				t.Fatal(err)
			}
			ref1, err := old.Enqueue(ctx, workflow.Job{Workflow: "w", Payload: []byte(`"v1"`)})
			if err != nil {
				t.Fatal(err)
			}
			waitStatus(t, old, ref1, workflow.StatusSucceeded)
			time.Sleep(10 * testPoll)
			if st, _ := old.Status(ctx, ref2); st != workflow.StatusQueued {
				t.Fatalf("v2 run on a v1-only fleet is %v; want queued (waiting for a v2 worker)", st)
			}
			if info := runInfo(t, old, ref2); info.Version != 2 {
				t.Fatalf("RunInfo.Version = %d; want 2", info.Version)
			}
			if info := runInfo(t, old, ref1); info.Version != 1 {
				t.Fatalf("RunInfo.Version of the v1 run = %d; want 1", info.Version)
			}

			// A pinned v2 worker runs the v2 run; a v1 run enqueued now
			// waits for a worker that runs version 1.
			pinned := startVersioned(t, e, db, prefix, versioned("pinned", 2, 2, log))
			waitStatus(t, pinned, ref2, workflow.StatusSucceeded)
			old.Stop()
			ref1b, err := web.Enqueue(ctx, workflow.Job{Workflow: "w"}) // v2
			if err != nil {
				t.Fatal(err)
			}
			waitStatus(t, pinned, ref1b, workflow.StatusSucceeded)

			// With only v2 workers left, a v1 run (written by a v1 client
			// that does not run work) waits for the draining worker.
			oldWeb := startVersioned(t, e, db, prefix, versioned("oldweb", 1, 0, log), sqlstore.WithEnqueueOnly())
			ref1c, err := oldWeb.Enqueue(ctx, workflow.Job{Workflow: "w"})
			if err != nil {
				t.Fatal(err)
			}
			time.Sleep(10 * testPoll)
			if st, _ := pinned.Status(ctx, ref1c); st != workflow.StatusQueued {
				t.Fatalf("v1 run with only a pinned v2 worker is %v; want queued", st)
			}
			drain := startVersioned(t, e, db, prefix, versioned("drain", 2, 0, log))
			waitStatus(t, drain, ref1c, workflow.StatusSucceeded)

			for _, line := range log.all() {
				var who, step string
				var v int
				if _, err := fmt.Sscanf(line, "%s %s v%d", &who, &step, &v); err != nil {
					t.Fatal(line, err)
				}
				switch {
				case who == "old" && v != 1, who == "pinned" && v != 2, who == "drain" && v > 2:
					t.Errorf("%s: a worker ran a run of a version it does not run", line)
				}
			}
		})
	}
}

// TestWorkflowVersionSleepNotAdvancedByOldCode: a v2 run asleep between
// its steps is not woken by a v1 worker (which would apply its own step
// graph); it finishes once a v2 worker is back.
func TestWorkflowVersionSleepNotAdvancedByOldCode(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			ctx := context.Background()
			db, prefix := e.open(t)
			log := &versionLog{}
			v2 := startVersioned(t, e, db, prefix, versioned("new", 2, 0, log))
			ref, err := v2.Enqueue(ctx, workflow.Job{Workflow: "w"})
			if err != nil {
				t.Fatal(err)
			}
			waitFor(t, 10*time.Second, "the v2 run asleep after step a", func() bool {
				st, _ := v2.Status(ctx, ref)
				return st == workflow.StatusWaiting
			})
			v2.Stop()
			old := startVersioned(t, e, db, prefix, versioned("old", 1, 0, log))
			time.Sleep(200*time.Millisecond + 10*testPoll)
			if st, _ := old.Status(ctx, ref); st.IsTerminal() {
				t.Fatalf("the v1 worker advanced the sleeping v2 run to %v", st)
			}
			var nap string
			q := "SELECT status FROM " + e.dialect.QuoteIdent(prefix+"steps") + " WHERE run_id = " + e.dialect.Placeholder(1) + " AND name = 'nap'"
			if err := db.QueryRowContext(ctx, q, string(ref.RunID)).Scan(&nap); err != nil {
				t.Fatal(err)
			}
			if nap != "sleeping" {
				t.Fatalf("the v2 run's sleep is %q after the v1 worker ran past its end; want sleeping (only a v2 worker wakes it)", nap)
			}
			again := startVersioned(t, e, db, prefix, versioned("new2", 2, 0, log))
			waitStatus(t, again, ref, workflow.StatusSucceeded)
			if got := log.all(); len(got) != 2 || got[1] != "new2 b v2" {
				t.Fatalf("log = %v; want step a by new and step b by new2, both v2", got)
			}
		})
	}
}
