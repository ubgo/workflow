package sqlstore_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/sqlstore"
)

// runInfo reads one run through RunQuerier.
func runInfo(t *testing.T, c *sqlstore.Client, ref workflow.RunRef) workflow.RunInfo {
	t.Helper()
	page, err := c.Runs(context.Background(), workflow.RunQuery{Ref: ref})
	if err != nil || len(page.Runs) != 1 {
		t.Fatalf("Runs(%v): %d runs, %v", ref, len(page.Runs), err)
	}
	return page.Runs[0]
}

// TestErrorHistoryIsBounded runs a step that fails five times and checks
// WithErrorHistory keeps only the newest entries, oldest first, and that
// a non-positive bound records nothing.
func TestErrorHistoryIsBounded(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			for _, tc := range []struct {
				keep int
				want []int // attempt numbers kept
			}{{2, []int{4, 5}}, {0, nil}} {
				t.Run(fmt.Sprintf("keep %d", tc.keep), func(t *testing.T) {
					db, prefix := e.open(t)
					wf := workflow.Workflow{Name: "flaky", Retry: workflow.RetryPolicy{Max: 4}, Steps: []workflow.Step{{Name: "s",
						Run: func(ctx context.Context, _ []byte) ([]byte, error) {
							info, _ := workflow.StepInfoFrom(ctx)
							return nil, fmt.Errorf("boom %d", info.Attempt)
						}}}}
					c := newClient(t, e, db, prefix, sqlstore.WithErrorHistory(tc.keep))
					if err := c.Register(wf); err != nil {
						t.Fatal(err)
					}
					ctx, cancel := context.WithCancel(context.Background())
					t.Cleanup(cancel)
					if err := c.Start(ctx); err != nil {
						t.Fatal(err)
					}
					ref, err := c.Enqueue(ctx, workflow.Job{Workflow: wf.Name})
					if err != nil {
						t.Fatal(err)
					}
					waitFor(t, 20*time.Second, "the run to fail", func() bool {
						st, _ := c.Status(ctx, ref)
						return st == workflow.StatusFailed
					})
					got := runInfo(t, c, ref).Errors
					if len(got) != len(tc.want) {
						t.Fatalf("history %+v, want attempts %v", got, tc.want)
					}
					for i, a := range tc.want {
						if got[i].Attempt != a || got[i].Step != "s" || got[i].Error != fmt.Sprintf("boom %d", a) || got[i].At.IsZero() {
							t.Errorf("history[%d] = %+v, want attempt %d of step s", i, got[i], a)
						}
					}
				})
			}
		})
	}
}

// TestEnqueueOnlyLeavesKeysToTheWorker enqueues, on an enqueue-only client
// that never registers the workflow, a job of a workflow whose default
// queue is "slow"; the worker serves only "slow". The run can start only
// if the admitting worker filled in the queue the enqueuing side could not
// know.
func TestEnqueueOnlyLeavesKeysToTheWorker(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			db, prefix := e.open(t)
			ran := make(chan struct{}, 1)
			wf := workflow.Workflow{Name: "routed", Queue: "slow", Steps: []workflow.Step{{Name: "s", Run: func(context.Context, []byte) ([]byte, error) {
				ran <- struct{}{}
				return nil, nil
			}}}}
			worker := newClient(t, e, db, prefix, sqlstore.WithQueues(workflow.QueueConfig{Name: "slow"}))
			if err := worker.Register(wf); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			if err := worker.Start(ctx); err != nil {
				t.Fatal(err)
			}
			web := newClient(t, e, db, prefix, sqlstore.WithEnqueueOnly())
			if err := web.Start(ctx); err != nil {
				t.Fatalf("Start of an enqueue-only client: %v, want nil", err)
			}
			ref, err := web.Enqueue(ctx, workflow.Job{Workflow: wf.Name})
			if err != nil {
				t.Fatal(err)
			}
			select {
			case <-ran:
			case <-time.After(10 * time.Second):
				t.Fatal("the worker never ran the job: its queue was not evaluated at admission")
			}
			waitFor(t, 10*time.Second, "the run to succeed", func() bool {
				st, _ := web.Status(ctx, ref)
				return st == workflow.StatusSucceeded
			})
			if q := runInfo(t, worker, ref).Queue; q != "slow" {
				t.Errorf("stored queue %q, want slow", q)
			}
			if _, err := web.Enqueue(ctx, workflow.Job{Workflow: ""}); !errors.Is(err, workflow.ErrInvalid) {
				t.Errorf("an invalid job on an enqueue-only client: %v, want ErrInvalid", err)
			}
			web.Stop()
			select {
			case <-web.Done():
			default:
				t.Error("Done is open after Stop of an enqueue-only client")
			}
			if _, err := web.Enqueue(ctx, workflow.Job{Workflow: wf.Name}); !errors.Is(err, workflow.ErrNotStarted) {
				t.Errorf("Enqueue after Stop: %v, want ErrNotStarted", err)
			}
		})
	}
}

// TestTriggerOverlapRace triggers one OverlapSkip schedule from several
// clients at once while its runs block: exactly one trigger may start a
// run, because the fire moves the last run with a compare-and-set on the
// value the overlap decision read.
func TestTriggerOverlapRace(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			db, prefix := e.open(t)
			release := make(chan struct{})
			t.Cleanup(func() { close(release) })
			wf := workflow.Workflow{Name: "nightly", Steps: []workflow.Step{{Name: "s", Run: func(ctx context.Context, _ []byte) ([]byte, error) {
				select {
				case <-release:
				case <-ctx.Done():
				}
				return nil, nil
			}}}}
			cs := startCluster(t, e, db, prefix, wf)
			id, err := cs[0].Schedule(context.Background(), workflow.Schedule{Workflow: wf.Name, Cron: "0 3 * * *", Location: time.UTC, Overlap: workflow.OverlapSkip})
			if err != nil {
				t.Fatal(err)
			}
			const callers = 6
			var wg sync.WaitGroup
			errs := make([]error, callers)
			for i := range callers {
				wg.Add(1)
				go func() {
					defer wg.Done()
					_, errs[i] = cs[i%nodes].TriggerSchedule(context.Background(), id)
				}()
			}
			wg.Wait()
			fired := 0
			for _, err := range errs {
				switch {
				case err == nil:
					fired++
				case errors.Is(err, workflow.ErrOverlap):
				default:
					t.Errorf("TriggerSchedule: %v, want nil or ErrOverlap", err)
				}
			}
			if fired != 1 {
				t.Fatalf("%d triggers started a run, want exactly 1 (errors %v)", fired, errs)
			}
		})
	}
}

// TestForcedIsAFlagNotAMessage fails one run with a step error whose text
// begins exactly like a forced failure's and force-fails another: only the
// second run's end may match workflow.ErrForced. Then the forced run is
// retried and fails on its own, and must no longer read as forced.
func TestForcedIsAFlagNotAMessage(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			db, prefix := e.open(t)
			hold := make(chan struct{})
			wf := workflow.Workflow{Name: "f", Steps: []workflow.Step{{Name: "s", Run: func(ctx context.Context, in []byte) ([]byte, error) {
				if string(in) == "hold" {
					select {
					case <-hold:
					case <-ctx.Done():
						return nil, ctx.Err()
					}
				}
				return nil, workflow.Permanent(errors.New(workflow.ErrForced.Error() + ": not really"))
			}}}}
			c := newClient(t, e, db, prefix)
			if err := c.Register(wf); err != nil {
				t.Fatal(err)
			}
			var mu sync.Mutex
			ends := map[workflow.RunID][]error{}
			if err := c.OnRunFinished(func(_ context.Context, ev workflow.RunEvent) {
				mu.Lock()
				defer mu.Unlock()
				ends[ev.Ref.RunID] = append(ends[ev.Ref.RunID], ev.Err)
			}); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			if err := c.Start(ctx); err != nil {
				t.Fatal(err)
			}
			endsOf := func(id workflow.RunID) []error {
				mu.Lock()
				defer mu.Unlock()
				return append([]error(nil), ends[id]...)
			}
			natural, err := c.Enqueue(ctx, workflow.Job{Workflow: wf.Name})
			if err != nil {
				t.Fatal(err)
			}
			forced, err := c.Enqueue(ctx, workflow.Job{Workflow: wf.Name, Payload: []byte("hold")})
			if err != nil {
				t.Fatal(err)
			}
			if err := c.ForceFail(ctx, forced, "stuck"); err != nil {
				t.Fatal(err)
			}
			waitFor(t, 10*time.Second, "both run ends delivered", func() bool {
				return len(endsOf(natural.RunID)) == 1 && len(endsOf(forced.RunID)) == 1
			})
			if err := endsOf(natural.RunID)[0]; err == nil || errors.Is(err, workflow.ErrForced) {
				t.Errorf("a step error that only looks forced: %v; matches ErrForced %v, want false", err, errors.Is(err, workflow.ErrForced))
			}
			if err := endsOf(forced.RunID)[0]; !errors.Is(err, workflow.ErrForced) {
				t.Errorf("a force-failed run's end: %v, want ErrForced", err)
			}

			// No message the driver writes for a natural failure starts like a
			// forced one, so the stored text is made to, and the end delivered
			// again: the text alone must not make it forced.
			imitated := workflow.ErrForced.Error() + ": imitated"
			q := "UPDATE " + e.dialect.QuoteIdent(prefix+"runs") + " SET err = '" + imitated + "', notified = 0, notify_until = 0 WHERE id = '" + string(natural.RunID) + "'"
			if _, err := db.ExecContext(ctx, q); err != nil {
				t.Fatal(err)
			}
			c.Wake()
			waitFor(t, 10*time.Second, "the imitated end delivered", func() bool { return len(endsOf(natural.RunID)) == 2 })
			if err := endsOf(natural.RunID)[1]; err == nil || !strings.HasSuffix(err.Error(), imitated) || errors.Is(err, workflow.ErrForced) {
				t.Errorf("a failure whose text begins like a forced one: %v; matches ErrForced %v, want false", err, errors.Is(err, workflow.ErrForced))
			}

			close(hold)
			if _, err := c.Retry(ctx, forced); err != nil {
				t.Fatal(err)
			}
			waitFor(t, 10*time.Second, "the retried run's end delivered", func() bool { return len(endsOf(forced.RunID)) == 2 })
			if err := endsOf(forced.RunID)[1]; err == nil || errors.Is(err, workflow.ErrForced) {
				t.Errorf("the retried run failed on its own: %v; matches ErrForced %v, want false", err, errors.Is(err, workflow.ErrForced))
			}
		})
	}
}
