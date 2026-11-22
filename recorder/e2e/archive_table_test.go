package e2e_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/recorder"
	"github.com/ubgo/workflow/recorder/sqlrecorder"
)

// TestSQLRecorderArchiveTableExactlyOnce: on every engine, prunes with
// WithArchiveTable that are cancelled at random points (inside the move
// transaction too) and prunes racing from two stores on one database
// leave each expired run in exactly one place: the archive table holds
// every one of them once, whole, and the hot tables only the live runs.
//
//ds:def id=e2e-testsqlrecorderarchivetableexactlyonce-c7vjhu87 owner=@khanakia stability=stable
func TestSQLRecorderArchiveTableExactlyOnce(t *testing.T) {
	const expired, live = 30, 3
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			ctx := context.Background()
			db, prefix := e.open(t)
			clk := clock.NewFake(start)
			ret := sqlrecorder.WithRetention(recorder.Retention{RetentionRule: recorder.RetentionRule{Runs: time.Hour}, Batch: 3})
			a := newSQLStore(t, e, db, prefix, ret, sqlrecorder.WithClock(clk), sqlrecorder.WithArchiveTable())
			b := newSQLStore(t, e, db, prefix, ret, sqlrecorder.WithClock(clk), sqlrecorder.WithArchiveTable())
			write := func(id string, finish bool) {
				ref := workflow.RunRef{RunID: workflow.RunID(id)}
				must(t, a.UpsertRun(ctx, recorder.RunWrite{Ref: ref, Workflow: "w", Labels: map[string]string{"id": id}, Status: workflow.StatusRunning, EnqueuedAt: start, At: start}))
				must(t, a.StartStep(ctx, recorder.StepStart{Ref: ref, Step: "s", Attempt: 1, Input: []byte(`"in"`), At: start}))
				must(t, a.AddEvent(ctx, recorder.EventWrite{Ref: ref, Step: "s", Attempt: 1, Message: "m " + id, At: start}))
				if finish {
					must(t, a.FinishRun(ctx, recorder.Finish{Ref: ref, Status: workflow.StatusSucceeded, FinishedAt: start, At: start}))
				}
			}
			want := map[string]recorder.RunRecord{}
			for i := range expired {
				id := fmt.Sprintf("old%02d", i)
				write(id, true)
				r, err := a.Run(ctx, workflow.RunRef{RunID: workflow.RunID(id)})
				must(t, err)
				want[r.Key()] = r
			}
			for i := range live {
				write(fmt.Sprintf("live%d", i), false)
			}
			clk.Advance(2 * time.Hour)

			var (
				mu           sync.Mutex
				deleted, cut int
			)
			prune := func(s *sqlrecorder.Store, ctx context.Context) {
				res, err := s.Prune(ctx)
				if err != nil && ctx.Err() == nil {
					t.Errorf("prune: %v", err) // only a cancel may stop a prune here
				}
				mu.Lock()
				deleted += res.RunsDeleted
				if err != nil {
					cut++
				}
				mu.Unlock()
			}
			rng := rand.New(rand.NewPCG(1, 2))
			for round := range 12 {
				cctx, cancel := context.WithCancel(ctx)
				// Later rounds allow longer runs, so some batches commit and some roll back.
				time.AfterFunc(time.Duration(rng.IntN((round+1)*8000))*time.Microsecond, cancel)
				if round%3 == 0 { // two processes prune at once
					var wg sync.WaitGroup
					wg.Add(2)
					go func() { defer wg.Done(); prune(a, cctx) }()
					go func() { defer wg.Done(); prune(b, cctx) }()
					wg.Wait()
				} else {
					prune(a, cctx)
				}
				cancel()
			}
			mu.Lock()
			t.Logf("the cut-off rounds deleted %d of %d runs", deleted, expired)
			mu.Unlock()
			prune(a, ctx)
			prune(b, ctx)

			t.Logf("%d prunes were cut off by a cancel", cut)
			// A cancel that races a commit can leave its outcome unknown to
			// the client, so a committed batch may go uncounted; a run is
			// never counted twice.
			if deleted > expired {
				t.Errorf("the prunes deleted %d runs in all, more than the %d expired", deleted, expired)
			}
			got := map[string]int{}
			cursor := ""
			for {
				page, err := a.ArchivedRuns(ctx, sqlrecorder.ArchiveQuery{Limit: 7, Cursor: cursor})
				must(t, err)
				for _, r := range page.Runs {
					got[r.Key()]++
					if w, ok := want[r.Key()]; !ok || !reflect.DeepEqual(r, w) {
						t.Errorf("archived %s =\n%+v\nwant\n%+v", r.Key(), r, w)
					}
				}
				if page.Next == "" {
					break
				}
				cursor = page.Next
			}
			if len(got) != expired {
				t.Errorf("archive holds %d runs, want %d", len(got), expired)
			}
			for k, n := range got {
				if n != 1 {
					t.Errorf("%s archived %d times", k, n)
				}
			}
			page, err := a.Records(ctx, recorder.Query{})
			must(t, err)
			if len(page.Runs) != live {
				t.Errorf("hot tables hold %d runs, want the %d live ones", len(page.Runs), live)
			}
			for k := range want {
				ref := workflow.RunRef{RunID: workflow.RunID(k[2:])}
				if _, err := a.Run(ctx, ref); !errors.Is(err, workflow.ErrNotFound) {
					t.Errorf("%s still in the hot tables: %v", k, err)
				}
				if _, err := a.ArchivedRun(ctx, ref); err != nil {
					t.Errorf("ArchivedRun(%s): %v", k, err)
				}
			}
		})
	}
}

// must fails the test on a non-nil error.
func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
