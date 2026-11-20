package enginetest

import (
	"context"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
)

// TestBackfillStartsEachTickOnce: Backfill starts one run per tick with
// the tick in its labels, a repeat after the runs finished starts nothing
// (the run-key lookup), an overlapping range starts only the new ticks,
// and FireAtStartup with StartupOncePer starts one run per day.
func TestBackfillStartsEachTickOnce(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			db, prefix := e.open(t)
			var mu sync.Mutex
			var ticks []time.Time
			wf := workflow.Workflow{Name: "report", Steps: []workflow.Step{{Name: "s", Run: func(ctx context.Context, _ []byte) ([]byte, error) {
				info, _ := workflow.StepInfoFrom(ctx)
				at, _ := info.TickAt()
				mu.Lock()
				ticks = append(ticks, at)
				mu.Unlock()
				return nil, nil
			}}}}
			c := newClient(t, e, db, prefix)
			ctx := startWith(t, c, wf)
			s := workflow.Schedule{Name: "daily", Workflow: "report", Cron: "0 2 * * *", Location: time.UTC}
			jan := func(d int) time.Time { return time.Date(2026, 1, d, 0, 0, 0, 0, time.UTC) }
			wait := func(refs []workflow.RunRef) {
				for _, r := range refs {
					waitFor(t, 10*time.Second, "a backfilled run", func() bool { st, _ := c.Status(ctx, r); return st == workflow.StatusSucceeded })
				}
			}
			refs, err := workflow.Backfill(ctx, c, workflow.BackfillSpec{Schedule: &s, From: jan(1), To: jan(4)})
			if err != nil || len(refs) != 3 {
				t.Fatalf("Backfill = %v, %v", refs, err)
			}
			wait(refs)
			again, err := workflow.Backfill(ctx, c, workflow.BackfillSpec{Schedule: &s, From: jan(1), To: jan(4)})
			if err != nil || !slices.Equal(again, refs) {
				t.Fatalf("repeat = %v, %v; want %v", again, err, refs)
			}
			more, err := workflow.Backfill(ctx, c, workflow.BackfillSpec{Schedule: &s, From: jan(3), To: jan(5)})
			if err != nil || len(more) != 2 || more[0] != refs[2] {
				t.Fatalf("overlap = %v, %v", more, err)
			}
			wait(more)
			mu.Lock()
			got := slices.Clone(ticks)
			mu.Unlock()
			slices.SortFunc(got, func(a, b time.Time) int { return a.Compare(b) })
			if len(got) != 4 || !got[0].Equal(jan(1).Add(2*time.Hour)) || !got[3].Equal(jan(4).Add(2*time.Hour)) {
				t.Fatalf("runs for ticks %v, want 02:00 on Jan 1-4, each once", got)
			}

			day := workflow.StartupOncePer(workflow.PeriodDay, time.UTC)
			boot := workflow.StartupClock(clock.NewFake(start))
			first, err := workflow.FireAtStartup(ctx, c, s, day, boot)
			if err != nil {
				t.Fatal(err)
			}
			wait([]workflow.RunRef{first})
			if second, err := workflow.FireAtStartup(ctx, c, s, day, boot); err != nil || second != first {
				t.Fatalf("a second boot the same day = %v, %v; want %v", second, err, first)
			}
		})
	}
}
