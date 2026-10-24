package workflow_test

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/memory"
)

func TestUpstreamLimitLeakyBucket(t *testing.T) {
	clk := clock.NewFake(time.Date(2026, 10, 14, 9, 0, 0, 0, time.UTC))
	lim, err := workflow.NewUpstreamLimit(nil, workflow.BucketSpec{Capacity: 4, LeakPerSecond: 2}, clk)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for range 4 {
		if err := lim.Take(ctx, "shop-1", 1); err != nil {
			t.Fatal(err)
		}
	}
	err = lim.Take(ctx, "shop-1", 1)
	if !errors.Is(err, workflow.ErrThrottled) {
		t.Fatalf("full bucket = %v, want ErrThrottled", err)
	}
	if d, ok := workflow.RetryAfterDelay(err, 1); !ok || d != 500*time.Millisecond {
		t.Fatalf("retry after = %v, %v; want 500ms", d, ok)
	}
	if err := lim.Take(ctx, "shop-2", 4); err != nil {
		t.Fatalf("other key shares the bucket: %v", err)
	}
	clk.Advance(500 * time.Millisecond)
	if err := lim.Take(ctx, "shop-1", 1); err != nil {
		t.Fatalf("after leak: %v", err)
	}
	// Cost based: a cost of 3 needs 3 units of room.
	clk.Advance(time.Second) // level 4 - 2 = 2
	if w, _ := lim.Wait(ctx, "shop-1", 3); w != 500*time.Millisecond {
		t.Fatalf("wait for cost 3 = %v, want 500ms", w)
	}
	// Observe follows the upstream's count.
	if err := lim.Observe(ctx, "shop-1", 0); err != nil {
		t.Fatal(err)
	}
	if err := lim.Take(ctx, "shop-1", 4); err != nil {
		t.Fatalf("after observe 0: %v", err)
	}
	if err := lim.TakeOrSnooze(ctx, "shop-1", 1); err == nil {
		t.Fatal("full bucket did not snooze")
	} else if d, ok := workflow.SnoozeDelay(err); !ok || d != 500*time.Millisecond {
		t.Fatalf("snooze = %v, %v; want 500ms", d, ok)
	}
	clk.Advance(500 * time.Millisecond)
	if err := lim.TakeOrSnooze(ctx, "shop-1", 1); err != nil {
		t.Fatalf("after leak: %v", err)
	}
	for _, cost := range []float64{-1, 5} {
		if err := lim.Take(ctx, "k", cost); !errors.Is(err, workflow.ErrInvalid) || !workflow.IsPermanent(err) {
			t.Errorf("cost %v = %v, want Permanent ErrInvalid", cost, err)
		}
	}
	if _, err := workflow.NewUpstreamLimit(nil, workflow.BucketSpec{Capacity: 1}, clk); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("zero leak = %v", err)
	}
}

func TestPeriodKey(t *testing.T) {
	ny, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Skip("no tz database")
	}
	// 2026-01-01 03:00 UTC is still 2025-12-31 in New York.
	at := time.Date(2026, 1, 1, 3, 0, 0, 0, time.UTC)
	cases := []struct {
		p    workflow.Period
		loc  *time.Location
		want string
	}{
		{workflow.PeriodDay, time.UTC, "2026-01-01"},
		{workflow.PeriodDay, ny, "2025-12-31"},
		{workflow.PeriodMonth, ny, "2025-12"},
		{workflow.PeriodQuarter, time.UTC, "2026-Q1"},
		{workflow.PeriodQuarter, ny, "2025-Q4"},
		{workflow.PeriodYear, ny, "2025"},
		{workflow.PeriodWeek, time.UTC, "2026-W01"},
		{workflow.PeriodWeek, ny, "2026-W01"}, // Wed 2025-12-31 is in ISO week 1 of 2026
	}
	for _, c := range cases {
		got, err := workflow.PeriodKey(at, c.p, c.loc)
		if err != nil || got != c.want {
			t.Errorf("PeriodKey(%s, %v) = %q, %v; want %q", c.p, c.loc, got, err, c.want)
		}
	}
	if _, err := workflow.PeriodKey(at, workflow.PeriodDay, time.Local); !errors.Is(err, workflow.ErrInvalid) {
		t.Errorf("time.Local = %v", err)
	}
	if _, err := workflow.PeriodKey(at, "fortnight", time.UTC); !errors.Is(err, workflow.ErrInvalid) {
		t.Errorf("unknown period = %v", err)
	}
	job, err := workflow.OncePerPeriod(workflow.Job{Workflow: "bill"}, "cust-42", workflow.PeriodMonth, at, ny)
	if err != nil || job.IdempotencyKey != "period/cust-42/2025-12" {
		t.Fatalf("OncePerPeriod = %q, %v", job.IdempotencyKey, err)
	}
	if _, err := workflow.OncePerPeriod(job, "cust-42", workflow.PeriodMonth, at, ny); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("existing key = %v", err)
	}
}

func TestRunDataAcrossSteps(t *testing.T) {
	type cursor struct{ Page int }
	var got atomic.Value
	wf := workflow.Workflow{Name: "data", Steps: []workflow.Step{
		{Name: "a", Run: func(ctx context.Context, _ []byte) ([]byte, error) {
			if _, ok, err := workflow.RunData[cursor](ctx, "cursor"); ok || err != nil {
				return nil, workflow.Permanent(errors.New("value before it was set"))
			}
			return nil, workflow.SetRunData(ctx, "cursor", cursor{Page: 7})
		}},
		{Name: "b", After: []string{"a"}, Run: func(ctx context.Context, _ []byte) ([]byte, error) {
			v, ok, err := workflow.RunData[cursor](ctx, "cursor")
			if err != nil || !ok {
				return nil, workflow.Permanent(errors.New("missing"))
			}
			got.Store(v.Page)
			return nil, nil
		}},
	}}
	c := flowClient(t, workflow.NewMemoryJournal(), wf)
	if st := waitRun(t, c, enqueue(t, c, workflow.Job{Workflow: "data"})); st != workflow.StatusSucceeded {
		t.Fatalf("status = %v", st)
	}
	if got.Load() != 7 {
		t.Fatalf("b read %v", got.Load())
	}
	if err := workflow.SetRunData(context.Background(), "x", 1); !errors.Is(err, workflow.ErrNoJournal) {
		t.Fatalf("no journal = %v", err)
	}
}

func TestProcessChunksResumesWithProgress(t *testing.T) {
	var mu sync.Mutex
	var reports []workflow.Progress
	var calls []int64
	record := func(next workflow.Handler) workflow.Handler {
		return func(ctx context.Context, in []byte) ([]byte, error) {
			return next(workflow.WithProgressReporter(ctx, workflow.ProgressFunc(func(_ context.Context, p workflow.Progress) error {
				mu.Lock()
				reports = append(reports, p)
				mu.Unlock()
				return nil
			})), in)
		}
	}
	failed := false
	wf := workflow.Workflow{Name: "export", Retry: workflow.RetryPolicy{Max: 1}, Steps: []workflow.Step{{Name: "s",
		Run: func(ctx context.Context, _ []byte) ([]byte, error) {
			n, err := workflow.ProcessChunks(ctx, "rows", workflow.ChunkOptions{Total: 25, Size: 10}, func(_ context.Context, off, size int64) (int64, error) {
				calls = append(calls, off)
				if off == 10 && !failed {
					failed = true
					return 0, errors.New("disk full")
				}
				return min(size, 25-off), nil
			})
			if err != nil {
				return nil, err
			}
			if n != 25 {
				return nil, workflow.Permanent(errors.New("wrong count"))
			}
			return nil, nil
		}}}}
	c, err := workflow.Use(memory.New(memory.WithMiddleware(record)), workflow.Journal(workflow.NewMemoryJournal()))
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Register(wf); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if st := waitRun(t, c, enqueue(t, c, workflow.Job{Workflow: "export"})); st != workflow.StatusSucceeded {
		t.Fatalf("status = %v", st)
	}
	if want := []int64{0, 10, 10, 20}; !slices.Equal(calls, want) {
		t.Fatalf("chunk offsets = %v, want %v (chunk 0 not redone)", calls, want)
	}
	if len(reports) != 3 || reports[0].Percent != 40 || reports[2].Percent != 100 || reports[2].Message != "25 of 25 items" {
		t.Fatalf("reports = %+v", reports)
	}

	// No progress reporter: the step fails rather than hiding it.
	_, err = workflow.RunInline(context.Background(), workflow.Workflow{Name: "x", Steps: []workflow.Step{{Name: "s",
		Run: func(ctx context.Context, _ []byte) ([]byte, error) {
			_, err := workflow.ProcessChunks(ctx, "rows", workflow.ChunkOptions{Total: 1, Size: 1}, func(context.Context, int64, int64) (int64, error) { return 1, nil })
			return nil, err
		}}}}, workflow.Job{Workflow: "x"}, workflow.InlineMiddleware(workflow.WithJournal(workflow.NewMemoryJournal())))
	if !errors.Is(err, workflow.ErrUnsupported) || !workflow.IsPermanent(err) {
		t.Fatalf("no reporter = %v, want Permanent ErrUnsupported", err)
	}
}
