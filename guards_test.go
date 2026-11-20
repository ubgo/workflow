package workflow_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/memory"
)

// TestRetryLadder pins the ladder: one delay per retry, the last
// repeating, negatives as zero, an empty ladder deferring to Backoff, and
// the caller's slice copied.
func TestRetryLadder(t *testing.T) {
	delays := []time.Duration{5 * time.Second, time.Minute, -time.Second, time.Hour}
	ladder := workflow.RetryLadder(delays...)
	delays[0] = 99 * time.Hour // must not reach the ladder
	for _, tc := range []struct {
		retry int
		want  time.Duration
	}{{0, 5 * time.Second}, {1, 5 * time.Second}, {2, time.Minute}, {3, 0}, {4, time.Hour}, {5, time.Hour}, {50, time.Hour}} {
		got, ok := ladder(errors.New("x"), tc.retry)
		if !ok || got != tc.want {
			t.Errorf("retry %d: %s, %v; want %s, true", tc.retry, got, ok, tc.want)
		}
	}
	if _, ok := workflow.RetryLadder()(errors.New("x"), 1); ok {
		t.Error("an empty ladder answered; it must defer to Backoff")
	}
}

// TestLadderPolicy: Max is the ladder's length and Next walks the ladder,
// stopping after the last step.
func TestLadderPolicy(t *testing.T) {
	p := workflow.LadderPolicy(time.Second, 10*time.Second, time.Minute)
	if p.Max != 3 {
		t.Fatalf("Max %d, want 3", p.Max)
	}
	err := errors.New("endpoint returned 503")
	var got []time.Duration
	a := workflow.Attempts{}
	for {
		a = a.Count(err)
		d, retry := p.Next(err, a, time.Time{}, nil)
		if !retry {
			break
		}
		got = append(got, d)
	}
	want := []time.Duration{time.Second, 10 * time.Second, time.Minute}
	if !slices.Equal(got, want) {
		t.Fatalf("delays %v, want %v", got, want)
	}
}

// TestInvariant: nil when the condition holds, else a permanent error
// matching ErrConstraint with the formatted message.
func TestInvariant(t *testing.T) {
	if err := workflow.Invariant(true, "never"); err != nil {
		t.Fatalf("holding invariant: %v", err)
	}
	cause := errors.New("ledger closed")
	err := workflow.Invariant(false, "order %s total %d: %w", "o-1", -5, cause)
	if !errors.Is(err, workflow.ErrConstraint) || !workflow.IsPermanent(err) || !errors.Is(err, cause) {
		t.Fatalf("broken invariant: %v (constraint %v, permanent %v)", err, errors.Is(err, workflow.ErrConstraint), workflow.IsPermanent(err))
	}
	if want := "workflow: constraint violated: order o-1 total -5: ledger closed"; err.Error() != want {
		t.Fatalf("message %q, want %q", err.Error(), want)
	}
}

// sweepClient is a scripted client for ExpireWaiting: a run listing served
// in pages, and a record of every cancel.
type sweepClient struct {
	workflow.Client
	caps      workflow.Caps
	runs      []workflow.RunInfo
	pageSize  int
	queries   []workflow.RunQuery
	cancelled []workflow.RunRef
	bulkQs    []workflow.RunQuery
	moved     map[workflow.RunID]bool // runs that started after the listing
	cancelErr map[workflow.RunID]error
}

func (s *sweepClient) Capabilities() workflow.Caps { return s.caps }

func (s *sweepClient) Runs(_ context.Context, q workflow.RunQuery) (workflow.RunPage, error) {
	s.queries = append(s.queries, q)
	var match []workflow.RunInfo
	for _, r := range s.runs {
		if q.Match(r) {
			match = append(match, r)
		}
	}
	start := 0
	if q.Cursor != "" {
		fmt.Sscan(q.Cursor, &start)
	}
	end := min(start+s.pageSize, len(match))
	page := workflow.RunPage{Runs: match[start:end]}
	if end < len(match) {
		page.Next = fmt.Sprint(end)
	}
	return page, nil
}

func (s *sweepClient) Cancel(_ context.Context, ref workflow.RunRef) error {
	if err := s.cancelErr[ref.RunID]; err != nil {
		return err
	}
	s.cancelled = append(s.cancelled, ref)
	return nil
}

type bulkSweepClient struct{ *sweepClient }

func (b bulkSweepClient) CancelAll(_ context.Context, q workflow.RunQuery) (int, error) {
	b.bulkQs = append(b.bulkQs, q)
	if err := b.cancelErr[q.Ref.RunID]; err != nil {
		return 0, err
	}
	if b.moved[q.Ref.RunID] {
		return 0, nil
	}
	b.cancelled = append(b.cancelled, q.Ref)
	return 1, nil
}

// TestExpireWaiting_Decisions pins which runs a sweep cancels: queued
// ones past the bound, a delayed one measured from its due time, never a
// scheduled run whose due time is unknown, never a started one.
func TestExpireWaiting_Decisions(t *testing.T) {
	now := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	ref := func(id string) workflow.RunRef { return workflow.RunRef{RunID: workflow.RunID(id)} }
	runs := []workflow.RunInfo{
		{Ref: ref("old"), Status: workflow.StatusQueued, EnqueuedAt: now.Add(-20 * time.Minute)},
		{Ref: ref("fresh"), Status: workflow.StatusQueued, EnqueuedAt: now.Add(-5 * time.Minute)},
		{Ref: ref("edge"), Status: workflow.StatusQueued, EnqueuedAt: now.Add(-10 * time.Minute)},                                  // exactly the bound: not over it
		{Ref: ref("delayed-recent"), Status: workflow.StatusQueued, EnqueuedAt: now.Add(-time.Hour), RunAt: now.Add(-time.Minute)}, // due a minute ago
		{Ref: ref("delayed-old"), Status: workflow.StatusQueued, EnqueuedAt: now.Add(-time.Hour), RunAt: now.Add(-11 * time.Minute)},
		{Ref: ref("sched-due"), Status: workflow.StatusScheduled, EnqueuedAt: now.Add(-time.Hour), RunAt: now.Add(-30 * time.Minute)},
		{Ref: ref("sched-unknown"), Status: workflow.StatusScheduled, EnqueuedAt: now.Add(-time.Hour)},
		{Ref: ref("running"), Status: workflow.StatusRunning, EnqueuedAt: now.Add(-time.Hour)},
	}
	c := &sweepClient{caps: workflow.CapsOf(workflow.CapRunQuery, workflow.CapCancel), runs: runs, pageSize: 2}
	res, err := workflow.ExpireWaiting(context.Background(), c, workflow.RunQuery{}, 10*time.Minute, now)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, r := range res.Cancelled {
		got = append(got, string(r.RunID))
	}
	want := []string{"old", "delayed-old", "sched-due"}
	if !slices.Equal(got, want) {
		t.Fatalf("cancelled %v, want %v", got, want)
	}
	// q.To is lowered to now-maxWait, so "fresh" and "edge" are never even listed.
	if res.Checked != 5 || len(c.queries) != 3 {
		t.Fatalf("checked %d runs over %d pages, want 5 over 3", res.Checked, len(c.queries))
	}
	if q := c.queries[0]; !q.To.Equal(now.Add(-10*time.Minute)) || !slices.Equal(q.Statuses, []workflow.Status{workflow.StatusQueued, workflow.StatusScheduled}) {
		t.Fatalf("query %+v", q)
	}
}

// TestExpireWaiting_BulkGuardsTheRace: with CapCancelAll each cancel is
// guarded by the listed status, so a run a worker took meanwhile is
// counted in Raced and left running; ErrNotFound is skipped and other
// errors are joined without stopping the sweep.
func TestExpireWaiting_BulkGuardsTheRace(t *testing.T) {
	now := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	old := now.Add(-time.Hour)
	ref := func(id string) workflow.RunRef { return workflow.RunRef{RunID: workflow.RunID(id)} }
	boom := errors.New("db down")
	base := &sweepClient{
		caps: workflow.CapsOf(workflow.CapRunQuery, workflow.CapCancel, workflow.CapCancelAll), pageSize: 100,
		runs: []workflow.RunInfo{
			{Ref: ref("a"), Status: workflow.StatusQueued, EnqueuedAt: old},
			{Ref: ref("taken"), Status: workflow.StatusQueued, EnqueuedAt: old},
			{Ref: ref("gone"), Status: workflow.StatusQueued, EnqueuedAt: old},
			{Ref: ref("err"), Status: workflow.StatusQueued, EnqueuedAt: old},
			{Ref: ref("b"), Status: workflow.StatusQueued, EnqueuedAt: old},
		},
		moved:     map[workflow.RunID]bool{"taken": true},
		cancelErr: map[workflow.RunID]error{"gone": workflow.ErrNotFound, "err": boom},
	}
	res, err := workflow.ExpireWaiting(context.Background(), bulkSweepClient{base}, workflow.RunQuery{}, time.Minute, now)
	if !errors.Is(err, boom) {
		t.Fatalf("err %v, want the cancel failure joined", err)
	}
	if len(res.Cancelled) != 2 || res.Raced != 1 {
		t.Fatalf("result %+v, want a and b cancelled, one raced", res)
	}
	for _, q := range base.bulkQs {
		if !slices.Equal(q.Statuses, []workflow.Status{workflow.StatusQueued}) || q.Ref.IsZero() {
			t.Fatalf("bulk cancel not guarded by the run and its status: %+v", q)
		}
	}
	if len(base.bulkQs) != 5 {
		t.Fatalf("%d bulk cancels, want 5", len(base.bulkQs))
	}
}

// TestExpireWaiting_Refusals: missing capabilities or interface, a bad
// maxWait and a started status are refused before anything is listed.
func TestExpireWaiting_Refusals(t *testing.T) {
	now := time.Now()
	ctx := context.Background()
	type plain struct{ workflow.Client }
	cases := []struct {
		name string
		c    workflow.Client
		q    workflow.RunQuery
		wait time.Duration
		want error
	}{
		{"no run query", &sweepClient{caps: workflow.CapCancel}, workflow.RunQuery{}, time.Minute, workflow.ErrUnsupported},
		{"no cancel", &sweepClient{caps: workflow.CapRunQuery}, workflow.RunQuery{}, time.Minute, workflow.ErrUnsupported},
		{"no interface", plain{&sweepClient{caps: workflow.CapsOf(workflow.CapRunQuery, workflow.CapCancel)}}, workflow.RunQuery{}, time.Minute, workflow.ErrUnsupported},
		{"zero wait", &sweepClient{caps: workflow.CapsOf(workflow.CapRunQuery, workflow.CapCancel)}, workflow.RunQuery{}, 0, workflow.ErrInvalid},
		{"started status", &sweepClient{caps: workflow.CapsOf(workflow.CapRunQuery, workflow.CapCancel)}, workflow.RunQuery{Statuses: []workflow.Status{workflow.StatusRunning}}, time.Minute, workflow.ErrInvalid},
	}
	for _, tc := range cases {
		if _, err := workflow.ExpireWaiting(ctx, tc.c, tc.q, tc.wait, now); !errors.Is(err, tc.want) {
			t.Errorf("%s: %v, want %v", tc.name, err, tc.want)
		}
	}
}

// TestExpireWaiting_Memory runs the sweep on the memory driver: with its
// one worker busy, the runs queued behind it are cancelled once they
// have waited past the bound, and the running one is left alone.
func TestExpireWaiting_Memory(t *testing.T) {
	clk := clock.NewFake(time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC))
	m := memory.New(memory.WithClock(clk), memory.WithWorkers(1))
	started, release := make(chan struct{}, 1), make(chan struct{})
	err := m.Register(workflow.Workflow{Name: "export", Steps: []workflow.Step{{Name: "run", Run: func(ctx context.Context, _ []byte) ([]byte, error) {
		started <- struct{}{}
		<-release
		return nil, nil
	}}}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer func() { close(release); cancel(); m.Stop() }()
	if err := m.Start(ctx); err != nil {
		t.Fatal(err)
	}
	busy, err := m.Enqueue(ctx, workflow.Job{Workflow: "export"})
	if err != nil {
		t.Fatal(err)
	}
	<-started
	var queued []workflow.RunRef
	for range 2 {
		ref, err := m.Enqueue(ctx, workflow.Job{Workflow: "export"})
		if err != nil {
			t.Fatal(err)
		}
		queued = append(queued, ref)
	}
	if res, err := workflow.ExpireWaiting(ctx, m, workflow.RunQuery{Workflow: "export"}, time.Minute, clk.Now()); err != nil || len(res.Cancelled) != 0 {
		t.Fatalf("before the bound: %+v, %v", res, err)
	}
	clk.Advance(2 * time.Minute)
	res, err := workflow.ExpireWaiting(ctx, m, workflow.RunQuery{Workflow: "export"}, time.Minute, clk.Now())
	if err != nil || len(res.Cancelled) != 2 {
		t.Fatalf("after the bound: %+v, %v", res, err)
	}
	for _, ref := range queued {
		if st, _ := m.Status(ctx, ref); st != workflow.StatusCancelled {
			t.Fatalf("queued run %s is %q, want cancelled", ref.RunID, st)
		}
	}
	if st, _ := m.Status(ctx, busy); st != workflow.StatusRunning {
		t.Fatalf("running run is %q, want still running", st)
	}
}
