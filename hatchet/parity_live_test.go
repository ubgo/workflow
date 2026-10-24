package hatchet_test

import (
	"context"
	"errors"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/cron"
	"github.com/ubgo/workflow/hatchet"
)

// skipWithoutEngine skips a live test when no engine token is set.
func skipWithoutEngine(t *testing.T) {
	t.Helper()
	if os.Getenv(envToken) == "" {
		t.Skipf("%s is not set: this live test needs a Hatchet engine (task test:live)", envToken)
	}
}

// startLive registers wfs on a new client built from cfg and starts it; the
// returned stop cancels its Start ctx.
func startLive(t *testing.T, cfg hatchet.Config, wfs ...workflow.Workflow) (*hatchet.Client, context.CancelFunc) {
	t.Helper()
	c := hatchet.New(cfg)
	for _, wf := range wfs {
		if err := c.Register(wf); err != nil {
			t.Fatal(err)
		}
	}
	ctx, stop := context.WithCancel(context.Background())
	if err := c.Start(ctx); err != nil {
		stop()
		t.Fatal(err)
	}
	t.Cleanup(stop)
	return c, stop
}

// waitStatus polls until ref reaches want, failing after liveWait.
func waitStatus(t *testing.T, c workflow.Client, ref workflow.RunRef, want workflow.Status) {
	t.Helper()
	deadline := time.Now().Add(liveWait)
	var st workflow.Status
	var err error
	for time.Now().Before(deadline) {
		st, err = c.Status(context.Background(), ref)
		if err == nil && st == want {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("run %+v: status %s (%v), want %s within %s", ref, st, err, want, liveWait)
}

// holdStatus fails if ref leaves want during d.
func holdStatus(t *testing.T, c workflow.Client, ref workflow.RunRef, want workflow.Status, d time.Duration) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		st, err := c.Status(context.Background(), ref)
		if err != nil || st != want {
			t.Fatalf("run %+v: status %s (%v) while it should stay %s", ref, st, err, want)
		}
		time.Sleep(300 * time.Millisecond)
	}
}

func okStep(context.Context, []byte) ([]byte, error) { return []byte(`"ok"`), nil }

// TestQueuesLive proves CapQueue on the engine: a client given
// Config.Queues runs only its queues' workflows, a run of an unserved
// queue stays queued until a client serving it starts, a Job.Queue other
// than the workflow's is refused, and CancelAll cancels queued runs.
func TestQueuesLive(t *testing.T) {
	skipWithoutEngine(t)
	prefix := "wfqueue_" + uuid.NewString()[:8]
	fast := workflow.Workflow{Name: prefix + "_fast", Queue: "fast", Steps: []workflow.Step{{Name: "a", Run: okStep}}}
	slow := workflow.Workflow{Name: prefix + "_slow", Queue: "slow", Steps: []workflow.Step{{Name: "a", Run: okStep}}}

	if err := hatchet.New(hatchet.Config{Queues: []workflow.QueueConfig{{Name: "fast", Priority: 1}}}).Start(context.Background()); !errors.Is(err, workflow.ErrUnsupported) {
		t.Fatalf("Start with a queue priority: %v", err)
	}

	// A process serving "slow" declares the workflow on the engine, then
	// goes away, so its runs have no worker.
	_, stopSlow := startLive(t, hatchet.Config{WorkerName: prefix + "-s1", Queues: []workflow.QueueConfig{{Name: "slow"}}}, slow)
	stopSlow()
	time.Sleep(2 * time.Second)

	c, _ := startLive(t, hatchet.Config{WorkerName: prefix + "-f", Queues: []workflow.QueueConfig{{Name: "fast", Workers: 2}}}, fast, slow)
	ctx := context.Background()
	fastRef, err := c.Enqueue(ctx, workflow.Job{Workflow: fast.Name})
	if err != nil {
		t.Fatal(err)
	}
	slowRef, err := c.Enqueue(ctx, workflow.Job{Workflow: slow.Name, Labels: map[string]string{"n": "1"}})
	if err != nil {
		t.Fatal(err)
	}
	waitStatus(t, c, fastRef, workflow.StatusSucceeded)
	holdStatus(t, c, slowRef, workflow.StatusQueued, 3*time.Second)
	if _, err := c.Enqueue(ctx, workflow.Job{Workflow: fast.Name, Queue: "slow"}); !errors.Is(err, workflow.ErrUnsupported) {
		t.Fatalf("Job.Queue override: %v", err)
	}

	page, err := c.Runs(ctx, workflow.RunQuery{Workflow: slow.Name})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Runs) != 1 || page.Runs[0].Queue != "slow" || page.Runs[0].Status != workflow.StatusQueued {
		t.Fatalf("slow runs = %+v", page.Runs)
	}

	_, _ = startLive(t, hatchet.Config{WorkerName: prefix + "-s2", Queues: []workflow.QueueConfig{{Name: "slow"}}}, slow)
	waitStatus(t, c, slowRef, workflow.StatusSucceeded)

	// CancelAll: two queued runs on a queue nobody serves now.
	stranded := workflow.Workflow{Name: prefix + "_stranded", Queue: "stranded", Steps: []workflow.Step{{Name: "a", Run: okStep}}}
	_, stopS := startLive(t, hatchet.Config{WorkerName: prefix + "-x", Queues: []workflow.QueueConfig{{Name: "stranded"}}}, stranded)
	stopS()
	time.Sleep(2 * time.Second)
	d, _ := startLive(t, hatchet.Config{WorkerName: prefix + "-d", Queues: []workflow.QueueConfig{{Name: "fast"}}}, fast, stranded)
	var refs []workflow.RunRef
	for range 2 {
		ref, err := d.Enqueue(ctx, workflow.Job{Workflow: stranded.Name})
		if err != nil {
			t.Fatal(err)
		}
		refs = append(refs, ref)
	}
	for _, ref := range refs {
		waitStatus(t, d, ref, workflow.StatusQueued)
	}
	n, err := d.CancelAll(ctx, workflow.RunQuery{Workflow: stranded.Name})
	if err != nil || n != 2 {
		t.Fatalf("CancelAll = %d, %v; want 2", n, err)
	}
	for _, ref := range refs {
		waitStatus(t, d, ref, workflow.StatusCancelled)
	}
}

// TestRunQueryLive proves CapRunQuery and CapCancelAll on the engine:
// filters by workflow, labels and status, paging, the failed step and
// error of a failed run, a delayed start listed as scheduled with the ref
// Enqueue returned, and CancelAll's default (runs not yet started).
func TestRunQueryLive(t *testing.T) {
	skipWithoutEngine(t)
	prefix := "wfquery_" + uuid.NewString()[:8]
	ok := workflow.Workflow{Name: prefix + "_ok", Steps: []workflow.Step{{Name: "a", Run: okStep}}}
	bad := workflow.Workflow{Name: prefix + "_bad", Steps: []workflow.Step{
		{Name: "a", Run: okStep},
		{Name: "b", After: []string{"a"}, Run: func(context.Context, []byte) ([]byte, error) { return nil, workflow.Permanent(errors.New("boom")) }},
	}}
	later := workflow.Workflow{Name: prefix + "_later", Steps: []workflow.Step{{Name: "a", Run: okStep}}}
	c, _ := startLive(t, hatchet.Config{WorkerName: prefix}, ok, bad, later)
	ctx := context.Background()

	var okRefs []workflow.RunRef
	for i, tenant := range []string{"t1", "t1", "t1", "t2"} {
		ref, err := c.Enqueue(ctx, workflow.Job{Workflow: ok.Name, Payload: []byte(`{"i":` + string(rune('0'+i)) + `}`), Labels: map[string]string{"tenant": tenant}, Priority: 2})
		if err != nil {
			t.Fatal(err)
		}
		okRefs = append(okRefs, ref)
	}
	badRef, err := c.Enqueue(ctx, workflow.Job{Workflow: bad.Name})
	if err != nil {
		t.Fatal(err)
	}
	laterRef, err := c.Enqueue(ctx, workflow.Job{Workflow: later.Name, RunAt: time.Now().Add(time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	for _, ref := range okRefs {
		waitStatus(t, c, ref, workflow.StatusSucceeded)
	}
	waitStatus(t, c, badRef, workflow.StatusFailed)

	q := workflow.RunQuery{Workflow: ok.Name, Labels: map[string]string{"tenant": "t1"}, Limit: 2}
	first, err := c.Runs(ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Runs) != 2 || first.Next == "" {
		t.Fatalf("first page = %d runs, next %q", len(first.Runs), first.Next)
	}
	q.Cursor = first.Next
	second, err := c.Runs(ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	all := append(first.Runs, second.Runs...)
	if len(all) != 3 {
		t.Fatalf("t1 runs = %d, want 3: %+v", len(all), all)
	}
	for i, r := range all {
		if r.Status != workflow.StatusSucceeded || r.Labels["tenant"] != "t1" || r.Queue != workflow.DefaultQueue || r.Priority != 2 || r.Attempts != 1 || r.EnqueuedAt.IsZero() || r.FinishedAt.IsZero() || len(r.Payload) == 0 {
			t.Fatalf("run %d = %+v", i, r)
		}
		if i > 0 && r.EnqueuedAt.After(all[i-1].EnqueuedAt) {
			t.Fatalf("runs not newest first: %s after %s", r.EnqueuedAt, all[i-1].EnqueuedAt)
		}
	}

	failed, err := c.Runs(ctx, workflow.RunQuery{Workflow: bad.Name, Statuses: []workflow.Status{workflow.StatusFailed}})
	if err != nil {
		t.Fatal(err)
	}
	if len(failed.Runs) != 1 || failed.Runs[0].Ref.RunID != badRef.RunID || failed.Runs[0].FailedStep != "b" || !strings.Contains(failed.Runs[0].Error, "boom") || failed.Runs[0].Attempts != 2 {
		t.Fatalf("failed runs = %+v", failed.Runs)
	}

	sched, err := c.Runs(ctx, workflow.RunQuery{Workflow: later.Name, Statuses: []workflow.Status{workflow.StatusScheduled}})
	if err != nil {
		t.Fatal(err)
	}
	if len(sched.Runs) != 1 || sched.Runs[0].Ref.ScheduledID != laterRef.ScheduledID {
		t.Fatalf("scheduled runs = %+v, want %v", sched.Runs, laterRef)
	}
	for _, ref := range []workflow.RunRef{laterRef, okRefs[0]} {
		one, err := c.Runs(ctx, workflow.RunQuery{Ref: ref})
		if err != nil || len(one.Runs) != 1 || one.Next != "" {
			t.Fatalf("Runs by ref %v = %+v, %v", ref, one, err)
		}
		got := one.Runs[0].Ref
		if (!ref.RunID.IsZero() && got.RunID != ref.RunID) || (!ref.ScheduledID.IsZero() && got.ScheduledID != ref.ScheduledID) {
			t.Fatalf("Runs by ref %v found %v", ref, got)
		}
	}
	if none, err := c.Runs(ctx, workflow.RunQuery{Ref: okRefs[0], Statuses: []workflow.Status{workflow.StatusFailed}}); err != nil || len(none.Runs) != 0 {
		t.Fatalf("Runs by ref with a status it lacks = %+v, %v", none, err)
	}
	if _, err := c.Runs(ctx, workflow.RunQuery{Cursor: "nope"}); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("foreign cursor: %v", err)
	}

	n, err := c.CancelAll(ctx, workflow.RunQuery{Workflow: later.Name})
	if err != nil || n != 1 {
		t.Fatalf("CancelAll = %d, %v; want 1", n, err)
	}
	waitStatus(t, c, laterRef, workflow.StatusCancelled)
	if _, err := c.CancelAll(ctx, workflow.RunQuery{Statuses: []workflow.Status{workflow.StatusSucceeded}}); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("CancelAll of a terminal status: %v", err)
	}
}

// TestRetryLive proves CapRetryRun on the engine: a failed run retried
// from its failed step keeps its RunID, does not run the step that
// succeeded, ends again (and run-finished observers see the second end),
// a run that did not fail is refused, and a key another unfinished run
// holds is refused with ErrDuplicate.
func TestRetryLive(t *testing.T) {
	skipWithoutEngine(t)
	prefix := "wfretry_" + uuid.NewString()[:8]
	var aRuns, bRuns atomic.Int32
	release := make(chan struct{})
	defer close(release)
	wf := workflow.Workflow{Name: prefix, Steps: []workflow.Step{
		{Name: "a", Run: func(_ context.Context, in []byte) ([]byte, error) { aRuns.Add(1); return in, nil }},
		{Name: "b", After: []string{"a"}, Run: func(ctx context.Context, in []byte) ([]byte, error) {
			switch string(in) {
			case `"block"`:
				select {
				case <-release:
				case <-ctx.Done():
				}
				return nil, nil
			case `"key"`:
				return nil, errors.New("fails")
			}
			if bRuns.Add(1) == 1 {
				return nil, errors.New("first attempt fails")
			}
			return nil, nil
		}},
	}}
	var mu sync.Mutex
	var ends []workflow.Status
	c := hatchet.New(hatchet.Config{WorkerName: prefix})
	if err := c.OnRunFinished(func(_ context.Context, ev workflow.RunEvent) {
		mu.Lock()
		defer mu.Unlock()
		if ev.Workflow == wf.Name && string(ev.Labels["kind"]) == "retry" {
			ends = append(ends, ev.Status)
		}
	}); err != nil {
		t.Fatal(err)
	}
	if err := c.Register(wf); err != nil {
		t.Fatal(err)
	}
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}

	ref, err := c.Enqueue(ctx, workflow.Job{Workflow: wf.Name, Payload: []byte(`"x"`), Labels: map[string]string{"kind": "retry"}})
	if err != nil {
		t.Fatal(err)
	}
	waitStatus(t, c, ref, workflow.StatusFailed)
	// The notifier polls, so wait for it to see the failure before the
	// retry makes the run live again (a quicker retry is reported only
	// as the final end; delivery is best-effort, see notify.go).
	waitEnds := func(want ...workflow.Status) {
		t.Helper()
		deadline := time.Now().Add(liveWait)
		for {
			mu.Lock()
			got := slices.Clone(ends)
			mu.Unlock()
			if slices.Equal(got, want) {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("run ends seen = %v, want %v", got, want)
			}
			time.Sleep(200 * time.Millisecond)
		}
	}
	waitEnds(workflow.StatusFailed)
	got, err := c.Retry(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	if got.RunID != ref.RunID {
		t.Fatalf("Retry ref = %v, want %v", got, ref)
	}
	waitStatus(t, c, ref, workflow.StatusSucceeded)
	if a, b := aRuns.Load(), bRuns.Load(); a != 1 || b != 2 {
		t.Fatalf("step runs a=%d b=%d, want a=1 (kept) b=2", a, b)
	}
	if _, err := c.Retry(ctx, ref); !errors.Is(err, workflow.ErrWrongState) {
		t.Fatalf("Retry of a succeeded run: %v", err)
	}
	waitEnds(workflow.StatusFailed, workflow.StatusSucceeded)

	key := prefix + "-key"
	failedRef, err := c.Enqueue(ctx, workflow.Job{Workflow: wf.Name, Payload: []byte(`"key"`), IdempotencyKey: key})
	if err != nil {
		t.Fatal(err)
	}
	waitStatus(t, c, failedRef, workflow.StatusFailed)
	holder, err := c.Enqueue(ctx, workflow.Job{Workflow: wf.Name, Payload: []byte(`"block"`), IdempotencyKey: key})
	if err != nil {
		t.Fatal(err)
	}
	waitStatus(t, c, holder, workflow.StatusRunning)
	if _, err := c.Retry(ctx, failedRef); !errors.Is(err, workflow.ErrDuplicate) {
		t.Fatalf("Retry while another run holds the key: %v", err)
	}
}

// TestSchedulesLive proves named schedules and CapScheduleList on the
// engine: a repeated Schedule with the same name and definition returns
// the same ID, a changed one replaces it (one trigger per name), and
// Schedules lists declared and run-time triggers with their next fire.
func TestSchedulesLive(t *testing.T) {
	skipWithoutEngine(t)
	prefix := "wfsched_" + uuid.NewString()[:8]
	wf := workflow.Workflow{Name: prefix, Cron: []string{"0 3 * * *"}, Steps: []workflow.Step{{Name: "a", Run: okStep}}}
	c, _ := startLive(t, hatchet.Config{WorkerName: prefix}, wf)
	ctx := context.Background()
	s := workflow.Schedule{Workflow: wf.Name, Name: "nightly", Cron: "0 1 1 1 *", Location: time.UTC, Labels: map[string]string{"k": "v"}}
	id1, err := c.Schedule(ctx, s)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Unschedule(context.Background(), id1) })
	id2, err := c.Schedule(ctx, s)
	if err != nil || id2 != id1 {
		t.Fatalf("repeated Schedule = %v, %v; want %v", id2, err, id1)
	}
	s.Cron = "0 2 1 1 *"
	id3, err := c.Schedule(ctx, s)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Unschedule(context.Background(), id3) })

	list, err := c.Schedules(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var mine []workflow.ScheduleInfo
	for _, info := range list {
		if info.Workflow == wf.Name {
			mine = append(mine, info)
		}
	}
	if len(mine) != 2 {
		t.Fatalf("schedules of %s = %+v, want the declared cron and one nightly", wf.Name, mine)
	}
	declared, named := mine[0], mine[1]
	if !declared.Declared || declared.Cron != "0 3 * * *" || declared.Name != "0 3 * * *" {
		t.Fatalf("declared = %+v", declared)
	}
	if named.Declared || named.ID != id3 || named.Name != "nightly" || named.Cron != "0 2 1 1 *" || named.Labels["k"] != "v" || named.Paused {
		t.Fatalf("named = %+v, want ID %v", named, id3)
	}
	sched, err := cron.Parse(named.Cron)
	if err != nil {
		t.Fatal(err)
	}
	if want := sched.Next(time.Now(), time.UTC); !named.Next.Equal(want) || named.Location != time.UTC {
		t.Fatalf("Next = %s in %v, want %s in UTC", named.Next, named.Location, want)
	}
}

// TestDrainLive proves CapDrain on the engine: Drain waits for the
// running step, the paused worker takes no new run meanwhile, Drain then
// returns nil and Done is closed, and the run it did not take stays
// queued for another worker.
func TestDrainLive(t *testing.T) {
	skipWithoutEngine(t)
	prefix := "wfdrain_" + uuid.NewString()[:8]
	release := make(chan struct{})
	started := make(chan struct{}, 4)
	wf := workflow.Workflow{Name: prefix, Steps: []workflow.Step{{Name: "a", Run: func(ctx context.Context, _ []byte) ([]byte, error) {
		started <- struct{}{}
		select {
		case <-release:
			return nil, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}}}}
	c, _ := startLive(t, hatchet.Config{WorkerName: prefix}, wf)
	ctx := context.Background()
	first, err := c.Enqueue(ctx, workflow.Job{Workflow: wf.Name})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(liveWait):
		t.Fatal("first run never started")
	}
	drained := make(chan error, 1)
	go func() {
		dctx, cancel := context.WithTimeout(ctx, liveWait)
		defer cancel()
		drained <- c.Drain(dctx)
	}()
	time.Sleep(2 * time.Second)
	second, err := c.Enqueue(ctx, workflow.Job{Workflow: wf.Name})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-drained:
		t.Fatalf("Drain returned %v while a step was running", err)
	case <-started:
		t.Fatal("a draining worker started a new run")
	case <-time.After(3 * time.Second):
	}
	close(release)
	select {
	case err := <-drained:
		if err != nil {
			t.Fatalf("Drain = %v", err)
		}
	case <-time.After(liveWait):
		t.Fatal("Drain did not return after the step finished")
	}
	select {
	case <-c.Done():
	default:
		t.Fatal("Done not closed after Drain")
	}
	if _, err := c.Enqueue(ctx, workflow.Job{Workflow: wf.Name}); !errors.Is(err, workflow.ErrNotStarted) {
		t.Fatalf("Enqueue after Drain: %v, want ErrNotStarted", err)
	}
	// A drained client is stopped: read the runs through another one.
	other, _ := startLive(t, hatchet.Config{WorkerName: prefix + "-reader"})
	waitStatus(t, other, first, workflow.StatusSucceeded)
	holdStatus(t, other, second, workflow.StatusQueued, 2*time.Second)
	if err := other.Cancel(ctx, second); err != nil {
		t.Fatal(err)
	}
}
