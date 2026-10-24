package memory_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/memory"
)

// eventually polls cond until it holds or pollFor passes.
func eventually(t *testing.T, desc string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(pollFor)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", desc)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestQueueWorkerCapAndPriority(t *testing.T) {
	var mu sync.Mutex
	active, peak := map[string]int{}, map[string]int{}
	var order []string
	release := make(chan struct{})
	h := func(ctx context.Context, in []byte) ([]byte, error) {
		info, _ := workflow.StepInfoFrom(ctx)
		mu.Lock()
		active[info.Queue]++
		peak[info.Queue] = max(peak[info.Queue], active[info.Queue])
		order = append(order, info.Queue)
		mu.Unlock()
		select {
		case <-release:
		case <-ctx.Done():
		}
		mu.Lock()
		active[info.Queue]--
		mu.Unlock()
		return in, nil
	}
	low := one("low", h)
	low.Queue = "low"
	high := one("high", h)
	high.Queue = "high"
	c, _ := newClient(t, []workflow.Workflow{low, high}, memory.WithWorkers(1),
		memory.WithQueues(workflow.QueueConfig{Name: "low"}, workflow.QueueConfig{Name: "high", Workers: 1, Priority: 10}))
	// The one worker is busy with the first low run while the rest queue.
	var refs []workflow.RunRef
	refs = append(refs, enqueue(t, c, workflow.Job{Workflow: "low"}))
	eventually(t, "the first run to start", func() bool { mu.Lock(); defer mu.Unlock(); return len(order) == 1 })
	refs = append(refs, enqueue(t, c, workflow.Job{Workflow: "low"}))
	refs = append(refs, enqueue(t, c, workflow.Job{Workflow: "high"}))
	refs = append(refs, enqueue(t, c, workflow.Job{Workflow: "high"}))
	close(release)
	for _, ref := range refs {
		waitStatus(t, c, ref, workflow.StatusSucceeded)
	}
	mu.Lock()
	defer mu.Unlock()
	if want := []string{"low", "high", "high", "low"}; len(order) != 4 || order[1] != want[1] || order[2] != want[2] || order[3] != want[3] {
		t.Errorf("order = %v, want %v: the high queue drains first", order, want)
	}
	if peak["high"] > 1 {
		t.Errorf("queue high ran %d at once, want at most its Workers 1", peak["high"])
	}
}

func TestQueueCapHoldsAQueueWhileOthersRun(t *testing.T) {
	release := make(chan struct{})
	started := make(chan string, 4)
	a := one("a", block(started, release))
	a.Queue = "a"
	b := one("b", block(started, release))
	b.Queue = "b"
	c, _ := newClient(t, []workflow.Workflow{a, b}, memory.WithWorkers(4),
		memory.WithQueues(workflow.QueueConfig{Name: "a", Workers: 1}, workflow.QueueConfig{Name: "b"}))
	enqueue(t, c, workflow.Job{Workflow: "a", Payload: []byte("a1")})
	enqueue(t, c, workflow.Job{Workflow: "a", Payload: []byte("a2")})
	enqueue(t, c, workflow.Job{Workflow: "b", Payload: []byte("b1")})
	got := map[string]bool{<-started: true, <-started: true}
	select {
	case s := <-started:
		t.Fatalf("%s started while queue a was at its cap", s)
	case <-time.After(50 * time.Millisecond):
	}
	if !got["b1"] || (got["a1"] == got["a2"]) {
		t.Fatalf("started %v; want b1 and exactly one of queue a", got)
	}
	close(release)
	<-started
}

func TestQueuesUnservedAreRefused(t *testing.T) {
	wf := one("w", ok)
	wf.Queue = "elsewhere"
	c := memory.New(memory.WithQueues(workflow.QueueConfig{Name: "here"}))
	if err := c.Register(wf); err != nil {
		t.Fatal(err)
	}
	if err := c.Start(context.Background()); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("Start with an unserved workflow queue: %v, want ErrInvalid", err)
	}
	served, _ := newClient(t, []workflow.Workflow{one("w", ok)}, memory.WithQueues(workflow.QueueConfig{Name: workflow.DefaultQueue}))
	if _, err := served.Enqueue(context.Background(), workflow.Job{Workflow: "w", Queue: "elsewhere"}); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("Enqueue to an unserved queue: %v, want ErrInvalid", err)
	}
	bad := memory.New(memory.WithQueues(workflow.QueueConfig{Name: "a b"}))
	if err := bad.Start(context.Background()); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("Start with an invalid queue name: %v, want ErrInvalid", err)
	}
}

func TestPauseHoldsTheNextStepOfARunningRun(t *testing.T) {
	release := make(chan struct{})
	started := make(chan string, 1)
	var second atomic.Int64
	wf := workflow.Workflow{Name: "w", Steps: []workflow.Step{
		{Name: "a", Run: block(started, release)},
		{Name: "b", After: []string{"a"}, Run: func(context.Context, []byte) ([]byte, error) { second.Add(1); return nil, nil }},
	}}
	c, _ := newClient(t, []workflow.Workflow{wf})
	ref := enqueue(t, c, workflow.Job{Workflow: "w"})
	<-started
	if err := c.Pause(context.Background(), workflow.PauseWorkflow("w")); err != nil {
		t.Fatal(err)
	}
	close(release)
	time.Sleep(30 * time.Millisecond)
	if second.Load() != 0 {
		t.Fatal("the next step of a paused workflow ran")
	}
	if err := c.Resume(context.Background(), workflow.PauseWorkflow("w")); err != nil {
		t.Fatal(err)
	}
	waitStatus(t, c, ref, workflow.StatusSucceeded)
}

func TestRetryResumesEveryBranchTheFailureCancelled(t *testing.T) {
	var fail atomic.Bool
	fail.Store(true)
	var root, left, right atomic.Int64
	blockRight := make(chan struct{})
	wf := workflow.Workflow{Name: "w", Steps: []workflow.Step{
		{Name: "root", Run: func(context.Context, []byte) ([]byte, error) { root.Add(1); return nil, nil }},
		{Name: "left", After: []string{"root"}, Run: func(context.Context, []byte) ([]byte, error) {
			left.Add(1)
			if fail.Load() {
				return nil, errors.New("boom")
			}
			return nil, nil
		}},
		{Name: "right", After: []string{"root"}, Run: func(ctx context.Context, _ []byte) ([]byte, error) {
			right.Add(1)
			if fail.Load() {
				<-ctx.Done() // cut off by the left branch's failure
				return nil, ctx.Err()
			}
			return nil, nil
		}},
	}}
	defer close(blockRight)
	c, _ := newClient(t, []workflow.Workflow{wf})
	ref := enqueue(t, c, workflow.Job{Workflow: "w", IdempotencyKey: "k"})
	waitStatus(t, c, ref, workflow.StatusFailed)
	runs, _ := c.Runs(context.Background(), workflow.RunQuery{Workflow: "w"})
	if len(runs.Runs) != 1 || runs.Runs[0].FailedStep != "left" {
		t.Fatalf("Runs = %+v, want one run failed at left", runs.Runs)
	}
	// Another run took the released key: Retry must not steal it.
	holder := enqueue(t, c, workflow.Job{Workflow: "w", IdempotencyKey: "k"})
	waitStatus(t, c, holder, workflow.StatusFailed)
	fail.Store(false)
	if _, err := c.Retry(context.Background(), ref); err != nil {
		t.Fatalf("Retry: %v", err)
	}
	waitStatus(t, c, ref, workflow.StatusSucceeded)
	if root.Load() != 2 { // once per run: the retried run did not rerun it
		t.Errorf("root ran %d times over two runs and one retry, want 2", root.Load())
	}
	if got, err := c.Retry(context.Background(), ref); err == nil || !errors.Is(err, workflow.ErrWrongState) {
		t.Errorf("Retry of a succeeded run = %v, %v", got, err)
	}
}

func TestRetryRefusesAKeyHeldByAnotherRun(t *testing.T) {
	var fail atomic.Bool
	fail.Store(true)
	release := make(chan struct{})
	defer close(release)
	wf := one("w", func(ctx context.Context, _ []byte) ([]byte, error) {
		if fail.Load() {
			return nil, errors.New("boom")
		}
		select {
		case <-release:
		case <-ctx.Done():
		}
		return nil, nil
	})
	c, _ := newClient(t, []workflow.Workflow{wf})
	ref := enqueue(t, c, workflow.Job{Workflow: "w", IdempotencyKey: "k"})
	waitStatus(t, c, ref, workflow.StatusFailed)
	fail.Store(false)
	holder := enqueue(t, c, workflow.Job{Workflow: "w", IdempotencyKey: "k"})
	waitStatus(t, c, holder, workflow.StatusRunning)
	if _, err := c.Retry(context.Background(), ref); !errors.Is(err, workflow.ErrDuplicate) {
		t.Fatalf("Retry while another run holds the key: %v, want ErrDuplicate", err)
	}
}

func TestStopWhenIdle(t *testing.T) {
	c, fc := newClient(t, []workflow.Workflow{one("w", ok)}, memory.WithStopWhenIdle(time.Minute))
	ref := enqueue(t, c, workflow.Job{Workflow: "w"})
	waitStatus(t, c, ref, workflow.StatusSucceeded)
	select {
	case <-c.Done():
		t.Fatal("stopped before the grace passed")
	case <-time.After(20 * time.Millisecond):
	}
	ctx, cancel := context.WithTimeout(context.Background(), pollFor)
	defer cancel()
	if err := fc.BlockUntil(ctx, 1); err != nil {
		t.Fatal(err)
	}
	fc.Advance(time.Minute)
	select {
	case <-c.Done():
	case <-time.After(pollFor):
		t.Fatal("the idle client did not stop")
	}
}

func TestStopWhenIdleWaitsForNewWork(t *testing.T) {
	c, fc := newClient(t, []workflow.Workflow{one("w", ok)}, memory.WithStopWhenIdle(time.Minute))
	waitStatus(t, c, enqueue(t, c, workflow.Job{Workflow: "w"}), workflow.StatusSucceeded)
	ctx, cancel := context.WithTimeout(context.Background(), pollFor)
	defer cancel()
	if err := fc.BlockUntil(ctx, 1); err != nil {
		t.Fatal(err)
	}
	fc.Advance(30 * time.Second)
	later := enqueue(t, c, workflow.Job{Workflow: "w", RunAt: fc.Now().Add(time.Hour)}) // unfinished work
	fc.Advance(time.Minute)
	select {
	case <-c.Done():
		t.Fatal("stopped with a delayed run pending")
	case <-time.After(20 * time.Millisecond):
	}
	if st, _ := c.Status(context.Background(), later); st != workflow.StatusScheduled {
		t.Fatalf("delayed run is %s", st)
	}
}

func TestTicksReportMisfires(t *testing.T) {
	fc := clock.NewFake(start)
	c := memory.New(memory.WithClock(fc))
	var mu sync.Mutex
	var ticks []workflow.Tick
	if err := c.OnTick(func(_ context.Context, tk workflow.Tick) { mu.Lock(); ticks = append(ticks, tk); mu.Unlock() }); err != nil {
		t.Fatal(err)
	}
	wf := one("w", ok)
	wf.Cron = []string{"* * * * *"}
	if err := c.Register(wf); err != nil {
		t.Fatal(err)
	}
	if err := c.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Stop)
	ctx, cancel := context.WithTimeout(context.Background(), pollFor)
	defer cancel()
	if err := fc.BlockUntil(ctx, 1); err != nil {
		t.Fatal(err)
	}
	fc.Advance(5 * time.Minute) // five ticks in one jump: the default fires one
	eventually(t, "a fired and a misfired tick", func() bool { mu.Lock(); defer mu.Unlock(); return len(ticks) >= 2 })
	mu.Lock()
	defer mu.Unlock()
	var fired, missed int
	for _, tk := range ticks {
		switch tk.Outcome {
		case workflow.TickFired:
			fired++
			if !tk.Declared || tk.Name != "* * * * *" || tk.Run.IsZero() {
				t.Errorf("fired tick = %+v", tk)
			}
		case workflow.TickMisfired:
			missed += tk.Missed
			if !tk.At.Equal(start.Add(4 * time.Minute)) {
				t.Errorf("misfired At = %v, want the newest dropped tick 10:04", tk.At)
			}
		}
	}
	if fired != 1 || missed != 4 {
		t.Fatalf("fired %d and missed %d, want 1 and 4", fired, missed)
	}
}

func TestDeclaredCronCannotBeUnscheduledButCanBePaused(t *testing.T) {
	var n atomic.Int64
	wf := one("w", func(context.Context, []byte) ([]byte, error) { n.Add(1); return nil, nil })
	wf.Cron = []string{"* * * * *"}
	c, fc := newClient(t, []workflow.Workflow{wf})
	list, err := c.Schedules(context.Background())
	if err != nil || len(list) != 1 || !list[0].Declared {
		t.Fatalf("Schedules = %+v, %v", list, err)
	}
	id := list[0].ID
	if err := c.Unschedule(context.Background(), id); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("Unschedule of a declared cron: %v, want ErrInvalid", err)
	}
	if err := c.PauseSchedule(context.Background(), workflow.ScheduleTarget{All: true}); err != nil {
		t.Fatal(err)
	}
	if list, _ = c.Schedules(context.Background()); !list[0].Paused || !list[0].Next.IsZero() {
		t.Fatalf("a paused schedule lists as %+v, want Paused and no Next", list[0])
	}
	ctx, cancel := context.WithTimeout(context.Background(), pollFor)
	defer cancel()
	if err := fc.BlockUntil(ctx, 1); err != nil {
		t.Fatal(err)
	}
	fc.Advance(time.Minute)
	time.Sleep(20 * time.Millisecond)
	if n.Load() != 0 {
		t.Fatal("a schedule paused by All fired")
	}
	if err := c.ResumeSchedule(context.Background(), workflow.ScheduleTarget{All: true}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	if n.Load() != 0 {
		t.Fatal("a tick of the pause fired on resume")
	}
	if err := fc.BlockUntil(ctx, 1); err != nil {
		t.Fatal(err)
	}
	fc.Advance(time.Minute)
	eventually(t, "the next tick after the resume to fire", func() bool { return n.Load() == 1 })
}

func TestCancelAllWithExplicitStatuses(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	started := make(chan string, 2)
	c, _ := newClient(t, []workflow.Workflow{one("w", block(started, release))})
	running := enqueue(t, c, workflow.Job{Workflow: "w"})
	<-started
	n, err := c.CancelAll(context.Background(), workflow.RunQuery{Workflow: "w", Statuses: []workflow.Status{workflow.StatusRunning}})
	if err != nil || n != 1 {
		t.Fatalf("CancelAll(running) = %d, %v; want 1", n, err)
	}
	waitStatus(t, c, running, workflow.StatusCancelled)
}

func TestDrainBeforeStart(t *testing.T) {
	if err := memory.New().Drain(context.Background()); !errors.Is(err, workflow.ErrNotStarted) {
		t.Fatalf("Drain before Start: %v, want ErrNotStarted", err)
	}
}

func TestRunRetentionMaxRuns(t *testing.T) {
	c, _ := newClient(t, []workflow.Workflow{one("w", ok)}, memory.WithRunRetention(memory.RunRetention{MaxRuns: 2}))
	var refs []workflow.RunRef
	for range 3 {
		ref := enqueue(t, c, workflow.Job{Workflow: "w"})
		waitStatus(t, c, ref, workflow.StatusSucceeded)
		refs = append(refs, ref)
	}
	if _, err := c.Status(context.Background(), refs[0]); !errors.Is(err, workflow.ErrNotFound) {
		t.Fatalf("the oldest of three finished runs under MaxRuns 2: %v, want ErrNotFound", err)
	}
	for _, ref := range refs[1:] {
		if _, err := c.Status(context.Background(), ref); err != nil {
			t.Fatalf("a kept run: %v", err)
		}
	}
	page, _ := c.Runs(context.Background(), workflow.RunQuery{})
	if len(page.Runs) != 2 {
		t.Fatalf("Runs lists %d runs, want 2", len(page.Runs))
	}
}

func TestRunRetentionMaxAgeKeepsUnfinishedRuns(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	started := make(chan string, 1)
	c, fc := newClient(t, []workflow.Workflow{one("w", ok), one("slow", block(started, release))},
		memory.WithRunRetention(memory.RunRetention{MaxAge: time.Hour}))
	done := enqueue(t, c, workflow.Job{Workflow: "w"})
	waitStatus(t, c, done, workflow.StatusSucceeded)
	running := enqueue(t, c, workflow.Job{Workflow: "slow"})
	<-started
	fc.Advance(2 * time.Hour)
	if _, err := c.Runs(context.Background(), workflow.RunQuery{}); err != nil { // listing prunes
		t.Fatal(err)
	}
	if _, err := c.Status(context.Background(), done); !errors.Is(err, workflow.ErrNotFound) {
		t.Fatalf("a run finished two hours ago under MaxAge 1h: %v, want ErrNotFound", err)
	}
	if st, err := c.Status(context.Background(), running); err != nil || st != workflow.StatusRunning {
		t.Fatalf("an unfinished run must never be forgotten: %v, %v", st, err)
	}
}

func TestRunRetentionZeroKeepsEverythingAndDefaultIsBounded(t *testing.T) {
	c, fc := newClient(t, []workflow.Workflow{one("w", ok)}, memory.WithRunRetention(memory.RunRetention{}))
	ref := enqueue(t, c, workflow.Job{Workflow: "w"})
	waitStatus(t, c, ref, workflow.StatusSucceeded)
	fc.Advance(365 * 24 * time.Hour)
	enqueue(t, c, workflow.Job{Workflow: "w"})
	if _, err := c.Status(context.Background(), ref); err != nil {
		t.Fatalf("RunRetention{} must keep every run: %v", err)
	}
	if d := memory.DefaultRunRetention; d.MaxAge <= 0 || d.MaxRuns <= 0 {
		t.Fatalf("DefaultRunRetention = %+v, want both limits set", d)
	}
	def, dfc := newClient(t, []workflow.Workflow{one("w", ok)})
	old := enqueue(t, def, workflow.Job{Workflow: "w"})
	waitStatus(t, def, old, workflow.StatusSucceeded)
	dfc.Advance(memory.DefaultRunRetention.MaxAge + time.Second)
	enqueue(t, def, workflow.Job{Workflow: "w"})
	if _, err := def.Status(context.Background(), old); !errors.Is(err, workflow.ErrNotFound) {
		t.Fatalf("the default retention kept a run older than MaxAge: %v", err)
	}
}

func TestRunsByRefFindsADelayedRunBeforeAndAfterItStarts(t *testing.T) {
	c, fc := newClient(t, []workflow.Workflow{one("w", ok)})
	ref := enqueue(t, c, workflow.Job{Workflow: "w", RunAt: start.Add(time.Hour)})
	page, err := c.Runs(context.Background(), workflow.RunQuery{Ref: ref})
	if err != nil || len(page.Runs) != 1 || page.Runs[0].Status != workflow.StatusScheduled {
		t.Fatalf("Runs(Ref) of a delayed run = %+v, %v", page.Runs, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), pollFor)
	defer cancel()
	if err := fc.BlockUntil(ctx, 1); err != nil {
		t.Fatal(err)
	}
	fc.Advance(time.Hour)
	waitStatus(t, c, ref, workflow.StatusSucceeded)
	page, err = c.Runs(context.Background(), workflow.RunQuery{Ref: ref}) // still only the ScheduledID
	if err != nil || len(page.Runs) != 1 || page.Runs[0].Ref.RunID.IsZero() {
		t.Fatalf("Runs(Ref) after the delayed run started = %+v, %v; want it with its RunID", page.Runs, err)
	}
}

func TestPausedTicksNeverFireEvenUnderFireAll(t *testing.T) {
	var n atomic.Int64
	c, fc := newClient(t, []workflow.Workflow{one("w", func(context.Context, []byte) ([]byte, error) { n.Add(1); return nil, nil })})
	id, err := c.Schedule(context.Background(), workflow.Schedule{Name: "s", Workflow: "w", Cron: "* * * * *", Location: time.UTC,
		Misfire: workflow.Misfire{Policy: workflow.MisfireFireAll, Max: 10}})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.PauseSchedule(context.Background(), workflow.ScheduleTarget{ID: id}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), pollFor)
	defer cancel()
	for range 3 { // three ticks fall due while paused
		if err := fc.BlockUntil(ctx, 1); err != nil {
			t.Fatal(err)
		}
		fc.Advance(time.Minute)
		time.Sleep(5 * time.Millisecond)
	}
	if err := c.ResumeSchedule(context.Background(), workflow.ScheduleTarget{ID: id}); err != nil {
		t.Fatal(err)
	}
	if err := fc.BlockUntil(ctx, 1); err != nil {
		t.Fatal(err)
	}
	fc.Advance(time.Minute)
	eventually(t, "the first tick after the resume", func() bool { return n.Load() >= 1 })
	time.Sleep(20 * time.Millisecond)
	if got := n.Load(); got != 1 {
		t.Fatalf("MisfireFireAll after a pause of three ticks fired %d runs, want only the tick after the resume", got)
	}
}
