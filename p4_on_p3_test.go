package workflow_test

// Patterns built from the P3 flow blocks (MultiChoice, Race, WithRules,
// Suspension) and the P2 DataStore, each proven on the memory driver:
// WC-37, WX-08, WP-12, WP-25, WP-29, AI-07 and PS-03 of the feature
// catalogue.

import (
	"context"
	"encoding/json"
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

// TestLocalSynchronizingMerge proves WC-37: a merge step listing several
// branches of a MultiChoice in After (not the choice's own join) runs once,
// after every branch that was taken, and does not wait for a branch that
// was not taken.
func TestLocalSynchronizingMerge(t *testing.T) {
	var mu sync.Mutex
	var order []string
	note := func(s string) { mu.Lock(); order = append(order, s); mu.Unlock() }
	branch := func(name string, d time.Duration) workflow.Branch {
		return workflow.Branch{Name: name, Run: func(context.Context, []byte) ([]byte, error) {
			time.Sleep(d)
			note(name)
			return json.Marshal(name)
		}}
	}
	steps, err := workflow.MultiChoice(workflow.MultiChoiceSpec{
		Name:     "notify",
		Decide:   func(context.Context, []byte) ([]string, error) { return []string{"email", "sms"}, nil },
		Branches: []workflow.Branch{branch("email", 40*time.Millisecond), branch("sms", 0), branch("push", 0)},
	})
	if err != nil {
		t.Fatal(err)
	}
	var merges atomic.Int32
	steps = append(steps, workflow.Step{
		Name: "audit", After: []string{"notify.email", "notify.sms", "notify.push"},
		Run: func(context.Context, []byte) ([]byte, error) {
			merges.Add(1)
			note("audit")
			return nil, nil
		},
	})
	c := ruleClient(t, workflow.Workflow{Name: "n", Steps: steps})
	if st := waitRun(t, c, enqueue(t, c, workflow.Job{Workflow: "n"})); st != workflow.StatusSucceeded {
		t.Fatalf("status %s", st)
	}
	mu.Lock()
	defer mu.Unlock()
	if merges.Load() != 1 {
		t.Fatalf("merge ran %d times, want once", merges.Load())
	}
	if len(order) != 3 || order[2] != "audit" || slices.Contains(order, "push") {
		t.Fatalf("order %v: want both taken branches, then the merge once, and push never", order)
	}
}

// TestDetourHoldAndRedo proves the detours of WX-08 that compose from
// existing operations: an operator holds one run between steps
// (SuspendRun), resumes it, and after a failure caused upstream re-runs it
// from an earlier step (ResetRun) without repeating the steps before it.
func TestDetourHoldAndRedo(t *testing.T) {
	journal := workflow.NewMemoryJournal()
	c, err := workflow.Use(memory.New(), workflow.Journal(journal), workflow.Suspension(journal, 2*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	gate := make(chan struct{})
	var fetches, transforms, loads atomic.Int32
	var fixed atomic.Bool
	if err := c.Register(workflow.Workflow{Name: "import", Steps: []workflow.Step{
		{Name: "fetch", Run: func(context.Context, []byte) ([]byte, error) {
			fetches.Add(1)
			<-gate
			return []byte(`"rows"`), nil
		}},
		{Name: "transform", After: []string{"fetch"}, Run: func(context.Context, []byte) ([]byte, error) {
			transforms.Add(1)
			if !fixed.Load() {
				return nil, workflow.Permanent(errors.New("bad mapping"))
			}
			return []byte(`"mapped"`), nil
		}},
		{Name: "load", After: []string{"transform"}, Run: func(context.Context, []byte) ([]byte, error) {
			loads.Add(1)
			return nil, nil
		}},
	}}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	ref := enqueue(t, c, workflow.Job{Workflow: "import"})
	statusEventually(t, c, ref, workflow.StatusRunning)
	if err := workflow.SuspendRun(ctx, journal, ref); err != nil {
		t.Fatal(err)
	}
	close(gate)
	time.Sleep(30 * time.Millisecond)
	if n := transforms.Load(); n != 0 {
		t.Fatalf("transform ran %d time(s) while the run was held", n)
	}
	if err := workflow.ResumeRun(ctx, journal, ref); err != nil {
		t.Fatal(err)
	}
	statusEventually(t, c, ref, workflow.StatusFailed)

	fixed.Store(true) // the mapping is deployed fixed
	rr, ok := c.(workflow.RunResetter)
	if !ok {
		t.Fatal("the plugin client does not forward ResetRun")
	}
	if _, err := rr.ResetRun(ctx, ref, "transform"); err != nil {
		t.Fatal(err)
	}
	statusEventually(t, c, ref, workflow.StatusSucceeded)
	if f, tr, l := fetches.Load(), transforms.Load(), loads.Load(); f != 1 || tr != 2 || l != 1 {
		t.Fatalf("fetch %d, transform %d, load %d: want 1, 2, 1 (fetch kept, transform redone)", f, tr, l)
	}
}

// TestSetupAndTeardown proves WP-12: a setup step acquires a resource and
// stores its ID as run data, a Finally step releases it after the work
// whether the work succeeded or failed, and the run's status stays honest.
func TestSetupAndTeardown(t *testing.T) {
	for _, tc := range []struct {
		name    string
		workErr error
		want    workflow.Status
	}{
		{"work succeeds", nil, workflow.StatusSucceeded},
		{"work fails", workflow.Permanent(errors.New("test run broke")), workflow.StatusFailed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var mu sync.Mutex
			var log []string
			note := func(s string) { mu.Lock(); log = append(log, s); mu.Unlock() }
			wf, err := workflow.WithRules(workflow.Workflow{Name: "e2e", Steps: []workflow.Step{
				{Name: "setup", Run: func(ctx context.Context, _ []byte) ([]byte, error) {
					note("create env-42")
					return nil, workflow.SetRunData(ctx, "env", "env-42")
				}},
				{Name: "work", After: []string{"setup"}, Run: func(context.Context, []byte) ([]byte, error) {
					note("test")
					return nil, tc.workErr
				}},
				{Name: "teardown", Run: func(ctx context.Context, _ []byte) ([]byte, error) {
					env, ok, err := workflow.RunData[string](ctx, "env")
					if err != nil || !ok {
						return nil, workflow.Permanent(errors.New("no environment recorded"))
					}
					note("delete " + env)
					return nil, nil
				}},
			}}, workflow.RulesSpec{Finally: []string{"teardown"}})
			if err != nil {
				t.Fatal(err)
			}
			c := flowClient(t, workflow.NewMemoryJournal(), wf)
			if st := waitRun(t, c, enqueue(t, c, workflow.Job{Workflow: "e2e"})); st != tc.want {
				t.Fatalf("status %s, want %s", st, tc.want)
			}
			mu.Lock()
			defer mu.Unlock()
			if !slices.Equal(log, []string{"create env-42", "test", "delete env-42"}) {
				t.Fatalf("log %v, want setup, work, then teardown of the same environment", log)
			}
		})
	}
}

// raceWithDeadline registers an offer workflow that races on the reply
// event, and a deadline workflow that answers "expire" for it: the
// default branch of WP-25, sent by a delayed job.
func raceWithDeadline(t *testing.T) workflow.Client {
	t.Helper()
	steps, err := workflow.Race(workflow.RaceSpec{
		Name: "reply", Event: "offer.reply", KeyFrom: workflow.KeyFunc(func(j workflow.Job) string { return string(j.Payload) }),
		Branches: []workflow.Branch{
			{Name: "accept", Run: func(context.Context, []byte) ([]byte, error) { return []byte(`"accepted"`), nil }},
			{Name: "decline", Run: func(context.Context, []byte) ([]byte, error) { return []byte(`"declined"`), nil }},
			{Name: "expire", Run: func(context.Context, []byte) ([]byte, error) { return []byte(`"expired"`), nil }},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	var c workflow.Client
	deadline := workflow.Workflow{Name: "offer.deadline", Steps: []workflow.Step{{Name: "expire", Run: func(ctx context.Context, in []byte) ([]byte, error) {
		return nil, workflow.SignalChoice(ctx, c, "offer.reply", string(in), "expire", nil)
	}}}}
	c = ruleClient(t, workflow.Workflow{Name: "offer", Steps: steps}, deadline)
	return c
}

// TestRaceDefaultBranchAtDeadline proves WP-25: a delayed job sends the
// default branch at the deadline, so an offer nobody answers takes the
// "expire" branch, and an answer before the deadline wins over it.
func TestRaceDefaultBranchAtDeadline(t *testing.T) {
	c := raceWithDeadline(t)
	ctx := context.Background()
	start := func(id string, after time.Duration) (workflow.RunRef, workflow.RunRef) {
		ref := enqueue(t, c, workflow.Job{Workflow: "offer", Payload: []byte(id)})
		dl := enqueue(t, c, workflow.Job{Workflow: "offer.deadline", Payload: []byte(id), RunAt: time.Now().Add(after)})
		return ref, dl
	}

	silent, dl := start("o1", 30*time.Millisecond)
	out, err := workflow.AwaitResult(awaitCtx(t), c, silent, fastAwait)
	if err != nil || string(out) != `"expired"` {
		t.Fatalf("unanswered offer: out %s err %v, want the default branch", out, err)
	}
	if st := waitRun(t, c, dl); st != workflow.StatusSucceeded {
		t.Fatalf("deadline job %s", st)
	}

	answered, dl := start("o2", 300*time.Millisecond)
	if err := workflow.SignalChoice(ctx, c, "offer.reply", "o2", "accept", nil); err != nil {
		t.Fatal(err)
	}
	out, err = workflow.AwaitResult(awaitCtx(t), c, answered, fastAwait)
	if err != nil || string(out) != `"accepted"` {
		t.Fatalf("answered offer: out %s err %v, want accept to beat the deadline", out, err)
	}
	if st := waitRun(t, c, dl); st != workflow.StatusSucceeded {
		t.Fatalf("late deadline job %s: its signal must lose quietly", st)
	}
	out, err = workflow.AwaitResult(awaitCtx(t), c, answered, fastAwait)
	if err != nil || string(out) != `"accepted"` {
		t.Fatalf("after the deadline fired: out %s err %v", out, err)
	}
}

// TestCompositeWaits proves WP-29: "all of" is wait steps joined by a
// step's After, "any of" is a Race every condition answers, and a mix,
// (paid AND packed) OR cancelled, is a child run doing the "all of" that
// answers the parent's race, cancelled by CancelChildren when the other
// condition wins.
func TestCompositeWaits(t *testing.T) {
	key := workflow.KeyFunc(func(j workflow.Job) string { return string(j.Payload) })
	var c workflow.Client
	both := workflow.Workflow{Name: "order.ready", Steps: []workflow.Step{
		{Name: "paid", Wait: &workflow.WaitSpec{Event: "order.paid", KeyFrom: key}},
		{Name: "packed", Wait: &workflow.WaitSpec{Event: "order.packed", KeyFrom: key}},
		{Name: "both", After: []string{"paid", "packed"}, Run: func(ctx context.Context, _ []byte) ([]byte, error) {
			info, _ := workflow.StepInfoFrom(ctx)
			return nil, workflow.SignalChoice(ctx, c, "order.decided", info.Labels["order"], "ship", nil)
		}},
	}}
	race, err := workflow.Race(workflow.RaceSpec{
		Name: "decided", After: []string{"watch"}, Event: "order.decided", KeyFrom: key,
		Branches: []workflow.Branch{
			{Name: "ship", Run: func(context.Context, []byte) ([]byte, error) { return []byte(`"shipped"`), nil }},
			{Name: "cancel", Run: func(ctx context.Context, _ []byte) ([]byte, error) {
				info, _ := workflow.StepInfoFrom(ctx)
				if _, err := workflow.CancelChildren(ctx, c, info.RunID, "watch"); err != nil {
					return nil, err
				}
				return []byte(`"cancelled"`), nil
			}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	order := workflow.Workflow{Name: "order", Steps: append([]workflow.Step{{Name: "watch", Run: func(ctx context.Context, in []byte) ([]byte, error) {
		_, err := workflow.Spawn(ctx, c, "watch", []workflow.Job{{Workflow: "order.ready", Payload: in, Labels: map[string]string{"order": string(in)}}})
		return nil, err
	}}}, race...)}
	c = flowClient(t, workflow.NewMemoryJournal(), order, both)
	ctx := context.Background()
	// child waits for the run's watcher child: an event reaches only runs
	// enqueued before it was sent (Client.Signal), so the conditions are
	// signalled once the watcher exists.
	child := func(ref workflow.RunRef) workflow.RunRef {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for {
			page, err := c.(workflow.RunQuerier).Runs(ctx, workflow.RunQuery{Workflow: "order.ready", Labels: workflow.ChildLabels(ref.RunID, "watch")})
			if err != nil {
				t.Fatal(err)
			}
			if len(page.Runs) == 1 {
				return page.Runs[0].Ref
			}
			if time.Now().After(deadline) {
				t.Fatalf("children of %s: %v", ref.RunID, page.Runs)
			}
			time.Sleep(time.Millisecond)
		}
	}

	// All of: one condition alone decides nothing; both ship the order.
	a := enqueue(t, c, workflow.Job{Workflow: "order", Payload: []byte("a")})
	child(a)
	if err := c.Signal(ctx, workflow.Event{Name: "order.paid", Key: "a"}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond)
	if st, _ := c.Status(ctx, a); st.IsTerminal() {
		t.Fatalf("one of two conditions ended the run: %s", st)
	}
	if err := c.Signal(ctx, workflow.Event{Name: "order.packed", Key: "a"}); err != nil {
		t.Fatal(err)
	}
	if out, err := workflow.AwaitResult(awaitCtx(t), c, a, fastAwait); err != nil || string(out) != `"shipped"` {
		t.Fatalf("paid and packed: out %s err %v", out, err)
	}

	// Any of: the cancel answers first and the half-met child is cancelled.
	b := enqueue(t, c, workflow.Job{Workflow: "order", Payload: []byte("b")})
	bw := child(b)
	if err := c.Signal(ctx, workflow.Event{Name: "order.paid", Key: "b"}); err != nil {
		t.Fatal(err)
	}
	if err := workflow.SignalChoice(ctx, c, "order.decided", "b", "cancel", nil); err != nil {
		t.Fatal(err)
	}
	if out, err := workflow.AwaitResult(awaitCtx(t), c, b, fastAwait); err != nil || string(out) != `"cancelled"` {
		t.Fatalf("cancelled: out %s err %v", out, err)
	}
	if st := waitRun(t, c, bw); st != workflow.StatusCancelled {
		t.Fatalf("the child waiting on packed is %s, want cancelled", st)
	}
}

// TestAcceleratorQueue proves AI-07 on one process: the GPU step runs as
// a child on a queue of its own with one worker, so GPU work never runs
// twice at once while the CPU queue's steps run side by side. Across a
// fleet the same split is a client per node with WithQueues
// (TestQueuesSplitAcrossClients in sqlstore/enginetest).
func TestAcceleratorQueue(t *testing.T) {
	var gpuNow, gpuPeak, cpuNow, cpuPeak atomic.Int32
	peak := func(now, max *atomic.Int32) func() {
		n := now.Add(1)
		for {
			m := max.Load()
			if n <= m || max.CompareAndSwap(m, n) {
				break
			}
		}
		return func() { now.Add(-1) }
	}
	c, err := workflow.Use(memory.New(memory.WithQueues(
		workflow.QueueConfig{Name: "cpu", Workers: 4},
		workflow.QueueConfig{Name: "gpu", Workers: 1},
	)), workflow.Journal(workflow.NewMemoryJournal()))
	if err != nil {
		t.Fatal(err)
	}
	parent := c
	for _, wf := range []workflow.Workflow{
		{Name: "infer", Queue: "gpu", Steps: []workflow.Step{{Name: "model", Run: func(context.Context, []byte) ([]byte, error) {
			defer peak(&gpuNow, &gpuPeak)()
			time.Sleep(15 * time.Millisecond)
			return []byte(`"labels"`), nil
		}}}},
		{Name: "photo", Queue: "cpu", Steps: []workflow.Step{
			{Name: "resize", Run: func(context.Context, []byte) ([]byte, error) {
				defer peak(&cpuNow, &cpuPeak)()
				time.Sleep(15 * time.Millisecond)
				return nil, nil
			}},
			{Name: "label", After: []string{"resize"}, Run: func(ctx context.Context, _ []byte) ([]byte, error) {
				return workflow.AwaitChild(ctx, parent, "label", workflow.Job{Workflow: "infer"}, workflow.ChildOptions{Poll: 2 * time.Millisecond})
			}},
		}},
	} {
		if err := c.Register(wf); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	var refs []workflow.RunRef
	for range 4 {
		refs = append(refs, enqueue(t, c, workflow.Job{Workflow: "photo"}))
	}
	for _, ref := range refs {
		if out, err := workflow.AwaitResult(awaitCtx(t), c, ref, fastAwait); err != nil || string(out) != `"labels"` {
			t.Fatalf("photo: out %s err %v", out, err)
		}
	}
	if g, p := gpuPeak.Load(), cpuPeak.Load(); g != 1 || p < 2 {
		t.Fatalf("peak GPU %d (want 1), peak CPU %d (want 2 or more)", g, p)
	}
}

// TestScheduleCursor proves PS-03: a scheduled run keeps a cursor in the
// schedule's own DataStore folder and advances it with UpdateData, so
// each tick takes up where the last one stopped and no item is processed
// twice.
func TestScheduleCursor(t *testing.T) {
	const batch = 3
	items := []int{1, 2, 3, 4, 5, 6, 7, 8}
	var mu sync.Mutex
	var done []int
	data := workflow.NewMemoryData()
	fc := clock.NewFake(time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC))
	c, err := workflow.Use(memory.New(memory.WithClock(fc)), workflow.Data(data))
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Register(workflow.Workflow{Name: "sync", Steps: []workflow.Step{{Name: "pull", Run: func(ctx context.Context, _ []byte) ([]byte, error) {
		info, _ := workflow.StepInfoFrom(ctx)
		folder := workflow.Folder("schedule/" + info.Labels[workflow.LabelSchedule])
		_, err := workflow.UpdateData(ctx, folder, "cursor", func(cur int, _ bool) (int, error) {
			for _, it := range items {
				if it > cur && it <= cur+batch {
					mu.Lock()
					done = append(done, it)
					mu.Unlock()
				}
			}
			return min(cur+batch, items[len(items)-1]), nil
		})
		return nil, err
	}}}}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	id, err := c.Schedule(ctx, workflow.Schedule{Name: "pull", Workflow: "sync", Cron: "0 * * * *", Location: time.UTC})
	if err != nil {
		t.Fatal(err)
	}
	for tick := 1; tick <= 3; tick++ {
		bctx, bcancel := context.WithTimeout(ctx, 5*time.Second)
		if err := fc.BlockUntil(bctx, 1); err != nil {
			bcancel()
			t.Fatal(err)
		}
		bcancel()
		fc.Advance(time.Hour)
		want := min(tick*batch, len(items))
		deadline := time.Now().Add(5 * time.Second)
		for {
			mu.Lock()
			n := len(done)
			mu.Unlock()
			if n == want {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("tick %d: %d items done, want %d", tick, n, want)
			}
			time.Sleep(time.Millisecond)
		}
		// Wait for the tick's run to store its cursor before the next one.
		for {
			raw, _, ok, err := data.Get(ctx, workflow.Folder("schedule/"+string(id)), "cursor")
			if err != nil {
				t.Fatal(err)
			}
			var cur int
			if ok {
				_ = json.Unmarshal(raw, &cur)
			}
			if cur == want {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("tick %d: cursor %d, want %d", tick, cur, want)
			}
			time.Sleep(time.Millisecond)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if !slices.Equal(done, items) {
		t.Fatalf("processed %v, want each item once in order", done)
	}
}
