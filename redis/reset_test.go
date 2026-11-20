package redis_test

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	wfredis "github.com/ubgo/workflow/redis"
)

// TestForkKeepsRunFields: a fork made on a client whose definition is a
// newer version runs on the original's version, queue, priority and
// tenant, starts at the chosen step, and is listed by Runs and by a label
// query on LabelForkOf.
func TestForkKeepsRunFields(t *testing.T) {
	rdb := redisClient(t)
	prefix := testPrefix(t, rdb)
	ctx := context.Background()
	log := &versionLog{}
	old := startVersioned(t, rdb, prefix, versioned("old", 1, 0, log))
	web := startVersioned(t, rdb, prefix, versioned("web", 2, 0, log), wfredis.WithEnqueueOnly())
	ref, err := old.Enqueue(ctx, workflow.Job{Workflow: "w", Queue: "q1", Priority: 5, Tenant: "t1", Payload: []byte(`"in"`)})
	if err != nil {
		t.Fatal(err)
	}
	waitStatus(t, old, ref, workflow.StatusSucceeded)
	fork, err := web.ForkRun(ctx, ref, "b", workflow.ForkOptions{})
	if err != nil {
		t.Fatal(err)
	}
	waitStatus(t, old, fork, workflow.StatusSucceeded)
	page, err := web.Runs(ctx, workflow.RunQuery{Labels: map[string]string{workflow.LabelForkOf: ref.RunID.String()}})
	if err != nil || len(page.Runs) != 1 {
		t.Fatalf("label query for the fork: %+v %v", page.Runs, err)
	}
	info := page.Runs[0]
	if info.Ref.RunID != fork.RunID || info.Version != 1 || info.Queue != "q1" || info.Priority != 5 || info.Tenant != "t1" ||
		string(info.Payload) != `"in"` || info.Labels[workflow.LabelRedo] != "true" || info.Labels[workflow.LabelForkStep] != "b" {
		t.Fatalf("fork info %+v: want version 1, queue q1, priority 5, tenant t1, the payload, LabelRedo and LabelForkStep b", info)
	}
	if string(info.Output) != `"in"` {
		t.Fatalf("fork output %q, want the kept steps' output passed through", info.Output)
	}
	ran := log.all()
	if got := countPrefix(ran, "old a"); got != 1 {
		t.Fatalf("step a ran %d times, want 1 (kept in the fork): %v", got, ran)
	}
	if got := countPrefix(ran, "old b v1"); got != 2 {
		t.Fatalf("step b ran %d times on v1, want 2: %v", got, ran)
	}
	if st, _ := old.Status(ctx, ref); st != workflow.StatusSucceeded {
		t.Fatalf("the original is %s after the fork, want succeeded", st)
	}
}

func countPrefix(lines []string, p string) int {
	n := 0
	for _, l := range lines {
		if strings.HasPrefix(l, p) {
			n++
		}
	}
	return n
}

// TestForkIdempotency: a workflow that requires a key refuses a fork
// without ForkOptions.IdempotencyKey, a key held by an unfinished run
// returns that run and writes nothing, a free key starts the fork, and
// the original's key is never copied.
func TestForkIdempotency(t *testing.T) {
	rdb := redisClient(t)
	prefix := testPrefix(t, rdb)
	ctx := context.Background()
	c := newClient(t, rdb, prefix)
	wf := workflow.Workflow{Name: "idem", Idempotency: workflow.IdempotencyRequired, Steps: []workflow.Step{
		{Name: "a", Run: func(context.Context, []byte) ([]byte, error) { return []byte(`1`), nil }},
		{Name: "b", After: []string{"a"}, Run: func(context.Context, []byte) ([]byte, error) { return nil, workflow.Permanent(errors.New("no")) }},
	}}
	if err := c.Register(wf); err != nil {
		t.Fatal(err)
	}
	sctx, cancel := context.WithCancel(ctx)
	t.Cleanup(cancel)
	if err := c.Start(sctx); err != nil {
		t.Fatal(err)
	}
	ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "idem", IdempotencyKey: "orig"})
	if err != nil {
		t.Fatal(err)
	}
	waitStatus(t, c, ref, workflow.StatusFailed)
	if _, err := c.ForkRun(ctx, ref, "b", workflow.ForkOptions{}); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("fork without a key of a key-required workflow: %v, want ErrInvalid", err)
	}
	holder, err := c.Enqueue(ctx, workflow.Job{Workflow: "idem", IdempotencyKey: "held", RunAt: time.Now().Add(time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	before, err := c.Runs(ctx, workflow.RunQuery{})
	if err != nil {
		t.Fatal(err)
	}
	dctx, outcome := workflow.WithEnqueueOutcome(ctx)
	got, err := c.ForkRun(dctx, ref, "b", workflow.ForkOptions{IdempotencyKey: "held"})
	if err != nil || got != holder || !outcome.Duplicate {
		t.Fatalf("fork with a held key: %+v %v (outcome %+v), want the holder %+v", got, err, *outcome, holder)
	}
	after, err := c.Runs(ctx, workflow.RunQuery{})
	if err != nil || len(after.Runs) != len(before.Runs) {
		t.Fatalf("a fork with a held key wrote a run: %d runs before, %d after (%v)", len(before.Runs), len(after.Runs), err)
	}
	fork, err := c.ForkRun(ctx, ref, "b", workflow.ForkOptions{IdempotencyKey: "fresh"})
	if err != nil || fork.RunID.IsZero() || fork.RunID == ref.RunID {
		t.Fatalf("fork with a free key: %+v %v", fork, err)
	}
	waitStatus(t, c, fork, workflow.StatusFailed)
	if again, err := c.Enqueue(ctx, workflow.Job{Workflow: "idem", IdempotencyKey: "orig"}); err != nil || again == ref {
		t.Fatalf("the original's key after a fork: %+v %v; want a new run (the fork must not hold it)", again, err)
	}
}

// TestResetSucceededChainPending: a succeeded run whose chain was not
// handed on (the next job could not be created) is refused with
// ErrWrongState by ResetRunWith with Succeeded, since a reset would hand
// that chain on later; a run that handed its chain on resets, carries
// LabelRedo where a label query finds it, and hands on nothing more.
func TestResetSucceededChainPending(t *testing.T) {
	rdb := redisClient(t)
	prefix := testPrefix(t, rdb)
	ctx := context.Background()
	c := newClient(t, rdb, prefix)
	var nexts atomic.Int32
	for _, wf := range []workflow.Workflow{
		{Name: "first", Steps: []workflow.Step{{Name: "a", Run: func(context.Context, []byte) ([]byte, error) { return nil, nil }}}},
		{Name: "keyed", Idempotency: workflow.IdempotencyRequired, Steps: []workflow.Step{{Name: "n",
			Run: func(context.Context, []byte) ([]byte, error) { nexts.Add(1); return nil, nil }}}},
	} {
		if err := c.Register(wf); err != nil {
			t.Fatal(err)
		}
	}
	sctx, cancel := context.WithCancel(ctx)
	t.Cleanup(cancel)
	if err := c.Start(sctx); err != nil {
		t.Fatal(err)
	}
	stuck, err := c.Enqueue(ctx, workflow.Job{Workflow: "first", Chain: []workflow.Job{{Workflow: "keyed"}}})
	if err != nil {
		t.Fatal(err)
	}
	waitStatus(t, c, stuck, workflow.StatusSucceeded)
	if _, err := c.ResetRunWith(ctx, stuck, "a", workflow.ResetOptions{Succeeded: true}); !errors.Is(err, workflow.ErrWrongState) {
		t.Fatalf("reset of a succeeded run holding its chain: %v, want ErrWrongState", err)
	}
	page, err := c.Runs(ctx, workflow.RunQuery{Labels: map[string]string{workflow.LabelRedo: "true"}})
	if err != nil || len(page.Runs) != 0 {
		t.Fatalf("a refused reset left LabelRedo: %+v %v", page.Runs, err)
	}

	ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "first", Chain: []workflow.Job{{Workflow: "keyed", IdempotencyKey: "k"}}})
	if err != nil {
		t.Fatal(err)
	}
	waitStatus(t, c, ref, workflow.StatusSucceeded)
	waitFor(t, 10*time.Second, "the chained run", func() bool {
		p, _ := c.Runs(ctx, workflow.RunQuery{Workflow: "keyed", Statuses: []workflow.Status{workflow.StatusSucceeded}})
		return len(p.Runs) == 1
	})
	if _, err := c.ResetRunWith(ctx, ref, "a", workflow.ResetOptions{Succeeded: true}); err != nil {
		t.Fatal(err)
	}
	waitStatus(t, c, ref, workflow.StatusSucceeded)
	page, err = c.Runs(ctx, workflow.RunQuery{Labels: map[string]string{workflow.LabelRedo: "true"}})
	if err != nil || len(page.Runs) != 1 || page.Runs[0].Ref.RunID != ref.RunID {
		t.Fatalf("label query for LabelRedo: %+v %v, want the reset run", page.Runs, err)
	}
	time.Sleep(20 * testPoll)
	p, err := c.Runs(ctx, workflow.RunQuery{Workflow: "keyed"})
	if err != nil || len(p.Runs) != 1 || nexts.Load() != 1 {
		t.Fatalf("chained runs after the reset: %d (%v), ran %d times; want 1", len(p.Runs), err, nexts.Load())
	}
}
