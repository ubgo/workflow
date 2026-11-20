package redis_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/ubgo/workflow"
	wfredis "github.com/ubgo/workflow/redis"
)

type versionLog struct {
	mu  sync.Mutex
	ran []string
}

func (l *versionLog) add(s string) { l.mu.Lock(); l.ran = append(l.ran, s); l.mu.Unlock() }
func (l *versionLog) all() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.ran...)
}

// versioned is one definition of workflow "w": step a, a 50 ms sleep, then
// step b, each logging who ran it and the run's version.
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

func startVersioned(t *testing.T, rdb goredis.UniversalClient, prefix string, wf workflow.Workflow, opts ...wfredis.Option) *wfredis.Client {
	t.Helper()
	c := newClient(t, rdb, prefix, opts...)
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

// TestWorkflowVersions (LO-11): a run enqueued by v2 code is never taken
// by a v1 worker and waits for a v2 one; a pinned v2 worker leaves a v1
// run alone; a draining v2 worker (MinVersion 0) runs it; a sleeping v2
// run is not resumed by v1 code; RunInfo and StepInfo report the version.
// Runs of a workflow with a concurrency policy wait for admission by a
// worker of their version too.
func TestWorkflowVersions(t *testing.T) {
	rdb := redisClient(t)
	prefix := testPrefix(t, rdb)
	ctx := context.Background()
	log := &versionLog{}
	old := startVersioned(t, rdb, prefix, versioned("old", 1, 0, log))
	web := startVersioned(t, rdb, prefix, versioned("web", 2, 0, log), wfredis.WithEnqueueOnly())
	ref2, err := web.Enqueue(ctx, workflow.Job{Workflow: "w"})
	if err != nil {
		t.Fatal(err)
	}
	ref1, err := old.Enqueue(ctx, workflow.Job{Workflow: "w"})
	if err != nil {
		t.Fatal(err)
	}
	waitStatus(t, old, ref1, workflow.StatusSucceeded)
	time.Sleep(10 * testPoll)
	if st, _ := old.Status(ctx, ref2); st != workflow.StatusQueued {
		t.Fatalf("v2 run on a v1-only fleet is %v; want queued", st)
	}
	if page, err := old.Runs(ctx, workflow.RunQuery{Ref: ref2}); err != nil || len(page.Runs) != 1 || page.Runs[0].Version != 2 {
		t.Fatalf("RunInfo of the v2 run = %+v, %v; want Version 2", page.Runs, err)
	}
	pinned := startVersioned(t, rdb, prefix, versioned("pinned", 2, 2, log))
	waitStatus(t, pinned, ref2, workflow.StatusSucceeded)

	// A sleeping v2 run is not resumed by the v1 worker.
	ref2b, err := web.Enqueue(ctx, workflow.Job{Workflow: "w"})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, 10*time.Second, "the v2 run asleep", func() bool {
		st, _ := pinned.Status(ctx, ref2b)
		return st == workflow.StatusWaiting
	})
	pinned.Stop()
	time.Sleep(200*time.Millisecond + 10*testPoll)
	if st, _ := old.Status(ctx, ref2b); st.IsTerminal() {
		t.Fatalf("the v1 worker resumed the sleeping v2 run to %v", st)
	}

	// With the v1 worker gone and only pinned v2 workers, a v1 run waits;
	// a draining worker takes it and the sleeping v2 run.
	old.Stop()
	oldWeb := startVersioned(t, rdb, prefix, versioned("oldweb", 1, 0, log), wfredis.WithEnqueueOnly())
	ref1c, err := oldWeb.Enqueue(ctx, workflow.Job{Workflow: "w"})
	if err != nil {
		t.Fatal(err)
	}
	pinned2 := startVersioned(t, rdb, prefix, versioned("pinned2", 2, 2, log))
	waitStatus(t, pinned2, ref2b, workflow.StatusSucceeded)
	if st, _ := pinned2.Status(ctx, ref1c); st != workflow.StatusQueued {
		t.Fatalf("v1 run with only a pinned v2 worker is %v; want queued", st)
	}
	drain := startVersioned(t, rdb, prefix, versioned("drain", 2, 0, log))
	waitStatus(t, drain, ref1c, workflow.StatusSucceeded)
	for _, line := range log.all() {
		var who, step string
		var v int
		if _, err := fmt.Sscanf(line, "%s %s v%d", &who, &step, &v); err != nil {
			t.Fatal(line, err)
		}
		switch {
		case who == "old" && v != 1, (who == "pinned" || who == "pinned2") && v != 2:
			t.Errorf("%s: a worker ran a run of a version it does not run", line)
		}
	}
}

// TestWorkflowVersionsAdmission: a workflow with a concurrency policy
// waits in its pending set until a worker of the run's version admits it.
func TestWorkflowVersionsAdmission(t *testing.T) {
	rdb := redisClient(t)
	prefix := testPrefix(t, rdb)
	ctx := context.Background()
	log := &versionLog{}
	limited := func(who string, v int) workflow.Workflow {
		wf := versioned(who, v, 0, log)
		wf.Concurrency = &workflow.Concurrency{MaxRuns: 1, Key: workflow.KeyFunc(func(workflow.Job) string { return "k" })}
		return wf
	}
	old := startVersioned(t, rdb, prefix, limited("old", 1))
	web := startVersioned(t, rdb, prefix, limited("web", 2), wfredis.WithEnqueueOnly())
	ref, err := web.Enqueue(ctx, workflow.Job{Workflow: "w"})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(10 * testPoll)
	if st, _ := old.Status(ctx, ref); st != workflow.StatusQueued || len(log.all()) != 0 {
		t.Fatalf("v2 run with a v1 worker = %v, log %v; want queued and untouched", st, log.all())
	}
	now := startVersioned(t, rdb, prefix, limited("new", 2))
	waitStatus(t, now, ref, workflow.StatusSucceeded)
}
