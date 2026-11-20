package workflow_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ubgo/workflow"
)

// gate is a child workflow whose runs finish when told: payload "ok"
// succeeds at once, "fail" fails at once, "hold" waits for release.
type gate struct {
	release chan struct{}
	started atomic.Int32
}

func newGate() *gate { return &gate{release: make(chan struct{})} }

func (g *gate) workflow() workflow.Workflow {
	return workflow.Workflow{Name: "child", Steps: []workflow.Step{{Name: "work", Run: func(ctx context.Context, in []byte) ([]byte, error) {
		g.started.Add(1)
		var mode string
		if err := json.Unmarshal(in, &mode); err != nil {
			return nil, workflow.Permanent(err)
		}
		switch mode {
		case "fail":
			return nil, workflow.Permanent(errors.New("child failed"))
		case "hold":
			select {
			case <-g.release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return json.Marshal("done " + mode)
	}}}}
}

func childJobs(modes ...string) []workflow.Job {
	jobs := make([]workflow.Job, len(modes))
	for i, m := range modes {
		jobs[i] = workflow.Job{Workflow: "child", Payload: []byte(`"` + m + `"`)}
	}
	return jobs
}

// joinParent registers a parent whose single step runs JoinChildren over
// modes and returns the JoinResult as JSON; the client is filled in after
// it is built.
func joinParent(c *workflow.Client, modes []string, spec workflow.JoinSpec) workflow.Workflow {
	spec.Poll = time.Millisecond
	return workflow.Workflow{Name: "parent", Retry: workflow.RetryPolicy{Max: 1}, Steps: []workflow.Step{{Name: "join", Run: func(ctx context.Context, _ []byte) ([]byte, error) {
		res, err := workflow.JoinChildren(ctx, *c, "children", childJobs(modes...), spec)
		if err != nil {
			return nil, err
		}
		return json.Marshal(res)
	}}}}
}

func runJoin(t *testing.T, g *gate, modes []string, spec workflow.JoinSpec) (workflow.Client, workflow.RunRef, workflow.JoinResult, error) {
	t.Helper()
	var c workflow.Client
	c = flowClient(t, workflow.NewMemoryJournal(), g.workflow(), joinParent(&c, modes, spec))
	ref := enqueue(t, c, workflow.Job{Workflow: "parent"})
	out, err := workflow.AwaitResult(context.Background(), c, ref, workflow.AwaitOptions{Poll: time.Millisecond})
	var res workflow.JoinResult
	if err == nil {
		if jerr := json.Unmarshal(out, &res); jerr != nil {
			t.Fatal(jerr)
		}
	}
	return c, ref, res, err
}

func childStatuses(t *testing.T, c workflow.Client, refs []workflow.RunRef) []workflow.Status {
	t.Helper()
	out := make([]workflow.Status, len(refs))
	for i, r := range refs {
		st, err := c.Status(context.Background(), r)
		if err != nil {
			t.Fatal(err)
		}
		out[i] = st
	}
	return out
}

// TestJoinDiscriminator is WC-09: the first child to succeed wins and the
// others run on, ignored.
func TestJoinDiscriminator(t *testing.T) {
	g := newGate()
	c, _, res, err := runJoin(t, g, []string{"hold", "ok", "hold"}, workflow.JoinSpec{Need: 1})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(res.Winners, []int{1}) || string(res.Outputs[0]) != `"done ok"` {
		t.Fatalf("winners %v outputs %s", res.Winners, res.Outputs)
	}
	if st := childStatuses(t, c, res.Refs); st[0].IsTerminal() || st[2].IsTerminal() {
		t.Fatalf("losers must keep running: %v", st)
	}
	close(g.release)
	for _, r := range res.Refs {
		if st := waitRun(t, c, r); st != workflow.StatusSucceeded {
			t.Fatalf("loser ended %s", st)
		}
	}
}

// TestJoinCancellingPartial is WC-32 and WC-35: two of three, the rest
// cancelled.
func TestJoinCancellingPartial(t *testing.T) {
	g := newGate()
	c, _, res, err := runJoin(t, g, []string{"ok", "hold", "ok"}, workflow.JoinSpec{Need: 2, Rest: workflow.JoinCancelRest})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(res.Winners, []int{0, 2}) {
		t.Fatalf("winners %v", res.Winners)
	}
	if st := waitRun(t, c, res.Refs[1]); st != workflow.StatusCancelled {
		t.Fatalf("rest ended %s, want cancelled", st)
	}
}

// TestJoinCompleteRest is WC-27: the instances still running are forced
// complete and the step moves on.
func TestJoinCompleteRest(t *testing.T) {
	g := newGate()
	c, _, res, err := runJoin(t, g, []string{"hold", "ok", "hold"}, workflow.JoinSpec{Need: 1, Rest: workflow.JoinCompleteRest})
	if err != nil {
		t.Fatal(err)
	}
	for _, i := range []int{0, 2} {
		if st := waitRun(t, c, res.Refs[i]); st != workflow.StatusSucceeded {
			t.Fatalf("child %d ended %s, want forced succeeded", i, st)
		}
	}
}

// TestJoinAll is the AND-join default and the partial join's failure:
// Need 0 waits for every child; a join that can no longer pass fails and
// cancels the rest.
func TestJoinAll(t *testing.T) {
	g := newGate()
	close(g.release)
	_, _, res, err := runJoin(t, g, []string{"ok", "hold", "ok"}, workflow.JoinSpec{})
	if err != nil || len(res.Winners) != 3 {
		t.Fatalf("all: %v %v", res.Winners, err)
	}

	g2 := newGate()
	c, _, _, err := runJoin(t, g2, []string{"fail", "hold", "fail"}, workflow.JoinSpec{Need: 2, Rest: workflow.JoinCancelRest})
	var rf *workflow.RunFailedError
	if !errors.As(err, &rf) || rf.Status != workflow.StatusFailed {
		t.Fatalf("parent: %v", err)
	}
	page, qerr := c.(workflow.RunQuerier).Runs(context.Background(), workflow.RunQuery{Workflow: "child"})
	if qerr != nil {
		t.Fatal(qerr)
	}
	var cancelled int
	for _, r := range page.Runs {
		if r.Status == workflow.StatusCancelled {
			cancelled++
		}
	}
	if cancelled != 1 {
		t.Fatalf("a failed cancelling join must cancel the rest: %d cancelled", cancelled)
	}
}

// TestJoinFailedErrorFromStep checks the error a step sees.
func TestJoinFailedErrorFromStep(t *testing.T) {
	g := newGate()
	var seen error
	var mu sync.Mutex
	var c workflow.Client
	parent := workflow.Workflow{Name: "parent", Steps: []workflow.Step{{Name: "join", Run: func(ctx context.Context, _ []byte) ([]byte, error) {
		_, err := workflow.JoinChildren(ctx, c, "children", childJobs("fail", "fail", "ok"), workflow.JoinSpec{Need: 2, Poll: time.Millisecond})
		if err != nil && !errors.Is(err, workflow.ErrSnoozed) {
			mu.Lock()
			seen = err
			mu.Unlock()
		}
		return nil, err
	}}}}
	c = flowClient(t, workflow.NewMemoryJournal(), g.workflow(), parent)
	waitRun(t, c, enqueue(t, c, workflow.Job{Workflow: "parent"}))
	mu.Lock()
	defer mu.Unlock()
	var jf *workflow.JoinFailedError
	if !errors.As(seen, &jf) || !workflow.IsPermanent(seen) || jf.Need != 2 || len(jf.Failed) != 2 {
		t.Fatalf("step error %v", seen)
	}
}

// TestJoinDynamic is WC-36: instances added while the join waits.
func TestJoinDynamic(t *testing.T) {
	g := newGate()
	var added atomic.Bool
	var c workflow.Client
	parent := workflow.Workflow{Name: "parent", Steps: []workflow.Step{{Name: "join", Run: func(ctx context.Context, _ []byte) ([]byte, error) {
		res, err := workflow.JoinChildren(ctx, c, "children", childJobs("fail"), workflow.JoinSpec{Need: 1, Poll: time.Millisecond,
			More: func(context.Context) ([]workflow.Job, bool, error) {
				if !added.Load() {
					return nil, false, nil
				}
				return childJobs("ok"), true, nil
			}})
		if err != nil {
			return nil, err
		}
		return json.Marshal(res.Winners)
	}}}}
	c = flowClient(t, workflow.NewMemoryJournal(), g.workflow(), parent)
	ref := enqueue(t, c, workflow.Job{Workflow: "parent"})
	time.Sleep(20 * time.Millisecond)
	if st, _ := c.Status(context.Background(), ref); st.IsTerminal() {
		t.Fatalf("join decided while More was open: %s", st)
	}
	added.Store(true)
	out, err := workflow.AwaitResult(context.Background(), c, ref, workflow.AwaitOptions{Poll: time.Millisecond})
	if err != nil || string(out) != "[1]" {
		t.Fatalf("out %s err %v", out, err)
	}
}

func TestJoinRefusals(t *testing.T) {
	g := newGate()
	var c workflow.Client
	errs := make(chan error, 4)
	parent := workflow.Workflow{Name: "parent", Steps: []workflow.Step{{Name: "join", Run: func(ctx context.Context, _ []byte) ([]byte, error) {
		_, err := workflow.JoinChildren(ctx, c, "a", childJobs("ok"), workflow.JoinSpec{Need: 2})
		errs <- err
		_, err = workflow.JoinChildren(ctx, c, "b", nil, workflow.JoinSpec{})
		errs <- err
		_, err = workflow.JoinChildren(ctx, c, "c", childJobs("ok"), workflow.JoinSpec{Rest: "nope"})
		errs <- err
		_, err = workflow.JoinChildren(ctx, nil, "d", childJobs("ok"), workflow.JoinSpec{})
		errs <- err
		return nil, nil
	}}}}
	c = flowClient(t, workflow.NewMemoryJournal(), g.workflow(), parent)
	waitRun(t, c, enqueue(t, c, workflow.Job{Workflow: "parent"}))
	for i := 0; i < 4; i++ {
		if err := <-errs; !errors.Is(err, workflow.ErrInvalid) || !workflow.IsPermanent(err) {
			t.Fatalf("case %d: %v", i, err)
		}
	}
	if g.started.Load() != 0 {
		t.Fatal("a refused join started children")
	}
}

// noSnooze hides CapSnooze, as a driver without it would.
type noSnooze struct{ workflow.Client }

func (n noSnooze) Capabilities() workflow.Caps { return n.Client.Capabilities() &^ workflow.CapSnooze }

func TestJoinNeedsSnooze(t *testing.T) {
	g := newGate()
	var c workflow.Client
	errs := make(chan error, 2)
	parent := workflow.Workflow{Name: "parent", Steps: []workflow.Step{{Name: "join", Run: func(ctx context.Context, _ []byte) ([]byte, error) {
		_, err := workflow.JoinChildren(ctx, noSnooze{c}, "a", childJobs("ok"), workflow.JoinSpec{})
		errs <- err
		_, err = workflow.MapRuns(ctx, noSnooze{c}, "m", func(context.Context, string) (workflow.MapPage[int], error) {
			return workflow.MapPage[int]{Done: true}, nil
		}, func(int, int) (workflow.Job, error) { return workflow.Job{}, nil }, workflow.MapRunsOptions{})
		errs <- err
		return nil, nil
	}}}}
	c = flowClient(t, workflow.NewMemoryJournal(), g.workflow(), parent)
	waitRun(t, c, enqueue(t, c, workflow.Job{Workflow: "parent"}))
	for i := 0; i < 2; i++ {
		var ue *workflow.UnsupportedError
		if err := <-errs; !errors.As(err, &ue) || ue.Missing != workflow.CapSnooze {
			t.Fatalf("case %d: %v", i, err)
		}
	}
}

// TestCancelChildren is WC-26.
func TestCancelChildren(t *testing.T) {
	g := newGate()
	var c workflow.Client
	runID := make(chan workflow.RunID, 1)
	parent := workflow.Workflow{Name: "parent", Steps: []workflow.Step{{Name: "fan", Run: func(ctx context.Context, _ []byte) ([]byte, error) {
		if _, err := workflow.Spawn(ctx, c, "kids", childJobs("hold", "hold", "ok")); err != nil {
			return nil, err
		}
		info, _ := workflow.StepInfoFrom(ctx)
		runID <- info.RunID
		return nil, nil
	}}}}
	c = flowClient(t, workflow.NewMemoryJournal(), g.workflow(), parent)
	waitRun(t, c, enqueue(t, c, workflow.Job{Workflow: "parent"}))
	id := <-runID
	deadline := time.Now().Add(5 * time.Second)
	for g.started.Load() < 3 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	n, err := workflow.CancelChildren(context.Background(), c, id, "fan")
	if err != nil || n != 2 {
		t.Fatalf("cancelled %d, %v; want the 2 unfinished children", n, err)
	}
	if _, err := workflow.CancelChildren(context.Background(), c, "", ""); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("zero run: %v", err)
	}
}

// TestMapRuns is WP-05: pages of items, a bounded number in flight,
// failures tolerated up to a limit.
func TestMapRuns(t *testing.T) {
	var running, peak atomic.Int32
	var mu sync.Mutex
	seen := map[int]int{}
	child := workflow.Workflow{Name: "item", Steps: []workflow.Step{{Name: "sync", Run: func(_ context.Context, in []byte) ([]byte, error) {
		n := running.Add(1)
		defer running.Add(-1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		var v int
		if err := json.Unmarshal(in, &v); err != nil {
			return nil, workflow.Permanent(err)
		}
		mu.Lock()
		seen[v]++
		mu.Unlock()
		time.Sleep(time.Millisecond)
		if v%10 == 7 {
			return nil, workflow.Permanent(fmt.Errorf("item %d is bad", v))
		}
		return nil, nil
	}}}}
	pages := func(_ context.Context, cursor string) (workflow.MapPage[int], error) {
		start := 0
		if cursor != "" {
			if _, err := fmt.Sscan(cursor, &start); err != nil {
				return workflow.MapPage[int]{}, err
			}
		}
		var items []int
		for i := start; i < start+10 && i < 25; i++ {
			items = append(items, i)
		}
		return workflow.MapPage[int]{Items: items, Next: fmt.Sprint(start + 10), Done: start+10 >= 25}, nil
	}
	var c workflow.Client
	maxFail := 3
	parent := workflow.Workflow{Name: "catalogue", Steps: []workflow.Step{{Name: "map", Run: func(ctx context.Context, _ []byte) ([]byte, error) {
		res, err := workflow.MapRuns(ctx, c, "products", pages, func(_ int, item int) (workflow.Job, error) {
			b, _ := json.Marshal(item)
			return workflow.Job{Workflow: "item", Payload: b}, nil
		}, workflow.MapRunsOptions{InFlight: 3, MaxFailures: maxFail, Poll: time.Millisecond})
		if err != nil {
			return nil, err
		}
		return json.Marshal(res)
	}}}}
	c = flowClient(t, workflow.NewMemoryJournal(), child, parent)
	ref := enqueue(t, c, workflow.Job{Workflow: "catalogue"})
	out, err := workflow.AwaitResult(context.Background(), c, ref, workflow.AwaitOptions{Poll: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	var res workflow.MapRunsResult
	if err := json.Unmarshal(out, &res); err != nil {
		t.Fatal(err)
	}
	if res.Started != 25 || res.Succeeded != 23 || res.Failed != 2 || len(res.FailedRefs) != 2 {
		t.Fatalf("result %+v", res)
	}
	if peak.Load() > 3 {
		t.Fatalf("%d items ran at once, want at most 3", peak.Load())
	}
	for i := 0; i < 25; i++ {
		if seen[i] != 1 {
			t.Fatalf("item %d ran %d times", i, seen[i])
		}
	}
}

func TestMapRunsFailsPastLimit(t *testing.T) {
	child := workflow.Workflow{Name: "item", Steps: []workflow.Step{{Name: "sync", Run: func(context.Context, []byte) ([]byte, error) {
		return nil, workflow.Permanent(errors.New("bad"))
	}}}}
	var c workflow.Client
	errc := make(chan error, 8)
	parent := workflow.Workflow{Name: "catalogue", Steps: []workflow.Step{{Name: "map", Run: func(ctx context.Context, _ []byte) ([]byte, error) {
		_, err := workflow.MapRuns(ctx, c, "products", func(context.Context, string) (workflow.MapPage[int], error) {
			return workflow.MapPage[int]{Items: []int{1, 2, 3, 4, 5}, Done: true}, nil
		}, func(_ int, item int) (workflow.Job, error) {
			return workflow.Job{Workflow: "item"}, nil
		}, workflow.MapRunsOptions{InFlight: 2, MaxFailures: 1, Poll: time.Millisecond})
		if err != nil && !errors.Is(err, workflow.ErrSnoozed) {
			errc <- err
		}
		return nil, err
	}}}}
	c = flowClient(t, workflow.NewMemoryJournal(), child, parent)
	if st := waitRun(t, c, enqueue(t, c, workflow.Job{Workflow: "catalogue"})); st != workflow.StatusFailed {
		t.Fatalf("status %s", st)
	}
	var me *workflow.MapRunsError
	if err := <-errc; !errors.As(err, &me) || me.Result.Failed < 2 || me.Result.Started == 5 && me.Result.Failed == 5 {
		t.Fatalf("map error %v", err)
	}
}
