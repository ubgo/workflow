package workflow_test

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

// recordStep returns a step body that appends name to log when it runs.
func recordStep(mu *sync.Mutex, log *[]string, name string) workflow.Handler {
	return func(context.Context, []byte) ([]byte, error) {
		mu.Lock()
		*log = append(*log, name)
		mu.Unlock()
		return []byte(`"` + name + `"`), nil
	}
}

func TestStagesRunInOrder(t *testing.T) {
	var mu sync.Mutex
	var log []string
	steps, err := workflow.Stages(
		[]workflow.Step{{Name: "a1", Run: recordStep(&mu, &log, "a1")}, {Name: "a2", Run: recordStep(&mu, &log, "a2")}},
		[]workflow.Step{{Name: "b1", Run: recordStep(&mu, &log, "b1")}, {Name: "b2", Run: recordStep(&mu, &log, "b2")}},
		[]workflow.Step{{Name: "c", Run: func(_ context.Context, in []byte) ([]byte, error) {
			mu.Lock()
			log = append(log, "c")
			mu.Unlock()
			return in, nil
		}}},
	)
	if err != nil {
		t.Fatal(err)
	}
	if got := steps[2].After; !slices.Equal(got, []string{"a1", "a2"}) {
		t.Fatalf("b1.After = %v, want [a1 a2]", got)
	}
	wf := workflow.Workflow{Name: "staged", Steps: steps}
	c := flowClient(t, workflow.NewMemoryJournal(), wf)
	ref := enqueue(t, c, workflow.Job{Workflow: "staged"})
	if st := waitRun(t, c, ref); st != workflow.StatusSucceeded {
		t.Fatalf("status %s", st)
	}
	pos := func(name string) int { return slices.Index(log, name) }
	for _, b := range []string{"b1", "b2"} {
		if pos(b) < pos("a1") || pos(b) < pos("a2") {
			t.Fatalf("%s ran before stage one finished: %v", b, log)
		}
		if pos("c") < pos(b) {
			t.Fatalf("c ran before %s: %v", b, log)
		}
	}
	out, err := workflow.AwaitResult(context.Background(), c, ref, workflow.AwaitOptions{Poll: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	var joined map[string]string
	if err := json.Unmarshal(out, &joined); err != nil || joined["b1"] != "b1" || joined["b2"] != "b2" {
		t.Fatalf("last stage input = %s (%v), want both stage-two outputs", out, err)
	}
}

func TestStagesRefuseBadInput(t *testing.T) {
	if _, err := workflow.Stages([]workflow.Step{}); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("empty stage: %v", err)
	}
	if _, err := workflow.Stages([]workflow.Step{{Run: recordStep(&sync.Mutex{}, &[]string{}, "x")}}); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("unnamed step: %v", err)
	}
	steps, err := workflow.Stages(
		[]workflow.Step{{Name: "a"}},
		[]workflow.Step{{Name: "b", After: []string{"x", "a"}}},
	)
	if err != nil || !slices.Equal(steps[1].After, []string{"x", "a"}) {
		t.Fatalf("own After entries must be kept once: %v %v", steps[1].After, err)
	}
}

func TestWhenPassesInputThrough(t *testing.T) {
	var ran atomic.Int32
	double := func(_ context.Context, in []byte) ([]byte, error) {
		ran.Add(1)
		return append(in, in...), nil
	}
	isLong := func(_ context.Context, in []byte) (bool, error) { return len(in) > 3, nil }
	wf := workflow.Workflow{Name: "cond", Steps: []workflow.Step{
		{Name: "maybe", Run: workflow.When(isLong, double)},
		{Name: "after", After: []string{"maybe"}, Run: func(_ context.Context, in []byte) ([]byte, error) { return in, nil }},
	}}
	c := flowClient(t, workflow.NewMemoryJournal(), wf)
	for _, tc := range []struct{ in, want string }{{"ab", "ab"}, {"abcd", "abcdabcd"}} {
		ref := enqueue(t, c, workflow.Job{Workflow: "cond", Payload: []byte(tc.in)})
		out, err := workflow.AwaitResult(context.Background(), c, ref, workflow.AwaitOptions{Poll: time.Millisecond})
		if err != nil {
			t.Fatal(err)
		}
		if string(out) != tc.want {
			t.Fatalf("payload %q: result %q, want %q", tc.in, out, tc.want)
		}
	}
	if ran.Load() != 1 {
		t.Fatalf("handler ran %d times, want 1", ran.Load())
	}
	boom := errors.New("boom")
	h := workflow.When(func(context.Context, []byte) (bool, error) { return false, boom }, double)
	if _, err := h(context.Background(), nil); !errors.Is(err, boom) {
		t.Fatalf("cond error = %v", err)
	}
	if _, err := workflow.When(nil, double)(context.Background(), nil); !errors.Is(err, workflow.ErrInvalid) || !workflow.IsPermanent(err) {
		t.Fatalf("nil cond = %v", err)
	}
}

func TestMilestoneWaitsForState(t *testing.T) {
	var shipped atomic.Int32
	release := make(chan struct{})
	wf := workflow.Workflow{Name: "order", Steps: []workflow.Step{
		{Name: "pay", Run: func(ctx context.Context, _ []byte) ([]byte, error) {
			<-release
			return nil, workflow.SetRunData(ctx, "state", "paid")
		}},
		{Name: "ship", Run: workflow.Milestone("state", "paid", func(context.Context, []byte) ([]byte, error) {
			shipped.Add(1)
			return []byte(`"shipped"`), nil
		}, workflow.MilestoneOptions{Poll: time.Millisecond})},
	}}
	c := flowClient(t, workflow.NewMemoryJournal(), wf)
	ref := enqueue(t, c, workflow.Job{Workflow: "order"})
	time.Sleep(20 * time.Millisecond)
	if shipped.Load() != 0 {
		t.Fatal("ship ran before the run reached its milestone")
	}
	close(release)
	if st := waitRun(t, c, ref); st != workflow.StatusSucceeded {
		t.Fatalf("status %s", st)
	}
	if shipped.Load() != 1 {
		t.Fatalf("ship ran %d times", shipped.Load())
	}
	page, err := c.(workflow.RunQuerier).Runs(context.Background(), workflow.RunQuery{Workflow: "order"})
	if err != nil || len(page.Runs) != 1 {
		t.Fatalf("runs: %v %v", page.Runs, err)
	}
	if page.Runs[0].Attempts > 2 {
		t.Fatalf("waiting spent attempts: %d", page.Runs[0].Attempts)
	}
}

func TestMilestoneSkip(t *testing.T) {
	var ran atomic.Int32
	h := workflow.Milestone("state", "open", func(context.Context, []byte) ([]byte, error) {
		ran.Add(1)
		return nil, nil
	}, workflow.MilestoneOptions{Skip: true})
	wf := workflow.Workflow{Name: "gate", Steps: []workflow.Step{
		{Name: "close", Run: func(ctx context.Context, in []byte) ([]byte, error) {
			return in, workflow.SetRunData(ctx, "state", "closed")
		}},
		{Name: "work", After: []string{"close"}, Run: h},
	}}
	c := flowClient(t, workflow.NewMemoryJournal(), wf)
	ref := enqueue(t, c, workflow.Job{Workflow: "gate", Payload: []byte("x")})
	out, err := workflow.AwaitResult(context.Background(), c, ref, workflow.AwaitOptions{Poll: time.Millisecond})
	if err != nil || string(out) != "x" || ran.Load() != 0 {
		t.Fatalf("out %q err %v ran %d: want input passed through and the handler not run", out, err, ran.Load())
	}
}

// TestInterleavedRouting proves WC-17 and WC-40 with MapEach and
// MapConcurrency(1): every task runs exactly once and never two at once.
func TestInterleavedRouting(t *testing.T) {
	var running, peak atomic.Int32
	counts := map[string]int{}
	var mu sync.Mutex
	wf := workflow.Workflow{Name: "interleave", Steps: []workflow.Step{{Name: "tasks", Run: func(ctx context.Context, _ []byte) ([]byte, error) {
		_, err := workflow.MapEach(ctx, "tasks", []string{"audit", "invoice", "notify"}, func(_ context.Context, _ int, task string) (string, error) {
			n := running.Add(1)
			defer running.Add(-1)
			if n > peak.Load() {
				peak.Store(n)
			}
			time.Sleep(2 * time.Millisecond)
			mu.Lock()
			counts[task]++
			mu.Unlock()
			return task, nil
		}, workflow.MapConcurrency(1))
		return nil, err
	}}}}
	c := flowClient(t, workflow.NewMemoryJournal(), wf)
	if st := waitRun(t, c, enqueue(t, c, workflow.Job{Workflow: "interleave"})); st != workflow.StatusSucceeded {
		t.Fatalf("status %s", st)
	}
	if peak.Load() != 1 {
		t.Fatalf("%d tasks ran at once, want 1", peak.Load())
	}
	for _, task := range []string{"audit", "invoice", "notify"} {
		if counts[task] != 1 {
			t.Fatalf("%s ran %d times", task, counts[task])
		}
	}
}

//ds:def id=teststartwhenholdsuntilready-ea7yfcvn owner=@khanakia stability=stable
func TestStartWhenHoldsUntilReady(t *testing.T) {
	clk := clock.NewFake(time.Date(1970, 1, 1, 0, 0, 10, 0, time.UTC))
	c := memory.New(memory.WithClock(clk))
	if err := c.Register(workflow.Workflow{Name: "w", Steps: []workflow.Step{{Name: "s", Run: func(context.Context, []byte) ([]byte, error) { return nil, nil }}}}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	floor := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	done := make(chan error, 1)
	go func() {
		done <- workflow.StartWhen(ctx, c, workflow.ClockAfter(clk, floor), workflow.StartWhenOptions{Poll: time.Second, Clock: clk})
	}()
	if err := clk.BlockUntil(ctx, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Enqueue(ctx, workflow.Job{Workflow: "w"}); !errors.Is(err, workflow.ErrNotStarted) {
		t.Fatalf("enqueue before the gate opened: %v, want ErrNotStarted", err)
	}
	clk.Set(floor.Add(time.Hour))
	clk.Advance(time.Second)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if _, err := c.Enqueue(ctx, workflow.Job{Workflow: "w"}); err != nil {
		t.Fatalf("enqueue after start: %v", err)
	}
}

//ds:def id=teststartwhenerrors-d9cyqwrc owner=@khanakia stability=stable
func TestStartWhenErrors(t *testing.T) {
	boom := errors.New("ntp down")
	c := memory.New()
	err := workflow.StartWhen(context.Background(), c, func(context.Context) (bool, error) { return false, boom }, workflow.StartWhenOptions{})
	if !errors.Is(err, boom) {
		t.Fatalf("ready error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err = workflow.StartWhen(ctx, c, func(context.Context) (bool, error) { return false, nil }, workflow.StartWhenOptions{Poll: time.Millisecond})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled ctx = %v", err)
	}
	if err := workflow.StartWhen(context.Background(), nil, nil, workflow.StartWhenOptions{}); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("nil args = %v", err)
	}
}
