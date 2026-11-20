package workflow_test

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/memory"
)

var fastAwait = workflow.AwaitOptions{Poll: 2 * time.Millisecond}

func awaitCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func runWithErr(t *testing.T, c workflow.Client, ref workflow.RunRef) error {
	t.Helper()
	_, err := workflow.AwaitResult(awaitCtx(t), c, ref, fastAwait)
	return err
}

func TestSagaCompensatesFinishedActionsOnce(t *testing.T) {
	var mu sync.Mutex
	var log []string
	var hotelFails atomic.Bool
	hotelFails.Store(true)
	var undoFlightFails atomic.Int32
	undoFlightFails.Store(1)
	rec := func(s string) func(context.Context) error {
		return func(context.Context) error { mu.Lock(); log = append(log, s); mu.Unlock(); return nil }
	}
	wf := workflow.Workflow{Name: "trip", Retry: workflow.RetryPolicy{Max: 3}, Steps: []workflow.Step{{
		Name: "book",
		Run: func(ctx context.Context, _ []byte) ([]byte, error) {
			saga := workflow.NewSaga("trip")
			steps := []struct {
				name     string
				do, undo func(context.Context) error
			}{
				{"car", rec("do car"), rec("undo car")},
				{"flight", rec("do flight"), func(ctx context.Context) error {
					if undoFlightFails.Add(-1) >= 0 {
						return errors.New("airline down")
					}
					return rec("undo flight")(ctx)
				}},
				{"hotel", func(ctx context.Context) error {
					if hotelFails.Load() {
						hotelFails.Store(false) // a retry would succeed: the saga must not try
						return errors.New("no rooms")
					}
					return rec("do hotel")(ctx)
				}, rec("undo hotel")},
			}
			for _, s := range steps {
				if err := saga.Do(ctx, s.name, s.do, s.undo); err != nil {
					return nil, saga.Abort(ctx, err)
				}
			}
			return []byte(`"booked"`), nil
		},
	}}}
	c := flowClient(t, workflow.NewMemoryJournal(), wf)
	err := runWithErr(t, c, enqueue(t, c, workflow.Job{Workflow: "trip"}))
	// The run's error crosses the driver as text, so match its message.
	if err == nil || !strings.Contains(err.Error(), workflow.ErrCompensated.Error()) || !strings.Contains(err.Error(), "no rooms") {
		t.Fatalf("err %v, want ErrCompensated with the first cause", err)
	}
	want := []string{"do car", "do flight", "undo flight", "undo car"}
	if !slices.Equal(log, want) {
		t.Fatalf("log %v, want %v (each action once, hotel never retried, undo in reverse, undo car once)", log, want)
	}
}

func TestMultiChoiceRunsChosenBranchesAndJoinsThem(t *testing.T) {
	var ran sync.Map
	branch := func(name string) workflow.Branch {
		return workflow.Branch{Name: name, Run: func(_ context.Context, in []byte) ([]byte, error) {
			ran.Store(name, true)
			return json.Marshal(name + ":" + string(in))
		}}
	}
	for _, tc := range []struct {
		pick []string
		want string
	}{
		{[]string{"email", "sms"}, `{"email":"email:7","sms":"sms:7"}`},
		{[]string{"push"}, `{"push":"push:7"}`},
	} {
		ran = sync.Map{}
		steps, err := workflow.MultiChoice(workflow.MultiChoiceSpec{
			Name:     "notify",
			Decide:   func(context.Context, []byte) ([]string, error) { return tc.pick, nil },
			Branches: []workflow.Branch{branch("email"), branch("sms"), branch("push")},
		})
		if err != nil {
			t.Fatal(err)
		}
		c := ruleClient(t, workflow.Workflow{Name: "n", Steps: steps})
		out, err := workflow.AwaitResult(awaitCtx(t), c, enqueue(t, c, workflow.Job{Workflow: "n", Payload: []byte("7")}), fastAwait)
		if err != nil || string(out) != tc.want {
			t.Fatalf("pick %v: out %s err %v, want %s", tc.pick, out, err, tc.want)
		}
		for _, b := range []string{"email", "sms", "push"} {
			_, got := ran.Load(b)
			if got != slices.Contains(tc.pick, b) {
				t.Fatalf("pick %v: branch %s ran = %v", tc.pick, b, got)
			}
		}
	}
	if _, err := workflow.MultiChoice(workflow.MultiChoiceSpec{Name: "x", Decide: func(context.Context, []byte) ([]string, error) { return nil, nil }, Branches: []workflow.Branch{{Name: "a"}}}); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("branch without Run: %v", err)
	}
}

func TestRaceFirstSignalWins(t *testing.T) {
	steps, err := workflow.Race(workflow.RaceSpec{
		Name: "reply", Event: "reply", KeyFrom: workflow.KeyFunc(func(j workflow.Job) string { return string(j.Payload) }),
		Branches: []workflow.Branch{
			{Name: "accept", Run: func(_ context.Context, in []byte) ([]byte, error) { return json.Marshal("accepted " + string(in)) }},
			{Name: "decline", Run: func(_ context.Context, in []byte) ([]byte, error) { return json.Marshal("declined " + string(in)) }},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	c := ruleClient(t, workflow.Workflow{Name: "offer", Steps: steps})
	ref := enqueue(t, c, workflow.Job{Workflow: "offer", Payload: []byte("o1")})
	ctx := context.Background()
	if err := workflow.SignalChoice(ctx, c, "reply", "o1", "decline", []byte("by mail")); err != nil {
		t.Fatal(err)
	}
	if err := workflow.SignalChoice(ctx, c, "reply", "o1", "accept", []byte("by phone")); err != nil {
		t.Fatal(err)
	}
	out, err := workflow.AwaitResult(awaitCtx(t), c, ref, fastAwait)
	if err != nil || string(out) != `"declined by mail"` {
		t.Fatalf("out %s err %v", out, err)
	}
}

func TestPollSnoozesUntilDoneAndTimesOut(t *testing.T) {
	var looks atomic.Int32
	wf := workflow.Workflow{Name: "sensor", Steps: []workflow.Step{{Name: "wait-file", Run: func(ctx context.Context, in []byte) ([]byte, error) {
		path, err := workflow.Poll(ctx, "file", workflow.PollSpec{Every: time.Millisecond, Factor: 2, Max: 4 * time.Millisecond}, func(context.Context) (string, bool, error) {
			n := looks.Add(1)
			return "/data/part-3", n >= 4, nil
		})
		if err != nil {
			return nil, err
		}
		return json.Marshal(path)
	}}}}
	timeout := workflow.Workflow{Name: "never", Steps: []workflow.Step{{Name: "p", Run: func(ctx context.Context, in []byte) ([]byte, error) {
		_, err := workflow.Poll(ctx, "never", workflow.PollSpec{Every: time.Millisecond, Timeout: 20 * time.Millisecond}, func(context.Context) (int, bool, error) { return 0, false, nil })
		return nil, err
	}}}}
	c := flowClient(t, workflow.NewMemoryJournal(), wf, timeout)
	out, err := workflow.AwaitResult(awaitCtx(t), c, enqueue(t, c, workflow.Job{Workflow: "sensor"}), fastAwait)
	if err != nil || string(out) != `"/data/part-3"` || looks.Load() != 4 {
		t.Fatalf("out %s err %v looks %d", out, err, looks.Load())
	}
	if err := runWithErr(t, c, enqueue(t, c, workflow.Job{Workflow: "never"})); err == nil || !strings.Contains(err.Error(), workflow.ErrPollTimeout.Error()) {
		t.Fatalf("err %v, want ErrPollTimeout", err)
	}
}

func TestPollSpecWait(t *testing.T) {
	s := workflow.PollSpec{Every: time.Second, Factor: 2, Max: 5 * time.Second}
	var got []time.Duration
	for n := 1; n <= 5; n++ {
		got = append(got, s.Wait(n))
	}
	want := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 5 * time.Second, 5 * time.Second}
	if !slices.Equal(got, want) {
		t.Fatalf("waits %v, want %v", got, want)
	}
}

func TestSleepUntil(t *testing.T) {
	clk := clock.NewFake(time.Date(2026, 11, 20, 9, 0, 0, 0, time.UTC))
	err := workflow.SleepUntil(clk, clk.Now().Add(90*time.Minute))
	if d, ok := workflow.SnoozeDelay(err); !ok || d != 90*time.Minute {
		t.Fatalf("snooze %v %v", d, ok)
	}
	clk.Advance(90 * time.Minute)
	if err := workflow.SleepUntil(clk, clk.Now()); err != nil {
		t.Fatalf("at the time: %v", err)
	}
	// Through a driver: the step comes back once the time has passed.
	at := time.Now().Add(30 * time.Millisecond)
	var calls atomic.Int32
	c := ruleClient(t, workflow.Workflow{Name: "renew", Steps: []workflow.Step{{Name: "s", Run: func(context.Context, []byte) ([]byte, error) {
		calls.Add(1)
		if err := workflow.SleepUntil(nil, at); err != nil {
			return nil, err
		}
		return []byte(`"renewed"`), nil
	}}}})
	out, err := workflow.AwaitResult(awaitCtx(t), c, enqueue(t, c, workflow.Job{Workflow: "renew"}), fastAwait)
	if err != nil || string(out) != `"renewed"` || time.Now().Before(at) || calls.Load() < 2 {
		t.Fatalf("out %s err %v calls %d", out, err, calls.Load())
	}
}

func TestApprovalStepAndSignedTokens(t *testing.T) {
	step, err := workflow.ApprovalStep(workflow.ApprovalSpec{Name: "approve", KeyFrom: workflow.KeyFunc(func(j workflow.Job) string { return j.Labels["order"] })})
	if err != nil {
		t.Fatal(err)
	}
	wf := workflow.Workflow{Name: "refund", Steps: []workflow.Step{step, {Name: "pay", After: []string{"approve"}, Run: func(_ context.Context, in []byte) ([]byte, error) {
		d, err := workflow.DecisionOf(in)
		if err != nil {
			return nil, err
		}
		if !d.Approved {
			return json.Marshal("refused by " + d.By)
		}
		return json.Marshal("paid, approved by " + d.By)
	}}}}
	c := ruleClient(t, wf)
	ref := enqueue(t, c, workflow.Job{Workflow: "refund", Labels: map[string]string{"order": "o-42"}})

	signer := workflow.ApprovalSigner{Keys: [][]byte{[]byte("new-key-0123456789"), []byte("old-key-0123456789")}}
	now := time.Date(2026, 11, 20, 9, 0, 0, 0, time.UTC)
	old := workflow.ApprovalSigner{Keys: [][]byte{[]byte("old-key-0123456789")}}
	token, err := old.Sign("o-42", now.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	key, err := signer.Verify(token, now)
	if err != nil || key != "o-42" {
		t.Fatalf("verify with a rotated key: %q %v", key, err)
	}
	if _, err := signer.Verify(token, now.Add(time.Hour)); !errors.Is(err, workflow.ErrApprovalToken) {
		t.Fatalf("expired: %v", err)
	}
	forged := strings.Replace(token, token[len(token)-4:], "AAAA", 1)
	if _, err := signer.Verify(forged, now); !errors.Is(err, workflow.ErrApprovalToken) {
		t.Fatalf("forged: %v", err)
	}
	if err := workflow.Approve(context.Background(), c, key, "ana", "ok"); err != nil {
		t.Fatal(err)
	}
	out, err := workflow.AwaitResult(awaitCtx(t), c, ref, fastAwait)
	if err != nil || string(out) != `"paid, approved by ana"` {
		t.Fatalf("out %s err %v", out, err)
	}
	if _, err := workflow.ApprovalStep(workflow.ApprovalSpec{Name: "a", Key: "k", KeyFrom: workflow.KeyFunc(func(workflow.Job) string { return "" })}); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("both keys: %v", err)
	}
}

func TestGroupPrefixesAndJoins(t *testing.T) {
	echo := func(s string) workflow.Handler {
		return func(_ context.Context, in []byte) ([]byte, error) { return json.Marshal(s) }
	}
	g, err := workflow.Group("import", []string{"start"}, []workflow.Step{
		{Name: "fetch", Run: echo("rows")},
		{Name: "clean", After: []string{"fetch"}, Run: echo("clean")},
		{Name: "count", After: []string{"fetch"}, Run: echo("count")},
	})
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, s := range g {
		names = append(names, s.Name+"<"+strings.Join(s.After, ","))
	}
	want := []string{"import/fetch<start", "import/clean<import/fetch", "import/count<import/fetch", "import<import/clean,import/count"}
	if !slices.Equal(names, want) {
		t.Fatalf("steps %v, want %v", names, want)
	}
	steps := append([]workflow.Step{{Name: "start", Run: echo("go")}}, g...)
	steps = append(steps, workflow.Step{Name: "done", After: []string{"import"}, Run: func(_ context.Context, in []byte) ([]byte, error) { return in, nil }})
	c := ruleClient(t, workflow.Workflow{Name: "g", Steps: steps})
	out, err := workflow.AwaitResult(awaitCtx(t), c, enqueue(t, c, workflow.Job{Workflow: "g"}), fastAwait)
	if err != nil || string(out) != `{"clean":"clean","count":"count"}` {
		t.Fatalf("out %s err %v", out, err)
	}
	if _, err := workflow.Group("a/b", nil, []workflow.Step{{Name: "x"}}); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("separator in name: %v", err)
	}
	if _, err := workflow.Group("g", nil, []workflow.Step{{Name: "x", After: []string{"outside"}}}); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("After outside the group: %v", err)
	}
}

func TestRecursionLimitStopsRunawayRecursion(t *testing.T) {
	var depths sync.Map
	var c workflow.Client
	wf := workflow.Workflow{Name: "walk", Steps: []workflow.Step{{Name: "visit", Run: func(ctx context.Context, in []byte) ([]byte, error) {
		info, _ := workflow.StepInfoFrom(ctx)
		depths.Store(info.Labels[workflow.LabelDepth], true)
		_, err := c.Enqueue(ctx, workflow.Job{Workflow: "walk"})
		if errors.Is(err, workflow.ErrDepth) {
			return []byte(`"bottom"`), nil
		}
		return nil, err
	}}}}
	c, err := workflow.Use(memory.New(), workflow.RecursionLimit(3))
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
	enqueue(t, c, workflow.Job{Workflow: "walk"})
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if _, ok := depths.Load("3"); ok {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	time.Sleep(20 * time.Millisecond)
	var got []string
	depths.Range(func(k, _ any) bool { got = append(got, k.(string)); return true })
	slices.Sort(got)
	if !slices.Equal(got, []string{"", "1", "2", "3"}) {
		t.Fatalf("depths %v, want root, 1, 2, 3 and no deeper", got)
	}
}

func TestEndRunSkipsLaterSteps(t *testing.T) {
	var ran sync.Map
	step := func(name string, after []string, end bool) workflow.Step {
		return workflow.Step{Name: name, After: after, Run: func(ctx context.Context, in []byte) ([]byte, error) {
			ran.Store(name, true)
			if end {
				if err := workflow.EndRun(ctx); err != nil {
					return nil, err
				}
			}
			return json.Marshal(name)
		}}
	}
	wf := workflow.Workflow{Name: "check", Steps: []workflow.Step{
		step("lookup", nil, true), step("enrich", []string{"lookup"}, false), step("save", []string{"enrich"}, false),
	}}
	c, err := workflow.Use(memory.New(), workflow.Journal(workflow.NewMemoryJournal()), workflow.EndRuns())
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
	if st := waitRun(t, c, enqueue(t, c, workflow.Job{Workflow: "check"})); st != workflow.StatusSucceeded {
		t.Fatalf("status %s", st)
	}
	_, enrich := ran.Load("enrich")
	_, save := ran.Load("save")
	if enrich || save {
		t.Fatalf("enrich %v save %v: steps after EndRun must not run", enrich, save)
	}
}

func TestShapeGuard(t *testing.T) {
	ok := func(context.Context, []byte) ([]byte, error) { return []byte(`1`), nil }
	v1 := workflow.Workflow{Name: "sync", Steps: []workflow.Step{{Name: "a", Run: ok}, {Name: "b", After: []string{"a"}, Run: ok}}}
	v2 := workflow.Workflow{Name: "sync", Steps: []workflow.Step{{Name: "a", Run: ok}, {Name: "b2", After: []string{"a"}, Run: ok}}}
	reordered := workflow.Workflow{Name: "sync", Steps: []workflow.Step{v1.Steps[1], v1.Steps[0]}}
	if workflow.Shape(v1) == workflow.Shape(v2) || workflow.Shape(v1) != workflow.Shape(reordered) {
		t.Fatal("Shape must change with a renamed step and not with the order of Steps")
	}
	// A run stamped under v1 meets v2's guard on its worker.
	guard := workflow.ShapeGuard(v2).(workflow.StepMiddlewareProvider).StepMiddleware()[0](ok)
	run := func(info workflow.StepInfo) ([]byte, error) {
		return guard(workflow.WithStepInfo(context.Background(), info), nil)
	}
	stamped := map[string]string{workflow.LabelShape: workflow.Shape(v1)}
	if _, err := run(workflow.StepInfo{Workflow: "sync", Step: "a", Labels: stamped}); !errors.Is(err, workflow.ErrShapeChanged) || !workflow.IsPermanent(err) {
		t.Fatalf("changed shape: %v", err)
	}
	if _, err := run(workflow.StepInfo{Workflow: "sync", Step: "a"}); err != nil {
		t.Fatalf("unstamped run: %v", err)
	}
	// End to end: the enqueue stamps the label and the same definition runs.
	c, err := workflow.Use(memory.New(), workflow.ShapeGuard(v1))
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Register(v1); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if st := waitRun(t, c, enqueue(t, c, workflow.Job{Workflow: "sync"})); st != workflow.StatusSucceeded {
		t.Fatalf("status %s", st)
	}
}

func TestTypedEnqueueAndResult(t *testing.T) {
	type in struct{ N int }
	type out struct{ Double int }
	wf := workflow.Workflow{Name: "double", Steps: []workflow.Step{{Name: "d", Run: workflow.Define(func(_ context.Context, v in) (out, error) {
		return out{Double: v.N * 2}, nil
	})}}}
	c := ruleClient(t, wf)
	ref, err := workflow.EnqueueOf(context.Background(), c, "double", in{N: 21}, func(j *workflow.Job) { j.Labels = map[string]string{"k": "v"} })
	if err != nil {
		t.Fatal(err)
	}
	got, err := workflow.AwaitResultOf[out](awaitCtx(t), c, ref, fastAwait)
	if err != nil || got.Double != 42 {
		t.Fatalf("got %+v err %v", got, err)
	}
}

func TestBoundaryCallsOnTimerWithoutStoppingTheStep(t *testing.T) {
	fired := make(chan workflow.StepInfo, 1)
	mw := workflow.Boundary(5*time.Millisecond, func(_ context.Context, info workflow.StepInfo) { fired <- info }, "slow")
	h := mw(func(ctx context.Context, in []byte) ([]byte, error) {
		time.Sleep(30 * time.Millisecond)
		return []byte("done"), ctx.Err()
	})
	ctx := workflow.WithStepInfo(context.Background(), workflow.StepInfo{Step: "slow"})
	out, err := h(ctx, nil)
	if err != nil || string(out) != "done" {
		t.Fatalf("out %s err %v: the step must finish", out, err)
	}
	select {
	case info := <-fired:
		if info.Step != "slow" {
			t.Fatalf("info %+v", info)
		}
	default:
		t.Fatal("onTimer did not fire")
	}
	fast := mw(func(context.Context, []byte) ([]byte, error) { return nil, nil })
	if _, err := fast(ctx, nil); err != nil {
		t.Fatal(err)
	}
	time.Sleep(15 * time.Millisecond)
	if len(fired) != 0 {
		t.Fatal("onTimer fired for a step that ended in time")
	}
}
