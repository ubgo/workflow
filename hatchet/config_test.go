package hatchet

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/hatchet-dev/hatchet/pkg/client/rest"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
)

// TestConfigDefaults pins what a zero Config gets for every tuning, so a
// change to a default is a visible decision.
func TestConfigDefaults(t *testing.T) {
	c := New(Config{})
	cfg := c.cfg
	if cfg.GroupChunkRuns != 1000 || cfg.GroupChunkBytes != 2<<20 || cfg.BatchCoordinatorMaxInput != 2<<20 {
		t.Fatalf("chunk defaults = %d runs, %d bytes, %d coordinator bytes", cfg.GroupChunkRuns, cfg.GroupChunkBytes, cfg.BatchCoordinatorMaxInput)
	}
	if cfg.RegisterTimeout != 30*time.Second || cfg.VisibilityLag != 5*time.Second {
		t.Fatalf("RegisterTimeout = %s, VisibilityLag = %s", cfg.RegisterTimeout, cfg.VisibilityLag)
	}
	want := workflow.RetryPolicy{Max: 10, Backoff: workflow.Backoff{Initial: 2 * time.Second, Factor: 2, Max: 2 * time.Minute}}
	if got := *cfg.BatchCoordinatorRetry; !reflect.DeepEqual(got, want) {
		t.Fatalf("BatchCoordinatorRetry = %+v, want %+v", got, want)
	}
	if cfg.Logger == nil {
		t.Fatal("Logger left nil")
	}
	if _, ok := cfg.Clock.(clock.Real); !ok {
		t.Fatalf("Clock = %T, want clock.Real", cfg.Clock)
	}
	if a, b := DefaultBatchCoordinatorRetry(), DefaultBatchCoordinatorRetry(); a == b {
		t.Fatal("DefaultBatchCoordinatorRetry shares one value between callers")
	}
}

// TestStartRefusesTunings pins the tunings Start refuses before it
// connects: chunk limits past the SDK's own split (which would lose which
// runs of a chunk failed) and a coordinator retry policy Hatchet cannot
// express.
func TestStartRefusesTunings(t *testing.T) {
	for name, tc := range map[string]struct {
		cfg  Config
		want error
	}{
		"chunk runs over the SDK limit":   {Config{GroupChunkRuns: 1001}, workflow.ErrInvalid},
		"chunk bytes over the SDK split":  {Config{GroupChunkBytes: 2<<20 + 1}, workflow.ErrInvalid},
		"negative coordinator retries":    {Config{BatchCoordinatorRetry: &workflow.RetryPolicy{Max: -1}}, workflow.ErrInvalid},
		"coordinator NoRetryOn":           {Config{BatchCoordinatorRetry: &workflow.RetryPolicy{Max: 1, NoRetryOn: func(error) bool { return true }}}, workflow.ErrUnsupported},
		"coordinator backoff off-formula": {Config{BatchCoordinatorRetry: &workflow.RetryPolicy{Max: 1, Backoff: workflow.Backoff{Initial: time.Second, Factor: 2}}}, workflow.ErrUnsupported},
		"negative recycle MaxJobs":        {Config{Recycle: workflow.Recycle{MaxJobs: -1}}, workflow.ErrInvalid},
	} {
		if err := checkConfig(New(tc.cfg).cfg); !errors.Is(err, tc.want) {
			t.Errorf("%s: checkConfig = %v, want %v", name, err, tc.want)
		}
	}
	// Start runs the check before it connects.
	if err := New(Config{GroupChunkRuns: 1001}).Start(t.Context()); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("Start with a bad tuning = %v, want ErrInvalid", err)
	}
	for name, cfg := range map[string]Config{
		"limits at the maximum": {GroupChunkRuns: 1000, GroupChunkBytes: 2 << 20},
		"lower limits":          {GroupChunkRuns: 10, GroupChunkBytes: 1 << 10},
		"no coordinator retry":  {BatchCoordinatorRetry: workflow.RetriesNone()},
		"custom backoff":        {BatchCoordinatorRetry: &workflow.RetryPolicy{Max: 3, Backoff: workflow.Backoff{Initial: 3 * time.Second, Factor: 3, Max: time.Minute}}},
	} {
		if err := checkConfig(New(cfg).cfg); err != nil {
			t.Errorf("%s: checkConfig = %v, want nil", name, err)
		}
	}
}

// TestCoordinatorRetryOptions pins how many task options a coordinator
// policy maps to: none for no retries, retries alone for a zero backoff,
// retries and backoff otherwise.
func TestCoordinatorRetryOptions(t *testing.T) {
	for name, tc := range map[string]struct {
		p    workflow.RetryPolicy
		want int
	}{
		"no retries":      {workflow.RetryPolicy{}, 0},
		"retry at once":   {workflow.RetryPolicy{Max: 2}, 1},
		"default backoff": {*DefaultBatchCoordinatorRetry(), 2},
	} {
		opts, err := coordinatorRetry(tc.p)
		if err != nil || len(opts) != tc.want {
			t.Errorf("%s: %d options, err %v; want %d", name, len(opts), err, tc.want)
		}
	}
}

// TestChunkRunsLimits pins that RunGroup's chunking follows the configured
// limits, not the defaults.
func TestChunkRunsLimits(t *testing.T) {
	names := []string{"w", "w", "w", "w", "w"}
	sizes := []int{10, 10, 10, 10, 10}
	if got, want := chunkRuns(names, sizes, 2, 1<<20), [][]int{{0, 1}, {2, 3}, {4}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("by runs: %v, want %v", got, want)
	}
	if got, want := chunkRuns(names, sizes, 1000, 25), [][]int{{0, 1}, {2, 3}, {4}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("by bytes: %v, want %v", got, want)
	}
}

// startedForTest marks c started with wf registered and no engine, so the
// checks that run before any engine call can be exercised.
func startedForTest(t *testing.T, c *Client, wf workflow.Workflow) {
	t.Helper()
	if err := c.Register(wf); err != nil {
		t.Fatal(err)
	}
	c.started = true
	c.workflows[wf.Name] = nil
}

func noopWorkflow(name string) workflow.Workflow {
	return workflow.Workflow{Name: name, Steps: []workflow.Step{{Name: "s", Run: func(context.Context, []byte) ([]byte, error) { return nil, nil }}}}
}

// TestCoordinatorMaxInput pins that SpawnGroup refuses a batch over
// Config.BatchCoordinatorMaxInput with ErrUnsupported (so Dispatch falls
// back to bulk triggers) and lets one under it through to the trigger.
func TestCoordinatorMaxInput(t *testing.T) {
	c := New(Config{BatchCoordinator: true, BatchCoordinatorMaxInput: 200})
	startedForTest(t, c, noopWorkflow("w"))
	big := []workflow.Job{{Workflow: "w", Payload: []byte(`"` + strings.Repeat("x", 300) + `"`)}}
	if _, err := c.SpawnGroup(t.Context(), "g", big); !errors.Is(err, workflow.ErrUnsupported) {
		t.Fatalf("SpawnGroup over the limit = %v, want ErrUnsupported", err)
	}
	small := []workflow.Job{{Workflow: "w", Payload: []byte(`"x"`)}}
	if _, err := c.SpawnGroup(t.Context(), "g", small); !errors.Is(err, workflow.ErrNotStarted) {
		t.Fatalf("SpawnGroup under the limit = %v, want it to reach the (absent) coordinator", err)
	}
}

// testNotifier builds a notifier for c with no engine behind it.
func testNotifier(c *Client, fns ...workflow.RunFinishedFunc) *notifier {
	n := &notifier{c: c, ctx: context.Background(), observers: fns, tracked: map[string]bool{}, delivered: map[string]time.Time{}}
	c.notify = n
	return n
}

// TestObserverPanicLogged pins that a panicking observer is logged through
// Config.Logger and does not stop the observers after it.
func TestObserverPanicLogged(t *testing.T) {
	var buf bytes.Buffer
	c := New(Config{Logger: slog.New(slog.NewTextHandler(&buf, nil))})
	var later bool
	n := testNotifier(c,
		func(context.Context, workflow.RunEvent) { panic("observer boom") },
		func(context.Context, workflow.RunEvent) { later = true },
	)
	n.call(workflow.RunEvent{Workflow: "w"})
	if !later {
		t.Fatal("the observer after a panicking one did not run")
	}
	if !strings.Contains(buf.String(), "observer boom") {
		t.Fatalf("panic not logged; log = %q", buf.String())
	}
}

// TestNotifierUsesClock pins the reads that follow Config.Clock: when a
// run was delivered, and FinishedAt of a delayed start cancelled before it
// fired.
func TestNotifierUsesClock(t *testing.T) {
	at := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	fake := clock.NewFake(at)
	c := New(Config{Clock: fake})
	var got workflow.RunEvent
	n := testNotifier(c, func(_ context.Context, ev workflow.RunEvent) { got = ev })

	n.deliver("run-1", workflow.RunEvent{})
	if n.delivered["run-1"] != at {
		t.Fatalf("delivered at %s, want the fake clock's %s", n.delivered["run-1"], at)
	}
	c.cancelledBeforeFire(workflow.RunRef{ScheduledID: "s"}, &rest.ScheduledWorkflows{})
	if !got.FinishedAt.Equal(at) {
		t.Fatalf("FinishedAt = %s, want the fake clock's %s", got.FinishedAt, at)
	}
}

// TestSleepCtx pins the poll loops' wait: it returns true once the clock
// has advanced past d, and false as soon as ctx ends.
func TestSleepCtx(t *testing.T) {
	fake := clock.NewFake(time.Unix(0, 0))
	done := make(chan bool, 1)
	go func() { done <- sleepCtx(t.Context(), fake, time.Second) }()
	if err := fake.BlockUntil(t.Context(), 1); err != nil {
		t.Fatal(err)
	}
	fake.Advance(time.Second)
	if !<-done {
		t.Fatal("sleepCtx returned false after the clock advanced")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if sleepCtx(ctx, fake, time.Hour) {
		t.Fatal("sleepCtx returned true on a cancelled ctx")
	}
}

// TestRefusesCoreOptions pins that every core option Hatchet cannot honour
// is refused when the workflow is registered or the schedule created,
// naming the field, the missing capability and Hatchet's reason (not
// only the core's generic capability check), and that the driver
// declares none of those capabilities: crons are UTC only and never catch
// up, and retries wait exactly factor^n seconds whatever the error says.
func TestRefusesCoreOptions(t *testing.T) {
	never := workflow.CapRetryJitter | workflow.CapRetryDelay | workflow.CapMisfireSkip | workflow.CapMisfireFireOnce | workflow.CapMisfireFireAll | workflow.CapCronZone
	if got := New(Config{}).Capabilities() & never; got != 0 {
		t.Fatalf("driver declares %s, which Hatchet cannot honour", got)
	}
	jitter := workflow.RetryPolicy{Max: 2, Backoff: workflow.Backoff{Initial: 2 * time.Second, Factor: 2, Jitter: 0.5}}
	delay := workflow.RetryPolicy{Max: 2, DelayFor: workflow.RetryAfterDelay}
	zone := time.FixedZone("UTC+5", 5*3600)
	for name, tc := range map[string]struct {
		edit    func(*workflow.Workflow)
		field   string
		missing workflow.Caps
	}{
		"cron zone": {func(w *workflow.Workflow) { w.Cron, w.CronLocation = []string{"0 9 * * *"}, zone }, "Workflow.CronLocation", workflow.CapCronZone},
		"misfire skip": {func(w *workflow.Workflow) {
			w.Cron, w.Misfire = []string{"0 9 * * *"}, workflow.Misfire{Policy: workflow.MisfireSkip}
		}, "Workflow.Misfire", workflow.CapMisfireSkip},
		"misfire fire once": {func(w *workflow.Workflow) {
			w.Cron, w.Misfire = []string{"0 9 * * *"}, workflow.Misfire{Policy: workflow.MisfireFireOnce}
		}, "Workflow.Misfire", workflow.CapMisfireFireOnce},
		"misfire fire all": {func(w *workflow.Workflow) {
			w.Cron, w.Misfire = []string{"0 9 * * *"}, workflow.Misfire{Policy: workflow.MisfireFireAll, Max: 3}
		}, "Workflow.Misfire", workflow.CapMisfireFireAll},
		"workflow jitter": {func(w *workflow.Workflow) { w.Retry = jitter }, "Retry.Backoff.Jitter", workflow.CapRetryJitter},
		"step jitter":     {func(w *workflow.Workflow) { w.Steps[0].Retry = &jitter }, "Retry.Backoff.Jitter", workflow.CapRetryJitter},
		"retry after":     {func(w *workflow.Workflow) { w.Retry = delay }, "Retry.DelayFor", workflow.CapRetryDelay},
	} {
		wf := noopWorkflow("w")
		tc.edit(&wf)
		err := New(Config{}).Register(wf)
		var ue *workflow.UnsupportedError
		if !errors.As(err, &ue) || ue.Field != tc.field || !ue.Missing.Has(tc.missing) || !strings.Contains(ue.Reason, "Hatchet") {
			t.Errorf("%s: Register = %v, want UnsupportedError on %s missing %s", name, err, tc.field, tc.missing)
		}
	}
	if err := New(Config{}).Register(noopWorkflow("w")); err != nil {
		t.Fatalf("Register of a plain workflow = %v", err)
	}

	for name, tc := range map[string]struct {
		s       workflow.Schedule
		field   string
		missing workflow.Caps
	}{
		"zone":         {workflow.Schedule{Location: zone}, "Schedule.Location", workflow.CapCronZone},
		"misfire skip": {workflow.Schedule{Location: time.UTC, Misfire: workflow.Misfire{Policy: workflow.MisfireSkip}}, "Schedule.Misfire", workflow.CapMisfireSkip},
		"misfire all":  {workflow.Schedule{Location: time.UTC, Misfire: workflow.Misfire{Policy: workflow.MisfireFireAll, Max: 2}}, "Schedule.Misfire", workflow.CapMisfireFireAll},
	} {
		tc.s.Workflow, tc.s.Cron = "w", "0 9 * * *"
		_, err := New(Config{}).Schedule(t.Context(), tc.s)
		var ue *workflow.UnsupportedError
		if !errors.As(err, &ue) || ue.Field != tc.field || !ue.Missing.Has(tc.missing) || !strings.Contains(ue.Reason, "Hatchet") {
			t.Errorf("%s: Schedule = %v, want UnsupportedError on %s missing %s", name, err, tc.field, tc.missing)
		}
	}

	for name, p := range map[string]workflow.RetryPolicy{"jitter": jitter, "retry after": delay} {
		if err := checkConfig(New(Config{BatchCoordinatorRetry: &p}).cfg); !errors.Is(err, workflow.ErrUnsupported) {
			t.Errorf("coordinator %s: checkConfig = %v, want ErrUnsupported", name, err)
		}
	}
}
