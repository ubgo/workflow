package hatchet

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/hatchet-dev/hatchet/pkg/client/rest"
	hatchetsdk "github.com/hatchet-dev/hatchet/sdks/go"

	"github.com/ubgo/workflow"
)

// TestCapabilitiesParity pins which of the queue, run-management and
// schedule-control capabilities the driver claims and which it refuses,
// so a change to either is a visible decision backed by a live test
// (parity_live_test.go, pause_live_test.go).
func TestCapabilitiesParity(t *testing.T) {
	have := New(Config{}).Capabilities()
	claimed := []workflow.Caps{workflow.CapQueue, workflow.CapRunQuery, workflow.CapCancelAll, workflow.CapRetryRun, workflow.CapScheduleList, workflow.CapDrain}
	refused := []workflow.Caps{
		workflow.CapQueueOverride, workflow.CapJobRetry, workflow.CapRetryUntil, workflow.CapRetryMaxErrors,
		workflow.CapDiscard, workflow.CapDeleteRun, workflow.CapPauseWorkflow, workflow.CapPauseQueue,
		workflow.CapSchedulePause, workflow.CapScheduleWhen, workflow.CapScheduleWindow, workflow.CapTickEvents,
	}
	for _, c := range claimed {
		if !have.Has(c) {
			t.Errorf("%v not claimed", c)
		}
	}
	for _, c := range refused {
		if have.Has(c) {
			t.Errorf("%v claimed, but the driver cannot honour it", c)
		}
	}
}

// TestRegisterRefusesParityFields pins the workflow fields Register
// refuses by name.
func TestRegisterRefusesParityFields(t *testing.T) {
	run := func(ctxStep workflow.Step) workflow.Workflow {
		return workflow.Workflow{Name: "wf", Steps: []workflow.Step{ctxStep}}
	}
	step := workflow.Step{Name: "a", Run: func(_ context.Context, in []byte) ([]byte, error) { return in, nil }}
	for name, tc := range map[string]struct {
		wf    workflow.Workflow
		field string
	}{
		"retry until": {func() workflow.Workflow {
			s := step
			s.Retry = &workflow.RetryPolicy{Max: 2, Until: time.Minute}
			return run(s)
		}(), "Retry.Until"},
		"retry max errors": {func() workflow.Workflow {
			s := step
			s.Retry = &workflow.RetryPolicy{Max: 2, MaxErrors: 1}
			return run(s)
		}(), "Retry.MaxErrors"},
		"schedule when": {func() workflow.Workflow {
			w := run(step)
			w.ScheduleWhen = func(context.Context, workflow.Tick) bool { return true }
			return w
		}(), "Workflow.ScheduleWhen"},
		"cron windows": {func() workflow.Workflow {
			w := run(step)
			w.Cron = []string{"0 * * * *"}
			w.CronWindows = workflow.Windows{Between: []workflow.Window{{From: "09:00", To: "17:00"}}}
			return w
		}(), "Workflow.CronWindows"},
	} {
		err := New(Config{}).Register(tc.wf)
		var u *workflow.UnsupportedError
		if !errors.As(err, &u) || u.Field != tc.field {
			t.Errorf("%s: Register = %v, want an UnsupportedError on %s", name, err, tc.field)
		}
	}
}

// TestJobChecks pins the job fields Enqueue refuses before it reaches the
// engine: a per-dispatch retry policy (fixed at declaration on Hatchet)
// and a Job.Queue other than the workflow's (no CapQueueOverride).
func TestJobChecks(t *testing.T) {
	if err := workflow.CheckJob(caps, workflow.Job{Workflow: "wf", Retry: &workflow.RetryPolicy{Max: 1}}); !errors.Is(err, workflow.ErrUnsupported) {
		t.Fatalf("Job.Retry: %v", err)
	}
	wf := workflow.Workflow{Name: "wf", Queue: "mail"}
	if err := workflow.CheckJobQueue(caps, wf, workflow.Job{Workflow: "wf", Queue: "sms"}); !errors.Is(err, workflow.ErrUnsupported) {
		t.Fatalf("Job.Queue override: %v", err)
	}
	if err := workflow.CheckJobQueue(caps, wf, workflow.Job{Workflow: "wf", Queue: "mail"}); err != nil {
		t.Fatalf("Job.Queue equal to the workflow's: %v", err)
	}
	if err := workflow.CheckJob(caps, workflow.Job{Workflow: "wf", Queue: "mail"}); err != nil {
		t.Fatalf("Job.Queue: %v", err)
	}
}

// TestCheckQueues pins the Config.Queues lists Start refuses.
func TestCheckQueues(t *testing.T) {
	if err := checkQueues([]workflow.QueueConfig{{Name: "mail", Workers: 2}, {Name: "default"}}); err != nil {
		t.Fatalf("valid list: %v", err)
	}
	if err := checkQueues([]workflow.QueueConfig{{Name: "mail", Priority: 1}}); !errors.Is(err, workflow.ErrUnsupported) {
		t.Fatalf("priority: %v", err)
	}
	if err := checkQueues([]workflow.QueueConfig{{Name: "a"}, {Name: "a"}}); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("duplicate: %v", err)
	}
	if err := checkConfig(New(Config{Queues: []workflow.QueueConfig{{Name: "bad name"}}}).cfg); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("checkConfig: %v", err)
	}
}

// TestWorkerPools pins how Start splits workflows into workers: one for
// everything without queues, one per served queue with them (slots from
// Workers, else Slots), nothing for an unserved queue, and the
// coordinator always on a worker.
func TestWorkerPools(t *testing.T) {
	specs := map[string]workflow.Workflow{
		"a": {Name: "a"},
		"b": {Name: "b", Queue: "mail"},
		"c": {Name: "c", Queue: "sms"},
	}
	order := []string{"a", "b", "c"}
	declared := map[string]*hatchetsdk.Workflow{}
	for _, n := range order {
		declared[n] = &hatchetsdk.Workflow{}
	}
	coord := &hatchetsdk.Workflow{}
	count := func(p []workerPool) []int {
		var out []int
		for _, w := range p {
			out = append(out, len(w.workflows))
		}
		return out
	}

	one := workerPools(Config{WorkerName: "w", Slots: 5}, order, specs, declared, nil)
	if len(one) != 1 || one[0].name != "w" || one[0].slots != 5 || len(one[0].workflows) != 3 {
		t.Fatalf("no queues: %+v", one)
	}
	cfg := Config{WorkerName: "w", Slots: 5, Queues: []workflow.QueueConfig{{Name: "default"}, {Name: "mail", Workers: 2}, {Name: "idle"}}}
	pools := workerPools(cfg, order, specs, declared, coord)
	if got := count(pools); !reflect.DeepEqual(got, []int{2, 1}) {
		t.Fatalf("workflows per pool = %v, want [2 1] (default + coordinator, mail; sms unserved, idle empty)", got)
	}
	if pools[0].name != "w-default" || pools[0].slots != 5 || pools[1].name != "w-mail" || pools[1].slots != 2 {
		t.Fatalf("pools = %+v", pools)
	}
	onlyCoord := workerPools(Config{WorkerName: "w", Queues: []workflow.QueueConfig{{Name: "idle"}}}, order, specs, declared, coord)
	if got := count(onlyCoord); !reflect.DeepEqual(got, []int{1}) {
		t.Fatalf("coordinator alone = %v", got)
	}
}

// TestListedStatuses pins how query statuses map to the two engine
// listings.
func TestListedStatuses(t *testing.T) {
	runs, sched, st := listedStatuses(nil)
	if !runs || !sched || st != nil {
		t.Fatalf("empty: %v %v %v", runs, sched, st)
	}
	runs, sched, _ = listedStatuses([]workflow.Status{workflow.StatusScheduled})
	if runs || !sched {
		t.Fatalf("scheduled only: runs=%v sched=%v", runs, sched)
	}
	runs, sched, st = listedStatuses([]workflow.Status{workflow.StatusWaiting, workflow.StatusFailed})
	want := []rest.V1TaskStatus{rest.V1TaskStatusRUNNING, rest.V1TaskStatusQUEUED, rest.V1TaskStatusFAILED}
	if !runs || sched || st == nil || !reflect.DeepEqual(*st, want) {
		t.Fatalf("waiting+failed: %v %v %v", runs, sched, st)
	}
}

// TestMetadataFilter pins the engine-side filter: sorted pairs, the
// workflow stamp, and no pair for a key the engine cannot split.
func TestMetadataFilter(t *testing.T) {
	got := metadataFilter(workflow.RunQuery{Workflow: "wf", Queue: "mail", Labels: map[string]string{"tenant": "t1", "a:b": "x"}})
	want := []string{"tenant:t1", metaWorkflow + ":wf"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("filter = %v, want %v", got, want)
	}
}

// TestCursor pins the cursor round trip and the refusal of a foreign one.
func TestCursor(t *testing.T) {
	r, s, err := parseCursor(formatCursor(120, 7))
	if err != nil || r != 120 || s != 7 {
		t.Fatalf("round trip = %d %d %v", r, s, err)
	}
	for _, bad := range []string{"x", cursorPrefix + "!!", cursorPrefix + "MTI", formatCursor(-1, 0)} {
		if _, _, err := parseCursor(bad); !errors.Is(err, workflow.ErrInvalid) {
			t.Errorf("cursor %q: %v", bad, err)
		}
	}
}

// TestTaskAttempts pins the attempt count of one task.
func TestTaskAttempts(t *testing.T) {
	two := 2
	now := time.Now()
	for name, tc := range map[string]struct {
		t    rest.V1TaskSummary
		want int
	}{
		"queued, never started": {rest.V1TaskSummary{Status: rest.V1TaskStatusQUEUED}, 0},
		"first attempt running": {rest.V1TaskSummary{Status: rest.V1TaskStatusRUNNING, StartedAt: &now}, 1},
		"failed after retries":  {rest.V1TaskSummary{Status: rest.V1TaskStatusFAILED, RetryCount: &two}, 3},
		"queued for a retry":    {rest.V1TaskSummary{Status: rest.V1TaskStatusQUEUED, RetryCount: &two}, 3},
	} {
		if got := taskAttempts(tc.t); got != tc.want {
			t.Errorf("%s: %d, want %d", name, got, tc.want)
		}
	}
}

// TestSameCron pins when a named schedule is already in place: same
// expression, payload and labels.
func TestSameCron(t *testing.T) {
	s := workflow.Schedule{Workflow: "wf", Name: "n", Cron: "0 * * * *", Input: []byte(`{"a":1}`), Labels: map[string]string{"k": "v"}, Location: time.UTC}
	in, err := encodeInput(s.Input)
	if err != nil {
		t.Fatal(err)
	}
	md := labelsAny(map[string]string{"k": "v", metaNamed: "1"})
	row := rest.CronWorkflows{Cron: s.Cron, Input: &in, AdditionalMetadata: &md}
	if !sameCron(row, s) {
		t.Fatal("identical definition not recognised")
	}
	// A trigger made before the named marker is replaced once, to get the
	// stable named ID.
	unmarked := labelsAny(map[string]string{"k": "v"})
	if sameCron(rest.CronWorkflows{Cron: s.Cron, Input: &in, AdditionalMetadata: &unmarked}, s) {
		t.Error("a trigger without the named marker counted as the same definition")
	}
	for name, mod := range map[string]func(workflow.Schedule) workflow.Schedule{
		"cron":   func(s workflow.Schedule) workflow.Schedule { s.Cron = "5 * * * *"; return s },
		"input":  func(s workflow.Schedule) workflow.Schedule { s.Input = []byte(`{"a":2}`); return s },
		"labels": func(s workflow.Schedule) workflow.Schedule { s.Labels = nil; return s },
	} {
		if sameCron(row, mod(s)) {
			t.Errorf("changed %s not noticed", name)
		}
	}
}

// TestDeliveryKey pins that a run ending again after Retry is a new
// delivery, and the same end listed twice is not.
func TestDeliveryKey(t *testing.T) {
	a, b := time.Unix(10, 0), time.Unix(20, 0)
	if deliveryKey("r", a) == deliveryKey("r", b) {
		t.Fatal("two ends of one run share a key")
	}
	if deliveryKey("r", a) != deliveryKey("r", a) {
		t.Fatal("the same end gets two keys")
	}
	if deliveryKey("r", time.Time{}) != "r" {
		t.Fatal("an end without a finish time is not keyed by the run alone")
	}
}

// TestNonRetryableDiscard pins that Discard stops retries on Hatchet,
// which has no CapDiscard and so treats it as Permanent.
func TestNonRetryableDiscard(t *testing.T) {
	if !nonRetryable(workflow.Discard(errors.New("gone")), workflow.RetryPolicy{Max: 3}) {
		t.Fatal("a discarded error is retried")
	}
}

// TestBatch2Refusals pins the second parity batch on this driver: none of
// run states, chains, worker scaling or closures is claimed, each is
// refused where a caller first reaches it, and the client implements
// neither WorkerScaler nor ClosureEnqueuer, so it never pretends to.
func TestBatch2Refusals(t *testing.T) {
	c := New(Config{})
	have := c.Capabilities()
	for _, cp := range []workflow.Caps{workflow.CapRunStates, workflow.CapChain, workflow.CapWorkerScale, workflow.CapClosure} {
		if have.Has(cp) {
			t.Errorf("%v claimed, but the driver cannot honour it", cp)
		}
	}
	var ue *workflow.UnsupportedError

	// Job.Chain: every enqueue path (Enqueue, RunGroup, SpawnGroup) goes
	// through prepare, which refuses before any engine call.
	_, err := c.prepare(workflow.Job{Workflow: "wf", Chain: []workflow.Job{{Workflow: "next"}}})
	if !errors.As(err, &ue) || ue.Field != "Job.Chain" || !ue.Missing.Has(workflow.CapChain) {
		t.Fatalf("Job.Chain: %v", err)
	}

	// Editing a chain from a step: no editor is put on the step ctx.
	if err := workflow.AppendToChain(context.Background(), workflow.Job{Workflow: "next"}); !errors.As(err, &ue) || !ue.Missing.Has(workflow.CapChain) {
		t.Fatalf("AppendToChain: %v", err)
	}

	// Closures and worker scaling: the interfaces are absent.
	var client workflow.Client = c
	if _, ok := client.(workflow.ClosureEnqueuer); ok {
		t.Fatal("implements ClosureEnqueuer")
	}
	if _, ok := client.(workflow.WorkerScaler); ok {
		t.Fatal("implements WorkerScaler: Hatchet fixes slots per worker process")
	}
	_, err = workflow.EnqueueClosure(context.Background(), c, workflow.ClosureJob{Name: "x", Fn: func(context.Context) error { return nil }})
	if !errors.As(err, &ue) || !ue.Missing.Has(workflow.CapClosure) {
		t.Fatalf("EnqueueClosure: %v", err)
	}

	// Status filters the engine cannot answer, refused before it is asked.
	for _, s := range []workflow.Status{workflow.StatusRetrying, workflow.StatusDiscarded} {
		q := workflow.RunQuery{Statuses: []workflow.Status{workflow.StatusQueued, s}}
		if _, err := c.Runs(context.Background(), q); !errors.As(err, &ue) || ue.Field != "RunQuery.Statuses" || !ue.Missing.Has(workflow.CapRunStates) {
			t.Fatalf("Runs %s: %v", s, err)
		}
	}
	if _, err := c.CancelAll(context.Background(), workflow.RunQuery{Statuses: []workflow.Status{workflow.StatusRetrying}}); !errors.As(err, &ue) || !ue.Missing.Has(workflow.CapRunStates) {
		t.Fatalf("CancelAll retrying: %v", err)
	}
	if _, err := c.Runs(context.Background(), workflow.RunQuery{Statuses: []workflow.Status{workflow.StatusFailed}}); errors.As(err, &ue) {
		t.Fatalf("Runs failed refused as unsupported: %v", err)
	}
}

// TestRunQueryRefusesLabelPrefixes pins that a LabelPrefixes filter is
// refused by Runs and CancelAll (no CapLabelPrefix), before the engine is
// reached, rather than ignored.
func TestRunQueryRefusesLabelPrefixes(t *testing.T) {
	c := New(Config{})
	q := workflow.RunQuery{LabelPrefixes: map[string]string{"tenant": "a"}}
	if _, err := c.Runs(t.Context(), q); !errors.Is(err, workflow.ErrUnsupported) {
		t.Fatalf("Runs with LabelPrefixes = %v, want ErrUnsupported", err)
	}
	if _, err := c.CancelAll(t.Context(), q); !errors.Is(err, workflow.ErrUnsupported) {
		t.Fatalf("CancelAll with LabelPrefixes = %v, want ErrUnsupported", err)
	}
}
