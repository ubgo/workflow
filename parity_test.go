package workflow_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/ubgo/workflow"
)

func TestRetryPolicyNext(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	boom := errors.New("boom")
	release := workflow.RetryAfter(boom, time.Second)
	minute := time.Minute
	for _, tc := range []struct {
		name      string
		p         workflow.RetryPolicy
		err       error
		a         workflow.Attempts
		now       time.Time
		wantRetry bool
		wantDelay time.Duration
	}{
		{"within Max", workflow.RetryPolicy{Max: 2, Backoff: workflow.Backoff{Initial: minute}}, boom, workflow.Attempts{N: 1, Errors: 1}, t0, true, minute},
		{"past Max", workflow.RetryPolicy{Max: 2}, boom, workflow.Attempts{N: 3, Errors: 3}, t0, false, 0},
		{"permanent", workflow.RetryPolicy{Max: 2}, workflow.Permanent(boom), workflow.Attempts{N: 1, Errors: 1}, t0, false, 0},
		{"discard", workflow.RetryPolicy{Max: 2}, workflow.Discard(boom), workflow.Attempts{N: 1, Errors: 1}, t0, false, 0},
		{"MaxErrors reached", workflow.RetryPolicy{Max: 9, MaxErrors: 2}, boom, workflow.Attempts{N: 2, Errors: 2}, t0, false, 0},
		{"MaxErrors not reached by releases", workflow.RetryPolicy{Max: 9, MaxErrors: 2}, release, workflow.Attempts{N: 5, Errors: 1}, t0, true, 0},
		{"Until not passed", workflow.RetryPolicy{Max: 9, Backoff: workflow.Backoff{Initial: minute}, Until: 2 * minute}, boom, workflow.Attempts{N: 1, Errors: 1, First: t0}, t0.Add(minute), true, minute},
		{"Until exactly", workflow.RetryPolicy{Max: 9, Backoff: workflow.Backoff{Initial: minute}, Until: 2 * minute}, boom, workflow.Attempts{N: 1, Errors: 1, First: t0}, t0.Add(minute), true, minute},
		{"Until passed by the delay", workflow.RetryPolicy{Max: 9, Backoff: workflow.Backoff{Initial: minute}, Until: 2 * minute}, boom, workflow.Attempts{N: 1, Errors: 1, First: t0}, t0.Add(minute + time.Second), false, 0},
		{"Until with no First is off", workflow.RetryPolicy{Max: 9, Until: time.Second}, boom, workflow.Attempts{N: 1, Errors: 1}, t0.Add(time.Hour), true, 0},
	} {
		d, ok := tc.p.Next(tc.err, tc.a, tc.now, nil)
		if ok != tc.wantRetry || d != tc.wantDelay {
			t.Errorf("%s: Next = %v, %v; want %v, %v", tc.name, d, ok, tc.wantDelay, tc.wantRetry)
		}
	}
}

func TestAttemptsCount(t *testing.T) {
	boom := errors.New("boom")
	a := workflow.Attempts{}.Count(boom).Count(workflow.RetryAfter(boom, 0)).Count(boom)
	if a.N != 3 || a.Errors != 2 {
		t.Fatalf("Count = %+v, want N 3 and Errors 2 (a release does not count as an error)", a)
	}
	if !workflow.IsRelease(workflow.RetryAfter(boom, 0)) || workflow.IsRelease(boom) {
		t.Fatal("IsRelease must hold only for RetryAfter errors")
	}
}

func TestEffectiveRetriesFor(t *testing.T) {
	stepP := &workflow.RetryPolicy{Max: 1}
	jobP := &workflow.RetryPolicy{Max: 7}
	wf := workflow.Workflow{Retry: workflow.RetryPolicy{Max: 3}}
	step := workflow.Step{Retry: stepP}
	if got := workflow.EffectiveRetriesFor(step, wf, workflow.Job{Retry: jobP}).Max; got != 7 {
		t.Errorf("job override: Max %d, want 7", got)
	}
	if got := workflow.EffectiveRetriesFor(step, wf, workflow.Job{}).Max; got != 1 {
		t.Errorf("no job override: Max %d, want the step's 1", got)
	}
	if got := workflow.EffectiveRetriesFor(workflow.Step{}, wf, workflow.Job{}).Max; got != 3 {
		t.Errorf("no step policy: Max %d, want the workflow's 3", got)
	}
}

func TestDiscard(t *testing.T) {
	boom := errors.New("boom")
	if workflow.Discard(nil) != nil {
		t.Fatal("Discard(nil) must be nil")
	}
	d := workflow.Discard(boom)
	if !workflow.IsDiscarded(d) || !errors.Is(d, boom) || workflow.IsPermanent(d) {
		t.Fatalf("Discard(boom): IsDiscarded %v, Is boom %v, IsPermanent %v", workflow.IsDiscarded(d), errors.Is(d, boom), workflow.IsPermanent(d))
	}
	if (workflow.RetryPolicy{Max: 5}).Retryable(d, 1) {
		t.Fatal("a discarded error must not be retryable")
	}
	run := workflow.DiscardedError("s", d)
	for _, target := range []error{workflow.ErrDiscarded, workflow.ErrCancelled, boom} {
		if !errors.Is(run, target) {
			t.Errorf("DiscardedError does not match %v", target)
		}
	}
}

func TestEffectiveQueueAndValidation(t *testing.T) {
	wf := workflow.Workflow{Name: "w", Queue: "wfq"}
	if got := workflow.EffectiveQueue(workflow.Workflow{}, workflow.Job{}); got != workflow.DefaultQueue {
		t.Errorf("EffectiveQueue with nothing set = %q", got)
	}
	if got := workflow.EffectiveQueue(wf, workflow.Job{}); got != "wfq" {
		t.Errorf("EffectiveQueue of the workflow = %q", got)
	}
	if got := workflow.EffectiveQueue(wf, workflow.Job{Queue: "job"}); got != "job" {
		t.Errorf("EffectiveQueue of the job = %q", got)
	}
	for _, bad := range []string{"", "has space", "slash/no", strings.Repeat("q", 65)} {
		if err := workflow.ValidateQueueName(bad); !errors.Is(err, workflow.ErrInvalid) {
			t.Errorf("ValidateQueueName(%q) = %v, want ErrInvalid", bad, err)
		}
	}
	for _, good := range []string{"default", "high.priority", "a:b_c-d", strings.Repeat("q", 64)} {
		if err := workflow.ValidateQueueName(good); err != nil {
			t.Errorf("ValidateQueueName(%q) = %v", good, err)
		}
	}
	if err := workflow.ValidateQueues([]workflow.QueueConfig{{Name: "a"}, {Name: "a"}}); !errors.Is(err, workflow.ErrInvalid) {
		t.Errorf("duplicate queues: %v, want ErrInvalid", err)
	}
	if err := workflow.ValidateQueues([]workflow.QueueConfig{{Name: "a", Workers: -1}}); !errors.Is(err, workflow.ErrInvalid) {
		t.Errorf("negative Workers: %v, want ErrInvalid", err)
	}
}

func TestValidateJob(t *testing.T) {
	for _, tc := range []struct {
		name string
		job  workflow.Job
		ok   bool
	}{
		{"plain", workflow.Job{Workflow: "w"}, true},
		{"no workflow", workflow.Job{}, false},
		{"bad queue", workflow.Job{Workflow: "w", Queue: "a b"}, false},
		{"data retry", workflow.Job{Workflow: "w", Retry: &workflow.RetryPolicy{Max: 2, Until: time.Hour, MaxErrors: 1}}, true},
		{"retry with NoRetryOn", workflow.Job{Workflow: "w", Retry: &workflow.RetryPolicy{NoRetryOn: func(error) bool { return true }}}, false},
		{"retry with DelayFor", workflow.Job{Workflow: "w", Retry: &workflow.RetryPolicy{DelayFor: workflow.RetryAfterDelay}}, false},
		{"negative Until", workflow.Job{Workflow: "w", Retry: &workflow.RetryPolicy{Until: -1}}, false},
		{"negative MaxErrors", workflow.Job{Workflow: "w", Retry: &workflow.RetryPolicy{MaxErrors: -1}}, false},
	} {
		err := workflow.ValidateJob(tc.job)
		if tc.ok != (err == nil) || (err != nil && !errors.Is(err, workflow.ErrInvalid)) {
			t.Errorf("%s: ValidateJob = %v, want ok=%v", tc.name, err, tc.ok)
		}
	}
}

func TestRequiresAndCheckJob(t *testing.T) {
	job := workflow.Job{Workflow: "w", Queue: "q", Retry: &workflow.RetryPolicy{Max: 1, Until: time.Hour}}
	want := workflow.CapQueue | workflow.CapJobRetry | workflow.CapRetryUntil
	if got := workflow.RequiresJob(job); got != want {
		t.Fatalf("RequiresJob = %s, want %s", got, want)
	}
	if err := workflow.CheckJob(want, job); err != nil {
		t.Fatalf("CheckJob with every cap: %v", err)
	}
	var ue *workflow.UnsupportedError
	if err := workflow.CheckJob(workflow.CapQueue, job); !errors.As(err, &ue) || ue.Field != "Job.Retry" {
		t.Fatalf("CheckJob without JobRetry = %v, want an UnsupportedError on Job.Retry", err)
	}
	if err := workflow.CheckJob(workflow.CapJobRetry|workflow.CapRetryUntil, job); !errors.As(err, &ue) || ue.Field != "Job.Queue" {
		t.Fatalf("CheckJob without Queue = %v, want an UnsupportedError on Job.Queue", err)
	}
	wf := workflow.Workflow{Name: "w", Queue: "a", Steps: []workflow.Step{{Name: "s", Run: noop}}}
	if err := workflow.CheckJobQueue(workflow.CapQueue, wf, workflow.Job{Queue: "a"}); err != nil {
		t.Errorf("the workflow's own queue without override: %v", err)
	}
	if err := workflow.CheckJobQueue(workflow.CapQueue, wf, workflow.Job{Queue: "b"}); !errors.As(err, &ue) || !ue.Missing.Has(workflow.CapQueueOverride) {
		t.Errorf("another queue without override: %v, want an UnsupportedError for QueueOverride", err)
	}
	if err := workflow.CheckJobQueue(workflow.CapQueue|workflow.CapQueueOverride, wf, workflow.Job{Queue: "b"}); err != nil {
		t.Errorf("another queue with override: %v", err)
	}
	if got := workflow.Requires(wf); !got.Has(workflow.CapQueue) {
		t.Errorf("Requires of a queued workflow = %s, want Queue", got)
	}
	wf.Queue = workflow.DefaultQueue
	if got := workflow.Requires(wf); got.Has(workflow.CapQueue) {
		t.Errorf("Requires of the default queue = %s, want no Queue", got)
	}
}

func TestParityCapNames(t *testing.T) {
	if got := workflow.CapDrain.String(); got != "Drain" {
		t.Fatalf("CapDrain = %q", got)
	}
	if got := (workflow.CapQueue | workflow.CapScheduleList).String(); got != "Queue|ScheduleList" {
		t.Fatalf("String = %q", got)
	}
	if workflow.CapQueue <= workflow.CapCronZone {
		t.Fatal("a new capability moved an existing bit")
	}
}

func TestWindowsAllows(t *testing.T) {
	at := func(h, m int) time.Time { return time.Date(2026, 3, 1, h, m, 0, 0, time.UTC) }
	day := workflow.Windows{Between: []workflow.Window{{From: "09:00", To: "17:00"}}}
	night := workflow.Windows{Between: []workflow.Window{{From: "22:00", To: "06:00"}}}
	lunch := workflow.Windows{Unless: []workflow.Window{{From: "12:00", To: "13:00"}}}
	for _, tc := range []struct {
		name string
		w    workflow.Windows
		t    time.Time
		want bool
	}{
		{"zero allows all", workflow.Windows{}, at(3, 0), true},
		{"inside", day, at(9, 0), true},
		{"To is exclusive", day, at(17, 0), false},
		{"before", day, at(8, 59), false},
		{"wraps midnight, late", night, at(23, 0), true},
		{"wraps midnight, early", night, at(5, 59), true},
		{"wraps midnight, day", night, at(12, 0), false},
		{"unless inside", lunch, at(12, 30), false},
		{"unless outside", lunch, at(13, 0), true},
	} {
		if got := tc.w.Allows(tc.t, time.UTC); got != tc.want {
			t.Errorf("%s: Allows(%s) = %v, want %v", tc.name, tc.t.Format("15:04"), got, tc.want)
		}
	}
	// The wall clock of the zone decides: 07:30 UTC is 12:30 in UTC+5.
	if got := lunch.Allows(at(7, 30), time.FixedZone("UTC+5", 5*3600)); got {
		t.Error("an Unless window must be read on the zone's wall clock")
	}
}

func TestValidateScheduleRefusesLocalAndBadWindows(t *testing.T) {
	s := workflow.Schedule{Workflow: "w", Cron: "* * * * *", Location: time.Local}
	if err := workflow.ValidateSchedule(s); !errors.Is(err, workflow.ErrInvalid) {
		t.Errorf("time.Local: %v, want ErrInvalid", err)
	}
	s.Location = time.UTC
	for _, w := range []workflow.Window{{From: "9:00", To: "10:00"}, {From: "09:00", To: "09:00"}, {From: "25:00", To: "10:00"}} {
		s.Windows = workflow.Windows{Between: []workflow.Window{w}}
		if err := workflow.ValidateSchedule(s); !errors.Is(err, workflow.ErrInvalid) {
			t.Errorf("window %+v: %v, want ErrInvalid", w, err)
		}
	}
	s.Windows = workflow.Windows{Between: []workflow.Window{{From: "09:00", To: "10:00"}}}
	if err := workflow.ValidateSchedule(s); err != nil {
		t.Errorf("a good window: %v", err)
	}
	if !workflow.RequiresSchedule(s).Has(workflow.CapScheduleWindow) {
		t.Error("RequiresSchedule must ask for ScheduleWindow")
	}
	if err := workflow.CheckScheduleCaps(workflow.CapCron, s); err == nil || !strings.Contains(err.Error(), "Schedule.Windows") {
		t.Errorf("CheckScheduleCaps without ScheduleWindow: %v, want a refusal naming Schedule.Windows", err)
	}
}

func TestValidateWorkflowParityFields(t *testing.T) {
	base := workflow.Workflow{Name: "w", Steps: []workflow.Step{{Name: "a", Run: noop}}}
	bad := base
	bad.Queue = "a b"
	if err := workflow.Validate(bad); !errors.Is(err, workflow.ErrInvalid) {
		t.Errorf("bad Queue: %v", err)
	}
	bad = base
	bad.CronWindows = workflow.Windows{Between: []workflow.Window{{From: "09:00", To: "10:00"}}}
	if err := workflow.Validate(bad); !errors.Is(err, workflow.ErrInvalid) {
		t.Errorf("CronWindows without Cron: %v", err)
	}
	bad.Cron = []string{"* * * * *"}
	if err := workflow.Validate(bad); err != nil {
		t.Errorf("CronWindows with Cron: %v", err)
	}
	bad.Retry = workflow.RetryPolicy{Until: -time.Second}
	if err := workflow.Validate(bad); !errors.Is(err, workflow.ErrInvalid) {
		t.Errorf("negative Until: %v", err)
	}
}

func TestMisfirePlan(t *testing.T) {
	t0 := time.Date(2026, 1, 5, 0, 0, 0, 0, time.UTC)
	hourly := ticks{t0.Add(time.Hour), t0.Add(2 * time.Hour), t0.Add(3 * time.Hour)}
	now := t0.Add(3*time.Hour + 30*time.Minute)
	fire, dropped := workflow.Misfire{Policy: workflow.MisfireFireOnce}.Plan(hourly, t0, now, time.UTC)
	if len(fire) != 1 || dropped != 2 {
		t.Errorf("fire once: %v fired, %d dropped; want 1 and 2", fire, dropped)
	}
	fire, dropped = workflow.Misfire{Policy: workflow.MisfireSkip}.Plan(hourly, t0, now, time.UTC)
	if len(fire) != 0 || dropped != 3 {
		t.Errorf("skip: %v fired, %d dropped; want 0 and 3", fire, dropped)
	}
	if _, dropped = (workflow.Misfire{}).Plan(hourly, t0, now, time.UTC); dropped != 0 {
		t.Errorf("default policy: %d dropped, want 0 (the driver decides)", dropped)
	}
	if got := workflow.LastTick(hourly, t0, t0.Add(150*time.Minute), time.UTC); !got.Equal(t0.Add(2 * time.Hour)) {
		t.Errorf("LastTick = %v", got)
	}
}

func TestRunQueryMatchAndValidate(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	info := workflow.RunInfo{Ref: workflow.RunRef{RunID: "r1", ScheduledID: "s1"}, Workflow: "w", Queue: "q", Status: workflow.StatusFailed, Labels: map[string]string{"a": "1"}, EnqueuedAt: t0}
	for _, tc := range []struct {
		name string
		q    workflow.RunQuery
		want bool
	}{
		{"zero", workflow.RunQuery{}, true},
		{"workflow", workflow.RunQuery{Workflow: "x"}, false},
		{"queue", workflow.RunQuery{Queue: "q"}, true},
		{"status in list", workflow.RunQuery{Statuses: []workflow.Status{workflow.StatusQueued, workflow.StatusFailed}}, true},
		{"status not in list", workflow.RunQuery{Statuses: []workflow.Status{workflow.StatusQueued}}, false},
		{"label", workflow.RunQuery{Labels: map[string]string{"a": "1"}}, true},
		{"label value", workflow.RunQuery{Labels: map[string]string{"a": "2"}}, false},
		{"From inclusive", workflow.RunQuery{From: t0}, true},
		{"To exclusive", workflow.RunQuery{To: t0}, false},
		{"Ref by RunID", workflow.RunQuery{Ref: workflow.RunRef{RunID: "r1"}}, true},
		{"Ref by ScheduledID", workflow.RunQuery{Ref: workflow.RunRef{ScheduledID: "s1"}}, true},
		{"Ref with both", workflow.RunQuery{Ref: workflow.RunRef{RunID: "r1", ScheduledID: "s1"}}, true},
		{"Ref another run", workflow.RunQuery{Ref: workflow.RunRef{RunID: "r2"}}, false},
		{"Ref another schedule", workflow.RunQuery{Ref: workflow.RunRef{RunID: "r1", ScheduledID: "s2"}}, false},
	} {
		if got := tc.q.Match(info); got != tc.want {
			t.Errorf("%s: Match = %v, want %v", tc.name, got, tc.want)
		}
	}
	for _, q := range []workflow.RunQuery{
		{Statuses: []workflow.Status{"nope"}},
		{Limit: -1},
		{Limit: workflow.MaxRunsLimit + 1},
		{From: t0, To: t0},
		{Queue: "a b"},
	} {
		if err := workflow.ValidateRunQuery(q); !errors.Is(err, workflow.ErrInvalid) {
			t.Errorf("ValidateRunQuery(%+v) = %v, want ErrInvalid", q, err)
		}
	}
	if err := workflow.ValidateCancelQuery(workflow.RunQuery{Statuses: []workflow.Status{workflow.StatusSucceeded}}); !errors.Is(err, workflow.ErrInvalid) {
		t.Error("ValidateCancelQuery must refuse a terminal status")
	}
	if err := workflow.ValidateCancelQuery(workflow.RunQuery{Limit: 5}); !errors.Is(err, workflow.ErrInvalid) {
		t.Error("ValidateCancelQuery must refuse a Limit")
	}
	if got := workflow.CancelStatuses(workflow.RunQuery{}); !slices.Equal(got, []workflow.Status{workflow.StatusScheduled, workflow.StatusQueued}) {
		t.Errorf("CancelStatuses default = %v", got)
	}
	if got := (workflow.RunQuery{}).PageLimit(); got != workflow.DefaultRunsLimit {
		t.Errorf("PageLimit default = %d", got)
	}
}

func TestPauseTargets(t *testing.T) {
	for _, bad := range []workflow.PauseTarget{{}, {Workflow: "w", Queue: "q"}, {All: true, Workflow: "w"}, {Queue: "a b"}} {
		if err := workflow.ValidatePauseTarget(bad); !errors.Is(err, workflow.ErrInvalid) {
			t.Errorf("ValidatePauseTarget(%+v) = %v", bad, err)
		}
	}
	if err := workflow.CheckPause(workflow.CapPauseWorkflow, workflow.PauseQueue("q")); !errors.Is(err, workflow.ErrUnsupported) {
		t.Errorf("CheckPause of a queue without PauseQueue = %v", err)
	}
	if err := workflow.CheckPause(workflow.CapPauseWorkflow, workflow.PauseAll()); err != nil {
		t.Errorf("CheckPause(All) with PauseWorkflow = %v", err)
	}
	if !workflow.PauseAll().Holds("w", "q") || !workflow.PauseQueue("q").Holds("w", "q") || workflow.PauseWorkflow("x").Holds("w", "q") {
		t.Error("Holds is wrong")
	}
	if err := workflow.ValidateScheduleTarget(workflow.ScheduleTarget{}); !errors.Is(err, workflow.ErrInvalid) {
		t.Error("an empty ScheduleTarget must be invalid")
	}
	if err := workflow.ValidateScheduleTarget(workflow.ScheduleTarget{ID: "x", All: true}); !errors.Is(err, workflow.ErrInvalid) {
		t.Error("a ScheduleTarget with both fields must be invalid")
	}
}

// bareClient implements Client and nothing optional.
type bareClient struct{ workflow.Client }

func TestForwardRefusesWhatTheInnerLacks(t *testing.T) {
	f := workflow.Forward{Inner: bareClient{}}
	ctx := context.Background()
	checks := map[string]error{}
	_, checks["Runs"] = f.Runs(ctx, workflow.RunQuery{})
	_, checks["Retry"] = f.Retry(ctx, workflow.RunRef{})
	checks["Delete"] = f.Delete(ctx, workflow.RunRef{})
	_, checks["CancelAll"] = f.CancelAll(ctx, workflow.RunQuery{})
	checks["Pause"] = f.Pause(ctx, workflow.PauseAll())
	checks["Resume"] = f.Resume(ctx, workflow.PauseAll())
	_, checks["Paused"] = f.Paused(ctx)
	checks["PauseSchedule"] = f.PauseSchedule(ctx, workflow.ScheduleTarget{All: true})
	checks["ResumeSchedule"] = f.ResumeSchedule(ctx, workflow.ScheduleTarget{All: true})
	_, checks["Schedules"] = f.Schedules(ctx)
	checks["OnTick"] = f.OnTick(func(context.Context, workflow.Tick) {})
	checks["Drain"] = f.Drain(ctx)
	for name, err := range checks {
		if !errors.Is(err, workflow.ErrUnsupported) {
			t.Errorf("%s: %v, want ErrUnsupported", name, err)
		}
	}
	if f.Done() != nil {
		t.Error("Done of an inner client without Drainer must be nil")
	}
}

// queryClient implements RunQuerier only.
type queryClient struct {
	workflow.Client
	got workflow.RunQuery
}

func (q *queryClient) Runs(_ context.Context, rq workflow.RunQuery) (workflow.RunPage, error) {
	q.got = rq
	return workflow.RunPage{Next: "n"}, nil
}

func TestForwardAndUseForwardOptionalInterfaces(t *testing.T) {
	inner := &queryClient{}
	c, err := workflow.Use(inner, namedPlugin("p"))
	if err != nil {
		t.Fatal(err)
	}
	rq, ok := c.(workflow.RunQuerier)
	if !ok {
		t.Fatal("the client Use returns must implement RunQuerier")
	}
	page, err := rq.Runs(context.Background(), workflow.RunQuery{Workflow: "w"})
	if err != nil || page.Next != "n" || inner.got.Workflow != "w" {
		t.Fatalf("Runs through Use = %+v, %v; inner saw %+v", page, err, inner.got)
	}
}

// namedPlugin is a plugin contributing nothing.
type namedPlugin string

func (p namedPlugin) Name() string { return string(p) }

// hookClient implements both notifiers and records the observers.
type hookClient struct {
	workflow.Client
	ticks []workflow.TickObserver
	runs  []workflow.RunFinishedFunc
}

func (h *hookClient) OnTick(fn workflow.TickObserver) error {
	h.ticks = append(h.ticks, fn)
	return nil
}
func (h *hookClient) OnRunFinished(fn workflow.RunFinishedFunc) error {
	h.runs = append(h.runs, fn)
	return nil
}

func TestAttachScheduleHooks(t *testing.T) {
	var got []string
	rec := func(s string) func(context.Context, workflow.RunEvent) {
		return func(context.Context, workflow.RunEvent) { got = append(got, s) }
	}
	h := &hookClient{}
	err := workflow.AttachScheduleHooks(h, workflow.ScheduleHooks{
		Workflow:  "report",
		Name:      "nightly",
		Fired:     func(context.Context, workflow.Tick) { got = append(got, "fired") },
		Skipped:   func(context.Context, workflow.Tick) { got = append(got, "skipped") },
		OnSuccess: rec("success"),
		OnFailure: rec("failure"),
		After:     rec("after"),
	})
	if err != nil || len(h.ticks) != 1 || len(h.runs) != 1 {
		t.Fatalf("AttachScheduleHooks = %v; %d tick and %d run observers, want 1 each", err, len(h.ticks), len(h.runs))
	}
	ctx := context.Background()
	h.ticks[0](ctx, workflow.Tick{Workflow: "report", Name: "nightly", Outcome: workflow.TickFired})
	h.ticks[0](ctx, workflow.Tick{Workflow: "report", Name: "other", Outcome: workflow.TickFired})
	h.ticks[0](ctx, workflow.Tick{Workflow: "report", Name: "nightly", Outcome: workflow.TickSkipped})
	labels := workflow.FiredLabels(nil, "id1", "nightly")
	h.runs[0](ctx, workflow.RunEvent{Workflow: "report", Status: workflow.StatusSucceeded, Labels: labels})
	h.runs[0](ctx, workflow.RunEvent{Workflow: "report", Status: workflow.StatusFailed, Labels: labels})
	h.runs[0](ctx, workflow.RunEvent{Workflow: "report", Status: workflow.StatusSucceeded}) // not from a schedule
	want := []string{"fired", "skipped", "success", "after", "failure", "after"}
	if !slices.Equal(got, want) {
		t.Fatalf("hooks ran %v, want %v", got, want)
	}
	if err := workflow.AttachScheduleHooks(bareClient{}, workflow.ScheduleHooks{Workflow: "w", After: rec("x")}); !errors.Is(err, workflow.ErrUnsupported) {
		t.Errorf("hooks on a client without RunNotifier: %v, want ErrUnsupported", err)
	}
	if err := workflow.AttachScheduleHooks(h, workflow.ScheduleHooks{}); !errors.Is(err, workflow.ErrInvalid) {
		t.Errorf("hooks without a Workflow: %v, want ErrInvalid", err)
	}
}

func TestFiredLabelsCopies(t *testing.T) {
	in := map[string]string{"tenant": "a"}
	out := workflow.FiredLabels(in, "id", "n")
	if len(in) != 1 || out["tenant"] != "a" || out[workflow.LabelSchedule] != "id" || out[workflow.LabelScheduleName] != "n" {
		t.Fatalf("FiredLabels = %v (input now %v)", out, in)
	}
	if _, ok := workflow.ReservedLabel(workflow.LabelSchedule); !ok {
		t.Fatal("LabelSchedule must be reserved")
	}
}
