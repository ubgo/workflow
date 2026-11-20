package cutover_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/cutover"
	"github.com/ubgo/workflow/memory"
)

var await = workflow.AwaitOptions{Poll: 2 * time.Millisecond}

// world is a cutover over two memory clients with a "work" workflow whose
// step blocks on gate and records the RunID it sees, and a "reply"
// workflow that waits for an event.
type world struct {
	c        *cutover.Client
	old, new workflow.Client
	gate     chan struct{}

	mu     sync.Mutex
	seen   []workflow.RunID
	events []workflow.RunEvent
}

func newWorld(t *testing.T, opts ...cutover.Option) *world {
	t.Helper()
	w := &world{old: memory.New(), new: memory.New(), gate: make(chan struct{})}
	w.c = cutover.New(w.old, w.new, opts...)
	must(t, w.c.Register(workflow.Workflow{Name: "work", Steps: []workflow.Step{{Name: "s", Run: func(ctx context.Context, in []byte) ([]byte, error) {
		info, _ := workflow.StepInfoFrom(ctx)
		w.mu.Lock()
		w.seen = append(w.seen, info.RunID)
		w.mu.Unlock()
		<-w.gate
		return in, nil
	}}}}))
	must(t, w.c.Register(workflow.Workflow{Name: "reply", Steps: []workflow.Step{
		{Name: "wait", Wait: &workflow.WaitSpec{Event: "answer", Key: "q1", Timeout: time.Minute}},
	}}))
	must(t, w.c.OnRunFinished(func(_ context.Context, ev workflow.RunEvent) {
		w.mu.Lock()
		w.events = append(w.events, ev)
		w.mu.Unlock()
	}))
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	must(t, w.c.Start(ctx))
	return w
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

// TestCutover_RoutesByPrefix: new work goes to the new backend with a
// "new:" ref; a run the old backend held before the cutover (an
// unprefixed ref) is still found, cancelled, listed and counted by
// Remaining; Signal reaches a run waiting on the old backend; StepInfo and
// run-finished events carry the prefixed refs.
func TestCutover_RoutesByPrefix(t *testing.T) {
	ctx := context.Background()
	w := newWorld(t)
	// Runs the app started on the old backend before the cutover.
	oldRef, err := w.old.Enqueue(ctx, workflow.Job{Workflow: "work", Payload: []byte(`"old"`)})
	must(t, err)
	oldCancel, err := w.old.Enqueue(ctx, workflow.Job{Workflow: "work", Payload: []byte(`"cancel me"`)})
	must(t, err)
	waiting, err := w.old.Enqueue(ctx, workflow.Job{Workflow: "reply"})
	must(t, err)

	newRef, err := w.c.Enqueue(ctx, workflow.Job{Workflow: "work", Payload: []byte(`"new"`)})
	must(t, err)
	if !strings.HasPrefix(string(newRef.RunID), "new:") {
		t.Fatalf("Enqueue ref = %+v; want a new: prefix", newRef)
	}
	if n, err := w.c.Remaining(ctx); err != nil || n != 3 {
		t.Fatalf("Remaining = %d, %v; want the 3 old runs", n, err)
	}
	for _, ref := range []workflow.RunRef{oldRef, {RunID: "old:" + oldRef.RunID}, newRef} {
		if st, err := w.c.Status(ctx, ref); err != nil || st.IsTerminal() {
			t.Fatalf("Status(%+v) = %v, %v", ref, st, err)
		}
	}
	// Two memory clients both number their runs from run-1, so oldRef's ID
	// exists on both: the prefix (or, unprefixed, the old-first lookup)
	// is what picks the right one. The third old run has no twin.
	if _, err := w.c.Status(ctx, workflow.RunRef{RunID: "new:" + waiting.RunID}); !errors.Is(err, workflow.ErrNotFound) {
		t.Fatalf("an old run under the new prefix: %v", err)
	}
	must(t, w.c.Cancel(ctx, oldCancel)) // unprefixed: found on the old backend
	if st, _ := w.old.Status(ctx, oldCancel); st != workflow.StatusCancelled {
		t.Fatalf("old run after Cancel = %v", st)
	}

	page, err := w.c.Runs(ctx, workflow.RunQuery{Workflow: "work"})
	must(t, err)
	prefixes := map[string]int{}
	for _, r := range page.Runs {
		prefixes[strings.SplitN(string(r.Ref.RunID), ":", 2)[0]]++
	}
	if prefixes["old"] != 2 || prefixes["new"] != 1 || len(page.Runs) != 3 {
		t.Fatalf("Runs = %v; want 2 old and 1 new", prefixes)
	}
	one, err := w.c.Runs(ctx, workflow.RunQuery{Ref: newRef})
	if err != nil || len(one.Runs) != 1 || one.Runs[0].Ref.RunID != newRef.RunID {
		t.Fatalf("Runs by a new ref = %+v, %v", one.Runs, err)
	}

	must(t, w.c.Signal(ctx, workflow.Event{Name: "answer", Key: "q1", Payload: []byte(`"yes"`)}))
	if st, err := workflow.Await(ctx, w.c, waiting, await); err != nil || st != workflow.StatusSucceeded {
		t.Fatalf("old waiting run after Signal = %v, %v", st, err)
	}
	close(w.gate)
	for _, ref := range []workflow.RunRef{oldRef, newRef} {
		if st, err := workflow.Await(ctx, w.c, ref, await); err != nil || st != workflow.StatusSucceeded {
			t.Fatalf("Await(%+v) = %v, %v", ref, st, err)
		}
	}
	if res, err := w.c.Result(ctx, newRef); err != nil || string(res) != `"new"` {
		t.Fatalf("Result = %s, %v", res, err)
	}
	// oldRef's bare ID names a run on both backends; unprefixed, the old
	// backend's answers.
	if res, err := w.c.Result(ctx, oldRef); err != nil || string(res) != `"old"` {
		t.Fatalf("Result of the unprefixed old ref = %s, %v", res, err)
	}
	if n, err := w.c.Remaining(ctx); err != nil || n != 0 {
		t.Fatalf("Remaining after the old runs ended = %d, %v", n, err)
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	for _, id := range w.seen {
		if !strings.HasPrefix(string(id), "old:") && !strings.HasPrefix(string(id), "new:") {
			t.Fatalf("a step saw the unprefixed RunID %q", id)
		}
	}
	deadline := time.Now().Add(2 * time.Second)
	for len(w.events) < 4 && time.Now().Before(deadline) {
		w.mu.Unlock()
		time.Sleep(2 * time.Millisecond)
		w.mu.Lock()
	}
	for _, ev := range w.events {
		if !strings.HasPrefix(string(ev.Ref.RunID), "old:") && !strings.HasPrefix(string(ev.Ref.RunID), "new:") {
			t.Fatalf("a run-finished event carried %+v", ev.Ref)
		}
	}
}

// capsClient masks a client's capabilities.
type capsClient struct {
	workflow.Client
	drop workflow.Caps
}

func (c capsClient) Capabilities() workflow.Caps { return c.Client.Capabilities().Without(c.drop) }

// TestCutover_Capabilities checks each family: the enqueue path follows
// the new backend, run management needs both, schedules follow the
// schedule backend, and the unrouted interfaces are never claimed.
func TestCutover_Capabilities(t *testing.T) {
	full := memory.New().Capabilities()
	none := workflow.Caps{}
	tests := []struct {
		name             string
		oldDrop, newDrop workflow.Caps
		opts             []cutover.Option
		want, wantAbsent workflow.Caps
	}{
		{"enqueue path is new's", workflow.CapsOf(workflow.CapIdempotencyKey, workflow.CapQueue), none, nil, workflow.CapsOf(workflow.CapIdempotencyKey, workflow.CapQueue), none},
		{"enqueue path is not old's", none, workflow.CapIdempotencyKey, nil, none, workflow.CapIdempotencyKey},
		{"run management needs both", workflow.CapsOf(workflow.CapRunQuery, workflow.CapCancelAll), none, nil, none, workflow.CapsOf(workflow.CapRunQuery, workflow.CapCancelAll)},
		{"schedules follow new by default", workflow.CapsOf(workflow.CapCron, workflow.CapScheduleList), none, nil, workflow.CapsOf(workflow.CapCron, workflow.CapScheduleList), none},
		{"schedules follow old when asked", none, workflow.CapCron, []cutover.Option{cutover.SchedulesOn(cutover.SideOld)}, workflow.CapCron, none},
		{"schedules on old lacking them", workflow.CapCron, none, []cutover.Option{cutover.SchedulesOn(cutover.SideOld)}, none, workflow.CapCron},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := cutover.New(capsClient{memory.New(), tt.oldDrop}, capsClient{memory.New(), tt.newDrop}, tt.opts...)
			got := c.Capabilities()
			if !got.Has(tt.want) || got.HasAny(tt.wantAbsent) {
				t.Fatalf("caps %v; want %v present and %v absent", got, tt.want, tt.wantAbsent)
			}
			never := workflow.CapsOf(workflow.CapScheduleTrigger, workflow.CapTickEvents, workflow.CapWorkerScale, workflow.CapClosure, workflow.CapForceRun, workflow.CapDrainReport)
			if got.HasAny(never) {
				t.Fatalf("claims unrouted caps %v", got.Intersect(never))
			}
		})
	}
	if !full.Has(workflow.CapRunFinished) {
		t.Skip("memory lost CapRunFinished")
	}
	if !cutover.New(memory.New(), memory.New()).Capabilities().Has(workflow.CapRunFinished) {
		t.Fatal("CapRunFinished lost with two notifiers")
	}
}

// TestCutover_NewOnlyWorkflow: a workflow the old backend cannot run is
// registered on the new backend only, and the client still starts.
func TestCutover_NewOnlyWorkflow(t *testing.T) {
	old := capsClient{memory.New(), workflow.CapQueue}
	c := cutover.New(old, memory.New())
	must(t, c.Register(workflow.Workflow{Name: "q", Queue: "mail", Steps: []workflow.Step{{Name: "s", Run: func(context.Context, []byte) ([]byte, error) { return nil, nil }}}}))
	must(t, c.Register(workflow.Workflow{Name: "plain", Steps: []workflow.Step{{Name: "s", Run: func(context.Context, []byte) ([]byte, error) { return nil, nil }}}}))
	if !c.NewOnly("q") || c.NewOnly("plain") {
		t.Fatalf("NewOnly q=%v plain=%v", c.NewOnly("q"), c.NewOnly("plain"))
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	must(t, c.Start(ctx))
	if _, err := old.Enqueue(ctx, workflow.Job{Workflow: "q"}); err == nil {
		t.Fatal("the old backend knows the new-only workflow")
	}
}

// TestCutover_Schedules: Schedule goes to the new backend with a "new:"
// ID; MoveSchedules copies the old backend's schedules (with the input the
// app supplies, paused ones paused) and removes them there; the listing
// shows where each lives; refusals.
func TestCutover_Schedules(t *testing.T) {
	ctx := context.Background()
	w := newWorld(t)
	id, err := w.c.Schedule(ctx, workflow.Schedule{Name: "fresh", Workflow: "work", Cron: "0 9 * * *", Location: time.UTC})
	must(t, err)
	if !strings.HasPrefix(string(id), "new:") {
		t.Fatalf("Schedule id = %q", id)
	}
	// Schedules the app made on the old backend before the cutover.
	daily, err := w.old.Schedule(ctx, workflow.Schedule{Name: "daily", Workflow: "work", Cron: "0 8 * * *", Location: time.UTC, Input: []byte(`"d"`)})
	must(t, err)
	paused, err := w.old.Schedule(ctx, workflow.Schedule{Name: "paused", Workflow: "work", Cron: "0 7 * * *", Location: time.UTC})
	must(t, err)
	must(t, w.c.PauseSchedule(ctx, workflow.ScheduleTarget{ID: paused})) // unprefixed: the old backend

	if _, err := w.c.MoveSchedules(ctx, nil); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("MoveSchedules without input: %v", err)
	}
	inputs := map[string][]byte{"daily": []byte(`"d"`)}
	moved, err := w.c.MoveSchedules(ctx, func(info workflow.ScheduleInfo) ([]byte, error) { return inputs[info.Name], nil })
	must(t, err)
	if len(moved) != 2 || moved[0].From != "old:"+daily {
		t.Fatalf("moved = %+v", moved)
	}
	list, err := w.c.Schedules(ctx)
	must(t, err)
	where := map[string]string{}
	pausedNew := false
	for _, s := range list {
		where[s.Name] = strings.SplitN(string(s.ID), ":", 2)[0]
		if s.Name == "paused" {
			pausedNew = s.Paused
		}
	}
	if where["daily"] != "new" || where["paused"] != "new" || where["fresh"] != "new" || len(list) != 3 {
		t.Fatalf("schedules after the move = %v", where)
	}
	if !pausedNew {
		t.Fatal("a paused schedule was moved unpaused")
	}
	if left, _ := w.old.(workflow.ScheduleLister).Schedules(ctx); len(left) != 0 {
		t.Fatalf("old backend still lists %d schedules", len(left))
	}
	must(t, w.c.Unschedule(ctx, moved[0].To))

	keepOld := cutover.New(memory.New(), memory.New(), cutover.SchedulesOn(cutover.SideOld))
	if _, err := keepOld.MoveSchedules(ctx, func(workflow.ScheduleInfo) ([]byte, error) { return nil, nil }); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("MoveSchedules under SchedulesOn(SideOld): %v", err)
	}
}

// TestCutover_RefusesHalfSupport: a run interface one backend lacks is
// refused, never answered from the other alone.
func TestCutover_RefusesHalfSupport(t *testing.T) {
	c := cutover.New(capsClient{memory.New(), workflow.CapRunQuery}, memory.New())
	if _, err := c.Runs(context.Background(), workflow.RunQuery{}); !errors.Is(err, workflow.ErrUnsupported) {
		t.Fatalf("Runs with the old backend unable to list: %v", err)
	}
	if _, err := c.Remaining(context.Background()); !errors.Is(err, workflow.ErrUnsupported) {
		t.Fatalf("Remaining with the old backend unable to list: %v", err)
	}
}
