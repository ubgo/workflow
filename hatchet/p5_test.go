package hatchet

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/hatchet-dev/hatchet/api/v1/server/oas/openapi"
	"github.com/hatchet-dev/hatchet/pkg/client/rest"
	openapi_types "github.com/oapi-codegen/runtime/types"

	"github.com/ubgo/workflow"
)

// TestCapabilitiesP5 pins what the driver claims of batch 5 (each proven
// by p5_live_test.go) and what it refuses (doc.go says why).
func TestCapabilitiesP5(t *testing.T) {
	have := New(Config{}).Capabilities()
	for _, c := range []workflow.Caps{workflow.CapTenant, workflow.CapTickTime, workflow.CapDueQuery, workflow.CapResult, workflow.CapCronSeconds, workflow.CapKeyFIFO} {
		if !have.Has(c) {
			t.Errorf("%v not claimed", c)
		}
	}
	for _, c := range []workflow.Caps{workflow.CapSnooze, workflow.CapNodeIdentity, workflow.CapHeartbeat, workflow.CapRunTimeout, workflow.CapScheduleActive, workflow.CapScheduleKeep} {
		if have.Has(c) {
			t.Errorf("%v claimed", c)
		}
	}
	var c any = New(Config{})
	if _, ok := c.(workflow.StepCanceller); ok {
		t.Error("the driver implements StepCanceller, which Hatchet cannot honour")
	}
	if _, ok := c.(workflow.LifecycleNotifier); ok {
		t.Error("the driver implements LifecycleNotifier")
	}
}

// TestP5Refusals checks every batch-5 field Hatchet cannot honour is
// refused naming the field, at Register, Schedule and Start.
func TestP5Refusals(t *testing.T) {
	run := func(context.Context, []byte) ([]byte, error) { return nil, nil }
	base := func() workflow.Workflow {
		return workflow.Workflow{Name: "w", Steps: []workflow.Step{{Name: "a", Run: run}}}
	}
	cases := map[string]func(*workflow.Workflow){
		"Workflow.CronActive": func(wf *workflow.Workflow) {
			wf.Cron = []string{"* * * * *"}
			wf.CronActive = workflow.Active{From: time.Unix(1, 0)}
		},
		"Workflow.CronKeepRuns": func(wf *workflow.Workflow) {
			wf.Cron = []string{"* * * * *"}
			wf.CronKeepRuns = 3
		},
		"Workflow.RunTimeout":           func(wf *workflow.Workflow) { wf.RunTimeout = time.Minute },
		"Step.HeartbeatTimeout":         func(wf *workflow.Workflow) { wf.Steps[0].HeartbeatTimeout = time.Minute },
		"Cron (five fields, Seconds)":   func(wf *workflow.Workflow) { wf.Cron, wf.CronSeconds = []string{"* * * * *"}, true },
		"Cron (six fields, no Seconds)": func(wf *workflow.Workflow) { wf.Cron = []string{"* * * * * *"} },
	}
	for field, mut := range cases {
		wf := base()
		mut(&wf)
		err := New(Config{}).Register(wf)
		if err == nil {
			t.Errorf("%s: registered", field)
			continue
		}
		if !errors.Is(err, workflow.ErrUnsupported) && !errors.Is(err, workflow.ErrInvalid) {
			t.Errorf("%s: %v, want ErrUnsupported or ErrInvalid", field, err)
		}
		// The driver's own refusal names the field and says why on Hatchet.
		if name, _, _ := strings.Cut(field, " "); errors.Is(err, workflow.ErrUnsupported) && (!strings.Contains(err.Error(), name) || !strings.Contains(err.Error(), "Hatchet")) {
			t.Errorf("%s: %v, want the field and the Hatchet reason", field, err)
		}
	}
	ok := base()
	ok.Cron, ok.CronSeconds = []string{"*/2 * * * * *"}, true
	if err := New(Config{}).Register(ok); err != nil {
		t.Fatalf("a seconds cron was refused: %v", err)
	}

	c := New(Config{EnqueueOnly: true})
	for field, s := range map[string]workflow.Schedule{
		"Schedule.Active":   {Workflow: "w", Cron: "* * * * *", Location: time.UTC, Active: workflow.Active{Until: time.Now().Add(time.Hour)}},
		"Schedule.KeepRuns": {Workflow: "w", Cron: "* * * * *", Location: time.UTC, KeepRuns: 2},
	} {
		if _, err := c.Schedule(context.Background(), s); !errors.Is(err, workflow.ErrUnsupported) || !strings.Contains(err.Error(), field) || !strings.Contains(err.Error(), "Hatchet") {
			t.Errorf("%s: %v, want ErrUnsupported naming the field and the Hatchet reason", field, err)
		}
	}
	err := New(Config{Queues: []workflow.QueueConfig{{Name: "a", Weight: 2}}}).Start(context.Background())
	if !errors.Is(err, workflow.ErrUnsupported) {
		t.Fatalf("QueueConfig.Weight: %v, want ErrUnsupported", err)
	}
}

// TestRunAtOf pins RunInfo.RunAt: Reschedule's time first, then the
// first RunAt of a delayed start, and zero for an immediate run.
func TestRunAtOf(t *testing.T) {
	first, moved := time.UnixMilli(1_700_000_000_000), time.UnixMilli(1_700_000_900_000)
	ms := func(at time.Time) string { return strconv.FormatInt(at.UnixMilli(), 10) }
	if got := runAtOf(map[string]string{metaEnqueuedAt: ms(first)}); !got.IsZero() {
		t.Fatalf("immediate run RunAt = %s, want zero", got)
	}
	delayed := map[string]string{metaEnqueuedAt: ms(first), metaScheduleToken: "t"}
	if got := runAtOf(delayed); !got.Equal(first) {
		t.Fatalf("delayed RunAt = %s, want %s", got, first)
	}
	delayed[metaRunAt] = ms(moved)
	if got := runAtOf(delayed); !got.Equal(moved) {
		t.Fatalf("rescheduled RunAt = %s, want %s", got, moved)
	}
}

// TestScheduleToken pins the v7 token: its time is the creation time, a
// v4 token (made before tokens were time-ordered) has none.
func TestScheduleToken(t *testing.T) {
	before := time.Now().Add(-time.Millisecond)
	at, ok := tokenTime(newScheduleToken())
	if !ok || at.Before(before) || at.After(time.Now().Add(time.Millisecond)) {
		t.Fatalf("v7 token time %s (ok %v), want about now", at, ok)
	}
	if _, ok := tokenTime(uuid.NewString()); ok {
		t.Fatal("a v4 token has a time")
	}
	if _, ok := tokenTime("not-a-uuid"); ok {
		t.Fatal("a non-UUID token has a time")
	}
}

// TestTickLabel pins workflow.LabelTickAt on engine cron runs: read from
// hatchet__cron_scheduled_at, never over a tick the run already carries,
// and absent on a run no cron fired.
func TestTickLabel(t *testing.T) {
	labels := jobLabels(map[string]string{metaCronScheduledAt: "2026-10-14T09:00:02Z", "k": "v"})
	tick, ok := workflow.TickTime(labels)
	if !ok || !tick.Equal(time.Date(2026, 10, 14, 9, 0, 2, 0, time.UTC)) || labels["k"] != "v" {
		t.Fatalf("cron run labels = %v", labels)
	}
	kept := jobLabels(map[string]string{metaCronScheduledAt: "2026-10-14T09:00:02Z", workflow.LabelTickAt: "2026-10-14T08:00:00Z"})
	if kept[workflow.LabelTickAt] != "2026-10-14T08:00:00Z" {
		t.Fatalf("an existing tick was overwritten: %v", kept)
	}
	if _, ok := workflow.TickTime(jobLabels(map[string]string{"k": "v"})); ok {
		t.Fatal("a run no cron fired has a tick")
	}
}

// TestShapeWorkflow pins the result of a run of an unregistered
// workflow: the steps come from the engine's shape, completed tasks only,
// so the sinks are the tasks no completed task lists as a parent.
func TestShapeWorkflow(t *testing.T) {
	id := func() openapi_types.UUID { return uuid.New() }
	root, left, right, onFail := id(), id(), id(), id()
	sRoot, sLeft, sRight, sFail := id(), id(), id(), id()
	d := &rest.V1WorkflowRunDetails{
		Shape: []rest.WorkflowRunShapeItemForWorkflowRunDetails{
			{TaskName: "root", TaskExternalId: root, StepId: sRoot, ChildrenStepIds: []openapi_types.UUID{sLeft, sRight}},
			{TaskName: "left", TaskExternalId: left, StepId: sLeft},
			{TaskName: "right", TaskExternalId: right, StepId: sRight},
			{TaskName: "w-on-failure", TaskExternalId: onFail, StepId: sFail},
		},
		Tasks: []rest.V1TaskSummary{
			{TaskExternalId: root, Status: rest.V1TaskStatusCOMPLETED, Output: openapi.NonNullableJSON(encodeOutput([]byte(`1`)))},
			{TaskExternalId: left, Status: rest.V1TaskStatusCOMPLETED, Output: openapi.NonNullableJSON(encodeOutput([]byte(`"l"`)))},
			{TaskExternalId: right, Status: rest.V1TaskStatusCOMPLETED, Output: openapi.NonNullableJSON(encodeOutput([]byte(`"r"`)))},
			{TaskExternalId: onFail, Status: rest.V1TaskStatusCANCELLED},
		},
	}
	wf := shapeWorkflow("w", d)
	if sinks := workflow.Sinks(wf); !slices.Equal(sinks, []string{"left", "right"}) {
		t.Fatalf("sinks = %v, want left and right", sinks)
	}
	c := New(Config{})
	out, ok := c.runOutput("w", d)
	if !ok || string(out) != `{"left":"l","right":"r"}` {
		t.Fatalf("runOutput = %s (ok %v)", out, ok)
	}
	d.Tasks[2].Status = rest.V1TaskStatusRUNNING
	if _, ok := c.runOutput("w", &rest.V1WorkflowRunDetails{Shape: d.Shape, Tasks: d.Tasks}); !ok {
		// right is not complete, so it is not a step and root's other
		// child is the only sink; the call still succeeds.
		t.Fatal("runOutput of a partial shape failed")
	}
}

// TestMergeWorker pins how engine workers fold into one node.
func TestMergeWorker(t *testing.T) {
	label := func(k, v string) rest.WorkerLabel { return rest.WorkerLabel{Key: k, Value: &v} }
	avail := 1
	t0 := time.Unix(100, 0)
	w := func(queue string, created, beat time.Time) rest.Worker {
		labels := []rest.WorkerLabel{label(workerLabelNode, "n1")}
		if queue != "" {
			labels = append(labels, label(workerLabelQueue, queue))
		}
		return rest.Worker{
			Labels:          &labels,
			SlotConfig:      &map[string]rest.WorkerSlotConfig{"default": {Limit: 3, Available: &avail}},
			Metadata:        rest.APIResourceMeta{CreatedAt: created},
			LastHeartbeatAt: &beat,
		}
	}
	n := workflow.NodeInfo{ID: "n1"}
	mergeWorker(&n, w("b", t0.Add(time.Second), t0.Add(5*time.Second)))
	mergeWorker(&n, w("a", t0, t0.Add(9*time.Second)))
	if !slices.Equal(n.Queues, []string{"b", "a"}) || n.Workers != 6 || n.Running != 4 || !n.StartedAt.Equal(t0) || !n.LastSeen.Equal(t0.Add(9*time.Second)) {
		t.Fatalf("merged node = %+v", n)
	}
	if got := workerLabel(w("", t0, t0), workerLabelQueue); got != "" {
		t.Fatalf("queue label of a worker serving every queue = %q", got)
	}
}

// TestPayloadLimitRefused pins the payload cap: a job over
// Config.PayloadLimit is refused by Enqueue before the client connects,
// with a *workflow.PayloadTooLargeError naming the limit.
func TestPayloadLimitRefused(t *testing.T) {
	c := New(Config{EnqueueOnly: true, PayloadLimit: 8})
	if err := c.Register(workflow.Workflow{Name: "w", Steps: []workflow.Step{{Name: "a", Run: func(context.Context, []byte) ([]byte, error) { return nil, nil }}}}); err != nil {
		t.Fatal(err)
	}
	_, err := c.Enqueue(context.Background(), workflow.Job{Workflow: "w", Payload: []byte("123456789")})
	var tooLarge *workflow.PayloadTooLargeError
	if !errors.As(err, &tooLarge) || tooLarge.Limit != 8 || tooLarge.Size != 9 {
		t.Fatalf("Enqueue over the limit: %v, want a PayloadTooLargeError of 9 over 8", err)
	}
	if got := New(Config{}).PayloadLimit(); got != DefaultPayloadLimit {
		t.Fatalf("default PayloadLimit = %d, want %d", got, DefaultPayloadLimit)
	}
}

// TestReservedLabelsRefused pins the label gate on every public entry
// that takes an app's job: Enqueue, RunGroup and SpawnGroup refuse a
// reserved label before anything connects.
func TestReservedLabelsRefused(t *testing.T) {
	c := New(Config{EnqueueOnly: true, BatchCoordinator: true})
	if err := c.Register(workflow.Workflow{Name: "w", Steps: []workflow.Step{{Name: "a", Run: func(context.Context, []byte) ([]byte, error) { return nil, nil }}}}); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	job := workflow.Job{Workflow: "w", Labels: map[string]string{workflow.LabelTickAt: "x"}}
	if _, err := c.Enqueue(ctx, job); !errors.Is(err, workflow.ErrInvalid) {
		t.Errorf("Enqueue: %v, want ErrInvalid", err)
	}
	if _, err := c.RunGroup(ctx, "g", []workflow.Job{job}); !errors.Is(err, workflow.ErrInvalid) {
		t.Errorf("RunGroup: %v, want ErrInvalid", err)
	}
	if _, err := c.SpawnGroup(ctx, "g", []workflow.Job{job}); !errors.Is(err, workflow.ErrInvalid) {
		t.Errorf("SpawnGroup: %v, want ErrInvalid", err)
	}
}

// TestSameCronTenant pins that a named schedule whose Tenant changed is
// replaced, not kept: the tenant is part of the trigger's definition.
func TestSameCronTenant(t *testing.T) {
	md := map[string]any{metaNamed: "1", metaTenant: "acme"}
	row := rest.CronWorkflows{Cron: "* * * * *", AdditionalMetadata: &md}
	s := workflow.Schedule{Workflow: "w", Name: "n", Cron: "* * * * *", Tenant: "acme"}
	if !sameCron(row, s) {
		t.Fatal("an unchanged schedule reads as changed")
	}
	s.Tenant = "globex"
	if sameCron(row, s) {
		t.Fatal("a schedule whose tenant changed reads as unchanged")
	}
}
