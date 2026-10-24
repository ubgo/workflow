package hatchet

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/hatchet-dev/hatchet/pkg/client/rest"
	hatchetsdk "github.com/hatchet-dev/hatchet/sdks/go"
	openapi_types "github.com/oapi-codegen/runtime/types"

	"github.com/ubgo/workflow"
)

// TestCapabilitiesP4 pins the schedule, enqueue-only and run-control
// capabilities: claimed ones are proven by p4_live_test.go, refused ones
// have no engine support (doc.go says why for each).
func TestCapabilitiesP4(t *testing.T) {
	have := New(Config{}).Capabilities()
	for _, c := range []workflow.Caps{workflow.CapScheduleTrigger, workflow.CapScheduleHistory, workflow.CapEnqueueOnly} {
		if !have.Has(c) {
			t.Errorf("%v not claimed", c)
		}
	}
	for _, c := range []workflow.Caps{workflow.CapOverlapSkip, workflow.CapErrorHistory, workflow.CapShutdownRequeue, workflow.CapForceRun} {
		if have.Has(c) {
			t.Errorf("%v claimed, but the driver cannot honour it", c)
		}
	}
	if _, ok := any(New(Config{})).(workflow.RunForcer); ok {
		t.Error("Client implements RunForcer without CapForceRun")
	}
}

// TestOverlapSkipRefused: OverlapSkip is refused by name at Register and
// at Schedule, before anything connects; OverlapAllow passes.
func TestOverlapSkipRefused(t *testing.T) {
	step := workflow.Step{Name: "a", Run: func(_ context.Context, in []byte) ([]byte, error) { return in, nil }}
	c := New(Config{})
	err := c.Register(workflow.Workflow{Name: "wf", Cron: []string{"* * * * *"}, CronOverlap: workflow.OverlapSkip, Steps: []workflow.Step{step}})
	if !errors.Is(err, workflow.ErrUnsupported) || !containsField(err, "Workflow.CronOverlap") {
		t.Fatalf("Register: %v, want ErrUnsupported naming Workflow.CronOverlap", err)
	}
	if err := c.Register(workflow.Workflow{Name: "ok", Cron: []string{"* * * * *"}, CronOverlap: workflow.OverlapAllow, Steps: []workflow.Step{step}}); err != nil {
		t.Fatalf("Register allow: %v", err)
	}
	_, err = c.Schedule(context.Background(), workflow.Schedule{Workflow: "ok", Cron: "* * * * *", Location: time.UTC, Overlap: workflow.OverlapSkip})
	if !errors.Is(err, workflow.ErrUnsupported) || !containsField(err, "Schedule.Overlap") {
		t.Fatalf("Schedule: %v, want ErrUnsupported naming Schedule.Overlap", err)
	}
}

func containsField(err error, field string) bool {
	var u *workflow.UnsupportedError
	return errors.As(err, &u) && u.Field == field
}

// TestEnqueueOnlyMode: Start starts nothing and connects nothing,
// OnRunFinished is refused, calls work without Start (here the lazy
// connect fails for want of a token, proving it is attempted), and once
// the Start ctx ends Done closes and calls return ErrNotStarted.
func TestEnqueueOnlyMode(t *testing.T) {
	t.Setenv("HATCHET_CLIENT_TOKEN", "")
	c := New(Config{EnqueueOnly: true})
	if err := c.OnRunFinished(func(context.Context, workflow.RunEvent) {}); !errors.Is(err, workflow.ErrUnsupported) {
		t.Fatalf("OnRunFinished: %v, want ErrUnsupported", err)
	}
	_, err := c.Enqueue(context.Background(), workflow.Job{Workflow: "never-registered"})
	if err == nil || errors.Is(err, workflow.ErrNotStarted) {
		t.Fatalf("Enqueue before Start: %v, want a connect error", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	if err := c.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if c.Native() != nil {
		t.Fatal("Start connected an enqueue-only client")
	}
	cancel()
	select {
	case <-c.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("Done did not close after the Start ctx ended")
	}
	if _, err := c.Enqueue(context.Background(), workflow.Job{Workflow: "x"}); !errors.Is(err, workflow.ErrNotStarted) {
		t.Fatalf("Enqueue after stop: %v, want ErrNotStarted", err)
	}
}

// TestEnqueueOnlyLookup: a registered workflow keeps its spec, an
// unregistered one gets a spec with only its name; a worker client still
// refuses lookups before Start.
func TestEnqueueOnlyLookup(t *testing.T) {
	sdkClient := offlineSDK(t)
	c := New(Config{EnqueueOnly: true, Client: sdkClient})
	step := workflow.Step{Name: "a", Run: func(_ context.Context, in []byte) ([]byte, error) { return in, nil }}
	if err := c.Register(workflow.Workflow{Name: "reg", Queue: "q", Idempotency: workflow.IdempotencyRequired, Steps: []workflow.Step{step}}); err != nil {
		t.Fatal(err)
	}
	hw, spec, err := c.lookup("reg")
	_, other, err2 := c.lookup("other")
	if err != nil || err2 != nil || hw == nil {
		t.Fatalf("lookup: %v, %v", err, err2)
	}
	if spec.Idempotency != workflow.IdempotencyRequired || other.Name != "other" || other.Queue != "" {
		t.Fatalf("specs: %+v / %+v", spec, other)
	}
	if !c.isRegistered("reg") || c.isRegistered("other") {
		t.Fatal("isRegistered")
	}
	w := New(Config{Client: sdkClient})
	if _, _, err := w.lookup("reg"); !errors.Is(err, workflow.ErrNotStarted) {
		t.Fatalf("worker lookup before Start: %v", err)
	}
}

// TestCronName: a trigger Schedule made is listed by its name, a
// declared one by its expression.
func TestCronName(t *testing.T) {
	name := "nightly"
	if got := cronName(rest.CronWorkflows{Cron: "0 2 * * *", Method: rest.CronWorkflowsMethodAPI, Name: &name}); got != name {
		t.Errorf("api trigger: %q", got)
	}
	if got := cronName(rest.CronWorkflows{Cron: "0 2 * * *", Method: rest.CronWorkflowsMethodDEFAULT}); got != "0 2 * * *" {
		t.Errorf("declared trigger: %q", got)
	}
}

// TestFireTime: a cron fire's tick time wins, then the enqueue stamp,
// then the run's creation.
func TestFireTime(t *testing.T) {
	created := time.Date(2026, 10, 13, 9, 0, 5, 0, time.UTC)
	tick := time.Date(2026, 10, 13, 9, 0, 0, 0, time.UTC)
	if got := fireTime(map[string]string{metaCronScheduledAt: tick.Format(time.RFC3339)}, created); !got.Equal(tick) {
		t.Errorf("tick: %v", got)
	}
	enq := time.UnixMilli(1760346000123)
	if got := fireTime(map[string]string{metaEnqueuedAt: "1760346000123"}, created); !got.Equal(enq) {
		t.Errorf("enqueued: %v", got)
	}
	if got := fireTime(map[string]string{}, created); !got.Equal(created) {
		t.Errorf("fallback: %v", got)
	}
}

// TestFailedStep: the first FAILED step task names the failed step; the
// on-failure task of a registered workflow does not count.
func TestFailedStep(t *testing.T) {
	c := New(Config{})
	step := workflow.Step{Name: "b", Run: func(_ context.Context, in []byte) ([]byte, error) { return in, nil }}
	if err := c.Register(workflow.Workflow{Name: "wf", Steps: []workflow.Step{{Name: "a", Run: step.Run}, step}}); err != nil {
		t.Fatal(err)
	}
	ids := []openapi_types.UUID{openapi_types.UUID(uuid.New()), openapi_types.UUID(uuid.New()), openapi_types.UUID(uuid.New())}
	d := &rest.V1WorkflowRunDetails{
		Shape: rest.WorkflowRunShapeForWorkflowRunDetails{
			{TaskExternalId: ids[0], TaskName: "on-failure"},
			{TaskExternalId: ids[1], TaskName: "a"},
			{TaskExternalId: ids[2], TaskName: "b"},
		},
		Tasks: []rest.V1TaskSummary{
			{TaskExternalId: ids[0], Status: rest.V1TaskStatusFAILED},
			{TaskExternalId: ids[1], Status: rest.V1TaskStatusCOMPLETED},
			{TaskExternalId: ids[2], Status: rest.V1TaskStatusFAILED},
		},
	}
	if got := c.failedStep("wf", d); got != "b" {
		t.Errorf("registered: %q, want b", got)
	}
	if got := c.failedStep("unknown", d); got != "on-failure" {
		t.Errorf("unregistered: %q, want the first failed task", got)
	}
}

// offlineSDK builds an SDK client from a made-up token pointing at a
// closed port: building it and declaring workflows does not dial.
func offlineSDK(t *testing.T) *hatchetsdk.Client {
	t.Helper()
	t.Setenv("HATCHET_CLIENT_TLS_STRATEGY", "none")
	claims, err := json.Marshal(map[string]any{
		"sub":                    uuid.NewString(),
		"server_url":             "http://127.0.0.1:1",
		"grpc_broadcast_address": "127.0.0.1:1",
		"exp":                    time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC).Unix(),
	})
	if err != nil {
		t.Fatal(err)
	}
	enc := base64.RawURLEncoding.EncodeToString
	token := enc([]byte(`{"alg":"none"}`)) + "." + enc(claims) + "." + enc([]byte("sig"))
	sdk, err := newSDK([]hatchetsdk.ClientOpt{hatchetsdk.WithToken(token)})
	if err != nil {
		t.Fatal(err)
	}
	return sdk
}

// TestNamedScheduleID: a named schedule's ID round-trips to its workflow
// and name, does not parse as anything else, and is what a trigger made
// for a named Schedule is listed under; an unmarked or declared trigger
// keeps the engine's ID.
func TestNamedScheduleID(t *testing.T) {
	id := namedScheduleID("wf", "nightly report")
	if wf, name, ok := parseNamedID(id); !ok || wf != "wf" || name != "nightly report" {
		t.Fatalf("parse %q = %q, %q, %v", id, wf, name, ok)
	}
	for _, bad := range []workflow.ScheduleID{"", workflow.ScheduleID(uuid.NewString()), "named:!!", namedIDPrefix + "d2Y"} {
		if _, _, ok := parseNamedID(bad); ok {
			t.Errorf("%q parsed as a named ID", bad)
		}
	}
	c := New(Config{Namespace: "ns"})
	name := "nightly report"
	marked := labelsAny(map[string]string{metaNamed: "1"})
	row := rest.CronWorkflows{Metadata: rest.APIResourceMeta{Id: uuid.NewString()}, Method: rest.CronWorkflowsMethodAPI, Name: &name, WorkflowName: engineName("ns", "wf"), AdditionalMetadata: &marked}
	if got := c.scheduleIDOf(row); got != id {
		t.Errorf("marked trigger listed as %q, want %q", got, id)
	}
	row.AdditionalMetadata = nil
	if got := c.scheduleIDOf(row); got != workflow.ScheduleID(row.Metadata.Id) {
		t.Errorf("unmarked trigger listed as %q", got)
	}
	row.AdditionalMetadata, row.Method = &marked, rest.CronWorkflowsMethodDEFAULT
	if got := c.scheduleIDOf(row); got != workflow.ScheduleID(row.Metadata.Id) {
		t.Errorf("declared trigger listed as %q", got)
	}
}

// TestScopedRateLimitExpr: the engine's key expression prefixes the
// caller's value with the declaration's static key, so equal values of
// two workflows or two limits never share a bucket.
func TestScopedRateLimitExpr(t *testing.T) {
	rl, err := rateLimit(workflow.RateLimit{Key: workflow.KeyExpr(`input.tenant`), Limit: 2, Per: time.Minute}, "ns_wf", 1)
	if err != nil {
		t.Fatal(err)
	}
	if rl.Key != "wf|ns_wf|1" || *rl.KeyExpr != `"wf|ns_wf|1|" + string(input.tenant)` {
		t.Fatalf("Key %q KeyExpr %q", rl.Key, *rl.KeyExpr)
	}
	other, err := rateLimit(workflow.RateLimit{Key: workflow.KeyExpr(`input.tenant`), Limit: 2, Per: time.Minute}, "ns_other", 1)
	if err != nil {
		t.Fatal(err)
	}
	if *other.KeyExpr == *rl.KeyExpr {
		t.Fatal("two workflows share a key expression")
	}
}

// TestScheduleTimeoutDefault pins that every step waits up to a day for
// a worker, not the engine's five minutes, unless Config says otherwise.
func TestScheduleTimeoutDefault(t *testing.T) {
	if got := New(Config{}).cfg.ScheduleTimeout; got != DefaultScheduleTimeout {
		t.Fatalf("default ScheduleTimeout %s", got)
	}
	if got := New(Config{ScheduleTimeout: time.Hour}).cfg.ScheduleTimeout; got != time.Hour {
		t.Fatalf("set ScheduleTimeout %s", got)
	}
}
