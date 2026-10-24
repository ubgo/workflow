package hatchet

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"testing"

	"github.com/ubgo/workflow"
)

// TestPauseRequestTTL pins the pause request the driver sends. The engine
// cancels a paused workflow's queued runs once the TTL passes, and turns
// an empty or unparsable TTL into five minutes, so the value must stay a
// long one in a form the engine parses exactly ("<n>y"), and cron and
// scheduled runs that come due must be queued, not dropped.
func TestPauseRequestTTL(t *testing.T) {
	if pauseQueueTTL != "100y" {
		t.Fatalf("pauseQueueTTL = %q, want 100y", pauseQueueTTL)
	}
	req, err := pauseRequest(pauseQueueTTL)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	var got struct {
		Pause map[string]string `json:"pause"`
	}
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	want := map[string]string{
		"action":                                  "pause",
		"pausedWorkflowQueueTTL":                  "100y",
		"pausedWorkflowCronRunQueueBehavior":      "QUEUE",
		"pausedWorkflowScheduledRunQueueBehavior": "QUEUE",
	}
	for k, v := range want {
		if got.Pause[k] != v {
			t.Errorf("%s = %q, want %q (body %s)", k, got.Pause[k], v, raw)
		}
	}
	un, err := unpauseRequest()
	if err != nil {
		t.Fatal(err)
	}
	if raw, _ := json.Marshal(un); string(raw) != `{"pause":{"action":"unpause"}}` {
		t.Fatalf("unpause body = %s", raw)
	}
}

// testToken is an unsigned JWT with the claims NewAPI reads.
func testToken(claims string) string {
	enc := base64.RawURLEncoding.EncodeToString
	return enc([]byte(`{"alg":"none"}`)) + "." + enc([]byte(claims)) + ".sig"
}

// TestNewAPI pins how NewAPI reads the token: tenant from sub, address
// from server_url unless given, and refusals for a token it cannot use.
func TestNewAPI(t *testing.T) {
	tenant := "707d0855-80ab-4e1f-a156-f1c4546cbf52"
	api, err := NewAPI(testToken(`{"sub":"`+tenant+`","server_url":"http://localhost:7003"}`), "")
	if err != nil || api.TenantID != tenant || api.Client == nil {
		t.Fatalf("NewAPI = %+v, %v", api, err)
	}
	if _, err := NewAPI(testToken(`{"sub":"`+tenant+`"}`), "http://engine:8080"); err != nil {
		t.Fatalf("explicit server URL: %v", err)
	}
	for name, tok := range map[string]string{
		"not a JWT":     "abc",
		"no server URL": testToken(`{"sub":"` + tenant + `"}`),
		"no tenant":     testToken(`{"server_url":"http://x"}`),
	} {
		if _, err := NewAPI(tok, ""); !errors.Is(err, workflow.ErrInvalid) {
			t.Errorf("%s: %v", name, err)
		}
	}
}

// TestPauseNeedsAPI pins that pause is declared and offered only with
// Config.API, and that the targets the engine cannot hold are refused.
func TestPauseNeedsAPI(t *testing.T) {
	ctx := context.Background()
	plain := New(Config{})
	if plain.Capabilities().Has(workflow.CapPauseWorkflow) || plain.Capabilities().Has(workflow.CapSchedulePause) {
		t.Fatal("pause declared without Config.API")
	}
	if err := plain.Pause(ctx, workflow.PauseWorkflow("wf")); !errors.Is(err, workflow.ErrUnsupported) {
		t.Fatalf("Pause without API: %v", err)
	}
	if err := plain.PauseSchedule(ctx, workflow.ScheduleTarget{ID: "x"}); !errors.Is(err, workflow.ErrUnsupported) {
		t.Fatalf("PauseSchedule without API: %v", err)
	}
	withAPI := New(Config{API: &API{}})
	if !withAPI.Capabilities().Has(workflow.CapPauseWorkflow | workflow.CapSchedulePause) {
		t.Fatal("pause not declared with Config.API")
	}
	if withAPI.Capabilities().Has(workflow.CapPauseQueue) {
		t.Fatal("CapPauseQueue declared")
	}
	for name, err := range map[string]error{
		"PauseQueue":         withAPI.Pause(ctx, workflow.PauseQueue("mail")),
		"PauseAll":           withAPI.Pause(ctx, workflow.PauseAll()),
		"PauseSchedule(All)": withAPI.PauseSchedule(ctx, workflow.ScheduleTarget{All: true}),
	} {
		if !errors.Is(err, workflow.ErrUnsupported) {
			t.Errorf("%s: %v", name, err)
		}
	}
	if err := withAPI.Pause(ctx, workflow.PauseWorkflow("wf")); !errors.Is(err, workflow.ErrNotStarted) {
		t.Fatalf("Pause before Start: %v", err)
	}
}
