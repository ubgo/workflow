package hatchet

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/hatchet-dev/hatchet/pkg/client/rest"
	hatchetsdk "github.com/hatchet-dev/hatchet/sdks/go"

	"github.com/ubgo/workflow"
)

// step is a pass-through run step named name, after the given parents.
func step(name string, after ...string) workflow.Step {
	return workflow.Step{Name: name, After: after, Run: func(_ context.Context, in []byte) ([]byte, error) { return in, nil }}
}

func TestEncodeInputRoundTrip(t *testing.T) {
	for _, payload := range [][]byte{nil, []byte(`"plain string"`), []byte("not json"), []byte(`{"tenant":"t1","n":12345678901234567890}`), []byte(`[1,2]`)} {
		in, err := encodeInput(payload)
		if err != nil {
			t.Fatalf("encodeInput(%q): %v", payload, err)
		}
		got, err := decodeEnvelope(map[string]any(in), payloadField)
		if err != nil || !bytes.Equal(got, payload) {
			t.Fatalf("round trip of %q = %q, %v", payload, got, err)
		}
	}
	in, _ := encodeInput([]byte(`{"tenant":"t1"}`))
	if _, ok := in["tenant"]; !ok {
		t.Fatal("a JSON object payload's fields are not visible to CEL as input.<field>")
	}
	if _, err := encodeInput([]byte(`{"_wf_payload":"x"}`)); !errors.Is(err, workflow.ErrUnsupported) {
		t.Fatalf("a payload using the reserved key: err = %v, want ErrUnsupported", err)
	}
}

func TestOutputRoundTrip(t *testing.T) {
	out := []byte(`{"a":1}`)
	got, err := decodeEnvelope(map[string]any(encodeOutput(out)), outputField)
	if err != nil || !bytes.Equal(got, out) {
		t.Fatalf("output round trip = %q, %v", got, err)
	}
	if got, _ := decodeEnvelope(map[string]any(encodeOutput(nil)), outputField); got != nil {
		t.Fatalf("nil output decoded to %q", got)
	}
}

func TestRetryBackoff(t *testing.T) {
	if _, ok, err := retryBackoff(workflow.Backoff{}); ok || err != nil {
		t.Fatalf("zero backoff: ok=%v err=%v, want no backoff", ok, err)
	}
	b, ok, err := retryBackoff(workflow.Backoff{Initial: 2 * time.Second, Factor: 2, Max: 30 * time.Second})
	if err != nil || !ok || b.factor != 2 || b.maxSeconds != 30 {
		t.Fatalf("2s x2 cap 30s = %+v ok=%v err=%v", b, ok, err)
	}
	b, _, _ = retryBackoff(workflow.Backoff{Initial: time.Second})
	if b.factor != 1 || b.maxSeconds != hatchetMaxBackoffSeconds {
		t.Fatalf("constant 1s = %+v, want factor 1 and the engine cap", b)
	}
	for _, bad := range []workflow.Backoff{
		{Initial: 3 * time.Second, Factor: 2},                               // Initial must equal Factor seconds
		{Initial: 2 * time.Second, Factor: 2, Max: 1500 * time.Millisecond}, // whole seconds only
		{Initial: 2 * time.Second, Factor: 2, Max: 48 * time.Hour},          // above 86400s
	} {
		if _, _, err := retryBackoff(bad); !errors.Is(err, workflow.ErrUnsupported) {
			t.Fatalf("retryBackoff(%+v): err = %v, want ErrUnsupported", bad, err)
		}
	}
}

func TestConcurrencyStrategy(t *testing.T) {
	want := map[workflow.ConcurrencyStrategy]hatchetsdk.ConcurrencyLimitStrategy{
		"":                                   hatchetsdk.GroupRoundRobin,
		workflow.ConcurrencyQueueNewest:      hatchetsdk.GroupRoundRobin,
		workflow.ConcurrencyRoundRobin:       hatchetsdk.GroupRoundRobin,
		workflow.ConcurrencyDropNewest:       hatchetsdk.CancelNewest,
		workflow.ConcurrencyCancelNewest:     hatchetsdk.CancelNewest,
		workflow.ConcurrencyCancelInProgress: hatchetsdk.CancelInProgress,
	}
	for in, w := range want {
		if got, err := concurrencyStrategy(in); err != nil || got != w {
			t.Errorf("concurrencyStrategy(%q) = %q, %v; want %q", in, got, err, w)
		}
	}
	if _, err := concurrencyStrategy("bogus"); !errors.Is(err, workflow.ErrUnsupported) {
		t.Fatalf("unknown strategy: err = %v", err)
	}
	if got := concurrencyExpression(workflow.ConcurrencyKey{}); got != `"all"` {
		t.Fatalf("zero key expression = %s, want one constant bucket", got)
	}
}

func TestCheckSupported(t *testing.T) {
	base := func() workflow.Workflow {
		return workflow.Workflow{Name: "w", Steps: []workflow.Step{step("a")}}
	}
	cases := []struct {
		field string
		edit  func(*workflow.Workflow)
	}{
		{"Workflow.Concurrency.Key.Func", func(w *workflow.Workflow) {
			w.Concurrency = &workflow.Concurrency{Key: workflow.KeyFunc(func(workflow.Job) string { return "" }), MaxRuns: 1}
		}},
		{"WaitSpec.KeyFrom.Expr", func(w *workflow.Workflow) {
			w.Steps = append(w.Steps, workflow.Step{Name: "wait", After: []string{"a"}, Wait: &workflow.WaitSpec{Event: "e", KeyFrom: workflow.KeyExpr("input.x")}})
		}},
		{"Cron", func(w *workflow.Workflow) { w.Cron = []string{"@every 5m"} }},
		{"Workflow.RateLimits[0].Per", func(w *workflow.Workflow) {
			w.RateLimits = []workflow.RateLimit{{Key: workflow.KeyExpr("input.t"), Limit: 1, Per: 90 * time.Second}}
		}},
		{"Workflow.RateLimits[0].Key", func(w *workflow.Workflow) {
			w.RateLimits = []workflow.RateLimit{{Limit: 1, Per: time.Minute}}
		}},
		{"Workflow.RateLimits", func(w *workflow.Workflow) {
			w.Steps = append(w.Steps, step("b"))
			w.RateLimits = []workflow.RateLimit{{Key: workflow.KeyExpr("input.t"), Limit: 1, Per: time.Minute}}
		}},
		{"Retry.Backoff.Initial", func(w *workflow.Workflow) {
			w.Retry = workflow.RetryPolicy{Max: 2, Backoff: workflow.Backoff{Initial: 5 * time.Second, Factor: 2}}
		}},
	}
	for _, tc := range cases {
		wf := base()
		tc.edit(&wf)
		err := checkSupported(wf)
		var ue *workflow.UnsupportedError
		if !errors.As(err, &ue) || ue.Field != tc.field {
			t.Errorf("%s: err = %v, want UnsupportedError naming it", tc.field, err)
		}
	}
	ok := base()
	ok.Cron = []string{"*/5 * * * *"}
	ok.Concurrency = &workflow.Concurrency{Key: workflow.KeyExpr("input.tenant"), MaxRuns: 2}
	ok.Steps = append(ok.Steps, workflow.Step{Name: "wait", After: []string{"a"}, Wait: &workflow.WaitSpec{Event: "e", KeyFrom: workflow.KeyFunc(func(workflow.Job) string { return "" }), Deadline: time.Unix(1, 0)}})
	if err := checkSupported(ok); err != nil {
		t.Fatalf("a supported workflow was refused: %v", err)
	}
	if err := checkCron("not a cron", false); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("bad cron: err = %v, want ErrInvalid", err)
	}
}

func TestTopoOrder(t *testing.T) {
	steps := []workflow.Step{step("c", "a", "b"), step("b", "a"), step("a")}
	var got []string
	for _, s := range topoOrder(steps) {
		got = append(got, s.Name)
	}
	if strings.Join(got, ",") != "a,b,c" {
		t.Fatalf("topoOrder = %v, want a,b,c", got)
	}
}

func TestRunPriority(t *testing.T) {
	if p, err := runPriority(0); p != nil || err != nil {
		t.Fatalf("zero priority = %v, %v; want the engine default", p, err)
	}
	if p, _ := runPriority(1); *p != priorityMedium {
		t.Fatalf("priority 1 = %v", *p)
	}
	if p, _ := runPriority(7); *p != priorityHigh {
		t.Fatalf("priority 7 = %v", *p)
	}
	if _, err := runPriority(-1); !errors.Is(err, workflow.ErrUnsupported) {
		t.Fatalf("negative priority: err = %v", err)
	}
}

func TestIdempotencyKeys(t *testing.T) {
	fresh := func() string { return "fresh" }
	if got := runIdempotencyKey("ns_w", "k1", workflow.IdempotencyOptional, fresh); got != "ns_w|key|k1" {
		t.Fatalf("caller key = %q", got)
	}
	if got := runIdempotencyKey("w", "k1", workflow.IdempotencyOff, fresh); got != "w|run|fresh" {
		t.Fatalf("key with policy off = %q, want a fresh per-run key", got)
	}
	expr := idempotencyExpression("w")
	for _, part := range []string{`"wf__idempotency_key" in additional_metadata`, `"hatchet__cron_scheduled_at"`, `"hatchet__event_id"`, `"w|untracked"`} {
		if !strings.Contains(expr, part) {
			t.Fatalf("idempotency expression lacks %s: %s", part, expr)
		}
	}
	if engineName("Prod", "w") != "prod_w" || engineName("", "w") != "w" {
		t.Fatal("engineName does not match the SDK's namespace prefix")
	}
}

func TestRunMetadataAndLabels(t *testing.T) {
	md, err := runMetadata(map[string]string{"tenant": "t1"}, "key")
	if err != nil || md["tenant"] != "t1" || md[metaIdempotencyKey] != "key" {
		t.Fatalf("runMetadata = %v, %v", md, err)
	}
	md[traceparentKey] = "00-x"
	md["hatchet__event_id"] = "e"
	if got := jobLabels(md); len(got) != 1 || got["tenant"] != "t1" {
		t.Fatalf("jobLabels = %v, want only the app's labels", got)
	}
	for _, bad := range []string{"wf__x", "hatchet__x", "traceparent"} {
		if _, err := runMetadata(map[string]string{bad: "v"}, "k"); !errors.Is(err, workflow.ErrUnsupported) {
			t.Fatalf("label %q: err = %v, want ErrUnsupported", bad, err)
		}
	}
}

func TestScheduledIDRoundTrip(t *testing.T) {
	at := time.UnixMilli(1791632641695)
	ref := scheduledRef{token: "t-uuid", runAt: at}
	got, ok := parseScheduledID(ref.id())
	if !ok || got.token != ref.token || !got.runAt.Equal(at) {
		t.Fatalf("parseScheduledID(%q) = %+v, %v", ref.id(), got, ok)
	}
	md := map[string]string{metaScheduleToken: "t-uuid", metaEnqueuedAt: "1791632641695"}
	if got := scheduledIDFromMetadata(md); got != ref.id() {
		t.Fatalf("ScheduledID rebuilt from a fired run's metadata = %q, want %q", got, ref.id())
	}
	if got := scheduledIDFromMetadata(map[string]string{metaEnqueuedAt: "1"}); got != "" {
		t.Fatalf("a run started now got ScheduledID %q", got)
	}
	for _, bad := range []workflow.ScheduledID{"", "plain-uuid", "a~notanumber", "~1", "a~b~1"} {
		if _, ok := parseScheduledID(bad); ok {
			t.Fatalf("parseScheduledID(%q) accepted a foreign ID", bad)
		}
	}
}

func TestSuspendedOn(t *testing.T) {
	wf := workflow.Workflow{Name: "w", Steps: []workflow.Step{
		step("a"),
		{Name: "wait", After: []string{"a"}, Wait: &workflow.WaitSpec{Event: "e"}},
		{Name: "nap", After: []string{"a"}, Sleep: time.Minute},
	}}
	cases := []struct {
		active    []string
		unstarted []string
		want      bool
	}{
		{[]string{"a"}, []string{"wait", "nap"}, false}, // a step is running
		{[]string{"wait"}, nil, true},                   // the durable wait holds the run
		{nil, []string{"nap"}, true},                    // held back by a sleep condition
		{nil, nil, false},                               // nothing to wait on
	}
	for _, tc := range cases {
		un := map[string]bool{}
		for _, n := range tc.unstarted {
			un[n] = true
		}
		if got := suspendedOn(wf, tc.active, un); got != tc.want {
			t.Errorf("suspendedOn(active=%v, unstarted=%v) = %v, want %v", tc.active, tc.unstarted, got, tc.want)
		}
	}
}

func TestRunStatus(t *testing.T) {
	want := map[rest.V1TaskStatus]workflow.Status{
		rest.V1TaskStatusQUEUED:    workflow.StatusQueued,
		rest.V1TaskStatusRUNNING:   workflow.StatusRunning,
		rest.V1TaskStatusCOMPLETED: workflow.StatusSucceeded,
		rest.V1TaskStatusFAILED:    workflow.StatusFailed,
		rest.V1TaskStatusCANCELLED: workflow.StatusCancelled,
	}
	for in, w := range want {
		if got, err := runStatus(in); err != nil || got != w {
			t.Errorf("runStatus(%s) = %q, %v", in, got, err)
		}
	}
	if _, err := runStatus("PAUSED"); err == nil {
		t.Fatal("an unknown engine status was mapped instead of refused")
	}
}

func TestEventKeyAndTrigger(t *testing.T) {
	if eventKey("a:b", "c") == eventKey("a", "b:c") {
		t.Fatal("eventKey is ambiguous")
	}
	if k, ok := eventTrigger([]string{"sleep:5s", "wf:1:e:k"}); !ok || k != "wf:1:e:k" {
		t.Fatalf("eventTrigger picked %q, %v", k, ok)
	}
	if _, ok := eventTrigger([]string{"sleep:5s"}); ok {
		t.Fatal("a timeout was read as an event")
	}
}

func TestEngineVersion(t *testing.T) {
	for v, want := range map[string]bool{"v0.110.5": true, "v0.98.9": true, "v0.98.8": false, "1.0.0-rc1": true} {
		if got, err := engineVersionAtLeast(v, minIdempotencyEngineVersion); err != nil || got != want {
			t.Errorf("engineVersionAtLeast(%q) = %v, %v; want %v", v, got, err, want)
		}
	}
	if _, err := engineVersionAtLeast("dev", minIdempotencyEngineVersion); err == nil {
		t.Fatal("an unreadable engine version was trusted")
	}
}

func TestNonRetryable(t *testing.T) {
	boom := errors.New("boom")
	p := workflow.RetryPolicy{Max: 3, NoRetryOn: func(err error) bool { return errors.Is(err, boom) }}
	if !nonRetryable(workflow.Permanent(errors.New("x")), workflow.RetryPolicy{}) || !nonRetryable(boom, p) || nonRetryable(errors.New("other"), p) {
		t.Fatal("nonRetryable disagrees with Permanent / NoRetryOn")
	}
}

func TestNewOpensNothing(t *testing.T) {
	c := New(Config{})
	if c.Native() != nil {
		t.Fatal("New built an SDK client before Start")
	}
	if _, err := c.Enqueue(t.Context(), workflow.Job{Workflow: "w"}); !errors.Is(err, workflow.ErrNotStarted) {
		t.Fatalf("Enqueue before Start: err = %v", err)
	}
	if err := c.Unschedule(t.Context(), ""); err != nil {
		t.Fatalf("Unschedule(\"\") = %v, want a no-op", err)
	}
}

func TestDelayedKeyAndZoneRefusals(t *testing.T) {
	at := time.Unix(1, 0)
	var ue *workflow.UnsupportedError
	if err := checkDelayedKey("k", at); !errors.As(err, &ue) || ue.Field != "Job.IdempotencyKey" {
		t.Fatalf("key with RunAt: err = %v, want UnsupportedError naming Job.IdempotencyKey", err)
	}
	if checkDelayedKey("k", time.Time{}) != nil || checkDelayedKey("", at) != nil {
		t.Fatal("a key alone or a RunAt alone was refused")
	}
	kolkata := time.FixedZone("Asia/Kolkata", 5*3600+1800)
	if err := checkScheduleZone(kolkata); !errors.As(err, &ue) || ue.Field != "Schedule.Location" {
		t.Fatalf("non-UTC zone: err = %v, want UnsupportedError naming Schedule.Location", err)
	}
	if err := checkScheduleZone(time.UTC); err != nil {
		t.Fatalf("UTC refused: %v", err)
	}
}

func TestRunError(t *testing.T) {
	msg := "boom"
	if err := runError(workflow.StatusCancelled, nil); !errors.Is(err, workflow.ErrCancelled) {
		t.Fatalf("cancelled: %v", err)
	}
	if err := runError(workflow.StatusFailed, &msg); err == nil || err.Error() != "boom" {
		t.Fatalf("failed with a message: %v", err)
	}
	if err := runError(workflow.StatusFailed, nil); err == nil {
		t.Fatal("a failed run without a message got a nil Err")
	}
	if err := runError(workflow.StatusSucceeded, &msg); err != nil {
		t.Fatalf("succeeded: %v", err)
	}
}

func TestMetadataStrings(t *testing.T) {
	md := map[string]any{"tenant": "t1", "n": 3.0, metaWorkflow: "w"}
	got := metadataStrings(&md)
	if len(got) != 2 || got["tenant"] != "t1" || metadataString(&md, metaWorkflow) != "w" {
		t.Fatalf("metadataStrings = %v", got)
	}
	if len(metadataStrings(nil)) != 0 || metadataString(nil, "x") != "" {
		t.Fatal("nil metadata")
	}
}

func TestOnRunFinishedBeforeStartOnly(t *testing.T) {
	c := New(Config{})
	if err := c.OnRunFinished(func(context.Context, workflow.RunEvent) {}); err != nil {
		t.Fatalf("OnRunFinished before Start: %v", err)
	}
	if c.Capabilities().Has(workflow.CapRunFinished) {
		t.Fatal("polling delivery is best-effort; CapRunFinished must not be claimed")
	}
}
