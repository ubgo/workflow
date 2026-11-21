package dsl_test

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/dsl"
)

// full is a definition that sets every field, so the round trip and the
// build check each one.
const full = `{
  "name": "order",
  "version": 2,
  "min_version": 1,
  "queue": "orders",
  "timeout": "30s",
  "run_timeout": "72h",
  "retry": {"max": 3, "backoff": {"initial": "1s", "factor": 2, "max": "1m", "jitter": 0.2}, "until": "1h", "max_errors": 2, "discard_on_exhaust": true},
  "concurrency": {"key_label": "customer", "max_runs": 1, "strategy": "queue_newest", "fifo": true},
  "steps": [
    {"name": "reserve", "handler": "reserve", "timeout": "10s", "retry": {"max": 0}},
    {"name": "await-payment", "after": ["reserve"], "wait": {"event": "payment.succeeded", "key_label": "order", "timeout": "48h"}},
    {"name": "cool-off", "after": ["await-payment"], "sleep": "24h"},
    {"name": "ship", "after": ["cool-off"], "handler": "ship"}
  ],
  "on_failure": "alert",
  "cron": ["0 9 * * *"],
  "cron_location": "Europe/Berlin"
}`

// noop is a handler for definitions whose steps never run.
func noop(context.Context, []byte) ([]byte, error) { return nil, nil }

// registry returns a Registry with the given names bound to noop.
func registry(t *testing.T, names ...string) *dsl.Registry {
	t.Helper()
	reg := dsl.NewRegistry()
	for _, n := range names {
		if err := reg.Register(n, noop); err != nil {
			t.Fatal(err)
		}
	}
	return reg
}

func TestParseMarshalRoundTrip(t *testing.T) {
	def, err := dsl.Parse([]byte(full))
	if err != nil {
		t.Fatal(err)
	}
	out, err := dsl.Marshal(def)
	if err != nil {
		t.Fatal(err)
	}
	back, err := dsl.Parse(out)
	if err != nil {
		t.Fatalf("Parse(Marshal(def)) = %v\n%s", err, out)
	}
	if !reflect.DeepEqual(def, back) {
		t.Fatalf("round trip changed the definition:\n got %+v\nwant %+v", back, def)
	}
}

func TestBuildMapsEveryField(t *testing.T) {
	wf, err := dsl.Load([]byte(full), registry(t, "reserve", "ship", "alert"))
	if err != nil {
		t.Fatal(err)
	}
	if wf.Name != "order" || wf.Version != 2 || wf.MinVersion != 1 || wf.Queue != "orders" {
		t.Errorf("name/version/queue = %q %d %d %q", wf.Name, wf.Version, wf.MinVersion, wf.Queue)
	}
	if wf.Timeout != 30*time.Second || wf.RunTimeout != 72*time.Hour {
		t.Errorf("timeouts = %v %v", wf.Timeout, wf.RunTimeout)
	}
	wantRetry := workflow.RetryPolicy{Max: 3, Backoff: workflow.Backoff{Initial: time.Second, Factor: 2, Max: time.Minute, Jitter: 0.2}, Until: time.Hour, MaxErrors: 2, DiscardOnExhaust: true}
	if !reflect.DeepEqual(wf.Retry, wantRetry) {
		t.Errorf("Retry = %+v, want %+v", wf.Retry, wantRetry)
	}
	c := wf.Concurrency
	if c == nil || c.MaxRuns != 1 || c.Strategy != workflow.ConcurrencyQueueNewest || !c.FIFO || c.Key.Func == nil {
		t.Fatalf("Concurrency = %+v", c)
	}
	if k := c.Key.Func(workflow.Job{Labels: map[string]string{"customer": "c-7"}}); k != "c-7" {
		t.Errorf("concurrency key = %q, want the customer label", k)
	}
	if wf.OnFailure == nil || wf.CronLocation == nil || wf.CronLocation.String() != "Europe/Berlin" || len(wf.Cron) != 1 {
		t.Errorf("OnFailure set %t, CronLocation %v, Cron %v", wf.OnFailure != nil, wf.CronLocation, wf.Cron)
	}
	kinds := []workflow.StepKind{workflow.StepRun, workflow.StepWait, workflow.StepSleep, workflow.StepRun}
	for i, s := range wf.Steps {
		if s.Kind() != kinds[i] {
			t.Errorf("step %q kind = %q, want %q", s.Name, s.Kind(), kinds[i])
		}
	}
	reserve, wait, sleep := wf.Steps[0], wf.Steps[1], wf.Steps[2]
	if reserve.Timeout != 10*time.Second || reserve.Retry == nil || reserve.Retry.Max != 0 {
		t.Errorf("reserve = timeout %v retry %+v", reserve.Timeout, reserve.Retry)
	}
	if w := wait.Wait; w.Event != "payment.succeeded" || w.Timeout != 48*time.Hour || w.KeyFrom.Func == nil {
		t.Errorf("wait = %+v", w)
	}
	if k, _ := wait.Wait.ResolveKey(workflow.Job{Labels: map[string]string{"order": "A-1"}}); k != "A-1" {
		t.Errorf("wait key = %q, want the order label", k)
	}
	if sleep.Sleep != 24*time.Hour || !reflect.DeepEqual(sleep.After, []string{"await-payment"}) {
		t.Errorf("cool-off = sleep %v after %v", sleep.Sleep, sleep.After)
	}
}

// TestRefusals checks every refusal names the place and wraps ErrInvalid,
// Parse's and Build's alike.
func TestRefusals(t *testing.T) {
	reg := registry(t, "a", "b")
	cases := []struct {
		name, doc string
		path      string // the FieldError path; "-" for a core validation error
		want      string // a substring of the message
	}{
		{"not json", `{"name":`, "", "not a JSON document"},
		{"not an object", `[]`, "", "want a JSON object"},
		{"trailing data", `{"name":"w","steps":[{"name":"s","handler":"a"}]} {}`, "", "data after"},
		{"unknown top field", `{"name":"w","stepz":[]}`, "stepz", "unknown field"},
		{"unknown step field", `{"name":"w","steps":[{"name":"s","handler":"a"},{"name":"t","handlr":"b"}]}`, "steps[1].handlr", "unknown field"},
		{"unknown nested field", `{"name":"w","retry":{"max":1,"backoff":{"start":"1s"}},"steps":[{"name":"s","handler":"a"}]}`, "retry.backoff.start", "unknown field"},
		{"wrong type", `{"name":"w","steps":[{"name":"s","handler":"a","after":"x"}]}`, "steps[0].after", "want an array"},
		{"fraction for an integer", `{"name":"w","version":1.5,"steps":[{"name":"s","handler":"a"}]}`, "version", "want an integer"},
		{"no name", `{"steps":[{"name":"s","handler":"a"}]}`, "name", "is empty"},
		{"no steps", `{"name":"w","steps":[]}`, "steps", "at least one step"},
		{"bad duration", `{"name":"w","steps":[{"name":"s","handler":"a","timeout":"10 seconds"}]}`, "steps[0].timeout", "not a Go duration"},
		{"bad nested duration", `{"name":"w","steps":[{"name":"s","wait":{"event":"e","timeout":"2d"}}]}`, "steps[0].wait.timeout", "not a Go duration"},
		{"zero sleep", `{"name":"w","steps":[{"name":"s","sleep":"0s"}]}`, "steps[0].sleep", "not a positive duration"},
		{"two kinds", `{"name":"w","steps":[{"name":"s","handler":"a","sleep":"1s"}]}`, "steps[0]", "exactly one"},
		{"no kind", `{"name":"w","steps":[{"name":"s"}]}`, "steps[0]", "exactly one"},
		{"wait without event", `{"name":"w","steps":[{"name":"s","wait":{"key":"k"}}]}`, "steps[0].wait.event", "is empty"},
		{"key and key_label", `{"name":"w","steps":[{"name":"s","wait":{"event":"e","key":"k","key_label":"l"}}]}`, "steps[0].wait", "both key and key_label"},
		{"bad zone", `{"name":"w","cron":["0 9 * * *"],"cron_location":"Mars/Olympus","steps":[{"name":"s","handler":"a"}]}`, "cron_location", "IANA zone"},
		{"unknown handler", `{"name":"w","steps":[{"name":"s","handler":"a"},{"name":"t","after":["s"],"handler":"shipp"}]}`, "steps[1].handler", `"shipp" (registered: a, b)`},
		{"unknown on_failure", `{"name":"w","on_failure":"alert","steps":[{"name":"s","handler":"a"}]}`, "on_failure", `"alert"`},
		{"missing dependency", `{"name":"w","steps":[{"name":"s","handler":"a","after":["nope"]}]}`, "-", `step "s" is After unknown step "nope"`},
		{"cycle", `{"name":"w","steps":[{"name":"s","handler":"a","after":["t"]},{"name":"t","handler":"b","after":["s"]}]}`, "-", "After cycle"},
		{"duplicate step", `{"name":"w","steps":[{"name":"s","handler":"a"},{"name":"s","handler":"b"}]}`, "-", "used twice"},
		{"unknown strategy", `{"name":"w","concurrency":{"max_runs":1,"strategy":"newest"},"steps":[{"name":"s","handler":"a"}]}`, "-", "unknown Concurrency.Strategy"},
		{"negative retries", `{"name":"w","retry":{"max":-1},"steps":[{"name":"s","handler":"a"}]}`, "-", "Retry.Max is negative"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := dsl.Load([]byte(tc.doc), reg)
			if !errors.Is(err, workflow.ErrInvalid) {
				t.Fatalf("Load = %v, want ErrInvalid", err)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error %q does not say %q", err, tc.want)
			}
			var fe *dsl.FieldError
			switch isField := errors.As(err, &fe); {
			case tc.path == "-" && isField:
				t.Errorf("got FieldError %q, want the core's own validation error", fe.Path)
			case tc.path != "-" && !isField:
				t.Errorf("error %v is not a FieldError", err)
			case isField && fe.Path != tc.path:
				t.Errorf("path = %q, want %q", fe.Path, tc.path)
			}
		})
	}
}

// TestBuildMatchesGoValidation pins that Build refuses a definition with
// the very error workflow.Validate gives the same workflow written in Go.
func TestBuildMatchesGoValidation(t *testing.T) {
	def := dsl.Definition{Name: "w", Steps: []dsl.StepDef{
		{Name: "s", Handler: "a", After: []string{"t"}},
		{Name: "t", Handler: "a", After: []string{"s"}},
	}}
	_, got := dsl.Build(def, registry(t, "a"))
	want := workflow.Validate(workflow.Workflow{Name: "w", Steps: []workflow.Step{
		{Name: "s", Run: noop, After: []string{"t"}},
		{Name: "t", Run: noop, After: []string{"s"}},
	}})
	if got == nil || want == nil || got.Error() != want.Error() {
		t.Fatalf("Build = %v, Go Validate = %v; want the same error", got, want)
	}
}

func TestBuildNilRegistryKnowsNoHandler(t *testing.T) {
	_, err := dsl.Build(dsl.Definition{Name: "w", Steps: []dsl.StepDef{{Name: "s", Handler: "a"}}}, nil)
	if !errors.Is(err, workflow.ErrInvalid) || !strings.Contains(err.Error(), "registered: none") {
		t.Fatalf("Build(nil registry) = %v", err)
	}
	// A definition of waits and sleeps alone needs no handler at all.
	if _, err := dsl.Build(dsl.Definition{Name: "w", Steps: []dsl.StepDef{{Name: "s", Sleep: "1h"}}}, nil); err != nil {
		t.Fatalf("Build(sleep only, nil registry) = %v", err)
	}
}

func TestRegistry(t *testing.T) {
	var reg dsl.Registry // the zero value is usable
	if err := reg.Register("a", noop); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		h    workflow.Handler
	}{{"", noop}, {"b", nil}, {"a", noop}} {
		if err := reg.Register(tc.name, tc.h); !errors.Is(err, workflow.ErrInvalid) {
			t.Errorf("Register(%q, nil handler %t) = %v, want ErrInvalid", tc.name, tc.h == nil, err)
		}
	}
	if _, ok := reg.Handler("a"); !ok {
		t.Error(`Handler("a") not found`)
	}
	if _, ok := reg.Handler("b"); ok {
		t.Error(`Handler("b") found after a refused Register`)
	}
	if got := reg.Names(); !reflect.DeepEqual(got, []string{"a"}) {
		t.Errorf("Names = %v", got)
	}
}

// TestBuildDoesNotAlias pins that editing the Workflow Build returned never
// changes the Definition it came from.
func TestBuildDoesNotAlias(t *testing.T) {
	def := dsl.Definition{Name: "w", Cron: []string{"0 9 * * *"}, Steps: []dsl.StepDef{
		{Name: "s", Handler: "a"},
		{Name: "t", Handler: "a", After: []string{"s"}},
	}}
	wf, err := dsl.Build(def, registry(t, "a"))
	if err != nil {
		t.Fatal(err)
	}
	wf.Cron[0] = "changed"
	wf.Steps[1].After[0] = "changed"
	if def.Cron[0] != "0 9 * * *" || def.Steps[1].After[0] != "s" {
		t.Fatalf("the definition changed with the workflow: %+v", def)
	}
}
