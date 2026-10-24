package workflow_test

import (
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/cron"
)

func TestScheduleJSONRoundTrip(t *testing.T) {
	berlin, err := cron.LoadZone("Europe/Berlin")
	if err != nil {
		t.Fatal(err)
	}
	in := workflow.Schedule{
		Name: "digest", Workflow: "send-digest", Location: berlin,
		Input:    []byte(`{"tenant":7}`),
		Labels:   map[string]string{"tenant": "7"},
		Misfire:  workflow.Misfire{Policy: workflow.MisfireFireAll, Within: 90 * time.Minute, Max: 3},
		Windows:  workflow.Windows{Between: []workflow.Window{{From: "08:00", To: "18:00"}}, Unless: []workflow.Window{{From: "12:00", To: "13:00"}}},
		Overlap:  workflow.OverlapSkip,
		Seconds:  true,
		Cron:     "0 0 9 * * 1-5",
		Active:   workflow.Active{From: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)},
		KeepRuns: 10,
	}
	b, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`"v":1`, `"location":"Europe/Berlin"`, `"within":"1h30m0s"`, `"active":{"from":"2026-01-01T00:00:00Z"}`, `"keep_runs":10`, `"seconds":true`} {
		if !strings.Contains(string(b), want) {
			t.Errorf("JSON %s lacks %s", b, want)
		}
	}
	var out workflow.Schedule
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	if out.Location.String() != "Europe/Berlin" {
		t.Fatalf("location = %v", out.Location)
	}
	out.Location = berlin
	if !reflect.DeepEqual(in, out) {
		t.Fatalf("round trip:\n in %+v\nout %+v", in, out)
	}
	if err := workflow.ValidateSchedule(out); err != nil {
		t.Fatalf("round-tripped schedule invalid: %v", err)
	}

	minimal, _ := json.Marshal(workflow.Schedule{Workflow: "w", Cron: "@daily", Location: time.UTC})
	var m workflow.Schedule
	if err := json.Unmarshal(minimal, &m); err != nil || m.Location.String() != "UTC" || !m.Misfire.IsZero() || !m.Windows.IsZero() {
		t.Fatalf("minimal round trip: %+v %v (%s)", m, err, minimal)
	}
}

func TestScheduleJSONRefusals(t *testing.T) {
	if _, err := json.Marshal(workflow.Schedule{Workflow: "w", Cron: "@daily", Location: time.Local}); !errors.Is(err, workflow.ErrScheduleJSON) {
		t.Errorf("time.Local marshalled: %v", err)
	}
	if _, err := json.Marshal(workflow.Schedule{Workflow: "w", Cron: "@daily", Location: time.FixedZone("X", 3600)}); !errors.Is(err, workflow.ErrScheduleJSON) {
		t.Errorf("fixed zone marshalled: %v", err)
	}
	for _, doc := range []string{
		`{"v":2,"workflow":"w","cron":"@daily"}`,
		`{"workflow":"w","cron":"@daily"}`,
		`{"v":1,"workflow":"w","cron":"@daily","location":"Mars/Base"}`,
		`{"v":1,"workflow":"w","cron":"@daily","location":"Local"}`,
		`{"v":1,"workflow":"w","cron":"@daily","future":true}`,
		`{"v":1,"workflow":"w","cron":"@daily","misfire":{"within":"soon"}}`,
	} {
		var s workflow.Schedule
		if err := json.Unmarshal([]byte(doc), &s); !errors.Is(err, workflow.ErrScheduleJSON) {
			t.Errorf("%s: err = %v, want ErrScheduleJSON", doc, err)
		}
	}
	custom := func(name string) (*time.Location, error) { return time.FixedZone(name, 0), nil }
	s, err := workflow.UnmarshalSchedule([]byte(`{"v":1,"workflow":"w","cron":"@daily","location":"Office"}`), custom)
	if err != nil || s.Location.String() != "Office" {
		t.Fatalf("custom zone loader: %v %v", s.Location, err)
	}
}
