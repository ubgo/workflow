package workflow_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/ubgo/workflow"
)

func TestValidateSource(t *testing.T) {
	for _, ok := range []workflow.Source{"api", "webhook", "a.b_c-1", workflow.Source(strings.Repeat("x", workflow.MaxSourceLen))} {
		if err := workflow.ValidateSource(ok); err != nil {
			t.Errorf("ValidateSource(%q) = %v, want nil", ok, err)
		}
	}
	for _, bad := range []workflow.Source{"", "Api", "has space", "slash/no", workflow.Source(strings.Repeat("x", workflow.MaxSourceLen+1))} {
		if err := workflow.ValidateSource(bad); !errors.Is(err, workflow.ErrInvalid) {
			t.Errorf("ValidateSource(%q) = %v, want ErrInvalid", bad, err)
		}
	}
	for _, s := range workflow.LibrarySources() {
		if err := workflow.ValidateSource(s); err != nil {
			t.Errorf("library source %q is invalid: %v", s, err)
		}
	}
}

func TestLibrarySourcesIsACopy(t *testing.T) {
	a := workflow.LibrarySources()
	a[0] = "changed"
	if workflow.LibrarySources()[0] == "changed" {
		t.Fatal("LibrarySources handed out its backing array")
	}
}

func TestWithSourceCopiesLabels(t *testing.T) {
	orig := map[string]string{"app": "x"}
	job := workflow.WithSource(workflow.Job{Workflow: "w", Labels: orig}, "webhook", "dlv-1")
	if len(orig) != 1 {
		t.Fatalf("caller's map changed: %v", orig)
	}
	if job.Labels[workflow.LabelSource] != "webhook" || job.Labels[workflow.LabelSourceRef] != "dlv-1" || job.Labels["app"] != "x" {
		t.Fatalf("labels = %v", job.Labels)
	}
	// An empty ref removes a stale one.
	job = workflow.WithSource(job, "api", "")
	if _, ok := job.Labels[workflow.LabelSourceRef]; ok || job.Labels[workflow.LabelSource] != "api" {
		t.Fatalf("labels after empty ref = %v", job.Labels)
	}
	// A nil Labels map works.
	if got := workflow.WithSource(workflow.Job{}, "api", "r"); got.Labels[workflow.LabelSource] != "api" {
		t.Fatalf("nil labels: %v", got.Labels)
	}
}

func TestSourceOf(t *testing.T) {
	cases := []struct {
		name    string
		labels  map[string]string
		wantSrc workflow.Source
		wantRef string
	}{
		{"none", nil, "", ""},
		{"app labels only", map[string]string{"x": "y"}, "", ""},
		{"explicit wins", map[string]string{workflow.LabelSource: "slack", workflow.LabelSourceRef: "t1", workflow.LabelSchedule: "s1"}, "slack", "t1"},
		{"empty explicit falls through", map[string]string{workflow.LabelSource: "", workflow.LabelSchedule: "s1"}, workflow.SourceSchedule, "s1"},
		{"manual trigger", map[string]string{workflow.LabelSchedule: "s1", workflow.LabelTrigger: workflow.LabelTriggerManual}, workflow.SourceScheduleManual, "s1"},
		{"schedule", map[string]string{workflow.LabelSchedule: "s1"}, workflow.SourceSchedule, "s1"},
		{"child", map[string]string{workflow.LabelParentRun: "r9"}, workflow.SourceChild, "r9"},
		{"batch", map[string]string{workflow.LabelPrefixBatch + "id": "b1"}, workflow.SourceBatch, "b1"},
		{"event", map[string]string{workflow.LabelPrefixInternal + "event": "order.paid"}, workflow.SourceEvent, "order.paid"},
	}
	for _, c := range cases {
		src, ref := workflow.SourceOf(c.labels)
		if src != c.wantSrc || ref != c.wantRef {
			t.Errorf("%s: SourceOf = (%q, %q), want (%q, %q)", c.name, src, ref, c.wantSrc, c.wantRef)
		}
	}
}

func TestSourceLabelsAreAppSettable(t *testing.T) {
	ctx := context.Background()
	job := workflow.WithSource(workflow.Job{Workflow: "w"}, "webhook", "d1")
	if err := workflow.CheckJobLabels(ctx, job); err != nil {
		t.Fatalf("CheckJobLabels with a valid source: %v", err)
	}
	if err := workflow.ValidateLabels(job.Labels); err != nil {
		t.Fatalf("ValidateLabels with a valid source: %v", err)
	}
	// An invalid value is refused by both.
	bad := workflow.WithSource(workflow.Job{Workflow: "w"}, "Bad Source", "")
	if err := workflow.CheckJobLabels(ctx, bad); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("CheckJobLabels with a bad source = %v, want ErrInvalid", err)
	}
	if err := workflow.ValidateLabels(bad.Labels); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("ValidateLabels with a bad source = %v, want ErrInvalid", err)
	}
	longRef := workflow.WithSource(workflow.Job{Workflow: "w"}, "api", strings.Repeat("r", workflow.MaxSourceRefLen+1))
	if err := workflow.CheckJobLabels(ctx, longRef); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("CheckJobLabels with a long ref = %v, want ErrInvalid", err)
	}
	// A chained job's source is checked too.
	chained := workflow.Job{Workflow: "w", Chain: []workflow.Job{bad}}
	if err := workflow.CheckJobLabels(ctx, chained); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("CheckJobLabels with a bad chained source = %v, want ErrInvalid", err)
	}
	// Other reserved keys stay owner-only.
	other := workflow.Job{Workflow: "w", Labels: map[string]string{workflow.LabelSchedule: "s"}}
	if err := workflow.CheckJobLabels(ctx, other); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("CheckJobLabels with LabelSchedule = %v, want ErrInvalid", err)
	}
}
