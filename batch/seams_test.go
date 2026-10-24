package batch_test

import (
	"context"
	"encoding/json"
	"errors"
	"regexp"
	"testing"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/batch"
)

// tenantRequired refuses every job without a tenant label, as an app's
// required-label enqueue middleware does.
type tenantRequired struct{ *retryStub }

var errNoTenant = errors.New("tenant label required")

func (c tenantRequired) Enqueue(ctx context.Context, job workflow.Job) (workflow.RunRef, error) {
	if job.Labels["tenant"] == "" {
		return workflow.RunRef{}, errNoTenant
	}
	return c.retryStub.Enqueue(ctx, job)
}

// TestBatchLabels_CopiedOntoCallbacks: Batch.Labels reach every callback,
// so a client that requires a label accepts them. Without Labels the same
// client refuses the callback. Reserved batch labels are refused by
// Dispatch (see TestReservedBatchLabelsRefused).
func TestBatchLabels_CopiedOntoCallbacks(t *testing.T) {
	ctx := context.Background()
	s := newRetryStub()
	m := batch.New(tenantRequired{s}, batch.NewMemoryStore(), batch.WithClock(newFake()))
	labels := map[string]string{"tenant": "acme"}
	id, err := m.Dispatch(ctx, batch.Batch{
		Jobs: []workflow.Job{{Workflow: "w", Labels: map[string]string{"tenant": "acme"}}},
		Then: "then", Finally: "fin", Labels: labels,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := finish(t, m, id, 0, workflow.StatusSucceeded); err != nil {
		t.Fatalf("callbacks refused: %v", err)
	}
	for _, name := range []string{"then", "fin"} {
		got := s.enqueued(name)
		if len(got) != 1 {
			t.Fatalf("%s enqueued %d times", name, len(got))
		}
		l := got[0].Labels
		if l["tenant"] != "acme" || len(l) != 1 {
			t.Fatalf("%s labels = %v, want only tenant=acme", name, l)
		}
	}
	if len(labels) != 1 {
		t.Fatal("Dispatch modified the caller's labels")
	}

	// Without Labels (the default) the callback carries none and the
	// label-requiring client refuses it, which is what Labels fixes.
	s2 := newRetryStub()
	m2 := batch.New(tenantRequired{s2}, batch.NewMemoryStore(), batch.WithClock(newFake()))
	id2, _ := m2.Dispatch(ctx, batch.Batch{Jobs: []workflow.Job{{Workflow: "w", Labels: map[string]string{"tenant": "acme"}}}, Finally: "fin"})
	if err := finish(t, m2, id2, 0, workflow.StatusSucceeded); !errors.Is(err, errNoTenant) {
		t.Fatalf("default callback err = %v, want the refusal", err)
	}
}

// TestBatchLabels_SurviveJSONStore: Labels live in State, so a store that
// keeps State as a JSON document (sqlstore's) round-trips them.
func TestBatchLabels_SurviveJSONStore(t *testing.T) {
	ctx := context.Background()
	store := batch.NewMemoryStore()
	m := batch.New(newRetryStub(), store, batch.WithClock(newFake()))
	id, err := m.Dispatch(ctx, batch.Batch{Jobs: []workflow.Job{{Workflow: "w"}}, Labels: map[string]string{"user": "u1"}})
	if err != nil {
		t.Fatal(err)
	}
	st, _ := store.Get(ctx, id)
	doc, _ := json.Marshal(st)
	var back batch.State
	if err := json.Unmarshal(doc, &back); err != nil || back.Labels["user"] != "u1" {
		t.Fatalf("labels after a JSON round trip = %v (%v)", back.Labels, err)
	}
}

func TestWithIDFunc(t *testing.T) {
	ctx := context.Background()
	jobs := []workflow.Job{{Workflow: "w"}}

	// Custom IDs are used and reach the job labels.
	s := newRetryStub()
	store := batch.NewMemoryStore()
	m := batch.New(s, store, batch.WithIDFunc(func() (batch.ID, error) { return "01JABCDEF", nil }))
	id, err := m.Dispatch(ctx, batch.Batch{Jobs: jobs})
	if err != nil || id != "01JABCDEF" {
		t.Fatalf("id = %q, %v", id, err)
	}
	if got := s.enqueued("w")[0].Labels[batch.LabelBatch]; got != "01JABCDEF" {
		t.Fatalf("job label = %q", got)
	}
	// A repeated ID is refused by the store.
	if _, err := m.Dispatch(ctx, batch.Batch{Jobs: jobs}); err == nil {
		t.Fatal("a repeated ID was accepted")
	}

	// An error or an empty ID fails closed: nothing saved, nothing enqueued.
	for name, fn := range map[string]func() (batch.ID, error){
		"error": func() (batch.ID, error) { return "", errEngineDown },
		"empty": func() (batch.ID, error) { return "", nil },
	} {
		s, store := newRetryStub(), batch.NewMemoryStore()
		_, err := batch.New(s, store, batch.WithIDFunc(fn)).Dispatch(ctx, batch.Batch{Jobs: jobs})
		if err == nil || (name == "empty" && !errors.Is(err, workflow.ErrInvalid)) || (name == "error" && !errors.Is(err, errEngineDown)) {
			t.Fatalf("%s: err = %v", name, err)
		}
		if open, _ := store.Unfinished(ctx); len(open) != 0 || len(s.enqueued("w")) != 0 {
			t.Fatalf("%s: a batch was saved or enqueued", name)
		}
	}

	// Default unchanged: 128 random bits as hex.
	id, err = batch.New(newRetryStub(), batch.NewMemoryStore()).Dispatch(ctx, batch.Batch{Jobs: jobs})
	if err != nil || !regexp.MustCompile(`^[0-9a-f]{32}$`).MatchString(string(id)) {
		t.Fatalf("default id = %q, %v", id, err)
	}
}

// TestReservedBatchLabelsRefused: a label under the batch prefix, on a job
// or in Batch.Labels, is refused with ErrInvalid before anything is saved
// or enqueued, instead of being overwritten.
func TestReservedBatchLabelsRefused(t *testing.T) {
	ctx := context.Background()
	for name, b := range map[string]batch.Batch{
		"job id":       {Jobs: []workflow.Job{{Workflow: "w", Labels: map[string]string{batch.LabelBatch: "forged"}}}},
		"job other":    {Jobs: []workflow.Job{{Workflow: "w"}, {Workflow: "w", Labels: map[string]string{workflow.LabelPrefixBatch + "x": "y"}}}},
		"batch labels": {Jobs: []workflow.Job{{Workflow: "w"}}, Labels: map[string]string{batch.LabelIndex: "7"}},
	} {
		s, store := newRetryStub(), batch.NewMemoryStore()
		if _, err := batch.New(s, store).Dispatch(ctx, b); !errors.Is(err, workflow.ErrInvalid) {
			t.Fatalf("%s: err = %v, want ErrInvalid", name, err)
		}
		if open, _ := store.Unfinished(ctx); len(open) != 0 || len(s.enqueued("w")) != 0 {
			t.Fatalf("%s: a batch was saved or enqueued", name)
		}
	}
	// App labels outside the prefix pass.
	s := newRetryStub()
	if _, err := batch.New(s, batch.NewMemoryStore()).Dispatch(ctx, batch.Batch{Jobs: []workflow.Job{{Workflow: "w", Labels: map[string]string{"workflow.batchy": "ok"}}}}); err != nil {
		t.Fatal(err)
	}
}
