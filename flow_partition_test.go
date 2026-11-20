package workflow_test

import (
	"context"
	"encoding/json"
	"errors"
	"maps"
	"slices"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/memory"
)

// partitionRecorder is the "daily" workflow: its step records the
// partition it saw, fails while fail reports true for its key, and, when
// hold is non-nil, blocks until hold is closed.
type partitionRecorder struct {
	mu   sync.Mutex
	seen []string
	fail func(key string) bool
	hold chan struct{}
}

func (p *partitionRecorder) workflow() workflow.Workflow {
	return workflow.Workflow{Name: "daily", Steps: []workflow.Step{{Name: "load", Run: func(ctx context.Context, in []byte) ([]byte, error) {
		set, key, ok := workflow.PartitionOf(ctx)
		if !ok {
			return nil, workflow.Permanent(errors.New("not a partition run"))
		}
		var payload string
		if err := json.Unmarshal(in, &payload); err != nil || payload != key {
			return nil, workflow.Permanent(errors.New("payload is not the key"))
		}
		if p.hold != nil {
			<-p.hold
		}
		p.mu.Lock()
		p.seen = append(p.seen, set+"/"+key)
		fail := p.fail != nil && p.fail(key)
		p.mu.Unlock()
		if fail {
			return nil, workflow.Permanent(errors.New("source down"))
		}
		return nil, nil
	}}}}
}

func partitionClient(t *testing.T, wfs ...workflow.Workflow) *memory.Client {
	t.Helper()
	c := memory.New()
	for _, wf := range wfs {
		if err := c.Register(wf); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	return c
}

func waitRefs(t *testing.T, c workflow.Client, refs []workflow.RunRef) {
	t.Helper()
	for _, r := range refs {
		waitRun(t, c, r)
	}
}

func partitionStates(t *testing.T, c workflow.Client, set string) map[string]workflow.Status {
	t.Helper()
	runs, err := workflow.PartitionStatus(context.Background(), c, set)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.IsSortedFunc(runs, func(a, b workflow.PartitionRun) int {
		if a.Key < b.Key {
			return -1
		}
		return 1
	}) {
		t.Fatalf("PartitionStatus is not sorted by key: %v", runs)
	}
	out := map[string]workflow.Status{}
	for _, r := range runs {
		out[r.Key] = r.Run.Status
	}
	return out
}

func TestPartitionsStatusAndRetryFailed(t *testing.T) {
	var broken atomic.Bool
	broken.Store(true)
	rec := &partitionRecorder{fail: func(key string) bool { return key == "2026-11-02" && broken.Load() }}
	c := partitionClient(t, rec.workflow())
	ctx := context.Background()
	set := workflow.PartitionSet{Name: "backfill", Workflow: "daily"}
	keys := []string{"2026-11-03", "2026-11-01", "2026-11-02"}

	refs, err := workflow.EnqueuePartitions(ctx, c, set, keys)
	if err != nil || len(refs) != 3 {
		t.Fatalf("enqueue = %v, %v", refs, err)
	}
	waitRefs(t, c, refs)
	got := partitionStates(t, c, "backfill")
	want := map[string]workflow.Status{"2026-11-01": workflow.StatusSucceeded, "2026-11-02": workflow.StatusFailed, "2026-11-03": workflow.StatusSucceeded}
	if !maps.Equal(got, want) {
		t.Fatalf("status = %v, want %v", got, want)
	}

	// The runs carry the set and the key, and the derived idempotency key.
	page, err := c.Runs(ctx, workflow.RunQuery{Ref: refs[0], Limit: 1})
	if err != nil || len(page.Runs) != 1 {
		t.Fatal(page, err)
	}
	l := page.Runs[0].Labels
	if l[workflow.LabelPartitionSet] != "backfill" || l[workflow.LabelPartitionKey] != "2026-11-03" {
		t.Fatalf("labels = %v", l)
	}

	// Only the failed partition starts again, and its new run becomes the
	// partition's state.
	broken.Store(false)
	retried, newRefs, err := workflow.RetryFailedPartitions(ctx, c, set)
	if err != nil || !slices.Equal(retried, []string{"2026-11-02"}) || len(newRefs) != 1 || newRefs[0] == refs[2] {
		t.Fatalf("retry = %v %v, %v", retried, newRefs, err)
	}
	waitRefs(t, c, newRefs)
	for k, st := range partitionStates(t, c, "backfill") {
		if st != workflow.StatusSucceeded {
			t.Fatalf("partition %s = %s after the retry", k, st)
		}
	}
	retried, _, err = workflow.RetryFailedPartitions(ctx, c, set)
	if err != nil || len(retried) != 0 {
		t.Fatalf("second retry = %v, %v; want nothing to retry", retried, err)
	}
	rec.mu.Lock()
	n := len(rec.seen)
	rec.mu.Unlock()
	if n != 4 {
		t.Fatalf("step ran %d times, want 4 (three partitions and one retry)", n)
	}

	// Another set on the same workflow is a view of its own.
	if st := partitionStates(t, c, "other"); len(st) != 0 {
		t.Fatalf("unknown set = %v", st)
	}
}

func TestPartitionsEnqueueAgainWhileUnfinished(t *testing.T) {
	rec := &partitionRecorder{hold: make(chan struct{})}
	c := partitionClient(t, rec.workflow())
	ctx := context.Background()
	set := workflow.PartitionSet{Name: "tenants", Workflow: "daily"}

	first, err := workflow.EnqueuePartitions(ctx, c, set, []string{"acme", "globex"})
	if err != nil {
		t.Fatal(err)
	}
	// A crashed backfill run again: the unfinished partitions return their
	// runs, a new key starts a run of its own.
	again, err := workflow.EnqueuePartitions(ctx, c, set, []string{"globex", "acme", "initech"})
	if err != nil {
		t.Fatal(err)
	}
	if again[0] != first[1] || again[1] != first[0] || again[2] == first[0] || again[2] == first[1] {
		t.Fatalf("again = %v, first = %v", again, first)
	}
	close(rec.hold)
	waitRefs(t, c, again)
	rec.mu.Lock()
	n := len(rec.seen)
	rec.mu.Unlock()
	if n != 3 {
		t.Fatalf("step ran %d times, want 3", n)
	}
	if got := partitionStates(t, c, "tenants"); len(got) != 3 {
		t.Fatalf("status = %v", got)
	}
}

func TestPartitionsJobBuilder(t *testing.T) {
	var mu sync.Mutex
	var payloads []string
	wf := workflow.Workflow{Name: "sync", Steps: []workflow.Step{{Name: "s", Run: func(ctx context.Context, in []byte) ([]byte, error) {
		_, key, _ := workflow.PartitionOf(ctx)
		mu.Lock()
		payloads = append(payloads, key+"="+string(in))
		mu.Unlock()
		return nil, nil
	}}}}
	c := partitionClient(t, wf)
	ctx := context.Background()
	set := workflow.PartitionSet{Name: "per-tenant", Job: func(key string) (workflow.Job, error) {
		return workflow.Job{Workflow: "sync", Payload: []byte(`{"tenant":"` + key + `"}`), Labels: map[string]string{"team": "data"}}, nil
	}}
	refs, err := workflow.EnqueuePartitions(ctx, c, set, []string{"a"})
	if err != nil {
		t.Fatal(err)
	}
	waitRefs(t, c, refs)
	if !slices.Equal(payloads, []string{`a={"tenant":"a"}`}) {
		t.Fatalf("payloads = %v", payloads)
	}
	page, _ := c.Runs(ctx, workflow.RunQuery{Ref: refs[0], Limit: 1})
	if page.Runs[0].Labels["team"] != "data" {
		t.Fatalf("labels = %v", page.Runs[0].Labels)
	}

	// A builder error and a reserved label fail their key only; the other
	// keys still start.
	bad := workflow.PartitionSet{Name: "bad", Job: func(key string) (workflow.Job, error) {
		switch key {
		case "err":
			return workflow.Job{}, errors.New("no such tenant")
		case "reserved":
			return workflow.Job{Workflow: "sync", Labels: map[string]string{workflow.LabelPartitionKey: "x"}}, nil
		}
		return workflow.Job{Workflow: "sync"}, nil
	}}
	refs, err = workflow.EnqueuePartitions(ctx, c, bad, []string{"err", "ok", "reserved"})
	if !errors.Is(err, workflow.ErrInvalid) || refs[0].RunID != "" || refs[1].RunID == "" || refs[2].RunID != "" {
		t.Fatalf("refs = %v, err = %v", refs, err)
	}
	waitRefs(t, c, refs[1:2])
}

// noRunQuery reports no capabilities, so it lacks the run query and
// idempotency keys.
type noRunQuery struct{ workflow.Client }

func (n noRunQuery) Capabilities() workflow.Caps { var none workflow.Caps; return none }

func TestPartitionsRefusals(t *testing.T) {
	c := partitionClient(t, (&partitionRecorder{}).workflow())
	ctx := context.Background()
	set := workflow.PartitionSet{Name: "s", Workflow: "daily"}
	for name, keys := range map[string][]string{"empty key": {"a", ""}, "repeated key": {"a", "a"}} {
		if _, err := workflow.EnqueuePartitions(ctx, c, set, keys); !errors.Is(err, workflow.ErrInvalid) {
			t.Errorf("%s: err = %v", name, err)
		}
	}
	if _, err := workflow.EnqueuePartitions(ctx, c, workflow.PartitionSet{Workflow: "daily"}, []string{"a"}); !errors.Is(err, workflow.ErrInvalid) {
		t.Errorf("no name: err = %v", err)
	}
	if _, err := workflow.EnqueuePartitions(ctx, c, workflow.PartitionSet{Name: "s"}, []string{"a"}); !errors.Is(err, workflow.ErrInvalid) {
		t.Errorf("no workflow: err = %v", err)
	}

	// An app cannot forge a partition run through a public Enqueue.
	_, err := c.Enqueue(ctx, workflow.Job{Workflow: "daily", Labels: map[string]string{workflow.LabelPartitionSet: "s"}})
	if !errors.Is(err, workflow.ErrInvalid) {
		t.Errorf("forged label: err = %v", err)
	}

	// Without a run query there is no status; without idempotency keys no
	// enqueue unless duplicates are accepted.
	weak := noRunQuery{c}
	var unsup *workflow.UnsupportedError
	if _, err := workflow.PartitionStatus(ctx, weak, "s"); !errors.As(err, &unsup) || unsup.Missing != workflow.CapRunQuery {
		t.Errorf("status without CapRunQuery: err = %v", err)
	}
	if _, err := workflow.EnqueuePartitions(ctx, weak, set, []string{"a"}); !errors.As(err, &unsup) || unsup.Missing != workflow.CapIdempotencyKey {
		t.Errorf("enqueue without CapIdempotencyKey: err = %v", err)
	}
	set.AllowDuplicates = true
	refs, err := workflow.EnqueuePartitions(ctx, weak, set, []string{"a"})
	if err != nil {
		t.Fatalf("AllowDuplicates: %v", err)
	}
	waitRefs(t, c, refs)
	if _, _, err := workflow.RetryFailedPartitions(ctx, weak, set); !errors.Is(err, workflow.ErrUnsupported) {
		t.Errorf("retry without CapRunQuery: err = %v", err)
	}
}

func TestPartitionKeyEscapes(t *testing.T) {
	if workflow.PartitionKey("w", "a/b", "c") == workflow.PartitionKey("w", "a", "b/c") {
		t.Fatal("a slash inside a name made two partitions share a key")
	}
	if got := workflow.PartitionKey("daily", "backfill", "2026-11-01"); got != "partition/daily/backfill/2026-11-01" {
		t.Fatalf("key = %q", got)
	}
}

func TestPartitionOfOutsideAStep(t *testing.T) {
	if _, _, ok := workflow.PartitionOf(context.Background()); ok {
		t.Fatal("PartitionOf outside a step reported a partition")
	}
}
