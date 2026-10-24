package hatchet

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ubgo/workflow"
)

// asRefusal fails unless err is an ErrUnsupported naming field and missing.
func asRefusal(t *testing.T, label string, err error, field string, missing workflow.Caps) {
	t.Helper()
	var u *workflow.UnsupportedError
	if !errors.Is(err, workflow.ErrUnsupported) || !errors.As(err, &u) {
		t.Fatalf("%s: err = %v, want an UnsupportedError", label, err)
	}
	if u.Field != field || !u.Missing.Has(missing) {
		t.Errorf("%s: Field %q Missing %s, want %q and %s", label, u.Field, u.Missing, field, missing)
	}
}

// TestQueueLimitIsRefused: Hatchet cannot count the in-flight steps of a
// queue across workers, so it declares no CapQueueLimitGlobal and refuses a
// non-zero QueueConfig.Limit rather than enforce only a per-worker cap. The
// matrix crosses Limit with the other queue fields so a refusal cannot hide
// behind (or be masked by) them, and checks Start's own path.
func TestQueueLimitIsRefused(t *testing.T) {
	if New(Config{}).Capabilities().Has(workflow.CapQueueLimitGlobal) {
		t.Fatal("hatchet declares CapQueueLimitGlobal, which its engine cannot enforce")
	}
	for _, tc := range []struct {
		name string
		qs   []workflow.QueueConfig
		ok   bool
	}{
		{"no limit", []workflow.QueueConfig{{Name: "mail", Workers: 2}}, true},
		{"limit alone", []workflow.QueueConfig{{Name: "mail", Limit: 3}}, false},
		{"limit with workers", []workflow.QueueConfig{{Name: "mail", Workers: 2, Limit: 3}}, false},
		{"limit on the default queue", []workflow.QueueConfig{{Name: workflow.DefaultQueue, Limit: 1}}, false},
		{"limit on the second of two", []workflow.QueueConfig{{Name: "a"}, {Name: "b", Limit: 1}}, false},
		{"limit of one", []workflow.QueueConfig{{Name: "a", Limit: 1}}, false},
	} {
		err := checkQueues(tc.qs)
		if tc.ok {
			if err != nil {
				t.Errorf("%s: %v, want accepted", tc.name, err)
			}
		} else {
			asRefusal(t, tc.name, err, "QueueConfig.Limit", workflow.CapQueueLimitGlobal)
		}
		// Start reaches the same check through the config, before any engine call.
		serr := New(Config{Queues: tc.qs}).Start(context.Background())
		if !tc.ok {
			asRefusal(t, tc.name+" via Start", serr, "QueueConfig.Limit", workflow.CapQueueLimitGlobal)
		}
	}
	if err := checkQueues([]workflow.QueueConfig{{Name: "a", Limit: -1}}); !errors.Is(err, workflow.ErrInvalid) {
		t.Errorf("negative Limit: %v, want ErrInvalid", err)
	}
}

// TestDiscardOnExhaustIsRefused: Hatchet has no discarded status, so a run
// out of retries is failed; a policy asking for discard is refused wherever
// it is declared, never accepted and then ignored.
func TestDiscardOnExhaustIsRefused(t *testing.T) {
	if New(Config{}).Capabilities().Has(workflow.CapDiscardExhausted) {
		t.Fatal("hatchet declares CapDiscardExhausted, but the engine has no discarded status")
	}
	run := func(context.Context, []byte) ([]byte, error) { return nil, nil }
	pol := workflow.RetryPolicy{Max: 2, DiscardOnExhaust: true}
	onWorkflow := workflow.Workflow{Name: "wf", Retry: pol, Steps: []workflow.Step{{Name: "a", Run: run}}}
	onStep := workflow.Workflow{Name: "wf", Steps: []workflow.Step{{Name: "a", Run: run, Retry: &pol}}}
	withBackoff := pol
	withBackoff.Backoff = workflow.Backoff{Initial: time.Second}
	onStepBackoff := workflow.Workflow{Name: "wf", Steps: []workflow.Step{{Name: "a", Run: run, Retry: &withBackoff}}}
	for name, wf := range map[string]workflow.Workflow{"workflow": onWorkflow, "step": onStep, "step with a backoff": onStepBackoff} {
		asRefusal(t, name, New(Config{}).Register(wf), "Retry.DiscardOnExhaust", workflow.CapDiscardExhausted)
	}
	// The same policy without the flag is accepted: the refusal is about the flag.
	plain := workflow.Workflow{Name: "wf", Steps: []workflow.Step{{Name: "a", Run: run, Retry: &workflow.RetryPolicy{Max: 2}}}}
	if err := New(Config{}).Register(plain); err != nil {
		t.Errorf("a plain Max 2 policy: %v, want accepted", err)
	}
	// A per-dispatch policy is refused before it reaches the engine.
	err := workflow.CheckJob(New(Config{}).Capabilities(), workflow.Job{Workflow: "wf", Retry: &pol})
	if !errors.Is(err, workflow.ErrUnsupported) {
		t.Errorf("Job.Retry with DiscardOnExhaust: %v, want ErrUnsupported", err)
	}
}
