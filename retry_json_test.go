package workflow_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/ubgo/workflow"
)

// TestRetryPolicyJSONSkipsFunctions pins that a job carrying a retry
// policy encodes: NoRetryOn and DelayFor are functions, which encoding/json
// refuses, so they are skipped and the data fields round-trip. A batch
// keeps its job specs as JSON, so a Job.Retry used to fail the encode.
func TestRetryPolicyJSONSkipsFunctions(t *testing.T) {
	job := workflow.Job{Workflow: "w", Retry: &workflow.RetryPolicy{
		Max:       3,
		Backoff:   workflow.Backoff{Initial: time.Second},
		NoRetryOn: func(error) bool { return false },
		DelayFor:  func(error, int) (time.Duration, bool) { return 0, false },
	}}
	doc, err := json.Marshal(job)
	if err != nil {
		t.Fatalf("Marshal(Job with Retry) = %v", err)
	}
	var back workflow.Job
	if err := json.Unmarshal(doc, &back); err != nil {
		t.Fatal(err)
	}
	if back.Retry == nil || back.Retry.Max != 3 || back.Retry.Backoff.Initial != time.Second {
		t.Fatalf("Retry after a round trip = %+v, want Max 3 and Initial 1s", back.Retry)
	}
	if back.Retry.NoRetryOn != nil || back.Retry.DelayFor != nil {
		t.Error("functions survived the round trip, want them dropped")
	}
}
