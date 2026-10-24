package redis

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/ubgo/workflow"
)

// A run's chain (Job.Chain, then the edits its steps made) is the chain
// field of its hash: written with the run, rewritten by the save that
// records a step success making an edit, and handed on by the script that
// marks the run succeeded, which creates the next job as a new run
// carrying the rest of the chain. So the chain is data at every moment,
// and a crash between two of its jobs loses nothing. A run that ends
// failed, discarded or cancelled keeps its chain and does not hand on;
// Retry or ForceComplete of a failed run resumes it.

// chainDoc is one workflow.Job as stored. Only data fields exist:
// workflow.ValidateJob refuses a Job.Retry with functions on every
// chained job, so nothing is lost. RunAt is Unix nanoseconds (0: none).
type chainDoc struct {
	Workflow       string            `json:"workflow"`
	Payload        []byte            `json:"payload,omitempty"`
	RunAt          int64             `json:"run_at,omitempty"`
	IdempotencyKey string            `json:"idempotency_key,omitempty"`
	Labels         map[string]string `json:"labels,omitempty"`
	Priority       int               `json:"priority,omitempty"`
	Queue          string            `json:"queue,omitempty"`
	Tenant         string            `json:"tenant,omitempty"`
	Retry          *storedRetry      `json:"retry,omitempty"`
	Chain          []chainDoc        `json:"chain,omitempty"`
}

func toChainDocs(jobs []workflow.Job) []chainDoc {
	if len(jobs) == 0 {
		return nil
	}
	out := make([]chainDoc, len(jobs))
	for i, j := range jobs {
		d := chainDoc{Workflow: j.Workflow, Payload: j.Payload, IdempotencyKey: j.IdempotencyKey,
			Labels: j.Labels, Priority: j.Priority, Queue: j.Queue, Tenant: j.Tenant, Chain: toChainDocs(j.Chain)}
		if !j.RunAt.IsZero() {
			d.RunAt = j.RunAt.UnixNano()
		}
		if p := j.Retry; p != nil {
			d.Retry = &storedRetry{Max: p.Max, Initial: p.Backoff.Initial, Factor: p.Backoff.Factor,
				MaxDelay: p.Backoff.Max, Jitter: p.Backoff.Jitter, Until: p.Until, MaxErrors: p.MaxErrors, DiscardOnExhaust: p.DiscardOnExhaust}
		}
		out[i] = d
	}
	return out
}

func fromChainDocs(docs []chainDoc) []workflow.Job {
	if len(docs) == 0 {
		return nil
	}
	out := make([]workflow.Job, len(docs))
	for i, d := range docs {
		j := workflow.Job{Workflow: d.Workflow, Payload: d.Payload, IdempotencyKey: d.IdempotencyKey,
			Labels: d.Labels, Priority: d.Priority, Queue: d.Queue, Tenant: d.Tenant, Chain: fromChainDocs(d.Chain)}
		if d.RunAt != 0 {
			j.RunAt = time.Unix(0, d.RunAt).UTC()
		}
		if r := d.Retry; r != nil {
			j.Retry = &workflow.RetryPolicy{Max: r.Max, Until: r.Until, MaxErrors: r.MaxErrors, DiscardOnExhaust: r.DiscardOnExhaust,
				Backoff: workflow.Backoff{Initial: r.Initial, Factor: r.Factor, Max: r.MaxDelay, Jitter: r.Jitter}}
		}
		out[i] = j
	}
	return out
}

// encodeChain is the stored chain field ("" for none).
func encodeChain(jobs []workflow.Job) (string, error) {
	if len(jobs) == 0 {
		return "", nil
	}
	b, err := json.Marshal(toChainDocs(jobs))
	if err != nil {
		return "", fmt.Errorf("redis: encode Job.Chain: %w", err)
	}
	return string(b), nil
}

// decodeChain reads a stored chain field.
func decodeChain(s string) ([]workflow.Job, error) {
	if s == "" {
		return nil, nil
	}
	var docs []chainDoc
	if err := json.Unmarshal([]byte(s), &docs); err != nil {
		return nil, err
	}
	return fromChainDocs(docs), nil
}
