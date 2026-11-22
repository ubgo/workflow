package middleware

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"

	"github.com/ubgo/workflow"
)

// ValidatePayload is enqueue middleware that checks each job's payload
// against the rule for its workflow before anything reaches the driver,
// so a malformed job is refused at the call site, where the caller can
// still fix it, instead of failing on a worker later. rules maps a
// workflow name to a check; a workflow without a rule (or with a nil
// one) passes. The jobs of Job.Chain are checked too, since they are
// enqueued later by the driver and never pass through middleware. A
// failing rule refuses the enqueue with an error wrapping
// workflow.ErrInvalid and the rule's error. The map is copied.
//
// Place it before WithCodec or ClaimCheck in the chain (WrapClient
// outside WithCodec), so it sees the payload the app wrote, not the
// encrypted or offloaded bytes. It runs on every driver.
//
//ds:def id=mw-validatepayload-jyms93jq owner=@khanakia stability=stable
func ValidatePayload(rules map[string]func([]byte) error) workflow.EnqueueMiddleware {
	table := maps.Clone(rules)
	var check func(job workflow.Job) error
	check = func(job workflow.Job) error {
		if rule := table[job.Workflow]; rule != nil {
			if err := rule(job.Payload); err != nil {
				return fmt.Errorf("%w: payload of workflow %q: %w", workflow.ErrInvalid, job.Workflow, err)
			}
		}
		for _, next := range job.Chain {
			if err := check(next); err != nil {
				return err
			}
		}
		return nil
	}
	return func(next workflow.EnqueueFunc) workflow.EnqueueFunc {
		return func(ctx context.Context, job workflow.Job) (workflow.RunRef, error) {
			if err := check(job); err != nil {
				return workflow.RunRef{}, err
			}
			return next(ctx, job)
		}
	}
}

// Validator is what JSONRule calls after decoding, when the payload type
// has it: the type's own rules (a positive amount, a known currency).
type Validator interface {
	Validate() error
}

// JSONRule returns a payload rule for ValidatePayload: the payload must
// decode as JSON into a T (encoding/json rules: unknown fields are
// ignored, an empty payload is an error), and when *T implements
// Validator, its Validate must return nil.
//
//ds:def id=mw-jsonrule-cgt26zfs owner=@khanakia stability=stable
func JSONRule[T any]() func([]byte) error {
	return func(payload []byte) error {
		var v T
		if err := json.Unmarshal(payload, &v); err != nil {
			return fmt.Errorf("decode as %T: %w", v, err)
		}
		if val, ok := any(&v).(Validator); ok {
			return val.Validate()
		}
		return nil
	}
}

// ValidateJSON is ValidatePayload with one rule, JSONRule[T], for the
// jobs of workflowName: their payload must decode as a T and pass its
// Validate method when it has one. Jobs of other workflows pass. Use one
// ValidatePayload with a JSONRule per workflow to check several.
func ValidateJSON[T any](workflowName string) workflow.EnqueueMiddleware {
	return ValidatePayload(map[string]func([]byte) error{workflowName: JSONRule[T]()})
}
