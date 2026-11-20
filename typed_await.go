package workflow

import (
	"context"
	"encoding/json"
	"fmt"
)

// EnqueueOf enqueues v, encoded as JSON, as the payload of a job of
// workflow (DE-17): the typed counterpart of Enqueue, with JobOf's
// encoding. edit, when set, adjusts the job (labels, key, RunAt, queue)
// before it is sent.
func EnqueueOf[T any](ctx context.Context, c Client, workflow string, v T, edit func(*Job)) (RunRef, error) {
	job, err := JobOf(workflow, v)
	if err != nil {
		return RunRef{}, err
	}
	if edit != nil {
		edit(&job)
	}
	return c.Enqueue(ctx, job)
}

// AwaitResultOf waits for the run like AwaitResult and decodes its result
// as T (DE-17), so a typed workflow (Define) is typed from enqueue to
// result. A result that does not decode into T is an error naming the
// run; a run that did not succeed returns AwaitResult's error. It needs
// CapResult, as AwaitResult does.
func AwaitResultOf[T any](ctx context.Context, c Client, ref RunRef, opts AwaitOptions) (T, error) {
	var zero T
	raw, err := AwaitResult(ctx, c, ref, opts)
	if err != nil {
		return zero, err
	}
	var v T
	if err := json.Unmarshal(raw, &v); err != nil {
		return zero, fmt.Errorf("workflow: result of run %v does not decode: %w", ref, err)
	}
	return v, nil
}
