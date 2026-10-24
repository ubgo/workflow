package hatchet

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/hatchet-dev/hatchet/pkg/client/rest"
	hatchetsdk "github.com/hatchet-dev/hatchet/sdks/go"
	openapi_types "github.com/oapi-codegen/runtime/types"

	"github.com/ubgo/workflow"
)

// A failed attempt with retries left is reported FAILED for a moment.
// Between a failed attempt and its retry the engine (hatchet-lite
// v0.110.5, read through SDK v0.107.1) shows the task, and often the run,
// as FAILED with finishedAt set, then QUEUED with retryCount one higher
// (a probe polling every 10ms saw it in about one retry in four; during
// a retry backoff the task is QUEUED, so FAILED is only that moment).
// Taken at face value that FAILED ends a run early: Status, Runs, the
// run-finished poller and BatchSource would report a failure, and fire a
// batch's Catch, for a run that then succeeds. The run details carry no
// event that tells the two apart (TaskEvents is empty, and the REST task
// event list returned no rows on hatchet-lite even for a failure that
// had ended), so endStatus decides from what the details hold:
//
//   - a failure the driver made non-retryable carries finalMarker in its
//     message (finalErr): final;
//   - a task whose retryCount reached the Retries the engine registered
//     for it (WorkflowConfig): final;
//   - a failed task that finished more than Config.FailedSettle ago:
//     final (an engine-side failure the engine does not retry, or a
//     Permanent failure of a run written before the marker existed);
//   - anything else is the moment before a retry: the run reads Running
//     when a task runs, else Queued, as it does between attempts.

// finalMarker prefixes the message of every failure the driver marks
// non-retryable. It is stripped (stripFinal) wherever the driver hands a
// run's error back, so callers see the step's own message.
const finalMarker = "[ubgo.workflow: not retried] "

// DefaultFailedSettle is how long after a failed task finished a FAILED
// read with retries left and no marker is still taken as the moment
// before a retry; see Config.FailedSettle. The moment seen live is tens
// of milliseconds; the margin covers clock skew between the engine and
// this process, since the task's finish time is the engine's.
const DefaultFailedSettle = 5 * time.Second

// markedError is a step error the driver ends without retry, with
// finalMarker in front of its message so a reader of the run can tell.
type markedError struct{ err error }

func (e markedError) Error() string { return finalMarker + e.err.Error() }
func (e markedError) Unwrap() error { return e.err }

// finalErr makes err non-retryable on the engine and marks it final.
func finalErr(err error) error {
	return hatchetsdk.NewNonRetryableError(markedError{err})
}

// stripFinal removes finalMarker from a stored error message.
func stripFinal(msg string) string {
	return strings.Replace(msg, finalMarker, "", 1)
}

// endStatus is the run's status from its details, and the one place a
// FAILED read is judged (see the top of this file): a final failure is
// StatusFailed, the moment before a retry is StatusRunning or
// StatusQueued. Every other engine status maps as runStatus does. now is
// wall-clock, compared with the engine's own finish times.
func endStatus(d *rest.V1WorkflowRunDetails, settle time.Duration, now time.Time) (workflow.Status, error) {
	st, err := runStatus(d.Run.Status)
	if err != nil || st != workflow.StatusFailed || failureFinal(d, settle, now) {
		return st, err
	}
	for _, t := range d.Tasks {
		if t.Status == rest.V1TaskStatusRUNNING {
			return workflow.StatusRunning, nil
		}
	}
	return workflow.StatusQueued, nil
}

// failureFinal reports whether a run read FAILED has ended: one of its
// failed tasks is marked final, used its registered retries, or failed
// more than settle ago. A run FAILED with no failed task (the task
// already re-queued) has not ended.
func failureFinal(d *rest.V1WorkflowRunDetails, settle time.Duration, now time.Time) bool {
	budgets := retryBudgets(d)
	names := make(map[openapi_types.UUID]string, len(d.Shape))
	for _, sh := range d.Shape {
		names[sh.TaskExternalId] = sh.TaskName
	}
	for _, t := range d.Tasks {
		if t.Status != rest.V1TaskStatusFAILED {
			continue
		}
		if t.ErrorMessage != nil && strings.Contains(*t.ErrorMessage, finalMarker) {
			return true
		}
		if budget, ok := budgets[names[t.TaskExternalId]]; ok && t.RetryCount != nil && *t.RetryCount >= budget {
			return true
		}
		if t.FinishedAt != nil && now.Sub(*t.FinishedAt) >= settle {
			return true
		}
	}
	return false
}

// endStatus is the package endStatus with this client's settle window
// and the wall clock (the engine's finish times are wall-clock).
func (c *Client) endStatus(d *rest.V1WorkflowRunDetails) (workflow.Status, error) {
	return endStatus(d, c.cfg.FailedSettle, time.Now())
}

// readEndStatus reads run id and judges it with endStatus.
func (c *Client) readEndStatus(ctx context.Context, sdk *hatchetsdk.Client, id string) (workflow.Status, *rest.V1WorkflowRunDetails, error) {
	d, err := sdk.Runs().Get(ctx, id)
	if err == nil && d == nil {
		err = errors.New("hatchet driver: engine returned no run")
	}
	if err != nil {
		return "", nil, err
	}
	st, err := c.endStatus(d)
	return st, d, err
}

// retryBudgets reads each task's registered Retries from the run's
// workflow config, keyed by task name. The config is a loose JSON map in
// the SDK, so a shape the driver does not expect yields no budget, which
// sends the run through the settle window instead of misreading it.
func retryBudgets(d *rest.V1WorkflowRunDetails) map[string]int {
	out := map[string]int{}
	if d.WorkflowConfig == nil {
		return out
	}
	tasks, _ := (*d.WorkflowConfig)["Tasks"].([]any)
	for _, raw := range tasks {
		t, _ := raw.(map[string]any)
		name, _ := t["ReadableId"].(string)
		n, ok := t["Retries"].(float64)
		if name != "" && ok {
			out[name] = int(n)
		}
	}
	return out
}
