package hatchet

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/hatchet-dev/hatchet/pkg/client/rest"
	hatchetsdk "github.com/hatchet-dev/hatchet/sdks/go"
	"github.com/hatchet-dev/hatchet/sdks/go/features"
	openapi_types "github.com/oapi-codegen/runtime/types"

	"github.com/ubgo/workflow"
)

var (
	_ workflow.ResultReader   = (*Client)(nil)
	_ workflow.RunRescheduler = (*Client)(nil)
)

// Result returns the result of a succeeded run (workflow.ResultReader):
// workflow.RunOutput of its final steps' outputs, which the engine keeps
// with the run's tasks for as long as it keeps the run. A run that has
// not succeeded returns ErrWrongState, an unknown one ErrNotFound.
func (c *Client) Result(ctx context.Context, ref workflow.RunRef) ([]byte, error) {
	sdk, err := c.ready()
	if err != nil {
		return nil, err
	}
	runID, state, err := c.resolve(ctx, sdk, ref)
	if err != nil {
		return nil, err
	}
	switch state {
	case statePending:
		return nil, fmt.Errorf("%w: run %s is scheduled, not succeeded", workflow.ErrWrongState, ref)
	case stateGone:
		return nil, fmt.Errorf("%w: run %s was cancelled before it started", workflow.ErrWrongState, ref)
	}
	d, err := runDetailsLagged(ctx, sdk, runID, c.cfg.VisibilityLag)
	if isNotFound(err) {
		return nil, fmt.Errorf("%w: run %q", workflow.ErrNotFound, runID)
	}
	if err != nil {
		return nil, engineErr(err, "read run")
	}
	st, err := c.endStatus(d)
	if err != nil {
		return nil, err
	}
	if st != workflow.StatusSucceeded {
		return nil, fmt.Errorf("%w: run %s is %s, not succeeded", workflow.ErrWrongState, runID, st)
	}
	if d.Run.PayloadsRestricted != nil && *d.Run.PayloadsRestricted {
		return nil, errors.New("hatchet driver: result: the engine withholds payloads from this token")
	}
	out, ok := c.runOutput(metadataStrings(d.Run.AdditionalMetadata)[metaWorkflow], d)
	if !ok {
		return nil, fmt.Errorf("hatchet driver: result of run %s: a final step's output is missing", runID)
	}
	return out, nil
}

// runOutput is workflow.RunOutput of succeeded run d of workflow name. A
// workflow registered here gives its steps; for any other the run's own
// shape does (the tasks no completed task lists as a parent, among the
// completed ones, which leaves out an on-failure task that never ran). ok
// is false when a final step's output cannot be read.
func (c *Client) runOutput(name string, d *rest.V1WorkflowRunDetails) ([]byte, bool) {
	outs := map[string][]byte{}
	byTask := map[openapi_types.UUID]rest.V1TaskSummary{}
	for _, t := range d.Tasks {
		byTask[t.TaskExternalId] = t
	}
	for _, sh := range d.Shape {
		t, ok := byTask[sh.TaskExternalId]
		if !ok || t.Status != rest.V1TaskStatusCOMPLETED {
			continue
		}
		out, err := decodeEnvelope(map[string]any(t.Output), outputField)
		if err != nil {
			return nil, false
		}
		outs[sh.TaskName] = out
	}
	c.mu.Lock()
	spec, registered := c.specs[name]
	c.mu.Unlock()
	if !registered {
		spec = shapeWorkflow(name, d)
	}
	for _, sink := range workflow.Sinks(spec) {
		if _, ok := outs[sink]; !ok {
			return nil, false
		}
	}
	return workflow.RunOutput(spec, outs), true
}

// shapeWorkflow rebuilds the step graph of run d from the engine's shape
// (task names and children), keeping only completed tasks, for a
// workflow this client did not register. The steps are in name order.
func shapeWorkflow(name string, d *rest.V1WorkflowRunDetails) workflow.Workflow {
	done := map[openapi_types.UUID]bool{}
	for _, t := range d.Tasks {
		done[t.TaskExternalId] = t.Status == rest.V1TaskStatusCOMPLETED
	}
	byStep := map[openapi_types.UUID]string{}
	for _, sh := range d.Shape {
		if done[sh.TaskExternalId] {
			byStep[sh.StepId] = sh.TaskName
		}
	}
	wf := workflow.Workflow{Name: name}
	after := map[string][]string{}
	for _, sh := range d.Shape {
		if !done[sh.TaskExternalId] {
			continue
		}
		for _, child := range sh.ChildrenStepIds {
			if cn, ok := byStep[child]; ok {
				after[cn] = append(after[cn], sh.TaskName)
			}
		}
	}
	for _, sh := range d.Shape {
		if done[sh.TaskExternalId] {
			wf.Steps = append(wf.Steps, workflow.Step{Name: sh.TaskName, After: after[sh.TaskName]})
		}
	}
	// The shape's order is the engine's, not the declaration's, so the
	// steps are put in name order: a result with several final steps then
	// has its keys in name order on every unregistered client.
	slices.SortFunc(wf.Steps, func(a, b workflow.Step) int { return strings.Compare(a.Name, b.Name) })
	return wf
}

// rescheduleGuard is how close to its due time a delayed start may no
// longer be moved. The engine loads a scheduled run into its ticker
// about five seconds before the run is due (polling every five seconds)
// and then fires it at the time it loaded, whatever a later change says
// (internal/services/ticker in hatchet v0.110.5), so a move inside that
// window could leave the run firing at the old time.
const rescheduleGuard = 15 * time.Second

// Reschedule moves a delayed start that has not fired to at
// (workflow.RunRescheduler); a zero or past at means now. The RunRef,
// payload, labels, priority and key are kept: the driver creates the
// scheduled run again at the new time with the same token and metadata,
// plus the new due time (metaRunAt), then deletes the old one, so the
// run's ScheduledID is unchanged. A run that has fired, or is due within
// rescheduleGuard (15s), returns ErrWrongState; an unknown or cancelled
// one ErrNotFound.
func (c *Client) Reschedule(ctx context.Context, ref workflow.RunRef, at time.Time) (workflow.RunRef, error) {
	sdk, err := c.ready()
	if err != nil {
		return workflow.RunRef{}, err
	}
	if ref.ScheduledID.IsZero() {
		if ref.RunID.IsZero() {
			return workflow.RunRef{}, fmt.Errorf("%w: empty RunRef", workflow.ErrNotFound)
		}
		return workflow.RunRef{}, fmt.Errorf("%w: run %s has started", workflow.ErrWrongState, ref.RunID)
	}
	sid, ok := parseScheduledID(ref.ScheduledID)
	if !ok {
		return workflow.RunRef{}, fmt.Errorf("%w: scheduled run %q", workflow.ErrNotFound, ref.ScheduledID)
	}
	old, err := findSchedule(ctx, sdk, sid.token)
	if err != nil {
		return workflow.RunRef{}, err
	}
	if old == nil || old.WorkflowRunId != nil {
		runID, state, err := c.resolve(ctx, sdk, ref)
		switch {
		case err != nil:
			return workflow.RunRef{}, err
		case state == stateRun:
			return workflow.RunRef{}, fmt.Errorf("%w: run %s has started", workflow.ErrWrongState, runID)
		}
		return workflow.RunRef{}, fmt.Errorf("%w: scheduled run %q was cancelled", workflow.ErrNotFound, ref.ScheduledID)
	}
	now := time.Now() // compared with the engine's due time: wall clock
	if old.TriggerAt.Sub(now) < rescheduleGuard {
		return workflow.RunRef{}, fmt.Errorf("%w: scheduled run %s is due within %s and may already be firing", workflow.ErrWrongState, ref.ScheduledID, rescheduleGuard)
	}
	if at.Before(now) {
		at = now
	}
	md := metadataStrings(old.AdditionalMetadata)
	md[metaRunAt] = strconv.FormatInt(at.UnixMilli(), 10)
	trigger := features.CreateScheduledRunTrigger{
		TriggerAt:          at.UTC(), // the engine stores triggerAt without a zone (Enqueue)
		AdditionalMetadata: labelsAny(md),
	}
	if old.Input != nil {
		trigger.Input = map[string]any(*old.Input)
	}
	if old.Priority != nil {
		p := features.RunPriority(*old.Priority)
		trigger.Priority = &p
	}
	if _, err := sdk.Schedules().Create(ctx, old.WorkflowName, trigger); err != nil {
		return workflow.RunRef{}, engineErr(err, "reschedule: create the run at the new time")
	}
	if err := deleteScheduled(ctx, sdk, old.Metadata.Id); err != nil {
		return workflow.RunRef{}, c.undoReschedule(ctx, sdk, sid.token, old.Metadata.Id, err)
	}
	return ref, nil
}

// undoReschedule removes the copy Reschedule created when the old
// scheduled run could not be deleted, so the run fires once, at its old
// time; err is the delete's error. If the copy cannot be removed either,
// both are left and the error says so: the run would fire twice.
func (c *Client) undoReschedule(ctx context.Context, sdk *hatchetsdk.Client, token, oldID string, err error) error {
	cause := engineErr(err, "reschedule: delete the run at the old time")
	limit := int64(10)
	md := []string{metaScheduleToken + ":" + token}
	list, lerr := sdk.Schedules().List(ctx, rest.WorkflowScheduledListParams{Limit: &limit, AdditionalMetadata: &md})
	if lerr != nil || list.Rows == nil {
		return fmt.Errorf("%w; the new copy could not be found to remove, so the run may fire twice", cause)
	}
	for _, row := range *list.Rows {
		if row.Metadata.Id == oldID {
			continue
		}
		if derr := deleteScheduled(ctx, sdk, row.Metadata.Id); derr != nil {
			return fmt.Errorf("%w; removing the new copy failed too (%v), so the run may fire twice", cause, derr)
		}
	}
	return cause
}

// deleteScheduled deletes one scheduled run, treating the 204 the SDK
// reports as an error as success.
func deleteScheduled(ctx context.Context, sdk *hatchetsdk.Client, id string) error {
	if err := sdk.Schedules().Delete(ctx, id); err != nil && !isNoContent(err) {
		return err
	}
	return nil
}
