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
//
//ds:def id=hatchet-client-result-vs7rxkce owner=@khanakia stability=stable
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
//
//ds:def id=hatchet-shapeworkflow-ugsy7nxq owner=@khanakia stability=stable
func shapeWorkflow(name string, d *rest.V1WorkflowRunDetails) workflow.Workflow {
	done := map[openapi_types.UUID]bool{}
	for _, t := range d.Tasks {
		done[t.TaskExternalId] = t.Status == rest.V1TaskStatusCOMPLETED
	}
	// The start gate is the driver's task, not a step.
	for _, sh := range d.Shape {
		if sh.TaskName == startGateTask {
			done[sh.TaskExternalId] = false
		}
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

// dueGuard is how close to its due time a delayed start may already be
// held by the engine. The engine's ticker polls every five seconds for
// scheduled runs due within the next five seconds and arms a timer for
// each; it notices a later delete only at its next poll, which comes at
// or after the due time, so the armed timer fires first and the run
// starts (internal/services/ticker in hatchet v0.110.5; the live probe
// TestLateCancelLive measured it with a plain delete: 3s before due let 6
// of 15 runs start, 1s before 12 of 15, 0.5s before 13 of 15, 5s and 10s
// before none). 15s is
// that lookahead plus room for clock skew between this host and the
// engine's database and for a slow poll. Reschedule refuses a start due
// within it; Cancel deletes such a start and then watches for the run it
// may still fire (settleLate).
//
//ds:def id=hatchet-dueguard-kj9nm3dg owner=@khanakia stability=stable
const dueGuard = 15 * time.Second

// errRescheduleGated refuses Reschedule of a delayed start with an
// idempotency key (see Client.Enqueue).
//
//ds:def id=hatchet-errreschedulegated-9akd6fmr owner=@khanakia stability=stable
var errRescheduleGated = workflow.Unsupported("Reschedule", "of a delayed start with an idempotency key: on Hatchet it is a started run whose start gate sleeps until RunAt, and the engine cannot move a durable sleep; Cancel it and Enqueue it again with the new RunAt (the cancel frees the key)")

// Reschedule moves a delayed start that has not fired to at
// (workflow.RunRescheduler); a zero or past at means now. The RunRef,
// payload, labels, priority and key are kept: the driver creates the
// scheduled run again at the new time with the same token and metadata,
// plus the new due time (metaRunAt), then deletes the old one, so the
// run's ScheduledID is unchanged. An at of now or earlier starts the run
// instead (startNow), so the ref returned carries its RunID as well. A
// run that has fired, or is due within dueGuard (15s), returns
// ErrWrongState; an unknown or cancelled one ErrNotFound. A delayed start
// with an idempotency key, which has a RunID and waits in its start gate,
// is refused with an UnsupportedError.
//
//ds:def id=hatchet-client-reschedule-3nfqbter owner=@khanakia stability=stable
func (c *Client) Reschedule(ctx context.Context, ref workflow.RunRef, at time.Time) (workflow.RunRef, error) {
	sdk, err := c.ready()
	if err != nil {
		return workflow.RunRef{}, err
	}
	if ref.ScheduledID.IsZero() {
		if ref.RunID.IsZero() {
			return workflow.RunRef{}, fmt.Errorf("%w: empty RunRef", workflow.ErrNotFound)
		}
		// A delayed start with an idempotency key is a run whose start
		// gate sleeps until RunAt; the engine cannot move a durable sleep.
		if d, err := runDetailsLagged(ctx, sdk, ref.RunID.String(), c.cfg.VisibilityLag); err == nil {
			if st, _ := c.endStatus(d); st == workflow.StatusScheduled {
				return workflow.RunRef{}, errRescheduleGated
			}
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
	if old.TriggerAt.Sub(now) < dueGuard {
		return workflow.RunRef{}, fmt.Errorf("%w: scheduled run %s is due within %s and may already be firing", workflow.ErrWrongState, ref.ScheduledID, dueGuard)
	}
	if !at.After(now) {
		return c.startNow(ctx, sdk, ref, old, now)
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

// startNow is Reschedule to now or earlier. A scheduled run fires only
// from the engine's ticker, so creating it again at now would return
// before the run exists and the ref could not carry its RunID (the
// workflow.RunRescheduler promise). Instead the scheduled run is deleted
// (old is at least dueGuard from firing, so the ticker cannot start it)
// and the run is started at once by name with the same input, priority
// and metadata, the token included, so the ScheduledID still resolves to
// it (findRunByToken) and RunInfo.RunAt reads now (metaRunAt). If the
// start fails, the scheduled run is created again at its old time; if
// that fails too, the error says the start is gone and must be enqueued
// again.
//
//ds:def id=hatchet-client-startnow-upqvz45e owner=@khanakia stability=stable
func (c *Client) startNow(ctx context.Context, sdk *hatchetsdk.Client, ref workflow.RunRef, old *rest.ScheduledWorkflows, now time.Time) (workflow.RunRef, error) {
	if err := deleteScheduled(ctx, sdk, old.Metadata.Id); err != nil {
		return workflow.RunRef{}, engineErr(err, "reschedule: delete the run at the old time")
	}
	var input map[string]any
	if old.Input != nil {
		input = map[string]any(*old.Input)
	}
	md := metadataStrings(old.AdditionalMetadata)
	md[metaRunAt] = strconv.FormatInt(now.UnixMilli(), 10)
	opts := []hatchetsdk.RunOptFunc{hatchetsdk.WithRunMetadata(md)}
	if old.Priority != nil {
		opts = append(opts, hatchetsdk.WithRunPriority(hatchetsdk.RunPriority(*old.Priority)))
	}
	run, err := sdk.RunNoWait(ctx, old.WorkflowName, input, opts...)
	if err == nil {
		return workflow.RunRef{RunID: workflow.RunID(run.RunId), ScheduledID: ref.ScheduledID}, nil
	}
	cause := engineErr(err, "reschedule: start the run now")
	back := features.CreateScheduledRunTrigger{
		TriggerAt:          old.TriggerAt.UTC(),
		Input:              input,
		AdditionalMetadata: labelsAny(metadataStrings(old.AdditionalMetadata)),
	}
	if old.Priority != nil {
		p := features.RunPriority(*old.Priority)
		back.Priority = &p
	}
	if _, rerr := sdk.Schedules().Create(ctx, old.WorkflowName, back); rerr != nil {
		return workflow.RunRef{}, fmt.Errorf("%w; putting the start back at its old time failed too (%v), so the run will not start: Enqueue it again", cause, rerr)
	}
	return workflow.RunRef{}, cause
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
