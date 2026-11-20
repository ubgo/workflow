package cutover

import (
	"context"
	"errors"
	"fmt"

	"github.com/ubgo/workflow"
)

// unfinished are the statuses of a run that has not ended: what the old
// backend still has to finish before it can be retired.
var unfinished = []workflow.Status{
	workflow.StatusScheduled, workflow.StatusQueued, workflow.StatusRunning,
	workflow.StatusWaiting, workflow.StatusRetrying,
}

// Remaining counts the runs on the old backend that have not finished
// (scheduled, queued, running, waiting or retrying), so the app knows when
// to retire it: at zero, and with MoveSchedules done (or Schedules
// listing nothing on "old:"), nothing more will run there. A run the app
// later retries with Retry re-opens there, so retire the old backend only
// when nothing will be retried. It pages through the old backend's
// RunQuerier, which it needs (an UnsupportedError naming CapRunQuery
// otherwise); a driver without CapRunStates reports retrying runs as
// queued or running, which are counted all the same.
func (c *Client) Remaining(ctx context.Context) (int, error) {
	q, ok := c.old.(workflow.RunQuerier)
	if !ok || !c.old.Capabilities().Has(workflow.CapRunQuery) {
		return 0, &workflow.UnsupportedError{Field: "Remaining", Missing: workflow.CapRunQuery, Reason: "the old backend lists no runs"}
	}
	statuses := unfinished
	if !c.old.Capabilities().Has(workflow.CapRunStates) {
		statuses = statuses[:len(statuses)-1]
	}
	n := 0
	cursor := ""
	for {
		page, err := q.Runs(ctx, workflow.RunQuery{Statuses: statuses, Limit: workflow.MaxRunsLimit, Cursor: cursor})
		if err != nil {
			return n, fmt.Errorf("cutover: Remaining: %w", err)
		}
		n += len(page.Runs)
		if page.Next == "" {
			return n, nil
		}
		cursor = page.Next
	}
}

// Moved is one schedule MoveSchedules moved: its ID on the old backend
// and on the new one (both prefixed, as this client names them).
type Moved struct {
	Name, Workflow string
	From, To       workflow.ScheduleID
}

// MoveSchedules moves the schedules the app made on the old backend
// (Client.Schedule before the cutover) to the new backend: for each one
// the old backend lists that is not a declared cron (those follow
// Register), it makes the same schedule on the new backend, pauses it
// there when it was paused, and then unschedules it on the old one. It
// returns what it moved, in the order listed.
//
// A ScheduleInfo does not carry the schedule's Input or its ScheduleWhen
// condition, so input returns the Input for each one (nil when the
// schedule has none) and the app sets a condition again itself; a nil
// input is refused with ErrInvalid rather than moving schedules without
// their payloads. It needs the old backend's ScheduleLister, and
// SchedulePauser on the new one for a paused schedule (an UnsupportedError
// otherwise); it refuses with ErrInvalid under SchedulesOn(SideOld), where
// the old backend is meant to keep them.
//
// It stops at the first failure and returns what it moved so far. A
// schedule is never left firing on both: when the old copy cannot be
// removed, the new copy is unscheduled again before the error is
// returned. Run it once, from one process: two runs at once could each
// copy a schedule before either removes it.
func (c *Client) MoveSchedules(ctx context.Context, input func(workflow.ScheduleInfo) ([]byte, error)) ([]Moved, error) {
	if c.sched != SideNew {
		return nil, fmt.Errorf("%w: cutover: MoveSchedules moves schedules to the new backend, and this client keeps them on the old one", workflow.ErrInvalid)
	}
	if input == nil {
		return nil, fmt.Errorf("%w: cutover: MoveSchedules needs the input func (a ScheduleInfo carries no Input)", workflow.ErrInvalid)
	}
	l, ok := c.old.(workflow.ScheduleLister)
	if !ok || !c.old.Capabilities().Has(workflow.CapScheduleList) {
		return nil, &workflow.UnsupportedError{Field: "MoveSchedules", Missing: workflow.CapScheduleList, Reason: "the old backend lists no schedules"}
	}
	infos, err := l.Schedules(ctx)
	if err != nil {
		return nil, fmt.Errorf("cutover: MoveSchedules: list the old backend: %w", err)
	}
	var moved []Moved
	for _, info := range infos {
		if info.Declared {
			continue
		}
		m, err := c.moveOne(ctx, info, input)
		if err != nil {
			return moved, fmt.Errorf("cutover: MoveSchedules: schedule %s (%q of %q): %w", info.ID, info.Name, info.Workflow, err)
		}
		moved = append(moved, m)
	}
	return moved, nil
}

func (c *Client) moveOne(ctx context.Context, info workflow.ScheduleInfo, input func(workflow.ScheduleInfo) ([]byte, error)) (Moved, error) {
	in, err := input(info)
	if err != nil {
		return Moved{}, err
	}
	var pauser workflow.SchedulePauser
	if info.Paused {
		p, ok := c.new.(workflow.SchedulePauser)
		if !ok || !c.new.Capabilities().Has(workflow.CapSchedulePause) {
			return Moved{}, &workflow.UnsupportedError{Field: "MoveSchedules", Missing: workflow.CapSchedulePause, Reason: "the schedule is paused and the new backend cannot pause it"}
		}
		pauser = p
	}
	s := workflow.Schedule{
		Name: info.Name, Workflow: info.Workflow, Cron: info.Cron, Location: info.Location, Input: in,
		Labels: info.Labels, Misfire: info.Misfire, Windows: info.Windows, Overlap: info.Overlap,
		Seconds: info.Seconds, Active: info.Active, KeepRuns: info.KeepRuns, Tenant: info.Tenant,
	}
	id, err := c.new.Schedule(ctx, s)
	if err != nil {
		return Moved{}, fmt.Errorf("schedule on the new backend: %w", err)
	}
	undo := func(cause error) error {
		if uerr := c.new.Unschedule(context.WithoutCancel(ctx), id); uerr != nil {
			return errors.Join(cause, fmt.Errorf("and removing the new copy %s failed, so it fires on both: %w", id, uerr))
		}
		return cause
	}
	if pauser != nil {
		if err := pauser.PauseSchedule(ctx, workflow.ScheduleTarget{ID: id}); err != nil {
			return Moved{}, undo(fmt.Errorf("pause the new copy: %w", err))
		}
	}
	if err := c.old.Unschedule(ctx, info.ID); err != nil {
		return Moved{}, undo(fmt.Errorf("unschedule on the old backend: %w", err))
	}
	return Moved{Name: info.Name, Workflow: info.Workflow, From: encodeScheduleID(SideOld, info.ID), To: encodeScheduleID(SideNew, id)}, nil
}
