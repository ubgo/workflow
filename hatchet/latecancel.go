package hatchet

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/hatchet-dev/hatchet/pkg/client/rest"
	hatchetsdk "github.com/hatchet-dev/hatchet/sdks/go"
	openapi_types "github.com/oapi-codegen/runtime/types"

	"github.com/ubgo/workflow"
)

// ErrCancelTooLate is returned by Cancel of a delayed start the engine
// fired although Cancel deleted it before its due time: the engine had
// already loaded it into its ticker (see dueGuard). Cancel has then
// cancelled the run it fired, unless that run had already ended; the
// error says which, and the start's RunRef now resolves to that run. It
// also matches workflow.ErrWrongState. A nil Cancel of a delayed start
// means the engine did not fire it and will not.
//
//ds:def id=hatchet-errcanceltoolate-s9c8yhrj owner=@khanakia stability=stable
var ErrCancelTooLate = errors.New("hatchet driver: delayed start fired before the cancel reached the engine")

// lateWatchPoll is how often a late cancel lists the run it watches for.
const lateWatchPoll = 250 * time.Millisecond

// lateStart is a delayed start whose cancel cannot be trusted yet: Cancel
// deleted its scheduled record within dueGuard of its due time, or found
// the record already gone, so the engine may still fire (or have fired)
// it. settleLate watches the run list for it until deadline (its due time
// plus Config.VisibilityLag).
type lateStart struct {
	ref      workflow.RunRef
	sid      scheduledRef
	sched    *rest.ScheduledWorkflows // the record Cancel deleted; nil when it was gone before the delete
	deadline time.Time
	runID    string          // the run the engine fired anyway, once seen
	ended    workflow.Status // the run's status when it had ended before the cancel; "" when Cancel cancelled it
}

// err is the error Cancel returns for a late start that fired.
func (s *lateStart) err() error {
	what := "Cancel cancelled that run"
	if s.ended != "" {
		what = fmt.Sprintf("that run had already ended %s", s.ended)
	}
	return fmt.Errorf("%w: %w: scheduled run %s was fired by the engine as run %s; %s", ErrCancelTooLate, workflow.ErrWrongState, s.ref.ScheduledID, s.runID, what)
}

// cancelRef is Cancel without the wait for late starts: it cancels a run,
// or deletes a pending delayed start. A start deleted at least dueGuard
// before its due time is final: it is recorded in deletedStarts, reported
// to run-finished observers, and no lateStart is returned. Otherwise the
// returned lateStart must go through settleLate before the cancel is
// known to have held.
//
//ds:def id=hatchet-client-cancelref-55mg6gda owner=@khanakia stability=stable
func (c *Client) cancelRef(ctx context.Context, sdk *hatchetsdk.Client, ref workflow.RunRef) (*lateStart, error) {
	runID, state, err := c.resolve(ctx, sdk, ref)
	if err != nil {
		return nil, err
	}
	switch state {
	case stateGone:
		return nil, nil
	case stateRun:
		_, err := c.cancelRun(ctx, sdk, runID)
		return nil, err
	}
	sid, _ := parseScheduledID(ref.ScheduledID) // parsed in resolve
	sched, err := findSchedule(ctx, sdk, sid.token)
	if err != nil {
		return nil, err
	}
	now := time.Now() // compared with the engine's due time: wall clock
	if sched == nil {
		// Fired or deleted elsewhere since resolve: watch for its run.
		return &lateStart{ref: ref, sid: sid, deadline: now.Add(c.cfg.VisibilityLag)}, nil
	}
	err = sdk.Schedules().Delete(ctx, sched.Metadata.Id)
	if err != nil && !isNotFound(err) && !isNoContent(err) {
		return nil, engineErr(err, "delete scheduled run")
	}
	due := sched.TriggerAt
	if due.Before(now) {
		due = now
	}
	if isNotFound(err) {
		// Gone under the delete: it may have just fired. Not this
		// client's delete, so not reported cancelled by it either.
		return &lateStart{ref: ref, sid: sid, deadline: due.Add(c.cfg.VisibilityLag)}, nil
	}
	if sched.TriggerAt.Sub(now) >= dueGuard {
		// Deleted before the engine could load it to fire: final.
		c.deleted.add(sid.token)
		c.cancelledBeforeFire(ref, sched)
		return nil, nil
	}
	return &lateStart{ref: ref, sid: sid, sched: sched, deadline: due.Add(c.cfg.VisibilityLag)}, nil
}

// settleLate watches each late start until its deadline for the run the
// engine may fire whatever the delete said. A start that fired gets its
// run cancelled as Cancel cancels any run (runID and ended are set). A
// start with no run listed by its deadline will not run: it is recorded
// in deletedStarts and, when this client's delete removed it, reported
// cancelled to run-finished observers. All are watched together, so
// CancelAll waits once, not once per start.
func (c *Client) settleLate(ctx context.Context, sdk *hatchetsdk.Client, late []*lateStart) error {
	open := late
	for len(open) > 0 {
		var next []*lateStart
		for _, s := range open {
			now := time.Now() // before the list: a run listed later is not missed
			runID, err := findRunByToken(ctx, sdk, s.sid)
			if err != nil {
				return err
			}
			switch {
			case runID != "":
				s.runID = runID
				if s.ended, err = c.cancelRun(ctx, sdk, runID); err != nil {
					return err
				}
			case now.After(s.deadline):
				c.deleted.add(s.sid.token)
				if s.sched != nil {
					c.cancelledBeforeFire(s.ref, s.sched)
				}
			default:
				next = append(next, s)
			}
		}
		open = next
		if len(open) == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("hatchet driver: cancel of delayed start %s: deleted within %s of its due time, the engine may still fire it: %w", open[0].ref.ScheduledID, dueGuard, ctx.Err())
		case <-time.After(lateWatchPoll):
		}
	}
	return nil
}

// cancelRun asks the engine to cancel a run. A run that has already ended
// is left alone and its status returned; "" means the cancel was sent.
func (c *Client) cancelRun(ctx context.Context, sdk *hatchetsdk.Client, runID string) (workflow.Status, error) {
	st, err := c.Status(ctx, workflow.RunRef{RunID: workflow.RunID(runID)})
	if err != nil {
		return "", err
	}
	if st.IsTerminal() {
		return st, nil
	}
	id, err := uuid.Parse(runID)
	if err != nil {
		return "", fmt.Errorf("%w: run %q", workflow.ErrNotFound, runID)
	}
	ids := []openapi_types.UUID{id}
	if _, err := sdk.Runs().Cancel(ctx, rest.V1CancelTaskRequest{ExternalIds: &ids}); err != nil {
		return "", engineErr(err, "cancel")
	}
	return "", nil
}
