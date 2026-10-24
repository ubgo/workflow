package admin

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
)

// Operation names one thing Admin can do. The names are part of the JSON
// the capabilities route returns, so they never change once released.
type Operation string

// The operations. Each needs the capabilities and optional interface
// listed in Requirements.
const (
	OpRuns      Operation = "runs"       // list and search runs (RunQuerier)
	OpRun       Operation = "run"        // show one run (RunQuerier)
	OpFailures  Operation = "failures"   // list failed runs (RunQuerier)
	OpDiscarded Operation = "discarded"  // list discarded runs (RunQuerier, CapRunStates)
	OpQueues    Operation = "queues"     // queue depth and oldest wait (RunQuerier)
	OpRetry     Operation = "retry"      // retry a failed run from its failed step (RunRetrier)
	OpDelete    Operation = "delete"     // forget a finished run (RunDeleter)
	OpCancel    Operation = "cancel"     // cancel one run (Client.Cancel)
	OpCancelAll Operation = "cancel_all" // cancel every run matching a filter (BulkCanceller)
	OpPause     Operation = "pause"      // pause, resume and list pauses (Pauser)
	OpSchedules Operation = "schedules"  // list schedules with their next fire time (ScheduleLister)
	// OpSchedulePause pauses and resumes one schedule (SchedulePauser).
	OpSchedulePause Operation = "schedule_pause"
	// OpScheduleTrigger runs one schedule now, outside its cron
	// (ScheduleTriggerer).
	OpScheduleTrigger Operation = "schedule_trigger"
	// OpForce ends a run from outside: force-complete or force-fail
	// (RunForcer).
	OpForce Operation = "force"
	// OpRetryAll retries every failed run a filter matches (RunQuerier
	// and RunRetrier).
	OpRetryAll Operation = "retry_all"
	// OpDeleteAll forgets every finished run a filter matches (RunQuerier
	// and RunDeleter).
	OpDeleteAll Operation = "delete_all"
	// OpNodes lists the nodes that are online (NodeLister).
	OpNodes Operation = "nodes"
	// OpHealth reads the client's health report (workflow.CheckHealth);
	// every client supports it, one without HealthChecker saying it
	// cannot tell.
	OpHealth Operation = "health"
	// OpReschedule moves a delayed run to a new time (RunRescheduler).
	OpReschedule Operation = "reschedule"
	// OpCancelStep withdraws one step of a run (StepCanceller).
	OpCancelStep Operation = "cancel_step"
)

// Operations lists every operation in a fixed order, the order Support
// reports them in.
var Operations = []Operation{OpRuns, OpRun, OpFailures, OpDiscarded, OpQueues, OpRetry, OpDelete, OpCancel, OpCancelAll, OpPause, OpSchedules, OpSchedulePause, OpScheduleTrigger, OpForce, OpRetryAll, OpDeleteAll, OpNodes, OpHealth, OpReschedule, OpCancelStep}

// requirements is the capability each operation needs. OpPause needs
// either pause capability; which one a call needs depends on its target
// (workflow.RequiresPause).
var requirements = map[Operation]workflow.Caps{
	OpRuns:      workflow.CapRunQuery,
	OpRun:       workflow.CapRunQuery,
	OpFailures:  workflow.CapRunQuery,
	OpDiscarded: workflow.CapRunQuery | workflow.CapRunStates,
	OpQueues:    workflow.CapRunQuery,
	OpRetry:     workflow.CapRetryRun,
	OpDelete:    workflow.CapDeleteRun,
	OpCancel:    workflow.CapCancel,
	OpCancelAll: workflow.CapCancelAll,
	OpPause:     workflow.CapPauseWorkflow | workflow.CapPauseQueue,
	OpSchedules: workflow.CapScheduleList,
	// Pausing one schedule from a list needs the list too, since the ID
	// a screen sends comes from it.
	OpSchedulePause: workflow.CapScheduleList | workflow.CapSchedulePause,
	// Triggering one schedule from a list needs the list for the same
	// reason.
	OpScheduleTrigger: workflow.CapScheduleList | workflow.CapScheduleTrigger,
	OpForce:           workflow.CapForceRun,
	// The bulk operations list the matches first, so they need the query
	// as well as the per-run action.
	OpRetryAll:  workflow.CapRunQuery | workflow.CapRetryRun,
	OpDeleteAll: workflow.CapRunQuery | workflow.CapDeleteRun,
	// OpNodes, OpHealth, OpReschedule and OpCancelStep have no capability: their
	// interfaces answer for themselves and refuse at the call (a wrapper
	// returns an UnsupportedError), so only the interface is checked.
	OpNodes:      0,
	OpHealth:     0,
	OpReschedule: 0,
	OpCancelStep: 0,
}

// Requirements returns the capabilities op needs. OpPause reports both
// pause capabilities; a client with either one supports pausing the
// targets that capability covers.
func Requirements(op Operation) workflow.Caps { return requirements[op] }

// DefaultMaxScan bounds how many queued runs QueueStats reads per call, so
// a queue of a million runs costs a bounded number of RunQuerier pages.
const DefaultMaxScan = 10_000

// Option configures New.
type Option func(*Admin)

// WithClock sets the clock QueueStats measures wait times with and the
// Monitor polls on. Nil means clock.Real.
func WithClock(clk clock.Clock) Option { return func(a *Admin) { a.clk = clk } }

// WithMaxScan bounds how many queued runs QueueStats reads per call; past
// it the stats are marked Truncated. Zero or less keeps DefaultMaxScan.
func WithMaxScan(n int) Option { return func(a *Admin) { a.maxScan = n } }

// Admin is the operations an admin screen or a CLI needs, over any
// workflow.Client. It holds no state of its own: every call goes to the
// client, so several Admins over one store agree.
//
// Each operation checks the client's capabilities first and then its
// optional interface, and refuses with a *workflow.UnsupportedError (which
// matches workflow.ErrUnsupported) naming the operation and the missing
// capabilities when either is absent. The capability is checked because a
// wrapper implements every optional interface and refuses at the call
// when the client it wraps lacks it.
type Admin struct {
	c       workflow.Client
	clk     clock.Clock
	maxScan int
	maxBulk int
	outputs OutputSource  // nil: no captured output is shown
	metrics MetricsSource // nil: no historical charts
}

// New returns an Admin over c. It never fails: what c cannot do is
// reported by Support and refused per call.
func New(c workflow.Client, opts ...Option) *Admin {
	a := &Admin{c: c}
	for _, o := range opts {
		if o != nil {
			o(a)
		}
	}
	if a.clk == nil {
		a.clk = clock.Real{}
	}
	if a.maxScan <= 0 {
		a.maxScan = DefaultMaxScan
	}
	if a.maxBulk <= 0 {
		a.maxBulk = DefaultMaxBulk
	}
	return a
}

// Client returns the client Admin works on.
func (a *Admin) Client() workflow.Client { return a.c }

// OperationSupport says whether the client supports one operation, and
// which capabilities it lacks when it does not.
type OperationSupport struct {
	Operation Operation
	Supported bool
	// Missing is the capabilities the client lacks for the operation;
	// zero when Supported. For OpPause it lists both pause capabilities
	// when the client has neither. It is zero for an unsupported
	// operation that needs no capability, only an interface; Reason then
	// says so.
	Missing workflow.Caps
	// Reason says why an operation that needs no capability is
	// unsupported (its interface is absent); empty otherwise.
	Reason string
}

// Support reports, for every operation in Operations order, whether the
// client supports it. A UI hides what is unsupported instead of showing
// a button that always fails.
func (a *Admin) Support() []OperationSupport {
	out := make([]OperationSupport, 0, len(Operations))
	for _, op := range Operations {
		err := a.check(op)
		s := OperationSupport{Operation: op, Supported: err == nil}
		var ue *workflow.UnsupportedError
		if errors.As(err, &ue) && requirements[op] == 0 {
			// Only an interface is missing: say so, since Missing is zero.
			s.Reason = ue.Reason
		}
		if err != nil {
			s.Missing = a.c.Capabilities().Missing(requirements[op])
			if s.Missing == 0 {
				// The capability is declared but the interface is absent:
				// report the capability anyway, so Missing is zero on an
				// unsupported operation only when it needs no capability.
				s.Missing = requirements[op]
			}
		}
		out = append(out, s)
	}
	return out
}

// Supports reports whether the client supports op.
func (a *Admin) Supports(op Operation) bool { return a.check(op) == nil }

// check returns nil when the client declares op's capability and
// implements its interface, else an UnsupportedError naming op.
func (a *Admin) check(op Operation) error {
	have := a.c.Capabilities()
	need := requirements[op]
	ok := have.Has(need)
	if op == OpPause {
		ok = have&need != 0
	}
	if ok {
		switch op {
		case OpRuns, OpRun, OpFailures, OpDiscarded, OpQueues:
			_, ok = a.c.(workflow.RunQuerier)
		case OpRetry:
			_, ok = a.c.(workflow.RunRetrier)
		case OpDelete:
			_, ok = a.c.(workflow.RunDeleter)
		case OpCancelAll:
			_, ok = a.c.(workflow.BulkCanceller)
		case OpPause:
			_, ok = a.c.(workflow.Pauser)
		case OpSchedules:
			_, ok = a.c.(workflow.ScheduleLister)
		case OpSchedulePause:
			_, okList := a.c.(workflow.ScheduleLister)
			_, ok = a.c.(workflow.SchedulePauser)
			ok = ok && okList
		case OpScheduleTrigger:
			_, okList := a.c.(workflow.ScheduleLister)
			_, ok = a.c.(workflow.ScheduleTriggerer)
			ok = ok && okList
		case OpForce:
			_, ok = a.c.(workflow.RunForcer)
		case OpRetryAll:
			_, okList := a.c.(workflow.RunQuerier)
			_, ok = a.c.(workflow.RunRetrier)
			ok = ok && okList
		case OpDeleteAll:
			_, okList := a.c.(workflow.RunQuerier)
			_, ok = a.c.(workflow.RunDeleter)
			ok = ok && okList
		// No capability bit: a wrapper (workflow.Forward, failover)
		// implements these interfaces whatever it wraps, so ask
		// workflow.Supports, which a wrapper answers for its backends.
		case OpNodes:
			ok = workflow.Supports(a.c, workflow.OpNodes)
		case OpReschedule:
			ok = workflow.Supports(a.c, workflow.OpReschedule)
		case OpCancelStep:
			ok = workflow.Supports(a.c, workflow.OpCancelStep)
		}
	}
	if ok {
		return nil
	}
	missing := have.Missing(need)
	if op == OpPause && have&need != 0 {
		missing = 0
	}
	reason := "the client does not support this operation"
	if missing == 0 {
		reason = "the client does not implement the interface this operation needs"
	}
	return &workflow.UnsupportedError{Field: "admin." + string(op), Missing: missing, Reason: reason}
}

// Runs lists runs matching q, one page at a time, newest enqueue first.
// Pass the returned RunPage.Next as q.Cursor for the next page.
func (a *Admin) Runs(ctx context.Context, q workflow.RunQuery) (workflow.RunPage, error) {
	if err := a.check(OpRuns); err != nil {
		return workflow.RunPage{}, err
	}
	if err := a.validateQuery(q); err != nil {
		return workflow.RunPage{}, err
	}
	return a.c.(workflow.RunQuerier).Runs(ctx, q)
}

// Run returns one run. An unknown ref returns workflow.ErrNotFound.
func (a *Admin) Run(ctx context.Context, ref workflow.RunRef) (workflow.RunInfo, error) {
	if err := a.check(OpRun); err != nil {
		return workflow.RunInfo{}, err
	}
	if ref.IsZero() {
		return workflow.RunInfo{}, fmt.Errorf("%w: admin: run ref is empty", workflow.ErrInvalid)
	}
	page, err := a.c.(workflow.RunQuerier).Runs(ctx, workflow.RunQuery{Ref: ref, Limit: 1})
	if err != nil {
		return workflow.RunInfo{}, err
	}
	if len(page.Runs) == 0 {
		return workflow.RunInfo{}, fmt.Errorf("%w: admin: run %s", workflow.ErrNotFound, refString(ref))
	}
	return page.Runs[0], nil
}

// validateQuery checks q's shape, then refuses a filter the client
// cannot serve (RunQuery.LabelPrefixes without CapLabelPrefix) with an
// UnsupportedError, before the driver is asked.
func (a *Admin) validateQuery(q workflow.RunQuery) error {
	if err := workflow.ValidateRunQuery(q); err != nil {
		return err
	}
	return workflow.CheckRunQuery(a.c.Capabilities(), q)
}

// Failures lists failed runs matching q. q.Statuses
// must be empty: the operation sets it to StatusFailed, and a caller that
// asks for other statuses wants Runs. Discarded runs are not failures (a
// step chose to drop them) and are listed by Discarded.
func (a *Admin) Failures(ctx context.Context, q workflow.RunQuery) (workflow.RunPage, error) {
	return a.listStatus(ctx, OpFailures, workflow.StatusFailed, q)
}

// Discarded lists runs a step ended with workflow.Discard, matching q,
// beside Failures: the run's Error keeps the reason. q.Statuses must be
// empty. It needs CapRunStates: on a driver without it a discarded run
// reads failed or cancelled and shows up there instead.
func (a *Admin) Discarded(ctx context.Context, q workflow.RunQuery) (workflow.RunPage, error) {
	return a.listStatus(ctx, OpDiscarded, workflow.StatusDiscarded, q)
}

// listStatus is Failures and Discarded: one status, set by the operation.
func (a *Admin) listStatus(ctx context.Context, op Operation, st workflow.Status, q workflow.RunQuery) (workflow.RunPage, error) {
	if err := a.check(op); err != nil {
		return workflow.RunPage{}, err
	}
	if len(q.Statuses) > 0 {
		return workflow.RunPage{}, fmt.Errorf("%w: admin: %s lists %s runs; use Runs to filter by status", workflow.ErrInvalid, op, st)
	}
	q.Statuses = []workflow.Status{st}
	if err := a.validateQuery(q); err != nil {
		return workflow.RunPage{}, err
	}
	return a.c.(workflow.RunQuerier).Runs(ctx, q)
}

// Retry re-runs a failed run from the step that failed; see
// workflow.RunRetrier for what is kept.
func (a *Admin) Retry(ctx context.Context, ref workflow.RunRef) (workflow.RunRef, error) {
	if err := a.check(OpRetry); err != nil {
		return workflow.RunRef{}, err
	}
	return a.c.(workflow.RunRetrier).Retry(ctx, ref)
}

// Delete forgets a finished run.
func (a *Admin) Delete(ctx context.Context, ref workflow.RunRef) error {
	if err := a.check(OpDelete); err != nil {
		return err
	}
	return a.c.(workflow.RunDeleter).Delete(ctx, ref)
}

// Cancel cancels one run, as Client.Cancel does.
func (a *Admin) Cancel(ctx context.Context, ref workflow.RunRef) error {
	if err := a.check(OpCancel); err != nil {
		return err
	}
	return a.c.Cancel(ctx, ref)
}

// CancelAll cancels every run matching q and returns
// how many; see workflow.BulkCanceller for which statuses an empty
// q.Statuses means.
func (a *Admin) CancelAll(ctx context.Context, q workflow.RunQuery) (int, error) {
	if err := a.check(OpCancelAll); err != nil {
		return 0, err
	}
	if err := workflow.ValidateCancelQuery(q); err != nil {
		return 0, err
	}
	if err := workflow.CheckRunQuery(a.c.Capabilities(), q); err != nil {
		return 0, err
	}
	return a.c.(workflow.BulkCanceller).CancelAll(ctx, q)
}

// pauser checks OpPause, then t and the capability it needs.
func (a *Admin) pauser(t workflow.PauseTarget) (workflow.Pauser, error) {
	if err := a.check(OpPause); err != nil {
		return nil, err
	}
	if err := workflow.CheckPause(a.c.Capabilities(), t); err != nil {
		return nil, err
	}
	return a.c.(workflow.Pauser), nil
}

// Pause holds the runs t matches without cancelling them; see
// workflow.Pauser.
func (a *Admin) Pause(ctx context.Context, t workflow.PauseTarget) error {
	p, err := a.pauser(t)
	if err != nil {
		return err
	}
	return p.Pause(ctx, t)
}

// Resume removes the pause on t; Resume(workflow.PauseAll()) removes every
// pause.
func (a *Admin) Resume(ctx context.Context, t workflow.PauseTarget) error {
	p, err := a.pauser(t)
	if err != nil {
		return err
	}
	return p.Resume(ctx, t)
}

// Paused lists the active pauses.
func (a *Admin) Paused(ctx context.Context) ([]workflow.PauseTarget, error) {
	if err := a.check(OpPause); err != nil {
		return nil, err
	}
	return a.c.(workflow.Pauser).Paused(ctx)
}

// Schedules lists the live schedules with their next fire time, sorted by workflow, then name.
func (a *Admin) Schedules(ctx context.Context) ([]workflow.ScheduleInfo, error) {
	if err := a.check(OpSchedules); err != nil {
		return nil, err
	}
	return a.c.(workflow.ScheduleLister).Schedules(ctx)
}

// Schedule returns one live schedule by ID, from the same list Schedules
// reads, with its next fire time and, on a client with
// CapScheduleHistory, its last fire (ScheduleInfo.Last). An empty id is
// refused with ErrInvalid and an unknown one returns ErrNotFound.
func (a *Admin) Schedule(ctx context.Context, id workflow.ScheduleID) (workflow.ScheduleInfo, error) {
	if err := a.check(OpSchedules); err != nil {
		return workflow.ScheduleInfo{}, err
	}
	if err := workflow.ValidateScheduleTarget(workflow.ScheduleTarget{ID: id}); err != nil {
		return workflow.ScheduleInfo{}, err
	}
	all, err := a.c.(workflow.ScheduleLister).Schedules(ctx)
	if err != nil {
		return workflow.ScheduleInfo{}, err
	}
	for _, s := range all {
		if s.ID == id {
			return s, nil
		}
	}
	return workflow.ScheduleInfo{}, fmt.Errorf("%w: admin: schedule %s", workflow.ErrNotFound, id)
}

// TriggerSchedule runs the schedule id now, with its payload and labels,
// and returns the run it enqueued; see workflow.ScheduleTriggerer. The
// schedule's overlap policy still applies: with OverlapSkip and its
// previous run unfinished it enqueues nothing and returns an error
// matching workflow.ErrOverlap. An empty id is refused with ErrInvalid.
func (a *Admin) TriggerSchedule(ctx context.Context, id workflow.ScheduleID) (workflow.RunRef, error) {
	if err := a.check(OpScheduleTrigger); err != nil {
		return workflow.RunRef{}, err
	}
	if err := workflow.ValidateScheduleTarget(workflow.ScheduleTarget{ID: id}); err != nil {
		return workflow.RunRef{}, err
	}
	return a.c.(workflow.ScheduleTriggerer).TriggerSchedule(ctx, id)
}

// MaxForceReason bounds the reason ForceFail stores on a run, so an
// operator's text cannot grow a run row without limit.
const MaxForceReason = 1024

// ForceComplete ends a run as succeeded without running its remaining
// steps; see workflow.RunForcer. A run that is not terminal, or is
// failed, is accepted; another terminal run returns ErrWrongState.
func (a *Admin) ForceComplete(ctx context.Context, ref workflow.RunRef) error {
	if err := a.check(OpForce); err != nil {
		return err
	}
	if ref.IsZero() {
		return fmt.Errorf("%w: admin: run ref is empty", workflow.ErrInvalid)
	}
	return a.c.(workflow.RunForcer).ForceComplete(ctx, ref)
}

// ForceFail ends a run that is not terminal as failed, with reason in its
// error and no retry; see workflow.RunForcer. A reason longer than
// MaxForceReason bytes is refused with ErrInvalid.
func (a *Admin) ForceFail(ctx context.Context, ref workflow.RunRef, reason string) error {
	if err := a.check(OpForce); err != nil {
		return err
	}
	if ref.IsZero() {
		return fmt.Errorf("%w: admin: run ref is empty", workflow.ErrInvalid)
	}
	if len(reason) > MaxForceReason {
		return fmt.Errorf("%w: admin: force reason is %d bytes, at most %d", workflow.ErrInvalid, len(reason), MaxForceReason)
	}
	return a.c.(workflow.RunForcer).ForceFail(ctx, ref, reason)
}

// PauseSchedule stops the schedule id firing without removing it; see
// workflow.SchedulePauser. An empty id is refused with ErrInvalid.
func (a *Admin) PauseSchedule(ctx context.Context, id workflow.ScheduleID) error {
	return a.schedulePause(ctx, id, workflow.SchedulePauser.PauseSchedule)
}

// ResumeSchedule lets a paused schedule fire again.
func (a *Admin) ResumeSchedule(ctx context.Context, id workflow.ScheduleID) error {
	return a.schedulePause(ctx, id, workflow.SchedulePauser.ResumeSchedule)
}

// schedulePause is PauseSchedule and ResumeSchedule: one schedule by ID,
// never All, so a screen's per-row button cannot pause every schedule.
func (a *Admin) schedulePause(ctx context.Context, id workflow.ScheduleID, op func(workflow.SchedulePauser, context.Context, workflow.ScheduleTarget) error) error {
	if err := a.check(OpSchedulePause); err != nil {
		return err
	}
	t := workflow.ScheduleTarget{ID: id}
	if err := workflow.ValidateScheduleTarget(t); err != nil {
		return err
	}
	return op(a.c.(workflow.SchedulePauser), ctx, t)
}

// RunCount is how many runs match a query and where each page of it
// starts, for a screen that shows "1–50 of N" and numbered pages over a
// driver whose cursors are opaque.
type RunCount struct {
	// Total is how many runs match, up to the max scan.
	Total int
	// Truncated is true when more runs match than the max scan read:
	// Total is then a lower bound and the last pages are missing.
	Truncated bool
	// Cursors holds the cursor that starts each page of the query's
	// page size: Cursors[0] is "" (the first page), Cursors[i] starts
	// page i+1. Pass one as RunQuery.Cursor with the same filters and
	// Limit to open that page.
	Cursors []string
}

// CountRuns pages through the runs q matches, at q's page size, and
// returns their count and each page's cursor. q.Cursor must be empty. It
// reads at most the max scan (WithMaxScan) of runs, so a screen pays a
// bounded cost; a driver that could count in one query is still asked
// page by page, because RunQuerier is the only portable way to ask.
func (a *Admin) CountRuns(ctx context.Context, q workflow.RunQuery) (RunCount, error) {
	if err := a.check(OpRuns); err != nil {
		return RunCount{}, err
	}
	if q.Cursor != "" {
		return RunCount{}, fmt.Errorf("%w: admin: CountRuns starts at the first page; Cursor must be empty", workflow.ErrInvalid)
	}
	if err := a.validateQuery(q); err != nil {
		return RunCount{}, err
	}
	rq := a.c.(workflow.RunQuerier)
	out := RunCount{Cursors: []string{""}}
	for {
		page, err := rq.Runs(ctx, q)
		if err != nil {
			return RunCount{}, err
		}
		out.Total += len(page.Runs)
		if page.Next == "" {
			return out, nil
		}
		if out.Total >= a.maxScan {
			out.Truncated = true
			return out, nil
		}
		out.Cursors = append(out.Cursors, page.Next)
		q.Cursor = page.Next
	}
}

// QueueStat is one queue's backlog at the moment QueueStats read it.
type QueueStat struct {
	// Queue is the queue name.
	Queue string
	// Depth is how many runs wait for a worker (StatusQueued), including
	// runs held by a pause. Delayed runs (StatusScheduled) are not counted
	// until they are due.
	Depth int
	// OldestEnqueuedAt is the enqueue time of the oldest queued run.
	OldestEnqueuedAt time.Time
	// OldestWait is how long that run has waited, on the Admin's clock.
	OldestWait time.Duration
	// Workers and Running are this client's worker count for the queue
	// and how many attempts those workers run now. They are filled only
	// when QueueReport.Workers is true; see QueueStats.
	Workers int
	Running int
}

// QueueReport is every queue with at least one queued run, sorted by
// name.
type QueueReport struct {
	Queues []QueueStat
	// Truncated is true when more than the max scan of queued runs exist:
	// every Depth is then a lower bound and the oldest run may be missed.
	Truncated bool
	// At is when the report was taken, on the Admin's clock.
	At time.Time
	// Workers is true when the client implements workflow.WorkerScaler
	// and declares CapWorkerScale: each QueueStat then carries Workers
	// and Running, and every queue the client serves is listed, even
	// with nothing queued.
	Workers bool
}

// QueueStats counts the queued runs per queue and finds the oldest one in
// each, by paging RunQuerier over StatusQueued. It reads at most the max
// scan (WithMaxScan) of runs, so its cost is bounded; a driver that can
// count with one query is still asked page by page, because RunQuerier is
// the only portable way to ask.
//
// Depth and the oldest run come from that scan on every driver, so they
// count the queue across every process sharing the store. On a client
// that implements workflow.WorkerScaler and declares CapWorkerScale,
// QueueLoads adds this client's own worker count and running attempts
// per queue (the state a workflow.BalanceLoop acts on); a QueueLoads
// error fails the call, since a report that silently drops the worker
// counts would read as "no workers".
func (a *Admin) QueueStats(ctx context.Context) (QueueReport, error) {
	if err := a.check(OpQueues); err != nil {
		return QueueReport{}, err
	}
	rq := a.c.(workflow.RunQuerier)
	byQueue := map[string]*QueueStat{}
	q := workflow.RunQuery{Statuses: []workflow.Status{workflow.StatusQueued}, Limit: min(workflow.MaxRunsLimit, a.maxScan)}
	scanned := 0
	truncated := false
	for {
		page, err := rq.Runs(ctx, q)
		if err != nil {
			return QueueReport{}, err
		}
		for _, r := range page.Runs {
			if scanned == a.maxScan {
				truncated = true
				break
			}
			scanned++
			s := byQueue[r.Queue]
			if s == nil {
				s = &QueueStat{Queue: r.Queue}
				byQueue[r.Queue] = s
			}
			s.Depth++
			if s.OldestEnqueuedAt.IsZero() || r.EnqueuedAt.Before(s.OldestEnqueuedAt) {
				s.OldestEnqueuedAt = r.EnqueuedAt
			}
		}
		if truncated || page.Next == "" {
			break
		}
		if scanned == a.maxScan {
			truncated = true
			break
		}
		q.Cursor = page.Next
	}
	scaler, scales := a.c.(workflow.WorkerScaler)
	scales = scales && a.c.Capabilities().Has(workflow.CapWorkerScale)
	if scales {
		loads, err := scaler.QueueLoads(ctx)
		if err != nil {
			return QueueReport{}, err
		}
		for _, l := range loads {
			s := byQueue[l.Queue]
			if s == nil {
				s = &QueueStat{Queue: l.Queue}
				byQueue[l.Queue] = s
			}
			s.Workers, s.Running = l.Workers, l.Running
		}
	}
	now := a.clk.Now()
	rep := QueueReport{Truncated: truncated, At: now, Workers: scales}
	for _, s := range byQueue {
		if !s.OldestEnqueuedAt.IsZero() {
			s.OldestWait = max(now.Sub(s.OldestEnqueuedAt), 0)
		}
		rep.Queues = append(rep.Queues, *s)
	}
	slices.SortFunc(rep.Queues, func(x, y QueueStat) int {
		switch {
		case x.Queue < y.Queue:
			return -1
		case x.Queue > y.Queue:
			return 1
		}
		return 0
	})
	return rep, nil
}

// QueueDepths returns QueueStats as queue name to depth. Its signature
// matches otel.QueueDepthFunc, so a method value plugs straight into the
// OpenTelemetry queue-depth gauge without either module importing the
// other.
func (a *Admin) QueueDepths(ctx context.Context) (map[string]int64, error) {
	rep, err := a.QueueStats(ctx)
	if err != nil {
		return nil, err
	}
	out := make(map[string]int64, len(rep.Queues))
	for _, s := range rep.Queues {
		out[s.Queue] = int64(s.Depth)
	}
	return out, nil
}

// refString renders a ref for an error message.
func refString(r workflow.RunRef) string {
	if !r.RunID.IsZero() {
		return r.RunID.String()
	}
	return "scheduled " + r.ScheduledID.String()
}
