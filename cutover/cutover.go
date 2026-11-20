package cutover

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/ubgo/workflow"
)

// Capability families; see the package doc for what each means.
var (
	// sharedCaps reach runs, which may sit on either backend: both must
	// have them.
	sharedCaps = workflow.CapsOf(workflow.CapCancel, workflow.CapRunQuery, workflow.CapRetryRun, workflow.CapDeleteRun,
		workflow.CapCancelAll, workflow.CapRunStates, workflow.CapErrorHistory, workflow.CapLabelPrefix,
		workflow.CapDueQuery, workflow.CapResult, workflow.CapPauseWorkflow, workflow.CapPauseQueue,
		workflow.CapDrain, workflow.CapRunFinished, workflow.CapNodeIdentity)

	// scheduleCaps are the schedule backend's alone.
	scheduleCaps = workflow.CapsOf(workflow.CapCron, workflow.CapCronZone, workflow.CapCronSeconds, workflow.CapScheduleWhen,
		workflow.CapScheduleWindow, workflow.CapSchedulePause, workflow.CapScheduleList, workflow.CapOverlapSkip,
		workflow.CapScheduleHistory, workflow.CapScheduleActive, workflow.CapScheduleKeep, workflow.CapTickTime,
		workflow.CapMisfireSkip, workflow.CapMisfireFireOnce, workflow.CapMisfireFireAll)

	// unroutedCaps belong to interfaces this client does not implement,
	// so it never claims them.
	unroutedCaps = workflow.CapsOf(workflow.CapScheduleTrigger, workflow.CapTickEvents, workflow.CapWorkerScale,
		workflow.CapClosure, workflow.CapForceRun, workflow.CapDrainReport)
)

// Option configures a Client.
type Option func(*Client)

// SchedulesOn says which backend holds schedules: Workflow.Cron and every
// Client.Schedule. The default is New. Old keeps them firing on the old
// backend until the app moves them (then build the client again with the
// default and call MoveSchedules). Any other value is ignored.
func SchedulesOn(s Side) Option {
	return func(c *Client) {
		if s.valid() {
			c.sched = s
		}
	}
}

// Client is a workflow.Client over an old and a new backend. Build it with
// New.
type Client struct {
	old, new workflow.Client
	sched    Side

	mu      sync.Mutex
	wfs     map[string]workflow.Workflow
	newOnly map[string]bool // workflows the old backend could not run
	started bool

	doneOnce sync.Once
	done     chan struct{}
}

var (
	_ workflow.Client       = (*Client)(nil)
	_ workflow.RunNotifier  = (*Client)(nil)
	_ workflow.ResultReader = (*Client)(nil)
)

// New returns a client that sends new work to newClient and keeps
// oldClient running what it holds. Both must be unstarted clients owned
// by this one: it registers and starts them.
func New(oldClient, newClient workflow.Client, opts ...Option) *Client {
	c := &Client{old: oldClient, new: newClient, sched: SideNew, wfs: map[string]workflow.Workflow{}, newOnly: map[string]bool{}}
	for _, o := range opts {
		o(c)
	}
	return c
}

func (c *Client) backend(s Side) workflow.Client {
	if s == SideOld {
		return c.old
	}
	return c.new
}

// Capabilities is the new backend's, except the shared family (both
// backends'), the schedule family (the schedule backend's) and the
// capabilities of interfaces this client does not route (never claimed). See the
// package doc for the lists.
func (c *Client) Capabilities() workflow.Caps {
	o, n := c.old.Capabilities(), c.new.Capabilities()
	caps := n.Without(sharedCaps, scheduleCaps).
		With(n.Intersect(o).Intersect(sharedCaps)).
		With(c.backend(c.sched).Capabilities().Intersect(scheduleCaps))
	if _, _, ok := c.notifiers(); !ok {
		caps = caps.Without(workflow.CapRunFinished)
	}
	return caps.Without(unroutedCaps)
}

// Old returns the old backend, for what this client does not route.
func (c *Client) Old() workflow.Client { return c.old }

// New returns the new backend, for what this client does not route.
func (c *Client) New() workflow.Client { return c.new }

// Register validates wf and registers it on both backends, with handlers
// that see the prefixed RunID in StepInfo. The backend that does not hold
// schedules gets a copy without Cron, CronWindows, ScheduleWhen, Misfire,
// CronOverlap, CronSeconds, CronActive and CronKeepRuns. A workflow whose
// old copy the old backend cannot run (workflow.CheckCaps) is registered
// on the new backend only; when the old backend holds schedules and the
// workflow declares a cron, that is an UnsupportedError instead, since its
// ticks would never fire.
func (c *Client) Register(wf workflow.Workflow) error {
	if err := workflow.Validate(wf); err != nil {
		return err
	}
	c.mu.Lock()
	started := c.started
	c.mu.Unlock()
	if started {
		return workflow.ErrAlreadyStarted
	}
	copies := map[Side]workflow.Workflow{SideOld: withSide(wf, SideOld), SideNew: withSide(wf, SideNew)}
	other := copies[c.sched.other()]
	other.Cron, other.CronWindows, other.ScheduleWhen = nil, workflow.Windows{}, nil
	other.Misfire, other.CronOverlap = workflow.Misfire{}, ""
	other.CronSeconds, other.CronActive, other.CronKeepRuns = false, workflow.Active{}, 0
	copies[c.sched.other()] = other

	newOnly := workflow.CheckCaps(c.old.Capabilities(), copies[SideOld]) != nil
	if newOnly && c.sched == SideOld && len(wf.Cron) > 0 {
		return fmt.Errorf("cutover: workflow %q: the old backend holds schedules and cannot run it: %w", wf.Name, workflow.CheckCaps(c.old.Capabilities(), copies[SideOld]))
	}
	if err := c.new.Register(copies[SideNew]); err != nil {
		return fmt.Errorf("cutover: new: %w", err)
	}
	if !newOnly {
		if err := c.old.Register(copies[SideOld]); err != nil {
			return fmt.Errorf("cutover: old: %w", err)
		}
	}
	c.mu.Lock()
	c.wfs[wf.Name] = wf
	c.newOnly[wf.Name] = newOnly
	c.mu.Unlock()
	return nil
}

// NewOnly reports whether Register kept the named workflow off the old
// backend because the old backend cannot run it.
func (c *Client) NewOnly(workflowName string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.newOnly[workflowName]
}

// withSide returns wf with every handler wrapped so the StepInfo it reads
// names the run by the ref this client hands out.
func withSide(wf workflow.Workflow, s Side) workflow.Workflow {
	steps := make([]workflow.Step, len(wf.Steps))
	for i, st := range wf.Steps {
		if st.Run != nil {
			st.Run = sideHandler(st.Run, s)
		}
		steps[i] = st
	}
	wf.Steps = steps
	if wf.OnFailure != nil {
		wf.OnFailure = sideHandler(wf.OnFailure, s)
	}
	if wf.OnTimeout != nil {
		wf.OnTimeout = sideHandler(wf.OnTimeout, s)
	}
	return wf
}

func sideHandler(h workflow.Handler, s Side) workflow.Handler {
	return func(ctx context.Context, in []byte) ([]byte, error) {
		if info, ok := workflow.StepInfoFrom(ctx); ok {
			if !info.RunID.IsZero() {
				info.RunID = workflow.RunID(s.prefix() + string(info.RunID))
			}
			info.Labels = sideLabels(info.Labels, s)
			ctx = workflow.WithStepInfo(ctx, info)
		}
		return h(ctx, in)
	}
}

// Start checks every registered workflow against Capabilities, then
// starts the new backend and the old one. When the old one fails to
// start, the new one is stopped again (its Start ctx is cancelled).
func (c *Client) Start(ctx context.Context) error {
	c.mu.Lock()
	if c.started {
		c.mu.Unlock()
		return workflow.ErrAlreadyStarted
	}
	caps := c.Capabilities()
	for _, wf := range c.wfs {
		if err := workflow.CheckCaps(caps, wf); err != nil {
			c.mu.Unlock()
			return err
		}
	}
	c.started = true
	c.mu.Unlock()

	ctx, cancel := context.WithCancel(ctx)
	if err := c.new.Start(ctx); err != nil {
		cancel()
		return fmt.Errorf("cutover: new: %w", err)
	}
	if err := c.old.Start(ctx); err != nil {
		cancel()
		return fmt.Errorf("cutover: old: %w", err)
	}
	_ = cancel // lives as long as the caller's ctx; cancelling that stops both
	return nil
}

// Enqueue sends job to the new backend and returns its RunRef prefixed
// "new:". Nothing ever goes to the old backend.
func (c *Client) Enqueue(ctx context.Context, job workflow.Job) (workflow.RunRef, error) {
	ref, err := c.new.Enqueue(ctx, job)
	if err != nil {
		return workflow.RunRef{}, err
	}
	return encodeRef(SideNew, ref), nil
}

// Status asks the backend the ref names; an unprefixed ref is tried on the
// old backend, then the new one.
func (c *Client) Status(ctx context.Context, ref workflow.RunRef) (workflow.Status, error) {
	var st workflow.Status
	err := c.onRef(ref, func(_ Side, b workflow.Client, inner workflow.RunRef) error {
		var err error
		st, err = b.Status(ctx, inner)
		return err
	})
	return st, err
}

// Cancel cancels the run on the backend the ref names.
func (c *Client) Cancel(ctx context.Context, ref workflow.RunRef) error {
	return c.onRef(ref, func(_ Side, b workflow.Client, inner workflow.RunRef) error {
		return b.Cancel(ctx, inner)
	})
}

// Result reads the run's result from the backend the ref names. Needs
// CapResult on both.
func (c *Client) Result(ctx context.Context, ref workflow.RunRef) ([]byte, error) {
	if err := c.need("Result", workflow.CapResult); err != nil {
		return nil, err
	}
	var out []byte
	err := c.onRef(ref, func(s Side, b workflow.Client, inner workflow.RunRef) error {
		r, ok := b.(workflow.ResultReader)
		if !ok {
			return &workflow.UnsupportedError{Field: "Result", Missing: workflow.CapResult, Reason: "the " + string(s) + " backend does not implement it"}
		}
		var err error
		out, err = r.Result(ctx, inner)
		return err
	})
	return out, err
}

// onRef calls fn on the backend ref names with the driver's own ref. An
// unprefixed ref (stored before the cutover) is tried on the old backend
// and then, when the old one answers ErrNotFound, on the new one; if
// neither knows it the old one's error is returned. A ref whose two IDs
// name different backends was not made by this client: ErrNotFound.
func (c *Client) onRef(ref workflow.RunRef, fn func(Side, workflow.Client, workflow.RunRef) error) error {
	s, inner, ok := decodeRef(ref)
	if !ok {
		return fmt.Errorf("%w: cutover: run ref %+v names two backends", workflow.ErrNotFound, ref)
	}
	if s != "" {
		return fn(s, c.backend(s), inner)
	}
	oerr := fn(SideOld, c.old, inner)
	if !errors.Is(oerr, workflow.ErrNotFound) {
		return oerr
	}
	if nerr := fn(SideNew, c.new, inner); !errors.Is(nerr, workflow.ErrNotFound) {
		return nerr
	}
	return oerr
}

// Signal delivers ev to both backends, since a waiting run may sit on
// either. When one fails, the other has already taken the event; the
// error names the failing backend.
func (c *Client) Signal(ctx context.Context, ev workflow.Event) error {
	return joinSides("Signal", c.old.Signal(ctx, ev), c.new.Signal(ctx, ev))
}

// Schedule registers s on the schedule backend (SideNew unless SchedulesOn)
// and returns its ID with that backend's prefix.
func (c *Client) Schedule(ctx context.Context, s workflow.Schedule) (workflow.ScheduleID, error) {
	id, err := c.backend(c.sched).Schedule(ctx, s)
	if err != nil {
		return "", err
	}
	return encodeScheduleID(c.sched, id), nil
}

// Unschedule removes the schedule from the backend its ID names. An
// unprefixed ID (stored before the cutover) is removed from the old
// backend, and from the new one when the old one answers ErrNotFound. An
// empty id is a no-op.
func (c *Client) Unschedule(ctx context.Context, id workflow.ScheduleID) error {
	if id.IsZero() {
		return nil
	}
	s, inner := splitID(string(id))
	if s != "" {
		return c.backend(s).Unschedule(ctx, workflow.ScheduleID(inner))
	}
	err := c.old.Unschedule(ctx, id)
	if errors.Is(err, workflow.ErrNotFound) {
		if nerr := c.new.Unschedule(ctx, id); !errors.Is(nerr, workflow.ErrNotFound) {
			return nerr
		}
	}
	return err
}

// notifiers returns both backends as RunNotifiers, and false unless both
// implement it.
func (c *Client) notifiers() (o, n workflow.RunNotifier, ok bool) {
	o, ook := c.old.(workflow.RunNotifier)
	n, nok := c.new.(workflow.RunNotifier)
	return o, n, ook && nok
}

// OnRunFinished registers fn on both backends, with each event's Ref (and
// a fired run's LabelSchedule) prefixed by the backend it came from. Both
// must implement workflow.RunNotifier, or it returns an UnsupportedError,
// since runs on the other backend would end unseen. Register before Start.
func (c *Client) OnRunFinished(fn workflow.RunFinishedFunc) error {
	on, nn, ok := c.notifiers()
	if !ok {
		return workflow.Unsupported("OnRunFinished", "both cutover backends must implement workflow.RunNotifier")
	}
	c.mu.Lock()
	started := c.started
	c.mu.Unlock()
	if started {
		return workflow.ErrAlreadyStarted
	}
	if err := nn.OnRunFinished(sideObserver(fn, SideNew)); err != nil {
		return fmt.Errorf("cutover: new: %w", err)
	}
	if err := on.OnRunFinished(sideObserver(fn, SideOld)); err != nil {
		return fmt.Errorf("cutover: old: %w", err)
	}
	return nil
}

func sideObserver(fn workflow.RunFinishedFunc, s Side) workflow.RunFinishedFunc {
	return func(ctx context.Context, ev workflow.RunEvent) {
		ev.Ref = encodeRef(s, ev.Ref)
		ev.Labels = sideLabels(ev.Labels, s)
		fn(ctx, ev)
	}
}

// need refuses a call when Capabilities lacks caps, so a method never
// half-works on one backend.
func (c *Client) need(field string, caps workflow.Caps) error {
	if missing := c.Capabilities().Missing(caps); !missing.IsZero() {
		return &workflow.UnsupportedError{Field: field, Missing: missing, Reason: "one of the cutover backends cannot do it"}
	}
	return nil
}

// joinSides names each backend's error, or returns nil when both are nil.
func joinSides(op string, oerr, nerr error) error {
	var errs []error
	if oerr != nil {
		errs = append(errs, fmt.Errorf("cutover: %s old: %w", op, oerr))
	}
	if nerr != nil {
		errs = append(errs, fmt.Errorf("cutover: %s new: %w", op, nerr))
	}
	return errors.Join(errs...)
}
