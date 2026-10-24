package failover

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
)

// ErrKeyRequired refuses a Job without an IdempotencyKey: an ambiguous
// primary failure followed by a failover could otherwise run it twice. It
// is returned wrapped together with workflow.ErrInvalid, so errors.Is
// matches both. AllowWithoutKey lifts the requirement.
var ErrKeyRequired = errors.New("failover: Job.IdempotencyKey is required, because a primary that accepted the job and then failed to answer would make failover run it twice (use failover.AllowWithoutKey to accept that)")

// Classifier reports whether an error from the primary's Enqueue means the
// primary is unavailable, so the job should go to the secondary. It is
// only consulted when the caller's context is still live.
type Classifier func(err error) bool

// callerFault lists the errors the secondary would return just the same,
// or that a different backend cannot fix; DefaultClassifier never fails
// over on them.
var callerFault = []error{
	context.Canceled,
	context.DeadlineExceeded,
	workflow.ErrInvalid,
	workflow.ErrUnsupported,
	workflow.ErrNotFound,
	workflow.ErrNotStarted,
	workflow.ErrAlreadyStarted,
	workflow.ErrDuplicate,
	workflow.ErrCancelled,
}

// DefaultClassifier fails over on an error a driver marked with
// workflow.Unavailable, and on every other error except the core sentinels
// that describe the request or the client's state rather than the
// backend's health (see callerFault). It never fails over on a Permanent
// error. Not every driver marks its transport errors yet, so the default
// errs towards failing over on unknown errors: an unknown error is most
// often a transport one. StrictClassifier is the opposite trade.
func DefaultClassifier(err error) bool {
	if err == nil || workflow.IsPermanent(err) {
		return false
	}
	// A driver's own timeout talking to its engine matches
	// context.DeadlineExceeded, which callerFault lists; marked
	// unavailable it is an outage. The caller's own ctx is checked
	// before any Classifier runs.
	if workflow.IsUnavailable(err) {
		return true
	}
	for _, target := range callerFault {
		if errors.Is(err, target) {
			return false
		}
	}
	return true
}

// StrictClassifier fails over only on an error a driver marked with
// workflow.Unavailable (errors.Is(err, workflow.ErrUnavailable)), never on
// an unknown one, so an app bug a driver surfaces cannot send jobs to the
// secondary. Use it when every backend marks its outages:
//
//	failover.New(primary, secondary, failover.WithClassifier(failover.StrictClassifier))
//
// A Permanent error never fails over.
func StrictClassifier(err error) bool {
	return err != nil && !workflow.IsPermanent(err) && workflow.IsUnavailable(err)
}

// EventKind names what happened to the Client, as an Observer sees it.
type EventKind string

// The events a Client reports to its Observer.
const (
	// EventFailedOver: the primary's Enqueue failed with an error the
	// Classifier called unavailable, and the job went to the secondary
	// (Err is the primary's error).
	EventFailedOver EventKind = "failed_over"
	// EventPrimarySkipped: the primary was cooling off, so the job went
	// straight to the secondary.
	EventPrimarySkipped EventKind = "primary_skipped"
	// EventCoolOffStarted: the failover streak reached WithCoolOff's n;
	// Until is when the primary is tried again.
	EventCoolOffStarted EventKind = "cool_off_started"
	// EventCoolOffEnded: the primary took a job again after a cool-off.
	EventCoolOffEnded EventKind = "cool_off_ended"
)

// Event is one failover event. It never carries the job's payload or
// labels, which can hold personal data.
type Event struct {
	Kind EventKind
	// Workflow is the job's workflow name.
	Workflow string
	// Err is the primary's error, for EventFailedOver.
	Err error
	// Until is the end of the cool-off, for EventCoolOffStarted.
	Until time.Time
	// At is when the event happened, read from the Client's clock.
	At time.Time
}

// Observer receives the Client's events, synchronously, on the Enqueue
// that caused them; it must return quickly and must not call Enqueue on
// the same Client. A secondary that fails too is reported to the caller
// by Enqueue's error, not here.
type Observer func(ctx context.Context, ev Event)

// Option configures a Client.
type Option func(*Client)

// WithObserver reports failovers and cool-offs to fn, for a log line, a
// metric or an alert. The default reports nothing. A nil fn is ignored.
//
//	failover.WithObserver(func(ctx context.Context, ev failover.Event) {
//		logger.WarnContext(ctx, "workflow failover", "kind", ev.Kind, "workflow", ev.Workflow, "error", ev.Err)
//	})
func WithObserver(fn Observer) Option {
	return func(f *Client) {
		if fn != nil {
			f.observe = fn
		}
	}
}

// WithClassifier replaces DefaultClassifier. A nil c is ignored.
func WithClassifier(c Classifier) Option {
	return func(f *Client) {
		if c != nil {
			f.classify = c
		}
	}
}

// AllowWithoutKey accepts jobs without an IdempotencyKey, accepting that
// an ambiguous primary failure may run such a job on both backends.
func AllowWithoutKey() Option { return func(f *Client) { f.allowNoKey = true } }

// WithSchedulesOnBoth registers Workflow.Cron and Client.Schedule on both
// backends, so schedules keep firing while the primary is down, at the
// price of every tick firing on both while both are up. Only for
// scheduled workflows that tolerate running twice per tick.
func WithSchedulesOnBoth() Option { return func(f *Client) { f.schedBoth = true } }

// WithCoolOff skips the primary for d after n consecutive enqueues failed
// over, sending jobs straight to the secondary. After d the next enqueue
// tries the primary again; one more failure starts a new cool-off, and a
// success ends the streak. n below 1 or d of zero or less disables it
// (the default).
func WithCoolOff(n int, d time.Duration) Option {
	return func(f *Client) {
		if n >= 1 && d > 0 {
			f.coolN, f.coolFor = n, d
		}
	}
}

// WithClock sets the time source of the cool-off. The default is
// clock.Real. A nil c is ignored.
func WithClock(c clock.Clock) Option {
	return func(f *Client) {
		if c != nil {
			f.clk = c
		}
	}
}

// Client is a workflow.Client over a primary and a secondary backend.
// Build it with New.
type Client struct {
	primary, secondary workflow.Client

	classify   Classifier
	allowNoKey bool
	schedBoth  bool
	coolN      int
	coolFor    time.Duration
	clk        clock.Clock
	observe    Observer // nil reports nothing

	mu      sync.Mutex
	wfs     map[string]workflow.Workflow
	started bool
	// stopBackends cancels the ctx both backends were started with. The
	// caller's Start ctx is its parent, so cancelling that stops both too.
	stopBackends context.CancelFunc
	failures     int       // consecutive enqueues that failed over
	coolUntil    time.Time // primary skipped before this; zero when not cooling

	doneOnce sync.Once     // builds done on the first Done call
	done     chan struct{} // closed once both backends' Done closed
}

var (
	_ workflow.Client      = (*Client)(nil)
	_ workflow.RunNotifier = (*Client)(nil)
)

// New returns a client that enqueues on primary and fails over to
// secondary. Both must be fresh, unstarted clients owned by this one: it
// registers and starts them.
func New(primary, secondary workflow.Client, opts ...Option) *Client {
	f := &Client{
		primary:   primary,
		secondary: secondary,
		classify:  DefaultClassifier,
		clk:       clock.Real{},
		wfs:       map[string]workflow.Workflow{},
	}
	for _, o := range opts {
		o(f)
	}
	return f
}

func (f *Client) backend(s side) workflow.Client {
	if s == secondarySide {
		return f.secondary
	}
	return f.primary
}

// Capabilities is what both backends can do, except the schedule family
// (CapCron, CapScheduleWhen, CapScheduleWindow, CapSchedulePause,
// CapScheduleList, CapTickEvents), which is the primary's alone unless
// WithSchedulesOnBoth puts schedules on both: only the backends holding
// schedules fire, condition, report, list or pause them. Every other
// capability, the run query and pause families included, is the
// intersection, because a run may sit on either backend. CapClosure and
// CapWorkerScale are the primary's alone: a closure cannot be retried on
// another backend without risking a second run (it has no idempotency
// key), and the worker counts scaled are the primary's (balance the
// secondary with a BalanceLoop of its own).
// CapRunFinished also needs both backends to implement
// workflow.RunNotifier, which OnRunFinished requires.
func (f *Client) Capabilities() workflow.Caps {
	p, s := f.primary.Capabilities(), f.secondary.Capabilities()
	caps := p & s
	if !f.schedBoth {
		caps = caps&^scheduleCaps | p&scheduleCaps
	}
	caps = caps&^primaryOnlyCaps | p&primaryOnlyCaps
	if _, _, ok := f.notifiers(); !ok {
		caps &^= workflow.CapRunFinished
	}
	return caps
}

// notifiers returns both backends as RunNotifiers, and false unless both
// implement it.
func (f *Client) notifiers() (p, s workflow.RunNotifier, ok bool) {
	p, pok := f.primary.(workflow.RunNotifier)
	s, sok := f.secondary.(workflow.RunNotifier)
	return p, s, pok && sok
}

// Register validates wf and registers it on both backends, with handlers
// that report the prefixed RunID in StepInfo. Without WithSchedulesOnBoth
// the secondary's copy has no Cron, CronWindows, ScheduleWhen, Misfire or
// CronOverlap.
func (f *Client) Register(wf workflow.Workflow) error {
	if err := workflow.Validate(wf); err != nil {
		return err
	}
	f.mu.Lock()
	started := f.started
	f.mu.Unlock()
	if started {
		return workflow.ErrAlreadyStarted
	}
	if err := f.primary.Register(withSide(wf, primarySide)); err != nil {
		return fmt.Errorf("failover: primary: %w", err)
	}
	sec := withSide(wf, secondarySide)
	if !f.schedBoth {
		// The secondary fires no schedules, so it gets none of the
		// schedule fields: a secondary without CapScheduleWhen must not
		// refuse a condition it would never be asked.
		sec.Cron, sec.CronWindows, sec.ScheduleWhen = nil, workflow.Windows{}, nil
		sec.Misfire, sec.CronOverlap = workflow.Misfire{}, ""
		sec.CronSeconds, sec.CronActive, sec.CronKeepRuns = false, workflow.Active{}, 0
	}
	if err := f.secondary.Register(sec); err != nil {
		return fmt.Errorf("failover: secondary: %w", err)
	}
	f.mu.Lock()
	f.wfs[wf.Name] = wf
	f.mu.Unlock()
	return nil
}

// withSide returns wf with every Run handler and OnFailure wrapped so the
// StepInfo the handler reads names the run by the ref this client hands
// out.
func withSide(wf workflow.Workflow, s side) workflow.Workflow {
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

func sideHandler(h workflow.Handler, s side) workflow.Handler {
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

// Start checks every registered workflow against Capabilities, so a
// workflow the secondary cannot run is refused before anything starts,
// then starts the primary and the secondary. When the secondary fails to
// start the primary is stopped again.
func (f *Client) Start(ctx context.Context) error {
	f.mu.Lock()
	if f.started {
		f.mu.Unlock()
		return workflow.ErrAlreadyStarted
	}
	caps := f.Capabilities()
	for _, wf := range f.wfs {
		if err := workflow.CheckCaps(caps, wf); err != nil {
			f.mu.Unlock()
			return err
		}
	}
	f.started = true
	f.mu.Unlock()

	ctx, cancel := context.WithCancel(ctx)
	if err := f.primary.Start(ctx); err != nil {
		cancel()
		return fmt.Errorf("failover: primary: %w", err)
	}
	if err := f.secondary.Start(ctx); err != nil {
		cancel()
		return fmt.Errorf("failover: secondary: %w", err)
	}
	f.mu.Lock()
	f.stopBackends = cancel
	f.mu.Unlock()
	return nil
}

// Enqueue sends job to the primary, and to the secondary when the primary
// is cooling off or fails with an error the Classifier calls unavailable.
// The RunRef names the backend that took the job.
func (f *Client) Enqueue(ctx context.Context, job workflow.Job) (workflow.RunRef, error) {
	if job.IdempotencyKey == "" && !f.allowNoKey {
		return workflow.RunRef{}, fmt.Errorf("%w (%w)", ErrKeyRequired, workflow.ErrInvalid)
	}
	if missing := f.Capabilities().Missing(workflow.RequiresJob(job)); missing != 0 {
		return workflow.RunRef{}, &workflow.UnsupportedError{Field: "Job", Missing: missing, Reason: "one of the failover backends cannot honour it"}
	}
	var perr error
	if f.tryPrimary() {
		ref, err := f.primary.Enqueue(ctx, job)
		if err == nil {
			if f.primaryOK() {
				f.emit(ctx, Event{Kind: EventCoolOffEnded, Workflow: job.Workflow})
			}
			return encodeRef(primarySide, ref), nil
		}
		if ctx.Err() != nil || !f.classify(err) {
			return workflow.RunRef{}, err
		}
		until := f.primaryFailed()
		perr = err
		f.emit(ctx, Event{Kind: EventFailedOver, Workflow: job.Workflow, Err: err})
		if !until.IsZero() {
			f.emit(ctx, Event{Kind: EventCoolOffStarted, Workflow: job.Workflow, Until: until})
		}
	} else {
		f.emit(ctx, Event{Kind: EventPrimarySkipped, Workflow: job.Workflow})
	}
	ref, err := f.secondary.Enqueue(ctx, job)
	if err != nil {
		if perr == nil {
			return workflow.RunRef{}, fmt.Errorf("failover: secondary (primary cooling off): %w", err)
		}
		return workflow.RunRef{}, fmt.Errorf("failover: primary: %w; secondary: %w", perr, err)
	}
	return encodeRef(secondarySide, ref), nil
}

// tryPrimary reports whether the primary is outside a cool-off.
func (f *Client) tryPrimary() bool {
	if f.coolN == 0 {
		return true
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.coolUntil.IsZero() || !f.clk.Now().Before(f.coolUntil)
}

// primaryOK ends the failover streak and reports whether that ended a
// cool-off.
func (f *Client) primaryOK() (coolOffEnded bool) {
	if f.coolN == 0 {
		return false
	}
	f.mu.Lock()
	coolOffEnded = !f.coolUntil.IsZero()
	f.failures, f.coolUntil = 0, time.Time{}
	f.mu.Unlock()
	return coolOffEnded
}

// primaryFailed counts a failover and returns the end of the cool-off it
// started, or zero when it started none.
func (f *Client) primaryFailed() (until time.Time) {
	if f.coolN == 0 {
		return time.Time{}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failures++
	if f.failures >= f.coolN {
		f.coolUntil = f.clk.Now().Add(f.coolFor)
		return f.coolUntil
	}
	return time.Time{}
}

// emit stamps ev with the time and hands it to the Observer, if any.
func (f *Client) emit(ctx context.Context, ev Event) {
	if f.observe == nil {
		return
	}
	ev.At = f.clk.Now()
	f.observe(ctx, ev)
}

// Status asks the backend the ref names. See onRef for unprefixed refs.
func (f *Client) Status(ctx context.Context, ref workflow.RunRef) (workflow.Status, error) {
	var st workflow.Status
	err := f.onRef(ref, func(c workflow.Client, inner workflow.RunRef) error {
		var err error
		st, err = c.Status(ctx, inner)
		return err
	})
	return st, err
}

// Cancel cancels the run on the backend the ref names.
func (f *Client) Cancel(ctx context.Context, ref workflow.RunRef) error {
	return f.onRef(ref, func(c workflow.Client, inner workflow.RunRef) error {
		return c.Cancel(ctx, inner)
	})
}

// onRef calls fn on the backend ref names with the driver's own ref. A ref
// without a prefix is tried on the primary first and on the secondary when
// the primary does not know it or is unavailable; if the secondary does
// not know it either, the primary's error is returned. A ref whose two IDs
// name different backends was not made by this client: ErrNotFound.
func (f *Client) onRef(ref workflow.RunRef, fn func(workflow.Client, workflow.RunRef) error) error {
	s, inner, ok := decodeRef(ref)
	if !ok {
		return fmt.Errorf("%w: failover: run ref %+v names two backends", workflow.ErrNotFound, ref)
	}
	if s != unknownSide {
		return fn(f.backend(s), inner)
	}
	perr := fn(f.primary, inner)
	if perr == nil || !(errors.Is(perr, workflow.ErrNotFound) || f.classify(perr)) {
		return perr
	}
	if serr := fn(f.secondary, inner); !errors.Is(serr, workflow.ErrNotFound) {
		return serr
	}
	return perr
}

// Signal delivers ev to both backends, since a waiting run may sit on
// either. When one fails, the other has already taken the event; the
// error names the failing backend.
func (f *Client) Signal(ctx context.Context, ev workflow.Event) error {
	var errs []error
	if err := f.primary.Signal(ctx, ev); err != nil {
		errs = append(errs, fmt.Errorf("failover: signal primary: %w", err))
	}
	if err := f.secondary.Signal(ctx, ev); err != nil {
		errs = append(errs, fmt.Errorf("failover: signal secondary: %w", err))
	}
	return errors.Join(errs...)
}

// Schedule registers s on the primary, or on both under
// WithSchedulesOnBoth. It never fails over: a schedule made on the
// secondary during an outage would fire beside the primary's copies once
// the app retries it there.
func (f *Client) Schedule(ctx context.Context, s workflow.Schedule) (workflow.ScheduleID, error) {
	pid, err := f.primary.Schedule(ctx, s)
	if err != nil {
		return "", err
	}
	if !f.schedBoth {
		return encodeScheduleID(primarySide, pid), nil
	}
	sid, err := f.secondary.Schedule(ctx, s)
	if err != nil {
		if uerr := f.primary.Unschedule(ctx, pid); uerr != nil {
			return "", fmt.Errorf("failover: secondary: %w (and removing the primary's copy %s: %v)", err, pid, uerr)
		}
		return "", fmt.Errorf("failover: secondary: %w", err)
	}
	return encodeBothScheduleID(pid, sid), nil
}

// Unschedule removes the schedule from every backend that holds it. An
// empty id is a no-op.
func (f *Client) Unschedule(ctx context.Context, id workflow.ScheduleID) error {
	if id.IsZero() {
		return nil
	}
	pid, sid := decodeScheduleID(id)
	var errs []error
	if !pid.IsZero() {
		if err := f.primary.Unschedule(ctx, pid); err != nil {
			errs = append(errs, fmt.Errorf("failover: primary: %w", err))
		}
	}
	if !sid.IsZero() {
		if err := f.secondary.Unschedule(ctx, sid); err != nil {
			errs = append(errs, fmt.Errorf("failover: secondary: %w", err))
		}
	}
	return errors.Join(errs...)
}

// OnRunFinished registers fn on both backends, with each event's Ref
// prefixed by the backend it came from. Both backends must implement
// workflow.RunNotifier; otherwise it returns an UnsupportedError, since
// runs on the other backend would end unseen.
func (f *Client) OnRunFinished(fn workflow.RunFinishedFunc) error {
	pn, sn, ok := f.notifiers()
	if !ok {
		return workflow.Unsupported("OnRunFinished", "both failover backends must implement workflow.RunNotifier")
	}
	f.mu.Lock()
	started := f.started
	f.mu.Unlock()
	if started {
		return workflow.ErrAlreadyStarted
	}
	if err := pn.OnRunFinished(sideObserver(fn, primarySide)); err != nil {
		return fmt.Errorf("failover: primary: %w", err)
	}
	if err := sn.OnRunFinished(sideObserver(fn, secondarySide)); err != nil {
		return fmt.Errorf("failover: secondary: %w", err)
	}
	return nil
}

// sideObserver prefixes the event's Ref, and the schedule label a fired
// run carries, so both match the IDs this client hands out (Tick.Run,
// Tick.ScheduleID, Schedules). The labels are copied, never changed in
// place: the driver owns its map.
func sideObserver(fn workflow.RunFinishedFunc, s side) workflow.RunFinishedFunc {
	return func(ctx context.Context, ev workflow.RunEvent) {
		ev.Ref = encodeRef(s, ev.Ref)
		ev.Labels = sideLabels(ev.Labels, s)
		fn(ctx, ev)
	}
}
