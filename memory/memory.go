package memory

import (
	"context"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/cron"
)

// caps is everything except durability (CapRunFinished holds because every
// terminal transition goes through finishLocked, which queues the event): the driver keeps state in memory,
// so a restart loses it, and claiming CapDurable would be a lie.
const caps = workflow.CapDelayedStart | workflow.CapWaitForEvent | workflow.CapDurableSleep |
	workflow.CapConcurrencyKey | workflow.CapRateLimit | workflow.CapIdempotencyKey |
	workflow.CapCron | workflow.CapCancel | workflow.CapRunFinished |
	workflow.CapRetryJitter | workflow.CapRetryDelay | workflow.CapCronZone |
	workflow.CapMisfireSkip | workflow.CapMisfireFireOnce | workflow.CapMisfireFireAll |
	workflow.CapQueue | workflow.CapQueueOverride | workflow.CapJobRetry |
	workflow.CapRetryUntil | workflow.CapRetryMaxErrors | workflow.CapDiscard | workflow.CapDiscardExhausted |
	workflow.CapRunQuery | workflow.CapRetryRun | workflow.CapDeleteRun | workflow.CapCancelAll |
	workflow.CapPauseWorkflow | workflow.CapPauseQueue | workflow.CapSchedulePause |
	workflow.CapScheduleWhen | workflow.CapScheduleWindow | workflow.CapTickEvents |
	workflow.CapScheduleList | workflow.CapDrain | workflow.CapDrainReport |
	workflow.CapRunStates | workflow.CapChain | workflow.CapWorkerScale | workflow.CapClosure | workflow.CapLabelPrefix |
	workflow.CapOverlapSkip | workflow.CapScheduleTrigger | workflow.CapScheduleHistory |
	workflow.CapErrorHistory | workflow.CapForceRun |
	workflow.CapSnooze | workflow.CapHeartbeat | workflow.CapTenant | workflow.CapRunTimeout |
	workflow.CapNodeIdentity | workflow.CapKeyFIFO | workflow.CapDueQuery | workflow.CapResult |
	workflow.CapCronSeconds | workflow.CapScheduleActive | workflow.CapScheduleKeep | workflow.CapTickTime | workflow.CapQueueLimitGlobal

// defaultWorkers is the step-execution pool size when WithWorkers is not
// given: enough for tests that run steps in parallel, small enough that a
// local run does not swamp the machine.
const defaultWorkers = 8

// Option configures a Client.
type Option func(*Client)

// WithClock sets the time source of every timer and schedule. The default
// is clock.Real; tests pass clock.NewFake.
func WithClock(c clock.Clock) Option { return func(m *Client) { m.clk = c } }

// WithWorkers sets how many step attempts may execute at once. Values
// below 1 are ignored.
func WithWorkers(n int) Option {
	return func(m *Client) {
		if n >= 1 {
			m.workers = n
		}
	}
}

// WithLogger sets where the driver reports what it cannot return to a
// caller (a cron fire that failed to enqueue, an OnFailure handler error).
// The default discards.
func WithLogger(l *slog.Logger) Option {
	return func(m *Client) {
		if l != nil {
			m.log = l
		}
	}
}

// WithMiddleware wraps every step handler and OnFailure handler. The first
// middleware is the outermost.
func WithMiddleware(mw ...workflow.Middleware) Option {
	return func(m *Client) { m.mw = append(m.mw, mw...) }
}

// WithCronParser replaces the cron parser behind Workflow.Cron and
// Schedule.Cron (cron.Parse by default), for a dialect the standard one
// lacks: the extended "L", "W" and "#" fields, a seconds field, hashed minutes. It is
// opt-in and gives up "the same spec on every driver" for the expressions
// only this parser understands; see cron.Parser. Nil is ignored.
func WithCronParser(p cron.Parser) Option {
	return func(m *Client) {
		if p != nil {
			m.parse = p
		}
	}
}

// WithJitterSource sets where Backoff.Jitter draws its uniform values in
// [0, 1), so a test can pin them. The default is math/rand/v2. It must be
// safe for concurrent use. Nil is ignored.
func WithJitterSource(rnd func() float64) Option {
	return func(m *Client) {
		if rnd != nil {
			m.rnd = rnd
		}
	}
}

// WithQueues makes the client serve only these queues, each with its own
// worker cap and priority (see workflow.QueueConfig). Without it the
// client serves every queue from one pool. Because this driver is one
// process, a run on a queue it does not serve could never run, so Start
// refuses a workflow whose queue is not listed and Enqueue refuses a
// Job.Queue that is not (both ErrInvalid). An invalid list is reported by
// Start. Calling it again replaces the list.
func WithQueues(qs ...workflow.QueueConfig) Option {
	return func(m *Client) { m.queueCfg = append([]workflow.QueueConfig(nil), qs...) }
}

// RunRetention bounds how many finished runs the client remembers for
// Status, Runs, Retry and Delete. A run that is not finished is never
// forgotten. Zero fields mean no limit on that axis.
type RunRetention struct {
	// MaxAge forgets a finished run once it finished more than MaxAge ago
	// on the injected clock.
	MaxAge time.Duration

	// MaxRuns forgets the oldest finished runs beyond this many.
	MaxRuns int
}

// DefaultRunRetention is what a client keeps without WithRunRetention:
// finished runs for a day, at most 10,000 of them. A long-lived process
// on this driver would otherwise grow without bound, since Runs and Retry
// need finished runs kept; a day covers "the job failed overnight, retry
// it in the morning", and 10,000 bounds memory however busy the client.
var DefaultRunRetention = RunRetention{MaxAge: 24 * time.Hour, MaxRuns: 10_000}

// WithRunRetention replaces DefaultRunRetention. RunRetention{} keeps
// every finished run until Delete, for a test or a short-lived tool that
// wants to inspect everything; negative fields are treated as zero.
// Finished runs past the limits are forgotten when a run ends, when one
// is enqueued and when Runs lists them; afterwards Status returns
// ErrNotFound for them, as after Delete.
func WithRunRetention(r RunRetention) Option {
	return func(m *Client) { m.retention = RunRetention{MaxAge: max(r.MaxAge, 0), MaxRuns: max(r.MaxRuns, 0)} }
}

// WithStopWhenIdle makes the client stop by itself, as Drain would, once
// its last unfinished run (delayed, queued, running or waiting) has ended
// and no run was enqueued for grace on the injected clock: a worker that
// stops when its queue is empty. It is for batch-style processes: Start, enqueue the
// work, wait on Done. A client that never ran anything never stops this
// way, so Start followed by Enqueue is safe. Schedules do not keep it
// alive. A grace below zero is ignored.
func WithStopWhenIdle(grace time.Duration) Option {
	return func(m *Client) {
		if grace >= 0 {
			m.idleGrace, m.stopWhenIdle = grace, true
		}
	}
}

// WithErrorHistory keeps the newest n failed attempts of each run in
// RunInfo.Errors (workflow.CapErrorHistory). Without the option a run
// keeps workflow.DefaultErrorHistory; n <= 0 keeps none.
func WithErrorHistory(n int) Option { return func(m *Client) { m.errHist = n } }

// Client is the in-memory workflow.Client. Build it with New. All state
// hangs off the value; two clients share nothing.
type Client struct {
	clk     clock.Clock
	workers int
	log     *slog.Logger
	mw      []workflow.Middleware
	parse   cron.Parser
	rnd     func() float64 // jitter draws; nil means math/rand/v2
	errHist int            // RunInfo.Errors bound (WithErrorHistory)

	retention    RunRetention
	queueCfg     []workflow.QueueConfig          // as given to WithQueues
	queues       map[string]workflow.QueueConfig // by name, built at Start; nil serves every queue
	idleGrace    time.Duration
	stopWhenIdle bool
	recycle      workflow.Recycle // WithRecycle; zero never recycles
	node         string           // WithNodeID; StepInfo.Node
	payloadLimit int              // WithPayloadLimit; zero means none

	mu       sync.Mutex
	taskCond *sync.Cond // signalled when tasks are pushed or the client stops
	wfs      map[string]*registered
	started  bool
	stopped  bool
	ctx      context.Context // root of every run; cancelled on shutdown
	cancel   context.CancelFunc
	wg       sync.WaitGroup // every goroutine the client starts
	seq      uint64         // IDs and first-come-first-served tiebreaks

	runs    map[workflow.RunID]*run
	delayed map[workflow.ScheduledID]*run
	idem    map[idemKey]*run
	waiters map[eventKey][]*waiter
	signals map[eventKey][]signal // kept for runs that reach their wait later; see Signal
	tasks   []*task               // step attempts waiting for a worker; see pickLocked
	pending []*run                // ready runs not yet admitted (concurrency or rate limit)
	buckets map[bucketKey][]*run
	rates   map[rateKey][]time.Time
	rateAt  time.Time   // when the armed rate-limit timer fires; zero if none
	rateTm  clock.Timer // the armed rate-limit timer
	scheds  map[workflow.ScheduleID]*schedule

	queueExec      map[string]int                // executing attempts per queue
	executing      int                           // executing attempts in all
	resultsPending int                           // returned attempts whose result drive has not applied yet; see run.resultsPending
	pauses         map[workflow.PauseTarget]bool // targets in force
	schedPaused    bool                          // PauseSchedule(All) in force
	draining       bool                          // Drain called: claim nothing more
	drained        chan struct{}                 // closed when draining and nothing executes
	drainTrack     workflow.DrainTracker         // what a drain finished and cut off; see DrainWithReport
	done           chan struct{}                 // closed once fully stopped
	live           int                           // unfinished runs, for stop-when-idle
	idleEpoch      uint64                        // bumped by every enqueue; an idle timer from an older epoch does nothing
	tickObserver   []workflow.TickObserver       // fixed at Start
	recycleWatch   *workflow.RecycleWatch        // set at Start when recycle is enabled
	stopCause      *workflow.RecycleError        // the limit that stopped the client; see StopCause
	finished       []*run                        // finished runs in the order they ended; see pruneLocked

	observers []workflow.RunFinishedFunc // fixed at Start
	lifecycle []workflow.LifecycleFunc   // fixed at Start; see OnLifecycle
	hurry     chan struct{}              // closed by RunAll to end delays, backoffs, snoozes and sleeps now
	startedAt time.Time                  // when Start ran: NodeInfo.StartedAt
	ended     []notice                   // queued for the notifier goroutine
	endedPoke chan struct{}              // buffered 1: wakes the notifier
}

var (
	_ workflow.Client           = (*Client)(nil)
	_ workflow.RunNotifier      = (*Client)(nil)
	_ workflow.TickNotifier     = (*Client)(nil)
	_ workflow.RunQuerier       = (*Client)(nil)
	_ workflow.RunRetrier       = (*Client)(nil)
	_ workflow.RunDeleter       = (*Client)(nil)
	_ workflow.BulkCanceller    = (*Client)(nil)
	_ workflow.Pauser           = (*Client)(nil)
	_ workflow.SchedulePauser   = (*Client)(nil)
	_ workflow.ScheduleLister   = (*Client)(nil)
	_ workflow.Drainer          = (*Client)(nil)
	_ workflow.ReportingDrainer = (*Client)(nil)
	_ workflow.WorkerScaler     = (*Client)(nil)
	_ workflow.ClosureEnqueuer  = (*Client)(nil)
	_ workflow.StopCauser       = (*Client)(nil)

	_ workflow.RunRescheduler    = (*Client)(nil)
	_ workflow.StepCanceller     = (*Client)(nil)
	_ workflow.ResultReader      = (*Client)(nil)
	_ workflow.NodeLister        = (*Client)(nil)
	_ workflow.HealthChecker     = (*Client)(nil)
	_ workflow.LifecycleNotifier = (*Client)(nil)
	_ workflow.PayloadLimiter    = (*Client)(nil)
)

// New returns a client that has started nothing: no goroutine runs until
// Start.
func New(opts ...Option) *Client {
	c := &Client{
		clk:       clock.Real{},
		retention: DefaultRunRetention,
		errHist:   workflow.DefaultErrorHistory,
		workers:   defaultWorkers,
		log:       slog.New(slog.DiscardHandler),
		parse:     cron.Parse,
		wfs:       map[string]*registered{},
		runs:      map[workflow.RunID]*run{},
		delayed:   map[workflow.ScheduledID]*run{},
		idem:      map[idemKey]*run{},
		waiters:   map[eventKey][]*waiter{},
		signals:   map[eventKey][]signal{},
		buckets:   map[bucketKey][]*run{},
		rates:     map[rateKey][]time.Time{},
		scheds:    map[workflow.ScheduleID]*schedule{},

		queueExec: map[string]int{},
		pauses:    map[workflow.PauseTarget]bool{},
		done:      make(chan struct{}),

		endedPoke: make(chan struct{}, 1),
		hurry:     make(chan struct{}),
	}
	c.taskCond = sync.NewCond(&c.mu)
	for _, o := range opts {
		o(c)
	}
	if c.node == "" {
		c.node = workflow.DefaultNodeID()
	}
	return c
}

// registered is a workflow with its handlers wrapped and its graph indexed
// once, at Register.
type registered struct {
	wf        workflow.Workflow
	crons     []cron.Schedule
	exprs     []string                    // wf.Cron, index-aligned with crons
	handlers  map[string]workflow.Handler // Run steps, middleware applied
	onFailure workflow.Handler            // middleware applied; nil if none
	onTimeout workflow.Handler            // middleware applied; nil if none
	children  map[string][]string         // step name -> steps listing it in After
	steps     map[string]workflow.Step
	closure   bool // a closure run's one-off definition; see EnqueueClosure
}

// Capabilities returns everything except CapDurable.
func (c *Client) Capabilities() workflow.Caps { return caps }

// Register validates wf and stores it. Cron expressions are parsed here so
// a typo fails now, not at the first fire.
func (c *Client) Register(wf workflow.Workflow) error {
	if err := workflow.Validate(wf); err != nil {
		return err
	}
	reg := &registered{
		wf:       wf,
		handlers: map[string]workflow.Handler{},
		children: map[string][]string{},
		steps:    map[string]workflow.Step{},
	}
	for i, expr := range wf.Cron {
		s, err := c.parseCronFor(expr, wf.CronSeconds)
		if err != nil {
			return fmt.Errorf("%w: workflow %q: Cron[%d]: %w", workflow.ErrInvalid, wf.Name, i, err)
		}
		reg.crons = append(reg.crons, bounded(s, wf.CronActive))
		reg.exprs = append(reg.exprs, expr)
	}
	for _, s := range wf.Steps {
		reg.steps[s.Name] = s
		if s.Run != nil {
			reg.handlers[s.Name] = c.wrap(s.Run)
		}
		for _, p := range s.After {
			reg.children[p] = append(reg.children[p], s.Name)
		}
	}
	if wf.OnFailure != nil {
		reg.onFailure = c.wrap(wf.OnFailure)
	}
	if wf.OnTimeout != nil {
		reg.onTimeout = c.wrap(wf.OnTimeout)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.started {
		return workflow.ErrAlreadyStarted
	}
	c.wfs[wf.Name] = reg
	return nil
}

// exprKeys refuses every expression key in wf (concurrency, rate limit,
// per-run wait key): only Go functions can be evaluated in process, and a
// refusal at Start beats a failure mid-run.
func exprKeys(wf workflow.Workflow) error {
	reason := "the memory driver evaluates only KeyFunc keys; workflow " + wf.Name
	if wf.Concurrency != nil && wf.Concurrency.Key.Expr != "" {
		return workflow.Unsupported("Workflow.Concurrency.Key.Expr", reason)
	}
	for i, r := range wf.RateLimits {
		if r.Key.Expr != "" {
			return workflow.Unsupported(fmt.Sprintf("Workflow.RateLimits[%d].Key.Expr", i), reason)
		}
	}
	for _, s := range wf.Steps {
		if s.Wait != nil && s.Wait.KeyFrom.Expr != "" {
			return workflow.Unsupported("WaitSpec.KeyFrom.Expr", reason+", step "+s.Name)
		}
	}
	return nil
}

// parseCron runs the configured parser and fails closed: a custom parser
// that returns neither a schedule nor an error is refused, never treated
// as "no fires".
func (c *Client) parseCron(expr string) (cron.Schedule, error) { return c.parseCronFor(expr, false) }

// parseCronFor parses with cron.ParseSeconds when seconds is set
// (Schedule.Seconds, Workflow.CronSeconds), else with the configured
// parser.
func (c *Client) parseCronFor(expr string, seconds bool) (cron.Schedule, error) {
	parse := c.parse
	if seconds {
		parse = cron.ParseSeconds
	}
	s, err := parse(expr)
	if err == nil && s == nil {
		err = fmt.Errorf("cron parser returned no schedule for %q", expr)
	}
	return s, err
}

func (c *Client) wrap(h workflow.Handler) workflow.Handler {
	for i := len(c.mw) - 1; i >= 0; i-- {
		h = c.mw[i](h)
	}
	return h
}

// Start checks every workflow against Capabilities and refuses expression
// keys (KeyExpr) with an UnsupportedError, then starts the workers and the
// declared cron schedules. It does not block. Cancelling ctx shuts the client
// down (see Stop).
func (c *Client) Start(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.started {
		return workflow.ErrAlreadyStarted
	}
	if err := workflow.ValidateQueues(c.queueCfg); err != nil {
		return err
	}
	if err := workflow.CheckQueueLimits(caps, c.queueCfg); err != nil {
		return err
	}
	if err := workflow.ValidateRecycle(c.recycle); err != nil {
		return err
	}
	if len(c.queueCfg) > 0 {
		c.queues = make(map[string]workflow.QueueConfig, len(c.queueCfg))
		for _, q := range c.queueCfg {
			c.queues[q.Name] = q
		}
	}
	for _, reg := range c.wfs {
		if err := workflow.CheckCaps(caps, reg.wf); err != nil {
			return err
		}
		if err := exprKeys(reg.wf); err != nil {
			return err
		}
		if q := workflow.EffectiveQueue(reg.wf, workflow.Job{}); !c.servesLocked(q) {
			return fmt.Errorf("%w: workflow %q runs on queue %q, which WithQueues does not list; this one-process driver would never run it", workflow.ErrInvalid, reg.wf.Name, q)
		}
	}
	c.started = true
	c.startedAt = c.clk.Now()
	c.ctx, c.cancel = context.WithCancel(ctx)
	context.AfterFunc(c.ctx, c.shutdown)
	c.startRecycleLocked()
	for range c.workers {
		c.wg.Add(1)
		go c.worker()
	}
	if len(c.observers) > 0 || len(c.tickObserver) > 0 || len(c.lifecycle) > 0 {
		c.wg.Add(1)
		go c.notifier()
	}
	for _, name := range slices.Sorted(maps.Keys(c.wfs)) {
		reg := c.wfs[name]
		loc := reg.wf.CronLocation
		if loc == nil {
			loc = time.UTC
		}
		for i, s := range reg.crons {
			id := declaredID(reg.wf.Name, i)
			sc := &schedule{
				id:       id,
				name:     reg.exprs[i],
				s:        s,
				loc:      loc,
				misfire:  reg.wf.Misfire,
				windows:  reg.wf.CronWindows,
				overlap:  reg.wf.CronOverlap,
				workflow: reg.wf.Name,
				declared: true,
				stop:     make(chan struct{}),
				seconds:  reg.wf.CronSeconds,
				active:   reg.wf.CronActive,
				keep:     reg.wf.CronKeepRuns,
			}
			c.scheds[id] = sc
			c.startSchedule(sc)
		}
	}
	go func() { // not in wg: it waits for wg
		<-c.ctx.Done()
		// Mark the client stopped here rather than trusting the AfterFunc
		// goroutine to have run: anyone woken by Done must see every call
		// refused with ErrNotStarted. shutdown is idempotent.
		c.shutdown()
		c.wg.Wait()
		close(c.done)
	}()
	return nil
}

// servesLocked reports whether this client's workers serve queue q.
func (c *Client) servesLocked(q string) bool {
	if c.queues == nil {
		return true
	}
	_, ok := c.queues[q]
	return ok
}

// Stop shuts the client down and waits until every goroutine it started
// has exited. Unfinished runs become StatusCancelled. It is safe to call
// more than once, and before Start (a no-op).
func (c *Client) Stop() {
	c.mu.Lock()
	cancel := c.cancel
	c.mu.Unlock()
	if cancel == nil {
		return
	}
	cancel()
	c.shutdown()
	c.wg.Wait()
}

// shutdown runs once the root context ends: it cancels every unfinished
// run and wakes idle workers so they exit.
func (c *Client) shutdown() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stopped {
		return
	}
	c.stopped = true
	for _, r := range c.runs {
		c.cancelLocked(r, errShutdown)
	}
	for _, r := range c.delayed {
		c.cancelLocked(r, errShutdown)
	}
	for _, s := range c.scheds {
		s.halt()
	}
	if c.drained != nil && c.executing == 0 {
		closeOnce(c.drained)
	}
	if c.rateTm != nil {
		c.rateTm.Stop()
	}
	c.taskCond.Broadcast()
	c.pokeNotifier() // let it drain the shutdown cancellations, then exit
}

// usable reports whether calls that need workers may proceed. Callers hold
// c.mu.
func (c *Client) usableLocked() error {
	switch {
	case !c.started:
		return workflow.ErrNotStarted
	case c.stopped:
		return fmt.Errorf("%w: the client was stopped", workflow.ErrNotStarted)
	case c.draining:
		return fmt.Errorf("%w: the client is draining", workflow.ErrNotStarted)
	}
	return nil
}

func (c *Client) nextID(prefix string) string {
	c.seq++
	return fmt.Sprintf("%s-%d", prefix, c.seq)
}

// Enqueue starts a run now, or at job.RunAt. See the package doc for how
// idempotency keys, concurrency and rate limits apply.
func (c *Client) Enqueue(ctx context.Context, job workflow.Job) (workflow.RunRef, error) {
	if err := ctx.Err(); err != nil {
		return workflow.RunRef{}, err
	}
	if err := workflow.CheckJobLabels(ctx, job); err != nil {
		return workflow.RunRef{}, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.enqueueLocked(ctx, job)
}

// enqueueLocked stores job as a run. ctx carries the caller's
// EnqueueOutcome, if any.
func (c *Client) enqueueLocked(ctx context.Context, job workflow.Job) (workflow.RunRef, error) {
	if err := c.usableLocked(); err != nil {
		return workflow.RunRef{}, err
	}
	if err := workflow.ValidateJob(job); err != nil {
		return workflow.RunRef{}, err
	}
	if err := workflow.CheckPayload(job, c.payloadLimit, "memory.WithPayloadLimit"); err != nil {
		return workflow.RunRef{}, err
	}
	reg, ok := c.wfs[job.Workflow]
	if !ok {
		return workflow.RunRef{}, fmt.Errorf("%w: workflow %q is not registered", workflow.ErrNotFound, job.Workflow)
	}
	if err := workflow.CheckJobQueue(caps, reg.wf, job); err != nil {
		return workflow.RunRef{}, err
	}
	if q := workflow.EffectiveQueue(reg.wf, job); !c.servesLocked(q) {
		return workflow.RunRef{}, fmt.Errorf("%w: queue %q is not served by this client (WithQueues); the run would never start", workflow.ErrInvalid, q)
	}
	c.pruneLocked()
	key := job.IdempotencyKey
	switch reg.wf.Idempotency {
	case workflow.IdempotencyOff:
		key = ""
	case workflow.IdempotencyRequired:
		if key == "" {
			return workflow.RunRef{}, fmt.Errorf("%w: workflow %q requires an IdempotencyKey", workflow.ErrInvalid, reg.wf.Name)
		}
	}
	ik := idemKey{workflow: reg.wf.Name, key: key}
	if key != "" {
		if holder, ok := c.idem[ik]; ok {
			workflow.MarkDuplicate(ctx)
			return holder.ref(), nil
		}
	}

	r := c.newRunLocked(reg, job)
	if key != "" {
		r.idem = &ik
		c.idem[ik] = r
	}
	ref := c.startRunLocked(r)
	workflow.MarkInserted(ctx)
	c.emitLocked(r, workflow.LifecycleInserted, "", 0, nil)
	return ref, nil
}

// startRunLocked arms a new run's delayed start, or readies it now.
func (c *Client) startRunLocked(r *run) workflow.RunRef {
	now := c.clk.Now()
	r.runAt = r.job.RunAt
	if at := r.job.RunAt; !at.IsZero() && at.After(now) {
		r.sid = workflow.ScheduledID(c.nextID("delayed"))
		r.status = workflow.StatusScheduled
		c.delayed[r.sid] = r
		c.armDelayLocked(r, at.Sub(now))
		return r.ref()
	}
	c.readyLocked(r)
	return r.ref()
}

// armDelayLocked starts the timer of a delayed run. Reschedule re-arms it:
// closing the old rearm channel retires the old timer's goroutine.
func (c *Client) armDelayLocked(r *run, d time.Duration) {
	if r.rearm != nil {
		close(r.rearm)
	}
	rearm := make(chan struct{})
	r.rearm = rearm
	t := c.clk.NewTimer(d)
	hurry := c.hurry
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		select {
		case <-t.C():
		case <-hurry:
			t.Stop()
		case <-rearm:
			t.Stop()
			return
		case <-r.ctx.Done():
			t.Stop()
			return
		}
		c.mu.Lock()
		if r.status == workflow.StatusScheduled && !c.stopped && r.rearm == rearm {
			c.readyLocked(r)
		}
		c.mu.Unlock()
	}()
}

// readyLocked gives r its RunID (a retried run keeps its own) and applies
// the concurrency strategy, then tries to admit it.
func (c *Client) readyLocked(r *run) {
	if r.id.IsZero() {
		r.id = workflow.RunID(c.nextID("run"))
	}
	r.status = workflow.StatusQueued
	c.runs[r.id] = r
	if conc := r.reg.wf.Concurrency; conc != nil {
		bk := bucketKey{workflow: r.reg.wf.Name, key: r.bucket}
		if len(c.buckets[bk]) >= conc.MaxRuns {
			switch conc.Strategy {
			case workflow.ConcurrencyDropNewest, workflow.ConcurrencyCancelNewest:
				c.cancelLocked(r, fmt.Errorf("%w: concurrency bucket %q is full (%s)", workflow.ErrCancelled, r.bucket, conc.Strategy))
				return
			case workflow.ConcurrencyCancelInProgress:
				for len(c.buckets[bk]) >= conc.MaxRuns {
					c.cancelLocked(c.buckets[bk][0], fmt.Errorf("%w: replaced by a newer run (%s)", workflow.ErrCancelled, conc.Strategy))
				}
			}
		}
	}
	c.pending = append(c.pending, r)
	c.admitLocked()
}

// Cancel stops a run or a delayed start; finished runs are left alone.
func (c *Client) Cancel(_ context.Context, ref workflow.RunRef) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	r, err := c.lookupLocked(ref)
	if err != nil {
		return err
	}
	c.endLocked(r, workflow.StatusCancelled)
	return nil
}

// Status reports where a run is.
func (c *Client) Status(_ context.Context, ref workflow.RunRef) (workflow.Status, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	r, err := c.lookupLocked(ref)
	if err != nil {
		return "", err
	}
	return r.statusLocked(), nil
}

// closeOnce closes ch unless it is already closed. Callers hold c.mu, so
// the check and the close cannot race.
func closeOnce(ch chan struct{}) {
	select {
	case <-ch:
	default:
		close(ch)
	}
}

func (c *Client) lookupLocked(ref workflow.RunRef) (*run, error) {
	if !ref.RunID.IsZero() {
		if r, ok := c.runs[ref.RunID]; ok {
			return r, nil
		}
	}
	if !ref.ScheduledID.IsZero() {
		if r, ok := c.delayed[ref.ScheduledID]; ok {
			return r, nil
		}
	}
	return nil, fmt.Errorf("%w: run %+v", workflow.ErrNotFound, ref)
}

// Signal resumes every run currently waiting on ev's name and key, and
// keeps ev so a run enqueued before this call that reaches a matching wait
// step later takes it then (once per run). Kept signals older than the
// oldest unfinished run are dropped, which bounds the buffer.
func (c *Client) Signal(ctx context.Context, ev workflow.Event) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.usableLocked(); err != nil {
		return err
	}
	c.seq++
	sg := signal{seq: c.seq, payload: ev.Payload}
	k := eventKey{name: ev.Name, key: ev.Key}
	for _, w := range c.waiters[k] {
		select {
		case w.ch <- ev.Payload:
			w.r.markConsumedLocked(sg.seq)
			w.deliverLocked()
		default: // already delivered or timed out
		}
	}
	delete(c.waiters, k)
	c.signals[k] = append(c.signals[k], sg)
	c.pruneSignalsLocked()
	return nil
}

// pruneSignalsLocked bounds the kept signals: a signal older than the
// oldest unfinished run can never be taken (runs only take signals sent
// after their enqueue), so it is dropped.
func (c *Client) pruneSignalsLocked() {
	oldest := c.seq + 1
	for _, r := range c.runs {
		if !r.status.IsTerminal() && r.seq < oldest {
			oldest = r.seq
		}
	}
	for _, r := range c.delayed {
		if !r.status.IsTerminal() && r.seq < oldest {
			oldest = r.seq
		}
	}
	for k, sgs := range c.signals {
		sgs = slices.DeleteFunc(sgs, func(sg signal) bool { return sg.seq < oldest })
		if len(sgs) == 0 {
			delete(c.signals, k)
		} else {
			c.signals[k] = sgs
		}
	}
}
