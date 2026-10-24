package redis

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"sync"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/cron"
)

// caps is what this driver really does, each proven by the workflowtest
// conformance suite against a real Redis. See the package doc for what is
// not declared and why.
const caps = workflow.CapDurable | workflow.CapDelayedStart | workflow.CapDurableSleep |
	workflow.CapIdempotencyKey | workflow.CapCron | workflow.CapCancel |
	workflow.CapRetryJitter | workflow.CapRetryDelay | workflow.CapJobRetry |
	workflow.CapRetryUntil | workflow.CapRetryMaxErrors |
	workflow.CapMisfireSkip | workflow.CapMisfireFireOnce | workflow.CapMisfireFireAll |
	workflow.CapCronZone | workflow.CapRunQuery | workflow.CapDeleteRun | workflow.CapDrain | workflow.CapDrainReport |
	workflow.CapScheduleList | workflow.CapScheduleTrigger | workflow.CapScheduleHistory |
	workflow.CapOverlapSkip | workflow.CapEnqueueOnly | workflow.CapErrorHistory |
	workflow.CapShutdownRequeue | workflow.CapForceRun |
	workflow.CapWaitForEvent | workflow.CapConcurrencyKey | workflow.CapRateLimit |
	workflow.CapRunFinished | workflow.CapQueue | workflow.CapQueueOverride |
	workflow.CapDiscard | workflow.CapDiscardExhausted | workflow.CapRunStates | workflow.CapRetryRun | workflow.CapCancelAll |
	workflow.CapPauseWorkflow | workflow.CapPauseQueue | workflow.CapChain | workflow.CapLabelPrefix |
	workflow.CapTenant | workflow.CapDueQuery | workflow.CapResult | workflow.CapNodeIdentity |
	workflow.CapSnooze | workflow.CapHeartbeat |
	workflow.CapCronSeconds | workflow.CapScheduleActive | workflow.CapScheduleKeep | workflow.CapTickTime |
	workflow.CapRunTimeout | workflow.CapKeyFIFO |
	workflow.CapScheduleWhen | workflow.CapScheduleWindow | workflow.CapTickEvents | workflow.CapSchedulePause |
	workflow.CapWorkerScale

// Defaults. Each has an option that changes it.
const (
	// defaultPrefix starts every key; it is wrapped in a hash tag, so on a
	// Redis Cluster every key of one prefix lives in one slot and the Lua
	// scripts can touch them together.
	defaultPrefix = "wf"
	// defaultLease is how long a claimed run stays claimed without a
	// renewal; a dead worker's run is claimed again after at most this.
	defaultLease = 30 * time.Second
	// defaultPoll is how often an idle worker looks for work when no
	// wake-up message arrived (a missed publish, a due time reached).
	defaultPoll = time.Second
	// defaultWorkers is how many runs one client executes at once.
	defaultWorkers = 10
	// defaultMaxReclaims is how many lease expiries in a row (with no step
	// finishing in between) a run survives before it fails with
	// ErrAbandoned, so a step that kills every worker it lands on cannot
	// loop through the cluster forever. Five tolerates a rolling restart.
	defaultMaxReclaims = 5
	// defaultRetention is how long a finished run stays readable (Status,
	// Runs) before Redis expires it; the same day the memory driver keeps.
	defaultRetention = 24 * time.Hour
	// releaseTimeout bounds the write that hands a run back when the
	// client stops, which runs after the client's own context ended.
	releaseTimeout = 5 * time.Second
	// housekeepEvery is how often (lease clock) a client drops kept
	// signals no run can take any more and recorded ends of expired runs.
	housekeepEvery = time.Minute
	// deliverBatch is how many run ends one delivery pass claims per
	// workflow.
	deliverBatch = 100
	// admitBatch is how many pending runs of one workflow one admission
	// pass reads.
	admitBatch = 100
)

// prefixPattern keeps prefixes to characters that cannot open or close a
// hash tag or collide with the key separator.
var prefixPattern = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,64}$`)

// ErrAbandoned is the error of a run whose lease expired more than
// WithMaxReclaims times in a row: every worker that took it died (or lost
// Redis) before a step finished.
var ErrAbandoned = errors.New("redis: run abandoned by its workers")

// Option configures a Client.
type Option func(*Client)

// WithPrefix sets the key prefix (default "wf"). Clients that share a
// prefix share every run and schedule; give each app or test its own.
// Keys look like {prefix}:run:<id>.
func WithPrefix(p string) Option { return func(c *Client) { c.prefix = p } }

// WithClock sets the time source of every due time: delayed starts,
// sleeps, retry backoff, step timeouts and cron fires. The default is
// clock.Real; tests pass clock.NewFake.
func WithClock(clk clock.Clock) Option {
	return func(c *Client) {
		if clk != nil {
			c.clk = clk
		}
	}
}

// WithLeaseClock sets the clock leases and polling are measured on. It is
// separate from WithClock because a lease is about a worker being alive,
// which is wall time: a test that moves a fake scheduling clock by an hour
// must not make every running step look abandoned. The default is
// clock.Real.
func WithLeaseClock(clk clock.Clock) Option {
	return func(c *Client) {
		if clk != nil {
			c.leaseClk = clk
		}
	}
}

// WithLease sets how long a claimed run stays claimed without renewal
// (renewed every third of it). Values of zero or less are ignored.
func WithLease(d time.Duration) Option {
	return func(c *Client) {
		if d > 0 {
			c.lease = d
		}
	}
}

// WithPollInterval sets how often an idle worker looks for work without a
// wake-up message. Values of zero or less are ignored.
func WithPollInterval(d time.Duration) Option {
	return func(c *Client) {
		if d > 0 {
			c.poll = d
		}
	}
}

// WithWorkers sets how many runs this client executes at once. Values
// below 1 are ignored.
func WithWorkers(n int) Option {
	return func(c *Client) {
		if n >= 1 {
			c.workers = n
		}
	}
}

// WithMaxReclaims sets how many lease expiries in a row a run survives
// before it fails with ErrAbandoned. Negative values are ignored.
func WithMaxReclaims(n int) Option {
	return func(c *Client) {
		if n >= 0 {
			c.maxReclaims = n
		}
	}
}

// WithRetention sets how long a finished run stays readable before Redis
// expires its key (measured by Redis, so wall time). Zero keeps finished
// runs until Delete; negative values are ignored.
func WithRetention(d time.Duration) Option {
	return func(c *Client) {
		if d >= 0 {
			c.retention = d
		}
	}
}

// WithEnqueueOnly makes an enqueue-only client, for a node that adds work
// but never runs it (a web server beside a separate worker fleet):
// Enqueue, Status, Cancel, Signal, Runs, Delete and the run forcers work
// without Start, Register is optional, and Start returns nil and starts
// nothing (no worker, no scheduler, no subscription). A job for a
// workflow this client registered gets that workflow's checks (its
// idempotency policy); a job for one it did not is checked by
// workflow.CheckJob only, and runs once a worker client that registers it
// claims it.
func WithEnqueueOnly() Option { return func(c *Client) { c.enqueueOnly = true } }

// WithErrorHistory sets how many failed attempts RunInfo.Errors keeps per
// run, newest kept (workflow.DefaultErrorHistory by default). Zero or less
// turns the history off.
func WithErrorHistory(n int) Option {
	return func(c *Client) {
		c.errHistory = n
	}
}

// WithLogger sets where background errors go (a failed renewal, a run
// that could not be handed back). The default discards them.
func WithLogger(l *slog.Logger) Option {
	return func(c *Client) {
		if l != nil {
			c.log = l
		}
	}
}

// WithMiddleware wraps every step handler and OnFailure handler, outermost
// first.
func WithMiddleware(mw ...workflow.Middleware) Option {
	return func(c *Client) { c.mw = append(c.mw, mw...) }
}

// WithCronParser sets the cron dialect. Every client of one prefix must
// use the same parser, since any of them may fire any schedule. The
// default is cron.Parse.
func WithCronParser(p cron.Parser) Option {
	return func(c *Client) {
		if p != nil {
			c.parse = p
		}
	}
}

// WithJitterSource sets the random source of retry jitter, a function
// returning values in [0, 1). Tests pin it; the default is math/rand.
func WithJitterSource(rnd func() float64) Option {
	return func(c *Client) { c.rnd = rnd }
}

// WithIDFunc sets how run IDs are made. IDs must be unique across every
// client of the prefix and may not contain ':' or '{'. The default is the
// creation time in milliseconds (13 hex digits, so IDs sort by time and
// equal-priority runs start oldest first) followed by 16 random hex
// digits. A function that returns IDs in another order changes only the
// order of equal-priority runs.
func WithIDFunc(f func() string) Option {
	return func(c *Client) {
		if f != nil {
			c.newID = f
		}
	}
}

// WithQueues makes this client serve only the named queues, each with its
// worker cap and priority (workflow.QueueConfig): runs of other queues wait
// for a client that serves them, which is how separate worker processes
// per queue are deployed. A run is claimed whole, so Workers caps how many
// runs of the queue this client executes at once (each may run parallel
// steps), within WithWorkers. Without it a client serves every queue from
// one pool. Start refuses an invalid list.
func WithQueues(qs ...workflow.QueueConfig) Option {
	return func(c *Client) { c.queues = append(c.queues, qs...) }
}

// Client is the Redis driver of workflow.Client. Build it with New,
// Register workflows, then Start.
type Client struct {
	rdb          goredis.UniversalClient
	prefix       string
	base         string // "{prefix}", the hash-tagged start of every key
	clk          clock.Clock
	leaseClk     clock.Clock
	lease        time.Duration
	poll         time.Duration
	workers      int
	maxReclaims  int
	retention    time.Duration
	log          *slog.Logger
	mw           []workflow.Middleware
	parse        cron.Parser
	rnd          func() float64
	newID        func() string
	enqueueOnly  bool
	errHistory   int
	queues       []workflow.QueueConfig
	node         string            // this client's node ID
	payloadLimit int               // WithPayloadLimit; 0 means maxPayloadBytes
	mode         workflow.NodeMode // "" (full), NodeScheduler or NodeWorker
	lifecycle    []workflow.LifecycleFunc
	lifeCh       chan workflow.LifecycleEvent

	// claimMu serialises claims while a queue has a worker cap, so two
	// workers of this client cannot both take the last slot of a queue.
	claimMu         sync.Mutex
	mu              sync.Mutex
	queueCfg        map[string]workflow.QueueConfig // by name; empty serves every queue
	heldQ           map[string]int                  // held runs per queue
	observers       []workflow.RunFinishedFunc
	tickObservers   []workflow.TickObserver
	rateWake        time.Time // earliest moment a rate-limited pending run may start
	lastHK          time.Time // last housekeeping pass, on the lease clock
	notifyCh        chan struct{}
	startedAt       time.Time // when Start ran, on the lease clock
	attemptsRunning int       // step attempts executing now
	wfs             map[string]*registered
	names           []string // served workflow names, fixed at Start
	started         bool
	stopped         bool
	draining        bool
	drainTrack      workflow.DrainTracker // attempts a drain finished and cut off
	ctx             context.Context
	cancel          context.CancelFunc
	running         map[string]*held // held runs by ID
	inflight        int              // runs being executed
	idle            chan struct{}    // closed when inflight drops to zero while draining
	wakeCh          chan struct{}    // closed and replaced to wake every idle worker
	dispCh          chan struct{}    // pokes the dispatcher (buffered 1)
	crashed         bool             // test seam: behave like a killed process
	wg              sync.WaitGroup
	done            chan struct{}
}

// New returns an unstarted client over rdb. It never touches Redis until
// Start.
func New(rdb goredis.UniversalClient, opts ...Option) (*Client, error) {
	if rdb == nil {
		return nil, fmt.Errorf("%w: redis: nil Redis client", workflow.ErrInvalid)
	}
	c := &Client{
		rdb:         rdb,
		prefix:      defaultPrefix,
		clk:         clock.Real{},
		leaseClk:    clock.Real{},
		lease:       defaultLease,
		poll:        defaultPoll,
		workers:     defaultWorkers,
		maxReclaims: defaultMaxReclaims,
		retention:   defaultRetention,
		errHistory:  workflow.DefaultErrorHistory,
		log:         slog.New(slog.DiscardHandler),
		parse:       cron.Parse,
		wfs:         map[string]*registered{},
		running:     map[string]*held{},
		wakeCh:      make(chan struct{}),
		dispCh:      make(chan struct{}, 1),
		notifyCh:    make(chan struct{}, 1),
		lifeCh:      make(chan workflow.LifecycleEvent, lifecycleBuffer),
		node:        workflow.DefaultNodeID(),
		heldQ:       map[string]int{},
		queueCfg:    map[string]workflow.QueueConfig{},
		done:        make(chan struct{}),
	}
	for _, o := range opts {
		o(c)
	}
	if !prefixPattern.MatchString(c.prefix) {
		return nil, fmt.Errorf("%w: redis: prefix %q must match %s", workflow.ErrInvalid, c.prefix, prefixPattern)
	}
	c.base = "{" + c.prefix + "}"
	if c.newID == nil {
		c.newID = c.defaultID
	}
	return c, nil
}

// defaultID is the creation time in ms as 13 hex digits plus 16 random hex
// digits. crypto/rand never fails on supported platforms.
func (c *Client) defaultID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return fmt.Sprintf("%013x", c.clk.Now().UnixMilli()) + hex.EncodeToString(b[:])
}

// registered is a workflow with its handlers wrapped and its graph indexed
// once, at Register.
type registered struct {
	wf        workflow.Workflow
	crons     []cron.Schedule
	handlers  map[string]workflow.Handler
	onFailure workflow.Handler
	onTimeout workflow.Handler
	steps     map[string]workflow.Step
}

// Capabilities returns what this driver does; see the package doc.
func (c *Client) Capabilities() workflow.Caps { return caps }

// Register validates wf and stores it. Cron expressions are parsed here so
// a typo fails now, not at the first fire.
func (c *Client) Register(wf workflow.Workflow) error {
	if err := workflow.Validate(wf); err != nil {
		return err
	}
	if len(wf.Cron) > 0 && wf.CronLocation != nil {
		// Refused here, not at the first fire: every client must be able to
		// rebuild the zone from what Redis stores.
		if _, err := encodeLoc(wf.CronLocation); err != nil {
			return fmt.Errorf("workflow %q: CronLocation: %w", wf.Name, err)
		}
	}
	reg := &registered{wf: wf, handlers: map[string]workflow.Handler{}, steps: map[string]workflow.Step{}}
	for i, expr := range wf.Cron {
		s, err := c.parseCronFor(expr, wf.CronSeconds)
		if err != nil {
			return fmt.Errorf("%w: workflow %q: Cron[%d]: %w", workflow.ErrInvalid, wf.Name, i, err)
		}
		reg.crons = append(reg.crons, s)
	}
	for _, s := range wf.Steps {
		reg.steps[s.Name] = s
		if s.Run != nil {
			reg.handlers[s.Name] = c.wrap(s.Run)
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

// parseCronFor parses a schedule's expression as workflow.ParseCron does
// (six fields with seconds, else the configured parser) and fails closed: a parser that
// returns neither a schedule nor an error is refused.
func (c *Client) parseCronFor(expr string, seconds bool) (cron.Schedule, error) {
	s, err := workflow.ParseCron(expr, seconds, c.parse)
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

// Start checks every workflow against Capabilities, writes the declared
// cron schedules, subscribes to wake-up messages and starts the workers
// and the dispatcher. It does not block; cancelling ctx stops the client,
// handing its running runs back so another client resumes them.
func (c *Client) Start(ctx context.Context) error {
	if c.enqueueOnly {
		return nil
	}
	c.mu.Lock()
	if c.started {
		c.mu.Unlock()
		return workflow.ErrAlreadyStarted
	}
	if err := workflow.ValidateQueues(c.queues); err != nil {
		c.mu.Unlock()
		return err
	}
	// A run is claimed whole and its parallel steps are not counted
	// individually, so a cluster-wide count of in-flight step attempts would
	// need a lease-scored slot set beside the claim script; until then the
	// driver refuses QueueConfig.Limit rather than enforce it per client.
	if err := workflow.CheckQueueLimits(caps, c.queues); err != nil {
		c.mu.Unlock()
		return err
	}
	for _, q := range c.queues {
		c.queueCfg[q.Name] = q
	}
	for _, reg := range c.wfs {
		if err := workflow.CheckCaps(caps, reg.wf); err != nil {
			c.mu.Unlock()
			return err
		}
		if err := exprKeys(reg.wf); err != nil {
			c.mu.Unlock()
			return err
		}
	}
	names := make([]string, 0, len(c.wfs))
	for n := range c.wfs {
		names = append(names, n)
	}
	c.mu.Unlock()

	if err := c.declareCrons(ctx); err != nil {
		return err
	}
	if err := c.declareFIFO(ctx); err != nil {
		return err
	}
	ps := c.rdb.Subscribe(ctx, c.base+":wake")
	if _, err := ps.Receive(ctx); err != nil {
		_ = ps.Close()
		return fmt.Errorf("redis: subscribe to wake-ups: %w", classify(err))
	}

	c.mu.Lock()
	if c.started {
		c.mu.Unlock()
		_ = ps.Close()
		return workflow.ErrAlreadyStarted
	}
	c.started = true
	c.names = names
	c.ctx, c.cancel = context.WithCancel(ctx)
	c.startedAt = c.leaseClk.Now()
	workers := c.workers
	if c.mode == workflow.NodeScheduler {
		workers = 0
	}
	c.mu.Unlock()

	c.wg.Add(5 + workers)
	go c.listen(ps)
	go c.dispatch()
	go c.notifier()
	go c.reporter()
	go c.lifecycleLoop()
	for range workers {
		go c.worker()
	}
	go func() { // not in wg: it waits for wg
		<-c.ctx.Done()
		// Stopped is set before Done closes, so anyone woken by Done finds
		// every call refused with ErrNotStarted.
		c.mu.Lock()
		c.stopped = true
		c.mu.Unlock()
		c.wakeAll()
		_ = ps.Close()
		c.wg.Wait()
		close(c.done)
	}()
	return nil
}

// Stop cancels the client and waits until it has stopped. Safe to call
// more than once and before Start.
func (c *Client) Stop() {
	c.mu.Lock()
	cancel := c.cancel
	c.mu.Unlock()
	if cancel == nil {
		return
	}
	cancel()
	<-c.done
}

// Done is closed once the client has fully stopped.
func (c *Client) Done() <-chan struct{} { return c.done }

// usable reports whether calls that need a running client may proceed.
func (c *Client) usable() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch {
	case c.enqueueOnly:
		return nil
	case !c.started:
		return workflow.ErrNotStarted
	case c.stopped:
		return fmt.Errorf("%w: the client was stopped", workflow.ErrNotStarted)
	case c.draining:
		return fmt.Errorf("%w: the client is draining", workflow.ErrNotStarted)
	}
	return nil
}

// wakeAll wakes every idle worker.
func (c *Client) wakeAll() {
	c.mu.Lock()
	close(c.wakeCh)
	c.wakeCh = make(chan struct{})
	c.mu.Unlock()
}

// declareFIFO records which registered workflows order by age alone
// (Concurrency.FIFO) in {prefix}:fifo, which the scripts read when they
// queue a run, whichever client enqueued it.
func (c *Client) declareFIFO(ctx context.Context) error {
	c.mu.Lock()
	var on, off []string
	for name, r := range c.wfs {
		if r.wf.Concurrency != nil && r.wf.Concurrency.FIFO {
			on = append(on, name)
		} else {
			off = append(off, name)
		}
	}
	c.mu.Unlock()
	pipe := c.rdb.Pipeline()
	for _, n := range on {
		pipe.HSet(ctx, c.base+":fifo", n, "1")
	}
	if len(off) > 0 {
		pipe.HDel(ctx, c.base+":fifo", off...)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("redis: record FIFO workflows: %w", classify(err))
	}
	return nil
}

// pokeNotifier asks the run-finished notifier for a pass now.
func (c *Client) pokeNotifier() {
	select {
	case c.notifyCh <- struct{}{}:
	default:
	}
}

// exprKeys refuses every expression key in wf: Redis has no engine
// expression language, so concurrency, rate and wait keys are Go
// functions (workflow.KeyFunc) evaluated by the client.
func exprKeys(wf workflow.Workflow) error {
	reason := "the Redis driver evaluates only KeyFunc keys; workflow " + wf.Name
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

// pokeDispatcher asks the dispatcher for a pass now.
func (c *Client) pokeDispatcher() {
	select {
	case c.dispCh <- struct{}{}:
	default:
	}
}

// listen turns wake-up messages into local wake-ups: "e" (new ready work)
// wakes the workers; "c:<id>" and "f:<id>" stop a run this client holds,
// "x:<id>\n<step>" withdraws one of its steps (CancelStep) and "t:<id>"
// ends it past its RunTimeout.
func (c *Client) listen(ps *goredis.PubSub) {
	defer c.wg.Done()
	ch := ps.Channel()
	for {
		select {
		case <-c.ctx.Done():
			return
		case m, ok := <-ch:
			if !ok {
				return
			}
			if id, cause := stopMessage(m.Payload); cause != nil {
				c.mu.Lock()
				h := c.running[id]
				c.mu.Unlock()
				if h != nil {
					h.cancel(cause)
				}
				continue
			}
			if rest, ok := cutPrefix(m.Payload, "x:"); ok {
				id, step, _ := strings.Cut(rest, "\n")
				c.mu.Lock()
				h := c.running[id]
				c.mu.Unlock()
				if h != nil {
					h.cancelStep(step)
				}
				continue
			}
			if id, ok := cutPrefix(m.Payload, "t:"); ok {
				c.mu.Lock()
				h := c.running[id]
				c.mu.Unlock()
				if h != nil {
					h.timeOut()
				}
				continue
			}
			c.wakeAll()
			c.pokeDispatcher()
			c.pokeNotifier()
		}
	}
}

// stopMessage decodes a published stop: "c:<id>" (Cancel) or "f:<id>"
// (ForceComplete, ForceFail), with the cause the holder's steps see.
func stopMessage(payload string) (id string, cause error) {
	if id, ok := cutPrefix(payload, "c:"); ok {
		return id, workflow.ErrCancelled
	}
	if id, ok := cutPrefix(payload, "f:"); ok {
		return id, workflow.ErrForced
	}
	return "", nil
}

func cutPrefix(s, p string) (string, bool) {
	if len(s) >= len(p) && s[:len(p)] == p {
		return s[len(p):], true
	}
	return "", false
}

// classify marks errors that mean Redis is unreachable as
// workflow.ErrUnavailable, so failover and callers can tell an outage
// from a refusal.
func classify(err error) error {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	var ne interface{ Timeout() bool }
	if errors.As(err, &ne) || errors.Is(err, goredis.ErrClosed) || isConnErr(err) {
		return workflow.Unavailable(err)
	}
	return err
}
