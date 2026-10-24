package sqlstore

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"regexp"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ubgo/sqlb"
	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/cron"
)

// caps is what the conformance suite proves on SQLite, Postgres and MySQL.
// CapDurable is the point of the driver: every run, step result, wait,
// sleep, schedule and signal is a row, so a restart or a dead worker loses
// nothing. Retry delays are computed in Go (workflow.RetryPolicy.NextDelay),
// so jitter and RetryAfter cost nothing; cron zones and misfire policies
// are stored with each schedule.
const caps = workflow.CapDurable | workflow.CapDelayedStart | workflow.CapWaitForEvent |
	workflow.CapDurableSleep | workflow.CapConcurrencyKey | workflow.CapRateLimit |
	workflow.CapIdempotencyKey | workflow.CapCron | workflow.CapCancel | workflow.CapRunFinished |
	workflow.CapRetryJitter | workflow.CapRetryDelay | workflow.CapCronZone |
	workflow.CapMisfireSkip | workflow.CapMisfireFireOnce | workflow.CapMisfireFireAll |
	workflow.CapLabelPrefix | workflow.CapQueueLimitGlobal | parityCaps

// parityCaps are the queue, retry, admin, pause, schedule and drain
// capabilities. Every one is stored state or a decision
// made in Go on the claiming or firing node, so all three engines honour
// them the same way: queues and pauses are rows the claim statement reads,
// per-run retry policies and attempt counts are rows the record step
// reads, schedule windows and pauses are rows the firing node reads.
const parityCaps = workflow.CapQueue | workflow.CapQueueOverride | workflow.CapJobRetry |
	workflow.CapRetryUntil | workflow.CapRetryMaxErrors | workflow.CapDiscard | workflow.CapDiscardExhausted |
	workflow.CapRunQuery | workflow.CapRetryRun | workflow.CapDeleteRun | workflow.CapCancelAll |
	workflow.CapPauseWorkflow | workflow.CapPauseQueue | workflow.CapSchedulePause |
	workflow.CapScheduleWhen | workflow.CapScheduleWindow | workflow.CapTickEvents |
	workflow.CapScheduleList | workflow.CapDrain | workflow.CapDrainReport | batch2Caps

// batch2Caps are the run-state, chain and worker-scale capabilities.
// Retrying is read from the step rows (a ready step that failed before and
// is not yet due), Discarded is a stored run status, a run's remaining
// chain is a row written in the same transaction as the step success that
// edits it, and worker counts are this client's per-queue caps. Closures
// are not among them: a Go function cannot be stored, so a closure could
// not survive the restart this driver exists for, and
// workflow.EnqueueClosure refuses this client.
const batch2Caps = workflow.CapRunStates | workflow.CapChain | workflow.CapWorkerScale | p4Caps

// p4Caps are overlap skip, trigger now and last-fire bookkeeping (one
// schedule_last row per schedule, moved in the firing transaction), the
// enqueue-only mode (WithEnqueueOnly), the bounded per-attempt error
// history (attempt_errors), shutdown requeue (a step cut short by a clean
// stop is handed back with its attempt refunded: attempt.release) and
// force-complete and force-fail.
const p4Caps = workflow.CapOverlapSkip | workflow.CapScheduleTrigger | workflow.CapScheduleHistory |
	workflow.CapEnqueueOnly | workflow.CapErrorHistory | workflow.CapShutdownRequeue | workflow.CapForceRun | p5Caps

// p5Caps are stored per run in run_info (migration 008): the tenant, the
// result written when the run succeeds and the nodes that claimed its
// attempts; due times are the runs table's run_at, kept after the start;
// every client reports itself in the nodes table.
const p5Caps = workflow.CapTenant | workflow.CapResult | workflow.CapDueQuery | workflow.CapNodeIdentity | workflow.CapSnooze | workflow.CapHeartbeat | workflow.CapRunTimeout | workflow.CapKeyFIFO |
	workflow.CapCronSeconds | workflow.CapScheduleActive | workflow.CapScheduleKeep | workflow.CapTickTime

// Run states as stored in runs.status. Scheduled, the three terminal ones
// and pending/active map onto workflow.Status (see Client.Status); failing
// is internal: the run has a step that failed for good and its OnFailure
// handler has not finished yet.
const (
	runScheduled = "scheduled" // a delayed start before RunAt
	runPending   = "pending"   // ready, not admitted (concurrency, rate limit)
	runActive    = "active"    // admitted; its steps are rows
	runFailing   = "failing"   // OnFailure in progress, then failed
	runSucceeded = "succeeded"
	runFailed    = "failed"
	runCancelled = "cancelled"
	// runDiscarded is a run a step ended with workflow.Discard. Migration
	// 004 rewrites the runs earlier releases stored as cancelled with
	// run_ext.discarded = 1.
	runDiscarded = "discarded"
)

// Step states as stored in steps.status.
const (
	stepPending = "pending" // a parent has not succeeded yet
	stepReady   = "ready"   // claimable from due on
	// stepSnoozed is a step put back by workflow.Snooze: claimable from due
	// on like ready, its attempt refunded; the run reports Waiting.
	stepSnoozed   = "snoozed"
	stepRunning   = "running"  // claimed; lease_token and lease_until set
	stepSleeping  = "sleeping" // a sleep step until due
	stepWaiting   = "waiting"  // a wait step until a signal or due (0: forever)
	stepSucceeded = "succeeded"
	stepFailed    = "failed"
	stepCancelled = "cancelled"
)

// Defaults, each overridable by an option.
const (
	// defaultWorkers is how many step attempts one client executes at once.
	defaultWorkers = 8
	// defaultPoll is how often a client looks for work other nodes made
	// due. Work this client makes due itself wakes it at once.
	defaultPoll = time.Second
	// defaultLease is how long a claimed step is held without a renewal.
	// A worker renews at a third of it, so a live worker never loses a
	// step and a dead one's step is reclaimed within one lease.
	defaultLease = 30 * time.Second
	// defaultMaxReclaims is how often one attempt of a step may be taken
	// over after its worker died (its lease expired) before the attempt
	// fails with ErrAbandoned. A worker dies for reasons unrelated to the
	// step (a deploy, an OOM elsewhere), so a few takeovers are normal;
	// a step that kills every worker it lands on would otherwise loop
	// through the cluster forever. Five tolerates a rolling restart.
	defaultMaxReclaims = 5
	// defaultTablePrefix starts every table name.
	defaultTablePrefix = "wf_"
	// claimBatch is the default of WithFetchBatch, and caps the cron ticks
	// one schedule reports or counts per pass, so one pass stays short
	// however much work is due.
	claimBatch = 100
)

// prefixPattern keeps table prefixes to characters every engine accepts
// unquoted, so a prefix can never change the meaning of a statement.
var prefixPattern = regexp.MustCompile(`^[a-z][a-z0-9_]{0,30}$`)

// Option configures a Client.
type Option func(*Client)

// WithClock sets the time source of every due time: delayed starts, sleeps,
// wait deadlines, retry backoff, step timeouts, rate-limit windows and cron
// fires. The default is clock.Real; tests pass clock.NewFake.
func WithClock(c clock.Clock) Option {
	return func(cl *Client) {
		if c != nil {
			cl.clk = c
		}
	}
}

// WithLeaseClock sets the clock leases are measured on. It is separate
// from WithClock because a lease is about a worker being alive, which is
// wall time: a test that moves a fake scheduling clock by an hour must not
// make every running step look abandoned. The default is clock.Real.
func WithLeaseClock(c clock.Clock) Option {
	return func(cl *Client) {
		if c != nil {
			cl.leaseClk = c
		}
	}
}

// WithLease sets how long a claimed step stays claimed without renewal;
// a dead worker's step is run again by another client after at most this
// long. Values of zero or less are ignored.
func WithLease(d time.Duration) Option {
	return func(cl *Client) {
		if d > 0 {
			cl.lease = d
		}
	}
}

// WithLeaseSkew sets the margin other clients wait past a lease's end
// before they take its step over, for clocks that disagree: a holder whose
// clock runs behind writes lease ends that look expired early to a node
// whose clock runs ahead. Set it to the largest clock difference expected
// between nodes (with WithDatabaseClock, a little above the database
// clock's Uncertainty). The holder stops its own attempt once a lease has
// gone unrenewed for the whole lease on its monotonic clock (ErrLeaseLost),
// so the margin is the gap between it stopping and another node starting.
// The same margin applies to the OnFailure, run-end delivery and prune
// leases. The default is zero; negative values are ignored.
func WithLeaseSkew(d time.Duration) Option {
	return func(cl *Client) {
		if d >= 0 {
			cl.leaseSkew = d
		}
	}
}

// WithMaxReclaims sets how often one attempt of a step may be taken over
// after its worker died before it fails with ErrAbandoned (then the retry
// policy applies, and without retries left the run fails). Zero fails the
// attempt at its first takeover. Negative values are ignored.
func WithMaxReclaims(n int) Option {
	return func(cl *Client) {
		if n >= 0 {
			cl.maxReclaims = n
		}
	}
}

// WithPollInterval sets how often the client looks for work that another
// node made due (a step another client queued, a signal for a run this
// client did not start). Values of zero or less are ignored.
func WithPollInterval(d time.Duration) Option {
	return func(cl *Client) {
		if d > 0 {
			cl.poll = d
		}
	}
}

// WithWorkers sets how many step attempts may execute at once. Values
// below 1 are ignored.
func WithWorkers(n int) Option {
	return func(cl *Client) {
		if n >= 1 {
			cl.workers = n
		}
	}
}

// WithFetchBatch sets how many rows one dispatcher query reads (default
// 100): pending runs to admit, delayed runs to start, due sleeps, waits
// and signals, chains to hand on, schedules to fire, failing runs and run
// ends. A larger batch drains a backlog in fewer round trips; a smaller
// one keeps each pass and its transactions short. Workers claim one step
// per query by design: a claim leases its step, so claiming steps no idle
// worker can start would hold them from every other node. Values below 1
// are ignored.
func WithFetchBatch(n int) Option {
	return func(cl *Client) {
		if n >= 1 {
			cl.fetch = n
		}
	}
}

// WithLogger sets where the driver reports what it cannot return to a
// caller (a failed poll, an OnFailure error, a cron fire that could not
// enqueue). The default discards.
func WithLogger(l *slog.Logger) Option {
	return func(cl *Client) {
		if l != nil {
			cl.log = l
		}
	}
}

// WithMiddleware wraps every step handler and OnFailure handler. The first
// middleware is the outermost.
func WithMiddleware(mw ...workflow.Middleware) Option {
	return func(cl *Client) { cl.mw = append(cl.mw, mw...) }
}

// WithTablePrefix sets the prefix of every table name (default "wf_"), so
// two independent queues can share one database. It must match
// ^[a-z][a-z0-9_]{0,30}$; New refuses anything else.
func WithTablePrefix(p string) Option { return func(cl *Client) { cl.prefix = p } }

// WithSchema puts every table in the named Postgres schema ("jobs" makes
// "jobs"."wf_runs"), for apps that keep queue tables apart for grants,
// backup or ownership, or one schema per tenant, without changing the
// pool's search_path. The schema must exist (Migrate does not create it)
// and match the table prefix's pattern. New refuses it on SQLite, which has
// no schemas, and on MySQL, whose schema is the database: point the pool
// at that database instead.
//
// The migration ledger stays in the connection's search_path (sqlb's
// ledger table is not schema-qualified), named <prefix>migrations as
// always; its rows for this store are owned as "<owner>@<schema>", so
// several schemas share one ledger table and keep separate histories.
// MigrationScripts with the same options gives the schema-qualified DDL.
func WithSchema(name string) Option { return func(cl *Client) { cl.schema = name } }

// WithCronParser replaces the cron parser behind Workflow.Cron and
// Schedule.Cron (cron.Parse by default), for a dialect the standard one
// lacks: the extended "L", "W" and "#" fields, a seconds field, hashed minutes. Stored
// expressions are parsed again when they fire, so every client of one
// database must use the same parser. It is opt-in and gives up "the same
// spec on every driver" for the expressions only this parser understands;
// see cron.Parser. Nil is ignored.
func WithCronParser(p cron.Parser) Option {
	return func(cl *Client) {
		if p != nil {
			cl.parse = p
		}
	}
}

// WithJitterSource sets where Backoff.Jitter draws its uniform values in
// [0, 1), so a test can pin them. The default is math/rand/v2. It must be
// safe for concurrent use. Nil is ignored.
func WithJitterSource(rnd func() float64) Option {
	return func(cl *Client) {
		if rnd != nil {
			cl.rnd = rnd
		}
	}
}

// WithErrorClassifier sets how database errors are sorted into lost unique
// races and retryable lock conflicts (see ErrorClassifier). The default,
// DefaultErrorClassifier, knows the drivers sqlb's contrib modules open; a
// driver, proxy or wrapper whose errors read differently needs its own, or
// a lost idempotency race comes back as an error and a deadlock is not
// retried. A nil classifier is ignored.
func WithErrorClassifier(f ErrorClassifier) Option {
	return func(cl *Client) {
		if f != nil {
			cl.classify = f
		}
	}
}

// WithIDGenerator sets how run, step, delayed-start and unnamed schedule
// IDs are made (see IDGenerator). The default is DefaultIDGenerator,
// "<kind>_<32 hex>". A nil generator is ignored.
func WithIDGenerator(g IDGenerator) Option {
	return func(cl *Client) {
		if g != nil {
			cl.ids = g
		}
	}
}

// WithTxAttempts sets how many times in all a transaction that lost a lock
// race is tried before its error reaches the caller (default 5). Raise it
// for a SQLite file many processes write; one turns retrying off. Values
// below 1 are ignored.
func WithTxAttempts(n int) Option {
	return func(cl *Client) {
		if n >= 1 {
			cl.txAttempts = n
		}
	}
}

// WithTickRetention sets how long a fired cron tick's row is kept (default
// 24 hours). A tick row is what stops a second node firing the same tick,
// which only matters while a node may still see that tick due, so keep it
// well above the longest pause a node can have between reading a schedule
// and firing it. Values of zero or less are ignored.
func WithTickRetention(d time.Duration) Option {
	return func(cl *Client) {
		if d > 0 {
			cl.tickRetention = d
		}
	}
}

// WithHousekeepingEvery sets how many dispatcher passes run between two
// clean-ups of consumed signals, consumed-signal markers, old tick rows,
// expired uniqueness locks and finished debounce bursts (default 50). Lower it when signals or unique
// locks pile up faster than that; raise it to spend fewer queries on a
// quiet database. Values below 1 are ignored. Retention of finished runs
// has its own cadence (WithPruneInterval).
func WithHousekeepingEvery(passes int) Option {
	return func(cl *Client) {
		if passes >= 1 {
			cl.housekeepEvery = passes
		}
	}
}

// WithQueues makes this client serve only the named queues, each with its
// own worker cap and priority (workflow.QueueConfig). Runs of other queues
// stay in the database for a client that serves them, which is how
// separate worker processes per queue are deployed. Workers caps how many attempts of one
// queue run at once on this client, within WithWorkers; a free worker
// takes the ready attempts of a higher Priority queue before any lower
// one, then Job.Priority, then arrival. Without it the client serves every
// queue from its one pool. New refuses an invalid list
// (workflow.ValidateQueues).
func WithQueues(qs ...workflow.QueueConfig) Option {
	return func(cl *Client) { cl.queues = append(cl.queues, qs...) }
}

// WithStopWhenIdle drains the client once it has had no unfinished run
// (scheduled, queued, running or waiting) of its workflows, on the queues
// it serves, for grace on the scheduling clock: a worker that stops when
// its queues are empty. Schedules do not keep it alive. Done
// closes when it has stopped. A grace of zero or less is ignored.
func WithStopWhenIdle(grace time.Duration) Option {
	return func(cl *Client) {
		if grace > 0 {
			cl.idleGrace = grace
		}
	}
}

// Client is the SQL workflow.Client. Build it with New, create the schema
// with Migrate, Register workflows, then Start. All state is in the
// database or hangs off the value: two clients over one database are two
// workers of one queue, and two clients over two prefixes share nothing.
type Client struct {
	st       *store
	clk      clock.Clock
	leaseClk clock.Clock
	lease    time.Duration
	poll     time.Duration
	// leaseSkew is WithLeaseSkew: how long past a lease's end other
	// clients wait before taking it over.
	leaseSkew time.Duration
	// dbClock is WithDatabaseClock's clock (nil: off), read again every
	// dbClockEvery while the client runs.
	dbClock      *DatabaseClock
	dbClockEvery time.Duration
	// maxReclaims caps lease takeovers per attempt (WithMaxReclaims).
	maxReclaims int
	// retention is nil unless WithRetention: nothing is ever deleted then.
	retention   *Retention
	pruneEvery  time.Duration
	noAutoPrune bool
	archive     ArchiveFunc
	// elector gates cron firing and housekeeping (WithElector,
	// WithLeaderElection; nil: every client does both). leaderRun is the
	// elector WithLeaderElection built, campaigned from Start.
	elector    Elector
	leaderRun  *LeaderElector
	leaderOn   bool
	leaderNode string
	leaderOpts []LeaderOption
	// nodeID is WithNodeID; startedAt is when Start ran and nodeSeen when
	// the nodes row was last written (lease clock, Unix nanoseconds;
	// nodeSeen is the dispatcher goroutine's).
	nodeID    string
	startedAt int64
	nodeSeen  int64
	// schedulerOnly and noScheduler are WithSchedulerOnly and
	// WithoutScheduler; payloadLimit is WithPayloadLimit (0: none).
	schedulerOnly bool
	noScheduler   bool
	payloadLimit  int
	// lc holds the lifecycle observers (OnLifecycle).
	lc lifecycle
	// archiveTable is WithArchiveTable: Prune moves runs to the cold tables.
	archiveTable bool
	workers      int
	// fetch is WithFetchBatch: rows per dispatcher query.
	fetch      int
	log        *slog.Logger
	mw         []workflow.Middleware
	prefix     string
	schema     string
	parse      cron.Parser
	rnd        func() float64 // jitter draws; nil means math/rand/v2
	classify   ErrorClassifier
	ids        IDGenerator
	txAttempts int
	// tickRetention, housekeepEvery and pruneChunk are the tunings of
	// WithTickRetention, WithHousekeepingEvery and WithPruneChunk.
	tickRetention  time.Duration
	housekeepEvery int
	pruneChunk     int

	mu        sync.Mutex
	wfs       map[string]*registered
	observers []workflow.RunFinishedFunc
	started   bool
	stopped   bool
	ctx       context.Context // root of every worker; cancelled on shutdown
	cancel    context.CancelFunc
	wg        sync.WaitGroup // every goroutine the client starts
	wake      chan struct{}  // closed and replaced by nudge; the dispatcher waits on it
	// work wakes one idle worker per token (signalWorker): new work wakes
	// one, and each worker whose claim lands passes the turn on, so a
	// burst ramps up worker by worker while a single job wakes a single
	// worker instead of every idle one racing to claim it.
	work     chan struct{}
	local    map[string]map[*attempt]bool // run id -> attempts executing here
	rateWake time.Time                    // earliest moment a rate-limited run may start
	names    []any                        // registered workflow names, the IN list of every query
	// queues is WithQueues: empty means every queue from one pool. tiers
	// is the same list grouped by Priority, highest first, as the claim
	// walks it; served is the names as IN-list arguments.
	queues []workflow.QueueConfig
	tiers  [][]workflow.QueueConfig
	served []any
	// limits are the queues with a QueueConfig.Limit, fixed at New and read
	// without c.mu: the cluster-wide cap on a queue's in-flight attempts.
	limits []queueLimit
	// claimMu serialises claims when a queue has a Workers cap, so the
	// count a claim checks cannot change before the claim lands.
	claimMu sync.Mutex
	// running counts this client's executing attempts per queue (c.mu).
	running map[string]int
	// draining stops workers claiming (Drain); done closes once every
	// goroutine has exited after shutdown.
	draining atomic.Bool
	done     chan struct{}
	// inflight counts attempts from claim to written outcome (c.mu);
	// drained, when set, is closed as it reaches zero (Drain waits on it).
	inflight int
	// drainTrack records the attempts a drain finished and cut off.
	drainTrack workflow.DrainTracker
	drained    chan struct{}
	// idleGrace is WithStopWhenIdle (0: off); idleSince is when the client
	// was first seen idle, Unix nanoseconds on the scheduling clock (0: not
	// idle; dispatcher goroutine only).
	idleGrace time.Duration
	idleSince int64
	// recycle is WithRecycle (zero: never). recycleWatch is its decision,
	// set at Start when a limit is set; stopCause is the limit that stopped
	// the client (c.mu), nil for every stop that was asked for.
	recycle      workflow.Recycle
	recycleWatch *workflow.RecycleWatch
	stopCause    *workflow.RecycleError
	// tickObservers are OnTick's observers, fixed at Start.
	tickObservers []workflow.TickObserver
	// enqueueOnly is WithEnqueueOnly; doneOnce closes done on its Stop.
	enqueueOnly bool
	doneOnce    sync.Once
	// errHistory is WithErrorHistory: how many attempt errors a run keeps
	// (0 or less: none).
	errHistory int
	// notifier is WithNotifier (nil: poll only); fleet holds at most one
	// pending wake-up for its sender goroutine (signalFleet).
	notifier Notifier
	fleet    chan struct{}
	// crashed makes a stop behave like a killed process: no step is handed
	// back and no result is written. Only tests set it (abandon), to prove
	// that a dead worker's lease expires and another client takes over.
	crashed atomic.Bool
}

var (
	_ workflow.Client      = (*Client)(nil)
	_ workflow.RunNotifier = (*Client)(nil)

	_ workflow.RunQuerier        = (*Client)(nil)
	_ workflow.RunRetrier        = (*Client)(nil)
	_ workflow.RunDeleter        = (*Client)(nil)
	_ workflow.BulkCanceller     = (*Client)(nil)
	_ workflow.Pauser            = (*Client)(nil)
	_ workflow.SchedulePauser    = (*Client)(nil)
	_ workflow.ScheduleLister    = (*Client)(nil)
	_ workflow.TickNotifier      = (*Client)(nil)
	_ workflow.Drainer           = (*Client)(nil)
	_ workflow.ReportingDrainer  = (*Client)(nil)
	_ workflow.StopCauser        = (*Client)(nil)
	_ workflow.ScheduleTriggerer = (*Client)(nil)
	_ workflow.RunForcer         = (*Client)(nil)

	_ workflow.RunRescheduler    = (*Client)(nil)
	_ workflow.StepCanceller     = (*Client)(nil)
	_ workflow.ResultReader      = (*Client)(nil)
	_ workflow.LifecycleNotifier = (*Client)(nil)
	_ workflow.NodeLister        = (*Client)(nil)
	_ workflow.HealthChecker     = (*Client)(nil)
	_ workflow.PayloadLimiter    = (*Client)(nil)
)

// New returns a client over db, whose SQL is written for d (sqlb's
// Postgres, SQLite or MySQL dialect). It opens nothing and starts nothing:
// db is the app's pool, used from Migrate and Start on.
func New(db *sql.DB, d sqlb.Dialect, opts ...Option) (*Client, error) {
	if db == nil || d == nil {
		return nil, fmt.Errorf("%w: sqlstore.New needs a *sql.DB and a dialect", workflow.ErrInvalid)
	}
	return build(db, d, opts)
}

// build is New without the nil-pool check: MigrationScripts renders SQL
// text from the options and never touches a pool, so it passes none.
func build(db *sql.DB, d sqlb.Dialect, opts []Option) (*Client, error) {
	if d == nil {
		return nil, fmt.Errorf("%w: sqlstore needs a dialect", workflow.ErrInvalid)
	}
	switch sqlb.DialectName(d) {
	case enginePostgres, engineSQLite, engineMySQL:
	default:
		return nil, workflow.Unsupported("dialect "+sqlb.DialectName(d), "sqlstore runs on Postgres, SQLite and MySQL")
	}
	c := &Client{
		clk:         clock.Real{},
		leaseClk:    clock.Real{},
		lease:       defaultLease,
		maxReclaims: defaultMaxReclaims,
		pruneEvery:  defaultPruneInterval,
		poll:        defaultPoll,
		workers:     defaultWorkers,
		fetch:       claimBatch,
		log:         slog.New(slog.DiscardHandler),
		prefix:      defaultTablePrefix,
		parse:       cron.Parse,
		classify:    DefaultErrorClassifier,
		ids:         DefaultIDGenerator,
		txAttempts:  defaultTxAttempts,
		errHistory:  workflow.DefaultErrorHistory,

		tickRetention:  defaultTickRetention,
		housekeepEvery: defaultHousekeepEvery,
		pruneChunk:     defaultPruneChunk,

		wfs:     map[string]*registered{},
		wake:    make(chan struct{}),
		work:    make(chan struct{}, 1),
		local:   map[string]map[*attempt]bool{},
		running: map[string]int{},
		done:    make(chan struct{}),
		fleet:   make(chan struct{}, 1),
	}
	for _, o := range opts {
		o(c)
	}
	if c.nodeID == "" {
		c.nodeID = workflow.DefaultNodeID()
	}
	if c.dbClockEvery > 0 {
		c.dbClock = &DatabaseClock{db: db, engine: sqlb.DialectName(d), local: clock.Real{}}
		c.clk, c.leaseClk = c.dbClock, c.dbClock
	}
	if err := workflow.ValidateQueues(c.queues); err != nil {
		return nil, err
	}
	if err := workflow.CheckQueueLimits(caps, c.queues); err != nil {
		return nil, err
	}
	c.tiers, c.served = queueTiers(c.queues)
	c.limits = queueLimits(c.queues)
	if !prefixPattern.MatchString(c.prefix) {
		return nil, fmt.Errorf("%w: table prefix %q must match %s", workflow.ErrInvalid, c.prefix, prefixPattern)
	}
	if c.schema != "" {
		if sqlb.DialectName(d) != enginePostgres {
			return nil, workflow.Unsupported("WithSchema on "+sqlb.DialectName(d), "only Postgres has schemas inside a database; on MySQL point the pool at the database")
		}
		if !prefixPattern.MatchString(c.schema) {
			return nil, fmt.Errorf("%w: schema %q must match %s", workflow.ErrInvalid, c.schema, prefixPattern)
		}
	}
	c.st = newStore(db, d, c)
	c.buildLeader()
	return c, nil
}

// DB returns the pool the client was built with: the one escape hatch, for
// reading the tables directly (a dashboard, a report) or running Migrate
// inside a wider schema step.
func (c *Client) DB() *sql.DB { return c.st.db }

// registered is a workflow with its handlers wrapped and its graph indexed
// once, at Register.
type registered struct {
	wf        workflow.Workflow
	crons     []string
	handlers  map[string]workflow.Handler // Run steps, middleware applied
	onFailure workflow.Handler            // middleware applied; nil if none
	onTimeout workflow.Handler            // Workflow.OnTimeout, middleware applied; nil if none
	children  map[string][]string         // step name -> steps listing it in After
	steps     map[string]workflow.Step
	// stub marks the stand-in of an enqueue-only client for a workflow it
	// did not register (enqueueReg): a name and nothing else.
	stub bool
}

// Capabilities returns every capability the conformance suite proves on
// this driver.
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
		if _, err := c.parseFor(expr, declaredSpec(wf)); err != nil {
			return fmt.Errorf("%w: workflow %q: Cron[%d]: %w", workflow.ErrInvalid, wf.Name, i, err)
		}
		reg.crons = append(reg.crons, expr)
	}
	if len(wf.Cron) > 0 {
		if _, err := encodeLocation(cronLocation(wf)); err != nil {
			return err
		}
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

// parseCron parses expr with the client's parser (WithCronParser), refusing
// a parser that returns neither a schedule nor an error.
func (c *Client) parseCron(expr string) (cron.Schedule, error) {
	s, err := c.parse(expr)
	if err == nil && s == nil {
		err = fmt.Errorf("cron parser returned no schedule for %q", expr)
	}
	return s, err
}

// cronLocation is the zone wf's declared crons run in: CronLocation, or
// UTC when it is nil.
func cronLocation(wf workflow.Workflow) *time.Location {
	if wf.CronLocation == nil {
		return time.UTC
	}
	return wf.CronLocation
}

func (c *Client) wrap(h workflow.Handler) workflow.Handler {
	for i := len(c.mw) - 1; i >= 0; i-- {
		h = c.mw[i](h)
	}
	return h
}

// OnRunFinished registers fn to observe run ends (workflow.RunNotifier).
// Delivery is durable: a run's end is marked delivered only after every
// observer returned, so a crash before that delivers it again (at least
// once). In a cluster each run's end is delivered on one client that
// registers its workflow and has observers, not on every client.
func (c *Client) OnRunFinished(fn workflow.RunFinishedFunc) error {
	if fn == nil {
		return fmt.Errorf("%w: OnRunFinished needs a function", workflow.ErrInvalid)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.started {
		return workflow.ErrAlreadyStarted
	}
	c.observers = append(c.observers, fn)
	return nil
}

// exprKeys refuses every expression key in wf: SQL has no engine
// expression language to evaluate, so keys are Go functions only.
func exprKeys(wf workflow.Workflow) error {
	reason := "the sql driver evaluates only KeyFunc keys; workflow " + wf.Name
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

// Start checks every workflow against Capabilities, refuses expression
// keys, checks that Migrate has run, records the declared cron schedules
// and then starts the dispatcher and the workers. It does not block.
// Cancelling ctx stops the client; runs stay in the database for the next
// client, and steps this client was executing are released so another
// client runs them at once.
func (c *Client) Start(ctx context.Context) error {
	if c.enqueueOnly {
		return c.syncClock(ctx) // nothing runs here (WithEnqueueOnly)
	}
	c.mu.Lock()
	if c.started {
		c.mu.Unlock()
		return workflow.ErrAlreadyStarted
	}
	if err := workflow.ValidateRecycle(c.recycle); err != nil {
		c.mu.Unlock()
		return err
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
	c.started = true
	for name := range c.wfs {
		c.names = append(c.names, name)
	}
	c.mu.Unlock()

	if err := c.syncClock(ctx); err != nil {
		return err
	}
	if err := c.checkSchema(ctx); err != nil {
		return err
	}
	if err := c.checkVersion(ctx); err != nil {
		return err
	}
	if err := c.prepare(ctx); err != nil {
		return err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	c.ctx, c.cancel = context.WithCancel(ctx)
	context.AfterFunc(c.ctx, c.shutdown)
	c.startedAt = c.leaseNanos()
	workers := c.workers
	if c.schedulerOnly {
		workers = 0
	}
	c.wg.Add(1 + workers)
	go c.dispatcher()
	if c.retention != nil && !c.noAutoPrune {
		c.wg.Add(1)
		go c.pruneLoop()
	}
	for range workers {
		go c.worker()
	}
	if c.dbClock != nil {
		c.wg.Add(1)
		go c.clockLoop()
	}
	if c.leaderRun != nil {
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			c.leaderRun.Run(c.ctx)
		}()
	}
	c.startRecycleLocked()
	c.startNotifierLocked()
	go func() {
		<-c.ctx.Done()
		c.wg.Wait()
		// Marked stopped before Done closes, on every path (Drain, the Start
		// ctx, Stop, stop-when-idle, a recycle): the AfterFunc that also
		// runs shutdown may not have run yet, and a caller woken by Done
		// must already get ErrNotStarted from Enqueue.
		c.shutdown()
		c.forgetNode()
		close(c.done)
	}()
	return nil
}

// prepare writes what Start needs before any worker runs: a lock row per
// workflow with an admission policy and the declared cron schedules.
func (c *Client) prepare(ctx context.Context) error {
	for _, reg := range c.wfs {
		if reg.wf.Concurrency != nil || len(reg.wf.RateLimits) > 0 {
			if err := c.ensureLock(ctx, lockName(reg.wf.Name)); err != nil {
				return err
			}
		}
		if err := c.syncDeclared(ctx, reg); err != nil {
			return err
		}
	}
	for _, l := range c.limits {
		if err := c.ensureLock(ctx, limitLockName(l.name)); err != nil {
			return err
		}
	}
	if c.retention != nil {
		return c.ensureLock(ctx, pruneLockName)
	}
	return nil
}

// ensureLock inserts the lock row name when it is missing. Two clients
// starting at once both try; the loser's duplicate is the expected
// outcome.
func (c *Client) ensureLock(ctx context.Context, name string) error {
	err := c.st.retry(ctx, func() error {
		_, err := c.st.exec(ctx, c.st.db, "INSERT INTO {locks} (name, n) VALUES (?, 0)", name)
		if c.st.isUnique(err) {
			return nil
		}
		return err
	})
	if err != nil {
		return err
	}
	return nil
}

// lockName is the lock row that serialises admission for one workflow.
func lockName(workflowName string) string { return "admit:" + workflowName }

// Stop shuts the client down and waits until every goroutine it started
// has exited. Runs are left as they are in the database. It is safe to
// call more than once, and before Start (a no-op).
func (c *Client) Stop() {
	if c.enqueueOnly {
		c.shutdown()
		c.doneOnce.Do(func() { close(c.done) })
		return
	}
	c.mu.Lock()
	cancel := c.cancel
	c.mu.Unlock()
	if cancel == nil {
		return
	}
	cancel()
	c.shutdown()
	c.wg.Wait()
	// The goroutine that closes done removes this node's registry row
	// first; waiting for it means nothing writes after Stop returns.
	<-c.done
}

// abandon stops c the way a killed process stops: every loop exits and
// nothing more is written, so its claimed steps stay leased until the
// lease expires. Tests reach it through export_test.go.
func (c *Client) abandon() {
	c.crashed.Store(true)
	c.mu.Lock()
	cancel := c.cancel
	c.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// shutdown runs once the root context ends: it wakes idle loops so they
// exit. Running attempts see their context cancelled and release their
// steps (see attempt.release).
func (c *Client) shutdown() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stopped {
		return
	}
	c.stopped = true
	close(c.wake)
	c.wake = make(chan struct{})
}

// usable reports whether calls that need workers may proceed.
func (c *Client) usable() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.enqueueOnly {
		if c.stopped {
			return fmt.Errorf("%w: the client was stopped", workflow.ErrNotStarted)
		}
		return nil
	}
	switch {
	case !c.started:
		return workflow.ErrNotStarted
	case c.stopped:
		return fmt.Errorf("%w: the client was stopped", workflow.ErrNotStarted)
	case c.ctx == nil:
		return workflow.ErrNotStarted
	}
	return nil
}

// reg returns the registered workflow name, or nil.
func (c *Client) reg(name string) *registered {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.wfs[name]
}

// nudge wakes the dispatcher and every idle worker, because this client
// just made work due (an enqueue, a finished step, a signal), and with a
// Notifier the other clients of the tables too.
func (c *Client) nudge() {
	c.wakeLocal()
	c.signalFleet()
}

// wakeLocal wakes this client's dispatcher and idle workers only: for work
// another client announced, or a due time this client's own timer saw.
func (c *Client) wakeLocal() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stopped {
		return
	}
	close(c.wake)
	c.wake = make(chan struct{})
	c.signalWorker()
}

// signalWorker wakes one idle worker, if any is waiting and none is
// already being woken. It never blocks.
func (c *Client) signalWorker() {
	select {
	case c.work <- struct{}{}:
	default:
	}
}

// wakeCh returns the channel the next nudge closes.
func (c *Client) wakeCh() <-chan struct{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.wake
}

// nowNanos is the scheduling clock's now as stored in the tables.
func (c *Client) nowNanos() int64 { return c.clk.Now().UnixNano() }

// leaseNanos is the lease clock's now as stored in the tables.
func (c *Client) leaseNanos() int64 { return c.leaseClk.Now().UnixNano() }

// expiredBefore is the lease end before which another client's lease
// counts as expired here: now on the lease clock less WithLeaseSkew.
func (c *Client) expiredBefore() int64 { return c.leaseNanos() - int64(c.leaseSkew) }
