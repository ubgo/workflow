package hatchet

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/hatchet-dev/hatchet/pkg/client/rest"
	hatchetsdk "github.com/hatchet-dev/hatchet/sdks/go"
	"github.com/hatchet-dev/hatchet/sdks/go/features"
	openapi_types "github.com/oapi-codegen/runtime/types"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
)

// caps is what this driver proves against a real Hatchet engine; see
// live_test.go for the test behind each one.
//
//ds:def id=hatchet-caps-d6x9fv8z owner=@khanakia stability=stable
var caps = workflow.CapsOf(
	workflow.CapDurable,
	workflow.CapDelayedStart,
	workflow.CapWaitForEvent,
	workflow.CapWaitOwnRun, // TestOwnRunWaitLive
	workflow.CapDurableSleep,
	workflow.CapConcurrencyKey,
	workflow.CapRateLimit,
	workflow.CapIdempotencyKey,
	workflow.CapCron,
	workflow.CapCancel,
	workflow.CapQueue,
	workflow.CapRunQuery,
	workflow.CapCancelAll,
	workflow.CapRetryRun,
	workflow.CapScheduleList,
	workflow.CapDrain,
	workflow.CapDrainReport,
	workflow.CapScheduleTrigger,
	workflow.CapScheduleHistory,
	workflow.CapEnqueueOnly,
	workflow.CapTenant,
	workflow.CapTickTime,
	workflow.CapDueQuery,
	workflow.CapResult,
	workflow.CapCronSeconds,
	workflow.CapKeyFIFO,
)

// Defaults for Config fields left zero.
const (
	// DefaultWorkerName is the worker name shown in Hatchet's dashboard.
	//ds:def id=hatchet-defaultworkername-25wkdr2z owner=@khanakia stability=stable
	DefaultWorkerName = "ubgo-workflow"
	// DefaultStepTimeout stands in for "no timeout": Hatchet always
	// applies an execution timeout, and its server default is short
	// enough to kill a long job minutes in.
	//ds:def id=hatchet-defaultsteptimeout-j3gvwcvw owner=@khanakia stability=stable
	DefaultStepTimeout = 24 * time.Hour
	// DefaultScheduleTimeout is how long a step may wait for a worker
	// when Config.ScheduleTimeout is zero: a day, like DefaultStepTimeout,
	// in place of the engine's five minutes (see Config.ScheduleTimeout).
	//ds:def id=hatchet-defaultscheduletimeout-pw5yeefh owner=@khanakia stability=stable
	DefaultScheduleTimeout = 24 * time.Hour
	// DefaultIdempotencyMaxHold caps how long a key stays held when its
	// run never finishes; normally the key is released when the run is
	// terminal (Hatchet's STATUS method).
	//ds:def id=hatchet-defaultidempotencymaxhold-9vxvg6ux owner=@khanakia stability=stable
	DefaultIdempotencyMaxHold = 24 * time.Hour
)

// Engine-facing tunings: defaults for the Config fields that override them.
const (
	// DefaultRegisterTimeout is how long Start waits for the engine to
	// list the new worker. An engine that starts slowly (a cold pod on
	// Kubernetes) may need longer.
	//ds:def id=hatchet-defaultregistertimeout-3vqg4k5w owner=@khanakia stability=stable
	DefaultRegisterTimeout = 30 * time.Second
	// DefaultVisibilityLag is how long a run may be missing from the
	// engine's read side right after it was created (see
	// runDetailsLagged). It also sizes the run-finished notifier's list
	// overlap and how long a batch waits after its coordinator ended.
	//ds:def id=hatchet-defaultvisibilitylag-n7hzhafh owner=@khanakia stability=stable
	DefaultVisibilityLag = 5 * time.Second
)

const (
	// registerPoll is how often the engine is asked again while waiting
	// for a worker to register or a run to become visible.
	registerPoll = 100 * time.Millisecond
	// scheduleLookback widens the run search around a scheduled run's
	// RunAt, for clock skew between this process and the engine.
	scheduleLookback = time.Minute
)

// Config configures a Client. The zero value builds an SDK client from the
// HATCHET_CLIENT_* environment variables at Start.
//
//ds:def id=hatchet-config-wdky3m9f owner=@khanakia stability=api
type Config struct {
	// Client is an SDK client the app already built. When nil, Start
	// builds one with ClientOptions.
	Client *hatchetsdk.Client

	// ClientOptions are passed to hatchet.NewClient when Client is nil
	// (token, host and port, namespace, TLS).
	ClientOptions []hatchetsdk.ClientOpt

	// Namespace must equal the namespace the SDK client uses (option or
	// HATCHET_CLIENT_NAMESPACE), so idempotency keys are scoped by the
	// name the engine knows each workflow by.
	Namespace string

	// WorkerName is the worker's name in Hatchet's dashboard.
	WorkerName string

	// Slots caps how many steps this worker runs at once. Zero keeps the
	// SDK default. With Queues it is the slots of a queue whose Workers is
	// zero.
	Slots int

	// Queues are the named queues this client's workers serve
	// (workflow.QueueConfig). Empty means one worker serving every
	// registered workflow, whatever its queue. Otherwise Start starts one
	// Hatchet worker per listed queue that has registered workflows, each
	// with Workers slots, and a workflow whose queue is not listed gets no
	// worker here: its runs wait for a process that serves that queue. A
	// queue is bound to a workflow on Hatchet (Workflow.Queue), because a
	// run goes to whichever worker registered its workflow, so a Job.Queue
	// other than the workflow's is refused (no CapQueueOverride), and a
	// non-zero Priority is refused at Start (Hatchet cannot order one
	// worker's queues against another's).
	Queues []workflow.QueueConfig

	// API is the engine's REST API (NewAPI), for workflow pause and cron
	// enable, which SDK v0.107.1 cannot reach. Set, the client also
	// declares CapPauseWorkflow and CapSchedulePause and implements
	// workflow.Pauser and workflow.SchedulePauser; nil refuses both.
	API *API

	// StepTimeout is the execution timeout of a step whose Workflow and
	// Step leave Timeout zero.
	StepTimeout time.Duration

	// ScheduleTimeout is how long a step may wait to be scheduled,
	// counted from its creation. The engine fails a waiting step past it,
	// with no error message and no attempt made; a step a rate limit
	// holds is checked each time its bucket's window lets it back into
	// the queue. The engine's own default is five minutes, which failed
	// runs a rate limit held for about six (TestScheduleTimeoutLive, the
	// ratelimit conformance group), against the core contract that a held
	// run is delayed, never dropped; so every step declares this instead.
	// Zero means DefaultScheduleTimeout. A run held longer than it still
	// fails, so set it above the longest wait you expect.
	ScheduleTimeout time.Duration

	// IdempotencyMaxHold caps how long an idempotency key stays held.
	// A delayed start with a key is refused when its RunAt is further
	// ahead than this, since the key would lapse before the run starts.
	IdempotencyMaxHold time.Duration

	// Middleware wraps every step handler, outermost first.
	Middleware []workflow.Middleware

	// RunFinishedPoll is how often OnRunFinished observers' notifier polls
	// the engine. Zero means DefaultRunFinishedPoll.
	RunFinishedPoll time.Duration

	// RunFinishedWindow is the oldest an unfinished run may be and still
	// be watched for its end. Zero means DefaultRunFinishedWindow.
	RunFinishedWindow time.Duration

	// RunFinishedCatchUp makes Start replay the ends of runs created this
	// long before it, so a restarted process delivers ends it missed while
	// down (again, if an earlier process delivered them). Zero replays
	// nothing older than Start.
	RunFinishedCatchUp time.Duration

	// BatchCoordinator makes batch.Manager.Dispatch start an eligible
	// batch (no RunAt, no IdempotencyKey, at most about 2 MiB of input) as
	// one coordinator run whose task starts every job as a child run,
	// instead of bulk triggers from the dispatching process (see
	// SpawnGroup). Start then registers the coordinator workflow on this
	// worker. The batch's Manager needs this client's BatchSource, which
	// finds the children by label; every process sharing the batch store
	// should set it alike, so a resumed dispatch finds the coordinator.
	BatchCoordinator bool

	// BatchCoordinatorTimeout bounds one attempt of the coordinator's
	// task. It is also how long a coordinator whose worker died waits
	// before the engine retries it elsewhere, so keep it near the time
	// starting the largest batch takes. Zero means
	// DefaultBatchCoordinatorTimeout.
	BatchCoordinatorTimeout time.Duration

	// BatchCoordinatorRetry is the coordinator task's retry policy. Nil
	// means DefaultBatchCoordinatorRetry (10 retries, 2^n seconds capped
	// at two minutes: about ten minutes of engine outage). Its Backoff
	// must fit Hatchet's factor^n-seconds formula (see Retry.Backoff in
	// doc.go) and NoRetryOn must be nil, or Start refuses it.
	BatchCoordinatorRetry *workflow.RetryPolicy

	// BatchCoordinatorMaxInput caps the JSON input and metadata one
	// coordinator trigger carries; a larger batch is started as bulk
	// triggers instead. Zero means DefaultBatchCoordinatorMaxInput, which
	// fits the engine's default 4 MiB gRPC message; raise it only when
	// the engine's message limit was raised.
	BatchCoordinatorMaxInput int

	// GroupChunkRuns caps the runs of one bulk trigger (RunGroup and the
	// coordinator). Zero means DefaultGroupChunkRuns; Start refuses more
	// than that, because the SDK splits a larger call itself and then
	// cannot say which runs of it failed.
	GroupChunkRuns int

	// GroupChunkBytes caps the JSON input and metadata of one bulk
	// trigger. Zero means DefaultGroupChunkBytes; lower it for an engine
	// whose gRPC message limit is below the default 4 MiB. Start refuses
	// more than the default, for the same reason as GroupChunkRuns (the
	// SDK splits at 3 MiB of protobuf whatever the engine accepts).
	GroupChunkBytes int

	// RegisterTimeout is how long Start waits for the engine to list the
	// worker. Zero means DefaultRegisterTimeout.
	RegisterTimeout time.Duration

	// VisibilityLag is how long the engine's read side may trail a write
	// (see DefaultVisibilityLag). Zero means DefaultVisibilityLag.
	VisibilityLag time.Duration

	// FailedSettle is how long after a task failed a FAILED read is still
	// taken as the moment before its retry when the failure carries no
	// driver marker and the task has retries left (see failed.go: the
	// engine shows FAILED for a moment between a failed attempt and its
	// retry). The run reads Queued or Running meanwhile. Zero means
	// DefaultFailedSettle.
	FailedSettle time.Duration

	// Recycle stops this client's workers once a limit is reached (a
	// number of finished handler attempts, a time since Start, a heap
	// size), so a supervisor can start a fresh process: the workers are
	// drained as Drain does with no deadline (paused, the handlers running
	// here finish, then stopped), Done closes and StopCause returns the
	// *workflow.RecycleError; workflow.Serve wraps that into one call. On-
	// failure handlers count as attempts; durable wait and sleep steps do
	// not, since they hold no worker. Runs left on the engine go to any
	// other worker serving their workflow. The zero value never recycles.
	Recycle workflow.Recycle

	// Logger receives the errors of the driver's background work (the
	// run-finished notifier's polls, observer panics, worker stop) and of
	// BatchSource when it is given no logger. Nil discards them.
	Logger *slog.Logger

	// Clock is the time source of everything the driver decides locally:
	// the run-finished notifier's poll ticker and its delivered-run
	// window, BatchSource's ticker, and FinishedAt of a delayed start
	// cancelled before it fired. Nil means clock.Real. Times compared
	// with the engine's own timestamps (run creation and finish times,
	// event look-back, the read-side lag deadlines, the worker
	// registration wait) stay on the wall clock the engine shares,
	// because a fake clock there would make the driver ask the engine
	// about a time the engine has never seen.
	Clock clock.Clock

	// EnqueueOnly makes a client that starts runs but never runs them
	// (workflow.CapEnqueueOnly), for web nodes beside separate worker
	// processes: Enqueue, Status, Cancel, Signal, Schedule and the run
	// and schedule listings work without Start, connecting on first use;
	// Start starts no worker and no run-finished poller and returns nil,
	// and when its ctx ends calls return ErrNotStarted as after any stop.
	// Register is optional: a registered workflow's job gets that
	// workflow's checks (idempotency policy, queue), an unregistered one
	// only workflow.CheckJob, and the engine refuses a workflow that no
	// worker has ever registered. OnRunFinished is refused, since the
	// poller does not run. The engine version check of Start is skipped:
	// the worker processes make it.
	EnqueueOnly bool

	// ScheduleHistoryWindow is how far back Schedules looks for each
	// trigger's last fire (ScheduleInfo.Last). Zero means
	// DefaultScheduleHistoryWindow.
	ScheduleHistoryWindow time.Duration

	// NodeID names this process in StepInfo.Node and Nodes. It travels
	// to the engine as a label on every worker this client registers, so
	// the engine's worker list maps back to it. RunInfo.AttemptedBy stays
	// empty and workflow.CapNodeIdentity is not claimed: the engine keeps
	// no per-attempt worker history the driver can read (see doc.go).
	// Empty means workflow.DefaultNodeID() (hostname:pid).
	NodeID string

	// PayloadLimit is the largest Job.Payload, in bytes, Enqueue and
	// RunGroup accept; a larger one is refused with a
	// *workflow.PayloadTooLargeError before anything reaches the engine
	// (workflow.PayloadLimiter). Zero means DefaultPayloadLimit, which
	// fits the engine's default gRPC message size; raise it only for an
	// engine and SDK configured with a larger one.
	PayloadLimit int
}

// Client is the Hatchet driver: a workflow.Client whose runs, waits,
// sleeps, retries, crons and concurrency are Hatchet's own.
//
// Nothing connects until Start: Register only records definitions, and
// Start builds the SDK client (unless Config.Client was given), declares
// every workflow, registers the worker and returns once it is running. The
// worker stops when Start's ctx is cancelled.
type Client struct {
	cfg Config

	mu        sync.Mutex
	started   bool
	specs     map[string]workflow.Workflow
	order     []string
	sdk       *hatchetsdk.Client
	workflows map[string]*hatchetsdk.Workflow
	// delayed holds each workflow's delayed variant (delayedName), the
	// copy with a start gate that runs a delayed start with an
	// idempotency key; only workflows with hasStartGate have one.
	delayed   map[string]*hatchetsdk.Workflow
	observers []workflow.RunFinishedFunc
	notify    *notifier // set by Start when observers exist

	// afterChunk, when set, runs after each bulk-trigger chunk RunGroup
	// started. Only tests set it (export_test.go), to kill a process
	// between chunks; it is nil in every client New returns.
	afterChunk func()

	coordinator *hatchetsdk.Workflow // set by Start when Config.BatchCoordinator is on

	workerIDs []string           // engine IDs of the workers Start registered
	stop      context.CancelFunc // set by Start: stops every worker
	done      chan struct{}      // closed once every worker has stopped
	draining  bool               // set by Drain: no further step attempts are wanted
	// drainDone is closed once the first Drain finished and drainReport /
	// drainErr (guarded by mu) hold its result, which later Drain calls return.
	drainDone   chan struct{}
	drainReport workflow.DrainReport
	drainErr    error
	drained     bool     // set when the workers are told to stop (Drain, recycle, Start ctx end): calls return ErrNotStarted
	inflight    inflight // step and on-failure handlers running on this client

	runCtx    context.Context        // set by Start: ends when the workers are told to stop
	watch     *workflow.RecycleWatch // set by Start when Config.Recycle is enabled; read by handlers
	stopCause *workflow.RecycleError // set once by recycle; nil for every requested stop

	deleted deletedStarts // delayed starts Cancel deleted before the engine could fire them
}

var _ workflow.Client = (*Client)(nil)

// New returns a driver for cfg. It opens no connection and starts no
// goroutine.
//
//ds:def id=hatchet-new-4dhudaw3 owner=@khanakia stability=stable
func New(cfg Config) *Client {
	if cfg.WorkerName == "" {
		cfg.WorkerName = DefaultWorkerName
	}
	if cfg.StepTimeout <= 0 {
		cfg.StepTimeout = DefaultStepTimeout
	}
	if cfg.ScheduleTimeout <= 0 {
		cfg.ScheduleTimeout = DefaultScheduleTimeout
	}
	if cfg.IdempotencyMaxHold <= 0 {
		cfg.IdempotencyMaxHold = DefaultIdempotencyMaxHold
	}
	if cfg.BatchCoordinatorTimeout <= 0 {
		cfg.BatchCoordinatorTimeout = DefaultBatchCoordinatorTimeout
	}
	if cfg.RunFinishedPoll <= 0 {
		cfg.RunFinishedPoll = DefaultRunFinishedPoll
	}
	if cfg.RunFinishedWindow <= 0 {
		cfg.RunFinishedWindow = DefaultRunFinishedWindow
	}
	if cfg.BatchCoordinatorRetry == nil {
		cfg.BatchCoordinatorRetry = DefaultBatchCoordinatorRetry()
	}
	if cfg.BatchCoordinatorMaxInput <= 0 {
		cfg.BatchCoordinatorMaxInput = DefaultBatchCoordinatorMaxInput
	}
	if cfg.GroupChunkRuns <= 0 {
		cfg.GroupChunkRuns = DefaultGroupChunkRuns
	}
	if cfg.GroupChunkBytes <= 0 {
		cfg.GroupChunkBytes = DefaultGroupChunkBytes
	}
	if cfg.RegisterTimeout <= 0 {
		cfg.RegisterTimeout = DefaultRegisterTimeout
	}
	if cfg.VisibilityLag <= 0 {
		cfg.VisibilityLag = DefaultVisibilityLag
	}
	if cfg.FailedSettle <= 0 {
		cfg.FailedSettle = DefaultFailedSettle
	}
	if cfg.ScheduleHistoryWindow <= 0 {
		cfg.ScheduleHistoryWindow = DefaultScheduleHistoryWindow
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.DiscardHandler)
	}
	if cfg.Clock == nil {
		cfg.Clock = clock.Real{}
	}
	if cfg.NodeID == "" {
		cfg.NodeID = workflow.DefaultNodeID()
	}
	if cfg.PayloadLimit <= 0 {
		cfg.PayloadLimit = DefaultPayloadLimit
	}
	return &Client{
		cfg:       cfg,
		specs:     map[string]workflow.Workflow{},
		workflows: map[string]*hatchetsdk.Workflow{},
		delayed:   map[string]*hatchetsdk.Workflow{},
		sdk:       cfg.Client,
		done:      make(chan struct{}),
	}
}

// Native returns the Hatchet SDK client, the one escape hatch for anything
// the workflow API does not cover. It is nil before Start when Config.Client
// was not given. Do not start a second worker on it.
//
//ds:def id=hatchet-client-native-zq53xzxt owner=@khanakia stability=stable
func (c *Client) Native() *hatchetsdk.Client {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.sdk
}

// Capabilities reports what the driver proves on a real engine.
func (c *Client) Capabilities() workflow.Caps { return c.capabilities() }

// Register checks wf against what Hatchet can honour (the capabilities
// and the exact fields, see doc.go) and records it; it returns an error
// wrapping workflow.ErrUnsupported for anything the driver would otherwise
// have to ignore or change. Its Hatchet declaration is built in Start.
//
//ds:def id=hatchet-client-register-zg59b2j4 owner=@khanakia stability=stable
func (c *Client) Register(wf workflow.Workflow) error {
	if err := workflow.Validate(wf); err != nil {
		return err
	}
	// The Hatchet-specific check first, so the error names the exact
	// field; the capability check then catches anything it does not name.
	if err := checkSupported(wf); err != nil {
		return err
	}
	if err := workflow.CheckCaps(caps, wf); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.started {
		return workflow.ErrAlreadyStarted
	}
	if _, ok := c.specs[wf.Name]; !ok {
		c.order = append(c.order, wf.Name)
	}
	c.specs[wf.Name] = wf
	return nil
}

// Start checks the Config tunings, connects, declares the workflows and
// starts the worker. The worker runs until ctx is cancelled.
//
//ds:def id=hatchet-client-start-mjwjna3p owner=@khanakia stability=stable
func (c *Client) Start(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.started {
		return workflow.ErrAlreadyStarted
	}
	// Register has refused every workflow the driver cannot honour.
	if err := checkConfig(c.cfg); err != nil {
		return err
	}
	if c.cfg.EnqueueOnly {
		c.startEnqueueOnly(ctx)
		return nil
	}
	if c.sdk == nil {
		sdk, err := newSDK(c.cfg.ClientOptions)
		if err != nil {
			return err
		}
		c.sdk = sdk
	}
	if err := c.checkEngine(ctx); err != nil {
		return err
	}
	// Each workflow's delayed variant joins the same worker as the
	// workflow itself (same queue), so poolOrder and poolSpecs list it
	// right after it.
	poolOrder := make([]string, 0, 2*len(c.order))
	poolSpecs := make(map[string]workflow.Workflow, 2*len(c.order))
	declared := make(map[string]*hatchetsdk.Workflow, 2*len(c.order))
	for _, name := range c.order {
		hw, err := c.declare(c.specs[name])
		if err != nil {
			return err
		}
		c.workflows[name] = hw
		poolOrder, poolSpecs[name], declared[name] = append(poolOrder, name), c.specs[name], hw
		if hasStartGate(c.specs[name]) {
			dw, err := c.declareDelayed(c.specs[name])
			if err != nil {
				return err
			}
			c.delayed[name] = dw
			dn := delayedName(name)
			poolOrder, poolSpecs[dn], declared[dn] = append(poolOrder, dn), c.specs[name], dw
		}
	}
	if c.cfg.BatchCoordinator {
		hw, err := c.declareCoordinator()
		if err != nil {
			return err
		}
		c.coordinator = hw
	}
	pools := workerPools(c.cfg, poolOrder, poolSpecs, declared, c.coordinator)
	runCtx, cancel := context.WithCancel(ctx)
	c.runCtx = runCtx
	// The watch is set before any worker starts, so every handler sees it.
	if c.cfg.Recycle.Enabled() {
		c.watch = workflow.NewRecycleWatch(c.cfg.Recycle, c.cfg.Clock.Now())
	}
	var stops []func() error
	stopAll := func() {
		for _, stop := range stops {
			if err := stop(); err != nil {
				c.cfg.Logger.Error("hatchet driver: stop worker", "error", err)
			}
		}
	}
	for _, p := range pools {
		stop, id, err := c.startWorker(ctx, p)
		if err != nil {
			cancel()
			stopAll()
			return err
		}
		stops = append(stops, stop)
		c.workerIDs = append(c.workerIDs, id)
	}
	c.stop = cancel
	go func() {
		<-runCtx.Done()
		// Marked before the workers stop, so a caller woken by Done
		// already gets ErrNotStarted from every call.
		c.mu.Lock()
		c.drained = true
		c.mu.Unlock()
		stopAll()
		close(c.done)
	}()
	c.started = true
	c.startNotifier(ctx)
	if c.watch != nil {
		go c.watchRecycle(runCtx, c.watch)
	}
	return nil
}

// startWorker starts one Hatchet worker for p and returns its stop
// function and engine ID once the engine lists it.
//
//ds:def id=hatchet-client-startworker-kd28sgmy owner=@khanakia stability=stable
func (c *Client) startWorker(ctx context.Context, p workerPool) (func() error, string, error) {
	opts := []hatchetsdk.WorkerOption{hatchetsdk.WithWorkflows(p.workflows...), hatchetsdk.WithLabels(workerLabels(c.cfg.NodeID, p.queue))}
	if p.slots > 0 {
		opts = append(opts, hatchetsdk.WithSlots(p.slots))
	}
	// A per-client suffix lets registerWait find this worker, and tells
	// restarts apart in the dashboard.
	name := p.name + "-" + uuid.NewString()[:8]
	w, err := c.sdk.NewWorker(name, opts...)
	if err != nil {
		return nil, "", fmt.Errorf("hatchet driver: new worker: %w", err)
	}
	stop, err := w.Start()
	if err != nil {
		return nil, "", engineErr(err, "start worker")
	}
	// The SDK panics when a worker is stopped before it has registered
	// (pkg/worker/worker.go Start), so Start returns only once the engine
	// lists the worker; stopping after that is safe.
	id, err := c.registerWait(ctx, name)
	if err != nil {
		return nil, "", err
	}
	return stop, id, nil
}

// checkConfig refuses tunings the SDK or the engine cannot honour, before
// anything connects.
//
//ds:def id=hatchet-checkconfig-wx933eq2 owner=@khanakia stability=stable
func checkConfig(cfg Config) error {
	if err := checkQueues(cfg.Queues); err != nil {
		return err
	}
	if err := workflow.ValidateRecycle(cfg.Recycle); err != nil {
		return fmt.Errorf("hatchet driver: Recycle: %w", err)
	}
	if cfg.GroupChunkRuns > DefaultGroupChunkRuns {
		return fmt.Errorf("%w: hatchet driver: GroupChunkRuns %d is over the SDK's bulk limit of %d", workflow.ErrInvalid, cfg.GroupChunkRuns, DefaultGroupChunkRuns)
	}
	if cfg.GroupChunkBytes > DefaultGroupChunkBytes {
		return fmt.Errorf("%w: hatchet driver: GroupChunkBytes %d is over %d, past which the SDK re-splits a bulk trigger", workflow.ErrInvalid, cfg.GroupChunkBytes, DefaultGroupChunkBytes)
	}
	if _, err := coordinatorRetry(*cfg.BatchCoordinatorRetry); err != nil {
		return fmt.Errorf("hatchet driver: BatchCoordinatorRetry: %w", err)
	}
	return nil
}

// registerWait polls the engine until it lists the worker named name, and
// returns the worker's engine ID. Engine readiness is wall-clock I/O, not
// scheduling logic, so it uses a real ticker: on an injected fake clock
// Start would never return.
//
//ds:def id=hatchet-client-registerwait-rum23crp owner=@khanakia stability=stable
func (c *Client) registerWait(ctx context.Context, name string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, c.cfg.RegisterTimeout)
	defer cancel()
	tick := time.NewTicker(registerPoll)
	defer tick.Stop()
	for {
		list, err := c.sdk.Workers().List(ctx)
		if err == nil && list.Rows != nil {
			for _, w := range *list.Rows {
				if w.Name == name {
					return w.Metadata.Id, nil
				}
			}
		}
		select {
		case <-ctx.Done():
			return "", fmt.Errorf("hatchet driver: worker %q did not register: %w", name, ctx.Err())
		case <-tick.C:
		}
	}
}

// checkEngine refuses an engine too old to honour idempotency keys, which
// every declared workflow relies on.
func (c *Client) checkEngine(ctx context.Context) error {
	version, err := c.sdk.GetEngineVersion(ctx)
	if err != nil {
		return engineErr(err, "engine version")
	}
	ok, err := engineVersionAtLeast(version, minIdempotencyEngineVersion)
	if err != nil {
		return err
	}
	if !ok {
		return workflow.Unsupported("Job.IdempotencyKey", "needs Hatchet engine "+minIdempotencyEngineVersion+" or later, engine is "+version)
	}
	return nil
}

// lookup returns the declared Hatchet workflow and spec for name.
//
//ds:def id=hatchet-client-lookup-ch832bcf owner=@khanakia stability=stable
func (c *Client) lookup(name string) (*hatchetsdk.Workflow, workflow.Workflow, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cfg.EnqueueOnly {
		return c.enqueueOnlyLookup(name)
	}
	if !c.started || c.drained {
		return nil, workflow.Workflow{}, workflow.ErrNotStarted
	}
	hw, ok := c.workflows[name]
	if !ok {
		return nil, workflow.Workflow{}, fmt.Errorf("%w: workflow %q", workflow.ErrNotFound, name)
	}
	return hw, c.specs[name], nil
}

// ready returns the SDK client once Start has run.
func (c *Client) ready() (*hatchetsdk.Client, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cfg.EnqueueOnly {
		return c.enqueueOnlySDK()
	}
	if !c.started || c.drained {
		return nil, workflow.ErrNotStarted
	}
	return c.sdk, nil
}

// preparedRun is what Enqueue and RunGroup send for one job: the declared
// workflow, the input envelope, the run metadata and the priority.
type preparedRun struct {
	hw       *hatchetsdk.Workflow
	wf       workflow.Workflow
	input    map[string]any
	md       map[string]string
	priority *hatchetsdk.RunPriority
	// gated is a delayed start with an idempotency key: it is started at
	// once as a run of the workflow's delayed variant (hw is that
	// variant), so the engine holds the key, and the variant's start gate
	// sleeps until the RunAt stamped as metaStartAt (checkDelayedKey,
	// hasStartGate).
	gated bool
}

// prepare checks job against its workflow's policies and builds the run
// it becomes, so every way of starting a run stamps the same metadata.
//
//ds:def id=hatchet-client-prepare-z4xnsdmz owner=@khanakia stability=stable
func (c *Client) prepare(job workflow.Job) (preparedRun, error) {
	if err := workflow.ValidateJob(job); err != nil {
		return preparedRun{}, err
	}
	if err := workflow.CheckJob(caps, job); err != nil {
		return preparedRun{}, err
	}
	if err := workflow.CheckPayload(job, c.PayloadLimit(), payloadLimitSource); err != nil {
		return preparedRun{}, err
	}
	hw, wf, err := c.lookup(job.Workflow)
	if err != nil {
		return preparedRun{}, err
	}
	// An enqueue-only client knows only the workflows it registered; an
	// unregistered one gets workflow.CheckJob alone (Config.EnqueueOnly).
	if c.isRegistered(wf.Name) {
		if err := workflow.CheckJobQueue(caps, wf, job); err != nil {
			return preparedRun{}, err
		}
	}
	key := job.IdempotencyKey
	switch wf.Idempotency {
	case workflow.IdempotencyRequired:
		if key == "" {
			return preparedRun{}, fmt.Errorf("%w: workflow %q requires Job.IdempotencyKey", workflow.ErrInvalid, wf.Name)
		}
	case workflow.IdempotencyOff:
		key = ""
	}
	if err := checkDelayedKey(wf, c.isRegistered(wf.Name), key, job.RunAt, time.Now(), c.cfg.IdempotencyMaxHold); err != nil {
		return preparedRun{}, err
	}
	priority, err := runPriority(job.Priority)
	if err != nil {
		return preparedRun{}, err
	}
	input, err := encodeInput(job.Payload)
	if err != nil {
		return preparedRun{}, err
	}
	scope := engineName(c.cfg.Namespace, wf.Name)
	md, err := runMetadata(job.Labels, runIdempotencyKey(scope, key, wf.Idempotency, uuid.NewString))
	if err != nil {
		return preparedRun{}, err
	}
	md[metaWorkflow] = wf.Name
	md[metaQueue] = workflow.EffectiveQueue(wf, job)
	if job.Priority != 0 {
		md[metaPriority] = strconv.Itoa(job.Priority)
	}
	if job.Tenant != "" {
		md[metaTenant] = job.Tenant
	}
	md[metaEnqueuedAt] = strconv.FormatInt(enqueueTime(job).UnixMilli(), 10)
	gated := key != "" && !job.RunAt.IsZero()
	if gated {
		md[metaStartAt] = strconv.FormatInt(job.RunAt.UnixMilli(), 10)
		if hw, err = c.delayedHandle(wf.Name); err != nil {
			return preparedRun{}, err
		}
	}
	return preparedRun{hw: hw, wf: wf, input: input, md: md, priority: priority, gated: gated}, nil
}

// runOptions are the SDK options of an immediate run.
func (r preparedRun) runOptions() []hatchetsdk.RunOptFunc {
	opts := []hatchetsdk.RunOptFunc{hatchetsdk.WithRunMetadata(r.md)}
	if r.priority != nil {
		opts = append(opts, hatchetsdk.WithRunPriority(*r.priority))
	}
	return opts
}

// Enqueue starts a run now (RunNoWait) or at job.RunAt (a Hatchet scheduled
// run, whose RunRef holds only the ScheduledID until it fires). A key held
// by an unfinished run returns that run instead of starting another.
//
// A delayed start with an idempotency key cannot be a scheduled run: the
// engine's scheduled runs take no key, and the key would be checked only
// when the run fires. It is started at once instead, as a run of the
// workflow's delayed variant (declareDelayed), so the engine holds the key
// from the enqueue, with its RunAt stamped on the run; the variant's
// start gate (a durable task before the first steps) sleeps on the engine
// until then, and the run reads StatusScheduled meanwhile. Its RunRef
// holds the RunID, a duplicate gets that same RunRef, and Cancel,
// CancelAll and CancelChildren cancel it like any run. See
// checkDelayedKey for when it is refused.
//
// A label under a reserved prefix is refused unless ctx allows it
// (workflow.CheckJobLabels): only the package owning a prefix may set it.
//
//ds:def id=hatchet-client-enqueue-ym8nfcz9 owner=@khanakia stability=stable
func (c *Client) Enqueue(ctx context.Context, job workflow.Job) (workflow.RunRef, error) {
	if err := workflow.CheckJobLabels(ctx, job); err != nil {
		return workflow.RunRef{}, err
	}
	return c.enqueue(ctx, job)
}

// enqueue is Enqueue without the reserved-label check, for the driver's
// own jobs (TriggerSchedule), whose labels the library wrote.
//
//ds:def id=hatchet-client-enqueue-537mqp9q owner=@khanakia stability=stable
func (c *Client) enqueue(ctx context.Context, job workflow.Job) (workflow.RunRef, error) {
	run, err := c.prepare(job)
	if err != nil {
		return workflow.RunRef{}, err
	}
	hw, wf, input, md, priority := run.hw, run.wf, run.input, run.md, run.priority

	if !job.RunAt.IsZero() && !run.gated {
		// A time-ordered (v7) token records when the run was scheduled,
		// which bounds the search for the run it fires after Reschedule
		// moved it earlier (findRunByToken).
		token := newScheduleToken()
		md[metaScheduleToken] = token
		trigger := features.CreateScheduledRunTrigger{
			// UTC on purpose: the engine stores triggerAt in a column
			// without a zone, so a local-zone time fires hours off
			// (seen on hatchet-lite v0.110.5 with an IST client).
			TriggerAt:          job.RunAt.UTC(),
			Input:              input,
			AdditionalMetadata: labelsAny(md),
		}
		if priority != nil {
			p := features.RunPriority(*priority)
			trigger.Priority = &p
		}
		sdk, err := c.ready()
		if err != nil {
			return workflow.RunRef{}, err
		}
		sched, err := sdk.Schedules().Create(ctx, wf.Name, trigger)
		if err != nil {
			return workflow.RunRef{}, engineErr(err, "schedule run")
		}
		if sched == nil {
			return workflow.RunRef{}, errors.New("hatchet driver: schedule run: engine returned no scheduled run")
		}
		sid := scheduledRef{token: token, runAt: job.RunAt}
		workflow.MarkInserted(ctx)
		return workflow.RunRef{ScheduledID: sid.id()}, nil
	}

	ref, err := hw.RunNoWait(ctx, input, run.runOptions()...)
	if collision, ok := hatchetsdk.IsIdempotencyCollisionError(err); ok {
		workflow.MarkDuplicate(ctx)
		return workflow.RunRef{RunID: workflow.RunID(collision.ExistingRunExternalId)}, nil
	}
	if err != nil {
		return workflow.RunRef{}, engineErr(err, "run")
	}
	workflow.MarkInserted(ctx)
	return workflow.RunRef{RunID: workflow.RunID(ref.RunId)}, nil
}

// runState is where a RunRef stands on the engine.
type runState int

const (
	stateRun     runState = iota // the run exists; runID is set
	statePending                 // a scheduled run that has not fired
	stateGone                    // a scheduled run deleted before it fired
)

// enqueueTime is when job asks to start: RunAt, or now. It is wall-clock
// on purpose: it is compared with the engine's event timestamps.
func enqueueTime(job workflow.Job) time.Time {
	if !job.RunAt.IsZero() {
		return job.RunAt
	}
	return time.Now()
}

// resolve finds the run behind ref. A RunID is used as is. A ScheduledID
// is looked up as a Hatchet scheduled run; once that fires the engine
// deletes it, so the fired run is then found by the token the driver
// stamped on it (see scheduledID). A scheduled run that neither exists nor
// fired was deleted, which only Cancel (or the dashboard) does; see
// resolveScheduled for how the two are told apart.
func (c *Client) resolve(ctx context.Context, sdk *hatchetsdk.Client, ref workflow.RunRef) (string, runState, error) {
	if !ref.RunID.IsZero() {
		if _, perr := uuid.Parse(ref.RunID.String()); perr != nil {
			return "", 0, fmt.Errorf("%w: run %q", workflow.ErrNotFound, ref.RunID)
		}
		return ref.RunID.String(), stateRun, nil
	}
	if ref.ScheduledID.IsZero() {
		return "", 0, fmt.Errorf("%w: empty RunRef", workflow.ErrNotFound)
	}
	sid, ok := parseScheduledID(ref.ScheduledID)
	if !ok {
		return "", 0, fmt.Errorf("%w: scheduled run %q", workflow.ErrNotFound, ref.ScheduledID)
	}
	return resolveScheduled(ctx, sdkLookup{sdk}, sid, c.cfg.VisibilityLag, c.deleted.has)
}

// scheduleLookup is what resolving a ScheduledID asks the engine: the
// scheduled run stamped with a token, and the run it fired. sdkLookup
// answers it from the engine; tests replay the engine's read lag with a
// fake.
type scheduleLookup interface {
	schedule(ctx context.Context, token string) (*rest.ScheduledWorkflows, error)
	firedRun(ctx context.Context, sid scheduledRef) (string, error)
}

// sdkLookup is the scheduleLookup of a connected client.
type sdkLookup struct{ sdk *hatchetsdk.Client }

func (l sdkLookup) schedule(ctx context.Context, token string) (*rest.ScheduledWorkflows, error) {
	return findSchedule(ctx, l.sdk, token)
}

func (l sdkLookup) firedRun(ctx context.Context, sid scheduledRef) (string, error) {
	return findRunByToken(ctx, l.sdk, sid)
}

// resolveScheduled is resolve for a ScheduledID. While the scheduled
// record exists the start is pending (or names the run it fired). Once
// the record is gone, either the engine fired it (it deletes the record
// as it creates the run) or it was deleted, and the run list tells them
// apart only after its read side caught up: the fired run may be missing
// for up to lag. So the start reads gone only when no run is listed for
// it after lag, or at once when deletedHere says this client deleted it
// before the engine could fire it (deletedStarts).
//
// The ScheduledID's RunAt is no bound here: Reschedule moves a start
// earlier without changing its ScheduledID, so a start can fire long
// before that RunAt. Treating "before RunAt" as "nothing fired" read a
// moved start cancelled in the moment between its record's delete and
// its run's listing (seen under load by the conformance case "Reschedule
// moves a delayed run earlier").
//
//ds:def id=hatchet-resolvescheduled-65msmc9t owner=@khanakia stability=stable
func resolveScheduled(ctx context.Context, look scheduleLookup, sid scheduledRef, lag time.Duration, deletedHere func(token string) bool) (string, runState, error) {
	sched, err := look.schedule(ctx, sid.token)
	switch {
	case err != nil:
		return "", 0, err
	case sched != nil && sched.WorkflowRunId != nil:
		return sched.WorkflowRunId.String(), stateRun, nil
	case sched != nil:
		return "", statePending, nil
	}
	deadline := time.Now().Add(lag)
	for {
		runID, err := look.firedRun(ctx, sid)
		if err != nil || runID != "" {
			return runID, stateRun, err
		}
		if deletedHere(sid.token) || time.Now().After(deadline) {
			return "", stateGone, nil
		}
		select {
		case <-ctx.Done():
			return "", 0, ctx.Err()
		case <-time.After(registerPoll):
		}
	}
}

// findSchedule returns the scheduled run stamped with token, or nil when
// the engine no longer has it (it fired, or was deleted).
func findSchedule(ctx context.Context, sdk *hatchetsdk.Client, token string) (*rest.ScheduledWorkflows, error) {
	limit := int64(1)
	md := []string{metaScheduleToken + ":" + token}
	list, err := sdk.Schedules().List(ctx, rest.WorkflowScheduledListParams{Limit: &limit, AdditionalMetadata: &md})
	if err != nil {
		return nil, engineErr(err, "find scheduled run")
	}
	if list.Rows == nil || len(*list.Rows) == 0 {
		return nil, nil
	}
	return &(*list.Rows)[0], nil
}

// findRunByToken returns the run a scheduled run fired, or "" when none is
// listed yet.
//
// It searches from a minute before the first RunAt, and when that finds
// nothing and the token records an earlier scheduling time (a v7 token,
// see newScheduleToken), once more from then: Reschedule may have moved
// the run earlier than RunAt, and it cannot have fired before it was
// scheduled. The second search costs a call only on that miss.
func findRunByToken(ctx context.Context, sdk *hatchetsdk.Client, sid scheduledRef) (string, error) {
	since := sid.runAt.Add(-scheduleLookback)
	runID, err := findRunByTokenSince(ctx, sdk, sid.token, since)
	if err != nil || runID != "" {
		return runID, err
	}
	if created, ok := tokenTime(sid.token); ok && created.Before(since) {
		return findRunByTokenSince(ctx, sdk, sid.token, created.Add(-scheduleLookback))
	}
	return "", nil
}

// findRunByTokenSince lists the run stamped with token created since since.
func findRunByTokenSince(ctx context.Context, sdk *hatchetsdk.Client, token string, since time.Time) (string, error) {
	limit := int64(1)
	md := []string{metaScheduleToken + ":" + token}
	list, err := sdk.Runs().List(ctx, rest.V1WorkflowRunListParams{
		Since:              since,
		Limit:              &limit,
		AdditionalMetadata: &md,
	})
	if err != nil {
		return "", engineErr(err, "find fired run")
	}
	for _, r := range list.Rows {
		return r.WorkflowRunExternalId.String(), nil
	}
	return "", nil
}

// newScheduleToken is the token of a new delayed start: a UUIDv7, whose
// first 48 bits are the creation time in Unix milliseconds. Tokens made
// before tokens were time-ordered are v4 and carry no time.
func newScheduleToken() string {
	id, err := uuid.NewV7()
	if err != nil {
		return uuid.NewString()
	}
	return id.String()
}

// tokenTime is when the delayed start with token was scheduled, for a v7
// token; false for any other.
func tokenTime(token string) (time.Time, bool) {
	id, err := uuid.Parse(token)
	if err != nil || id.Version() != 7 {
		return time.Time{}, false
	}
	sec, nsec := id.Time().UnixTime()
	return time.Unix(sec, nsec), true
}

// Status reports where the run is. A run whose only unstarted work is a
// wait or sleep step, with nothing queued or running, is StatusWaiting.
//
//ds:def id=hatchet-client-status-ykkq4x58 owner=@khanakia stability=stable
func (c *Client) Status(ctx context.Context, ref workflow.RunRef) (workflow.Status, error) {
	sdk, err := c.ready()
	if err != nil {
		return "", err
	}
	runID, state, err := c.resolve(ctx, sdk, ref)
	if err != nil {
		return "", err
	}
	switch state {
	case statePending:
		return workflow.StatusScheduled, nil
	case stateGone:
		return workflow.StatusCancelled, nil
	}
	d, err := runDetailsLagged(ctx, sdk, runID, c.cfg.VisibilityLag)
	if isNotFound(err) {
		return "", fmt.Errorf("%w: run %q", workflow.ErrNotFound, runID)
	}
	if err != nil {
		return "", engineErr(err, "run status")
	}
	st, err := c.endStatus(d)
	if err != nil || st.IsTerminal() || st == workflow.StatusScheduled {
		return st, err
	}
	if c.suspended(d) {
		return workflow.StatusWaiting, nil
	}
	return st, nil
}

// suspended reports whether a live run is parked on a wait or sleep step
// (see suspendedOn).
// The workflow is found by the name Enqueue stamps on the run.
func (c *Client) suspended(d *rest.V1WorkflowRunDetails) bool {
	if d.Run.AdditionalMetadata == nil {
		return false
	}
	name, _ := (*d.Run.AdditionalMetadata)[metaWorkflow].(string)
	c.mu.Lock()
	wf, ok := c.specs[name]
	c.mu.Unlock()
	if !ok {
		return false
	}
	names := make(map[openapi_types.UUID]string, len(d.Shape))
	unstarted := map[string]bool{}
	for _, sh := range d.Shape {
		if sh.TaskExternalId == (openapi_types.UUID{}) {
			unstarted[sh.TaskName] = true
			continue
		}
		names[sh.TaskExternalId] = sh.TaskName
	}
	var active []string
	for _, t := range d.Tasks {
		if t.Status == rest.V1TaskStatusQUEUED || t.Status == rest.V1TaskStatusRUNNING {
			active = append(active, names[t.TaskExternalId])
		}
	}
	return suspendedOn(wf, active, unstarted)
}

// runDetailsLagged reads a run's details, retrying a 404 for up to lag
// (Config.VisibilityLag): the engine answers RunNoWait before its read
// side lists the run, so a run enqueued a moment ago reads as missing. A
// run still missing after the window is reported as not found. The
// deadline is wall-clock: it measures the engine's own lag.
//
//ds:def id=hatchet-rundetailslagged-fm92bjc9 owner=@khanakia stability=stable
func runDetailsLagged(ctx context.Context, sdk *hatchetsdk.Client, runID string, lag time.Duration) (*rest.V1WorkflowRunDetails, error) {
	deadline := time.Now().Add(lag)
	for {
		d, err := sdk.Runs().Get(ctx, runID)
		if err == nil && d == nil {
			return nil, errors.New("hatchet driver: engine returned no run")
		}
		if !isNotFound(err) || time.Now().After(deadline) {
			return d, err
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(registerPoll):
		}
	}
}

// Cancel stops a run, or deletes a scheduled run that has not fired. A
// finished or already cancelled run is left alone and nil returned.
//
// A delayed start deleted within dueGuard (15s) of its due time may
// already be loaded into the engine's ticker, which fires it whatever the
// delete says. Cancel then waits until the start's due time plus
// Config.VisibilityLag for the run it may fire (ctx bounds the wait):
// none listed by then, it returns nil; one listed, it cancels that run
// (unless it already ended) and returns ErrCancelTooLate, which matches
// workflow.ErrWrongState. So a nil Cancel of a delayed start means it
// will not run.
//
//ds:def id=hatchet-client-cancel-mvr63tkp owner=@khanakia stability=stable
func (c *Client) Cancel(ctx context.Context, ref workflow.RunRef) error {
	sdk, err := c.ready()
	if err != nil {
		return err
	}
	late, err := c.cancelRef(ctx, sdk, ref)
	if err != nil || late == nil {
		return err
	}
	if err := c.settleLate(ctx, sdk, []*lateStart{late}); err != nil {
		return err
	}
	if late.runID != "" {
		return late.err()
	}
	return nil
}

// Signal pushes ev as a Hatchet event. Wait steps on ev.Name whose Key
// equals ev.Key resume with ev.Payload.
func (c *Client) Signal(ctx context.Context, ev workflow.Event) error {
	sdk, err := c.ready()
	if err != nil {
		return err
	}
	if ev.Name == "" {
		return fmt.Errorf("%w: event has no name", workflow.ErrInvalid)
	}
	evKey := eventKey(ev.Name, ev.Key)
	if err := sdk.Events().Push(ctx, evKey, eventDocument(ev), hatchetsdk.WithFilterScope(&evKey)); err != nil {
		return engineErr(err, "push event")
	}
	return nil
}

// Schedule creates a Hatchet cron trigger. Hatchet evaluates crons in UTC
// only, so any other Location is refused rather than shifted.
//
// A named schedule is one schedule per (Workflow, Name), as on the other
// drivers, so an app can call Schedule on every boot: when the workflow
// already has a cron trigger of that name with the same expression, input
// and labels, nothing is created. When the definition changed, the old
// trigger is deleted and a new one created (Hatchet cannot edit a
// trigger's expression, input or labels in place), keeping the old one's
// pause when Config.API is set. Either way the returned ID is the same:
// a named schedule's ID names the (Workflow, Name) pair ("named:" plus
// an encoding of both), not the engine trigger, and Unschedule,
// PauseSchedule, ResumeSchedule and TriggerSchedule find the trigger by
// it. A trigger of that name made before this ID form is replaced on the
// next Schedule call. An unnamed schedule always creates a new trigger,
// whose ID is the engine's.
//
//ds:def id=hatchet-client-schedule-ug2ype7f owner=@khanakia stability=stable
func (c *Client) Schedule(ctx context.Context, s workflow.Schedule) (workflow.ScheduleID, error) {
	if err := workflow.ValidateSchedule(s); err != nil {
		return "", err
	}
	if err := checkScheduleZone(s.Location); err != nil {
		return "", err
	}
	if err := checkScheduleMisfire(s.Misfire); err != nil {
		return "", err
	}
	if !s.Active.IsZero() {
		return "", unsupported("Schedule.Active", workflow.CapScheduleActive, "a Hatchet cron trigger has no start or end, and the engine fires it without asking a worker")
	}
	if s.KeepRuns > 0 {
		return "", unsupported("Schedule.KeepRuns", workflow.CapScheduleKeep, keepRunsReason)
	}
	if err := workflow.CheckScheduleCaps(caps, s); err != nil {
		return "", err
	}
	if err := checkCron(s.Cron, s.Seconds); err != nil {
		return "", err
	}
	if _, _, err := c.lookup(s.Workflow); err != nil {
		return "", err
	}
	sdk, err := c.ready()
	if err != nil {
		return "", err
	}
	input, err := encodeInput(s.Input)
	if err != nil {
		return "", err
	}
	md, err := runMetadata(s.Labels, "")
	if err != nil {
		return "", err
	}
	// The cron fire gets its idempotency key from the engine's cron
	// metadata (idempotencyExpression), not from this stamp.
	delete(md, metaIdempotencyKey)
	// Schedule.Tenant rides on the trigger's metadata, which the engine
	// copies onto every run it fires, as Enqueue stamps Job.Tenant.
	if s.Tenant != "" {
		md[metaTenant] = s.Tenant
	}
	name := s.Name
	var replaced replacedCron
	if name == "" {
		name = s.Workflow + "-" + uuid.NewString()[:8]
	} else {
		md[metaNamed] = "1"
		var found bool
		replaced, found, err = c.replaceNamedCron(ctx, sdk, s)
		if err != nil {
			return "", err
		}
		if found {
			return namedScheduleID(s.Workflow, s.Name), nil
		}
	}
	cr, err := sdk.Crons().Create(ctx, engineName(c.cfg.Namespace, s.Workflow), features.CreateCronTrigger{
		Name:               name,
		Expression:         s.Cron,
		Input:              input,
		AdditionalMetadata: labelsAny(md),
	})
	if err != nil {
		var bad *features.InvalidCronExpressionError
		if errors.As(err, &bad) {
			return "", fmt.Errorf("%w: %v", workflow.ErrInvalid, err)
		}
		if s.Name != "" {
			// Another process created the same name and expression first
			// (the engine's unique key on parent, cron and name): return
			// that trigger when it is the same definition.
			if _, found, lerr := c.replaceNamedCron(ctx, sdk, s); lerr == nil && found {
				return namedScheduleID(s.Workflow, s.Name), nil
			}
		}
		return "", engineErr(err, "create cron")
	}
	if s.Name == "" {
		return workflow.ScheduleID(cr.Metadata.Id), nil
	}
	id := namedScheduleID(s.Workflow, s.Name)
	// A replaced trigger that was disabled keeps its pause.
	if replaced.disabled && c.cfg.API != nil {
		if err := c.setCronEnabled(ctx, id, false); err != nil {
			return id, err
		}
	}
	return id, nil
}

// Unschedule deletes a cron trigger (every trigger of the name, for the
// ID of a named schedule). An empty or unknown ID is a no-op.
func (c *Client) Unschedule(ctx context.Context, id workflow.ScheduleID) error {
	if id.IsZero() {
		return nil
	}
	sdk, err := c.ready()
	if err != nil {
		return err
	}
	rows, err := c.cronRows(ctx, sdk, id)
	if err != nil {
		return err
	}
	for _, row := range rows {
		if err := c.deleteCron(ctx, sdk, row.Metadata.Id); err != nil {
			return err
		}
	}
	return nil
}

// deleteCron deletes the trigger with engine ID cronID; an unknown one
// is not an error.
func (c *Client) deleteCron(ctx context.Context, sdk *hatchetsdk.Client, cronID string) error {
	if err := sdk.Crons().Delete(ctx, cronID); err != nil && !isNotFound(err) && !isNoContent(err) {
		return engineErr(err, "delete cron")
	}
	return nil
}

// delayedHandle returns the handle that triggers workflow name's delayed
// variant: the one Start declared, or, on an enqueue-only client, a handle
// by the variant's name (the worker processes declared it).
func (c *Client) delayedHandle(name string) (*hatchetsdk.Workflow, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if hw, ok := c.delayed[name]; ok {
		return hw, nil
	}
	if c.cfg.EnqueueOnly {
		sdk, err := c.enqueueOnlySDK()
		if err != nil {
			return nil, err
		}
		hw := sdk.NewWorkflow(delayedName(name))
		c.delayed[name] = hw
		return hw, nil
	}
	return nil, fmt.Errorf("hatchet driver: workflow %q has no delayed variant", name)
}
