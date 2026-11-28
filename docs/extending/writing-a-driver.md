# Write a driver

A driver is any type that implements `workflow.Client`: a queue in Redis, a cloud service, an in-house engine, or a narrower in-process runner. This guide builds `Tiny`, a complete driver of about 300 lines that runs steps as a DAG with retries, timeouts and failure handlers, declares no capability beyond the core contract and refuses everything else, then holds it to the conformance suite every shipped driver runs ([`workflowtest.Run`](ds:block?id=workflowtest-run-2dxkjk9h)). The core gives a driver the shared rules as functions (validation with [`Validate`](ds:block?id=validate-mnthpzkp), capability checks with [`CheckCaps`](ds:block?id=checkcaps-eg44ezuh), step inputs with [`StepInput`](ds:block?id=stepinput-ykz7ud5d), retry delays with [`NextDelay`](ds:block?id=retrypolicy-nextdelay-94cqz66r), misfire maths with [`Misfire.Due`](ds:block?id=misfire-due-5butt4h7)), so two drivers cannot disagree on what a spec means.

## Before you start

- You know the [client](../concepts/client.md#the-client-interface) contract and [capabilities](../concepts/capabilities.md#what-a-driver-declares).
- A driver lives in its own package, ideally its own module if it needs a third-party SDK, and imports only `github.com/ubgo/workflow` and its subpackages from this repository.

## What a driver implements

### Step 1 — Declare what it can really do

A capability is a promise the conformance suite will test against your real backend. Declare only what you have proven; the empty set is honest and still runs every plain workflow, since [`workflow.Requires`](ds:block?id=requires-6tcjbcfj) asks nothing of a workflow that uses no optional field.

<!-- snippet: examples/custom-driver/driver.go#caps -->
```go
// tinyCaps is everything this driver can really do: nothing beyond the
// core contract. Run steps as a DAG, retries, timeouts, OnFailure and
// Status need no capability; delayed starts, idempotency keys, cron,
// cancel, waits and sleeps do, and tiny has none of them, so it refuses
// each one instead of ignoring it.
var tinyCaps workflow.Caps
```

### Step 2 — Hold state in the value, take a clock

No package-level state: everything hangs off the value the app constructs, so two clients in one process never share anything. Every timer comes from an injected [`clock.Clock`](ds:block?id=clock-clock-mw5ene3w) ([Time](../concepts/time.md#the-clock-interface)), and the constructor opens nothing.

<!-- snippet: examples/custom-driver/driver.go#type -->
```go
// Tiny is a minimal in-process driver of workflow.Client. Runs live in a
// map and each run executes on its own goroutine, step by step in DAG
// order. Every timer comes from the injected clock, so the conformance
// suite drives it on a fake clock.
type Tiny struct {
	clk clock.Clock

	mu      sync.Mutex
	wfs     map[string]workflow.Workflow
	runs    map[workflow.RunID]workflow.Status
	ctx     context.Context // the Start context; nil before Start
	lastRun int
}

// The build fails here if Tiny stops satisfying workflow.Client.
var _ workflow.Client = (*Tiny)(nil)

// NewTiny returns an unstarted driver reading time from clk. It starts no
// goroutine: nothing runs until Start.
func NewTiny(clk clock.Clock) *Tiny {
	return &Tiny{clk: clk, wfs: map[string]workflow.Workflow{}, runs: map[workflow.RunID]workflow.Status{}}
}

// Capabilities never changes over the life of a client.
func (t *Tiny) Capabilities() workflow.Caps { return tinyCaps }
```

### Step 3 — Register and Start

`Register` calls [`workflow.Validate`](ds:block?id=validate-mnthpzkp), the one shape check, and refuses after `Start` with [`ErrAlreadyStarted`](ds:block?id=erralreadystarted-prauyy24). `Start` calls [`workflow.CheckCaps`](ds:block?id=checkcaps-eg44ezuh) for every registered workflow, so a workflow that needs a capability you lack fails at start-up with an [`UnsupportedError`](ds:block?id=unsupportederror-46jkqrxt) naming it. The `Start` context is the client's lifetime: cancelling it stops everything. A driver that must refuse a field its backend cannot express (an expression language it does not have, a window size it cannot do) returns [`workflow.Unsupported(field, reason)`](ds:block?id=unsupported-4r9efjk8) from `Register` or `Start` too.

<!-- snippet: examples/custom-driver/driver.go#lifecycle -->
```go
// Register validates the shape of wf (workflow.Validate wraps
// ErrInvalid) and stores it; registering a name again replaces it.
func (t *Tiny) Register(wf workflow.Workflow) error {
	if err := workflow.Validate(wf); err != nil {
		return err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.ctx != nil {
		return workflow.ErrAlreadyStarted
	}
	t.wfs[wf.Name] = wf
	return nil
}

// Start refuses any registered workflow that needs a capability tiny
// lacks, then remembers ctx: cancelling it stops every run.
func (t *Tiny) Start(ctx context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.ctx != nil {
		return workflow.ErrAlreadyStarted
	}
	for _, wf := range t.wfs {
		if err := workflow.CheckCaps(tinyCaps, wf); err != nil {
			return err
		}
	}
	t.ctx = ctx
	return nil
}
```

### Step 4 — Enqueue, refusing what it cannot honour

[`workflow.ValidateJob`](ds:block?id=validatejob-a7nq3u2t) checks the job's shape, and [`workflow.CheckJob`](ds:block?id=checkjob-2rsfu9ma) compares what it asks for ([`workflow.RequiresJob`](ds:block?id=requiresjob-mrfs7vqd): a delayed start, an idempotency key, a queue, a per-job retry policy, a tenant, a chain) with the driver's capabilities. A driver without one returns an `UnsupportedError` naming the field; it never drops the field and runs the job anyway, which is the bug that makes a fake pass while production misbehaves. [`workflow.CheckJobLabels`](ds:block?id=checkjoblabels-p6m28b9h) refuses a label under a reserved prefix that the caller's context does not allow; every driver calls it in its public `Enqueue`, and the conformance group `reservedlabels` ([`reservedLabelCases`](ds:block?id=workflowtest-reservedlabelcases-m7p7wb5d)) fails a driver that does not.

<!-- snippet: examples/custom-driver/driver.go#enqueue -->
```go
// Enqueue refuses a Job field tiny cannot honour, naming the field, then
// starts the run on its own goroutine.
func (t *Tiny) Enqueue(ctx context.Context, job workflow.Job) (workflow.RunRef, error) {
	// The job's shape first (ErrInvalid), then what it asks of the driver:
	// an UnsupportedError naming the field (RunAt, a key, a queue, Retry).
	if err := workflow.ValidateJob(job); err != nil {
		return workflow.RunRef{}, err
	}
	if err := workflow.CheckJob(tinyCaps, job); err != nil {
		return workflow.RunRef{}, err
	}
	// Labels under a reserved prefix belong to the library; an app's
	// Enqueue may not set them (CheckJobLabels, first in every public entry).
	if err := workflow.CheckJobLabels(ctx, job); err != nil {
		return workflow.RunRef{}, err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.ctx == nil {
		return workflow.RunRef{}, workflow.ErrNotStarted
	}
	wf, ok := t.wfs[job.Workflow]
	if !ok {
		return workflow.RunRef{}, fmt.Errorf("%w: workflow %q", workflow.ErrNotFound, job.Workflow)
	}
	t.lastRun++
	id := workflow.RunID("tiny-" + strconv.Itoa(t.lastRun))
	t.runs[id] = workflow.StatusQueued
	go t.execute(t.ctx, wf, job, id)
	return workflow.RunRef{RunID: id}, nil
}
```

### Step 5 — Refuse the methods it has no capability for

`Cancel` without `CapCancel` and `Schedule` without `CapCron` return an `UnsupportedError`; [`workflow.CheckScheduleCaps`](ds:block?id=checkschedulecaps-45emsz38) builds the schedule one, naming the field responsible (`Schedule`, `Schedule.Location`, `Schedule.Misfire`, `Schedule.Windows`, `Schedule.Overlap` and so on). [`Unschedule`](ds:block?id=client-unschedule-2eju3acd) of an empty ID is a no-op. A [`Signal`](ds:block?id=client-signal-2sdhmcdy) nobody waits for is not an error.

<!-- snippet: examples/custom-driver/driver.go#refuse -->
```go
// Cancel, Schedule and Signal are where a driver without the capability
// says so. A Signal can match nothing, because Start refused every wait
// step, and a signal nobody waits for is not an error.
func (t *Tiny) Cancel(context.Context, workflow.RunRef) error {
	return &workflow.UnsupportedError{Field: "Cancel", Missing: workflow.CapCancel}
}

// Schedule implements workflow.Client: it checks the schedule's shape,
// then refuses it for the missing CapCron.
func (t *Tiny) Schedule(_ context.Context, s workflow.Schedule) (workflow.ScheduleID, error) {
	if err := workflow.ValidateSchedule(s); err != nil {
		return "", err
	}
	// Without CapCron this always refuses, before or after Start.
	return "", workflow.CheckScheduleCaps(tinyCaps, s)
}

// Unschedule implements workflow.Client: tiny stores no schedule, so any
// non-zero ID is ErrNotFound and the zero ID is a no-op.
func (t *Tiny) Unschedule(_ context.Context, id workflow.ScheduleID) error {
	if id.IsZero() {
		return nil
	}
	return fmt.Errorf("%w: schedule %q", workflow.ErrNotFound, id)
}

// Signal implements workflow.Client: it matches nothing, and only refuses
// before Start.
func (t *Tiny) Signal(context.Context, workflow.Event) error { return t.started() }

// started is ErrNotStarted before Start, as the contract asks of Signal.
func (t *Tiny) started() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.ctx == nil {
		return workflow.ErrNotStarted
	}
	return nil
}
```

### Step 6 — Run the steps by the shared rules

A step starts once all its `After` parents have succeeded, and its input is [`workflow.StepInput(payload, parents, outputs)`](ds:block?id=stepinput-ykz7ud5d): the payload, one parent's output, or an object keyed by parent name. When a step fails for good, `OnFailure` runs once with that step's input and the run fails.

<!-- snippet: examples/custom-driver/driver.go#execute -->
```go
// execute runs the steps in DAG order: a step starts once all its After
// parents succeeded, with the input workflow.StepInput builds. The first
// step that fails for good fails the run, after OnFailure runs once with
// that step's input.
func (t *Tiny) execute(ctx context.Context, wf workflow.Workflow, job workflow.Job, id workflow.RunID) {
	t.setStatus(id, workflow.StatusRunning)
	outs := map[string][]byte{}
	for len(outs) < len(wf.Steps) {
		for _, s := range wf.Steps {
			if _, done := outs[s.Name]; done || !parentsDone(s, outs) {
				continue
			}
			in := workflow.StepInput(job.Payload, s.After, outs)
			out, err := t.runStep(ctx, wf, s, job, id, in)
			if err != nil {
				if ctx.Err() != nil {
					t.setStatus(id, workflow.StatusCancelled)
					return
				}
				if wf.OnFailure != nil {
					_, _ = wf.OnFailure(ctx, in)
				}
				t.setStatus(id, workflow.StatusFailed)
				return
			}
			outs[s.Name] = out
		}
	}
	t.setStatus(id, workflow.StatusSucceeded)
}

// parentsDone reports whether every step s runs After has an output, so s
// may start; a step with no After is always ready.
func parentsDone(s workflow.Step, outs map[string][]byte) bool {
	for _, p := range s.After {
		if _, ok := outs[p]; !ok {
			return false
		}
	}
	return true
}
```

Each attempt runs with [`workflow.WithStepInfo`](ds:block?id=withstepinfo-t26f3eu3) on its context (so middleware and handlers know the workflow, step, run, attempt and labels), under the step's timeout or the workflow's. Retries follow [`workflow.EffectiveRetries(step, wf)`](ds:block?id=effectiveretries-52e5k4q9) (the step's policy, else the workflow's), [`RetryPolicy.Retryable`](ds:block?id=retrypolicy-retryable-ve8muksd) (not permanent, not cancelled, not excluded by `NoRetryOn`, within `Max`) and [`RetryPolicy.NextDelay`](ds:block?id=retrypolicy-nextdelay-94cqz66r) (the `DelayFor` answer, else the backoff with its jitter). The loop is in [Time](../concepts/time.md#the-clock-interface), where it serves as the clock example.

The rest of the shared rules, for the capabilities `Tiny` does not have:

| Capability you add | Core helper to call |
|---|---|
| `CapWaitForEvent` | [`WaitSpec.ResolveKey(job)`](ds:block?id=waitspec-resolvekey-ey3s9dka) for a run's wait key; keep the [`Signal`](ds:block?id=client-signal-2sdhmcdy) contract in the `Client` doc (an event sent after the run's enqueue is kept, one run never takes one event twice, one event resumes every matching run) |
| `CapCron`, misfire policies, `CapCronZone` | [`cron.Parse`](ds:block?id=cron-parse-85dsvh6b) (or a `cron.Parser` option), [`Misfire.Due(src, last, now, loc)`](ds:block?id=misfire-due-5butt4h7) for which missed ticks fire, [`workflow.IsUTC`](ds:block?id=isutc-aj4baxqq) |
| `CapRetryJitter`, `CapRetryDelay` | [`RetryPolicy.NextDelay`](ds:block?id=retrypolicy-nextdelay-94cqz66r), with a jitter source the app can pin in tests |
| `CapRunFinished` | implement [`workflow.RunNotifier`](ds:block?id=runnotifier-tbkv69uz), keeping its delivery contract (before `Start` only, after `Status` shows the end, panics recovered, at least once, every terminal run) |
| batches as one native call | implement [`batch.GroupRunner`](ds:block?id=batch-grouprunner-v7nf5w8m) |

Mark errors from an unreachable backend with [`workflow.Unavailable(err)`](ds:block?id=unavailable-wpp9cehg), so a failover client can tell an outage from a refusal ([Backend outages](../concepts/errors.md#backend-outages)). Accept step middleware with a `WithMiddleware` option and apply it around every step and `OnFailure`, as the shipped drivers do ([`memory.WithMiddleware`](ds:block?id=memory-withmiddleware-yea2tnqr)).

### Step 7 — Call the shared helper for each capability you add

Every optional operation and capability has a helper in the core that decides it the way every shipped driver does. Call it at the same point a shipped driver does, rather than writing the rule again: the conformance cases are written against these answers. [examples/custom-driver](../../examples/custom-driver/) calls each one in `helpers.go` and prints what it decides; `Tiny` itself declares none of these capabilities and uses only the step-timeout handshake, the last one below.

Resets and forks (`workflow.RunResetter`, `RunResetterWith`, `RunForker`). [`ResetStatusOK(status, opts)`](ds:block?id=resetstatusok-pdwz2avg) says whether a run may be reset (failed always, succeeded only with `ResetOptions.Succeeded`), [`ForkStatusOK(status)`](ds:block?id=forkstatusok-x544ybnq) whether it may be forked (it has finished), [`ForkRedo(status, labels)`](ds:block?id=forkredo-k36kx3xp) whether the fork redoes work that already succeeded (so it gets `LabelRedo`), and [`CheckFork(step, opts)`](ds:block?id=checkfork-a9wp7kp8) refuses an empty step or a reserved label before the driver looks the run up. Build the fork's labels with [`ForkLabels`](ds:block?id=forklabels-479w9npr), which drops the membership labels (batch, parent, schedule, object key...) a fork must not inherit:

<!-- snippet: examples/custom-driver/helpers.go#reset-fork -->
```go
// ResetRun and ForkRun: which runs may be reset or forked, whether a
// fork redoes succeeded work, and the call's own checks, before the
// driver looks the run up.
fmt.Fprintln(w, "reset failed:", workflow.ResetStatusOK(workflow.StatusFailed, workflow.ResetOptions{}),
	"succeeded:", workflow.ResetStatusOK(workflow.StatusSucceeded, workflow.ResetOptions{}),
	"succeeded with Succeeded:", workflow.ResetStatusOK(workflow.StatusSucceeded, workflow.ResetOptions{Succeeded: true}))
fmt.Fprintln(w, "fork running:", workflow.ForkStatusOK(workflow.StatusRunning),
	"cancelled:", workflow.ForkStatusOK(workflow.StatusCancelled),
	"redo of succeeded:", workflow.ForkRedo(workflow.StatusSucceeded, nil))
fmt.Fprintln(w, "fork without a step:", workflow.CheckFork("", workflow.ForkOptions{}))
```

Chain edits (`workflow.ChainEditor`, from a running step). Make one [`NewChainEdits(caps, storedChain)`](ds:block?id=newchainedits-5sfrvcrs) per attempt and put it on the step's context with [`WithChainEditor`](ds:block?id=withchaineditor-7k4ssxr9); it checks each edit with `ValidateJob` and `CheckJob` against your capabilities and the [`MaxChainJobs`](ds:block?id=maxchainjobs-sg4br535) cap, and buffers it. Call [`Apply(storedChain)`](ds:block?id=chainedits-apply-chcyg7d7) and store the result only when the attempt succeeds, so a failed attempt that edited the chain leaves no trace:

<!-- snippet: examples/custom-driver/helpers.go#chain-edits -->
```go
// A step attempt edits its run's chain through a ChainEdits made from
// the driver's capabilities and the stored chain; the driver applies
// the edits only when the attempt succeeds.
stored := []workflow.Job{{Workflow: "email"}}
edits := workflow.NewChainEdits(tinyCaps, stored)
if err := edits.Append(workflow.Job{Workflow: "audit"}); err != nil {
	return err
}
refused := edits.Append(workflow.Job{Workflow: "later", RunAt: time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)})
var chain []string
for _, j := range edits.Apply(stored) {
	chain = append(chain, j.Workflow)
}
fmt.Fprintln(w, "chain after the attempt:", chain, "changed:", edits.Changed())
fmt.Fprintln(w, "delayed chain job:", refused)
```

Schedules. [`LastTick(src, from, to, loc)`](ds:block?id=lasttick-zzx4bpqn) is the newest tick in `(from, to]`, which a driver reports as `Tick.At` of a `TickMisfired` outcome; [`TriggeredLabelsAt(labels, id, name, at)`](ds:block?id=triggeredlabelsat-6b93e4q3) is the label set of a run `TriggerSchedule` starts on a driver with `CapTickTime` (the schedule labels, the tick time and `LabelTrigger`; [`TriggeredLabels`](ds:block?id=triggeredlabels-xgkux53f) without the tick time):

<!-- snippet: examples/custom-driver/helpers.go#ticks -->
```go
// Schedules: the tick a misfired fire stands for, and the labels of a
// run a manual trigger starts (CapTickTime adds the tick time).
hourly, err := cron.Parse("0 * * * *")
if err != nil {
	return err
}
down := time.Date(2026, 1, 5, 9, 10, 0, 0, time.UTC)
back := time.Date(2026, 1, 5, 12, 30, 0, 0, time.UTC)
fmt.Fprintln(w, "last missed tick:", workflow.LastTick(hourly, down, back, time.UTC).Format(time.TimeOnly))
labels := workflow.TriggeredLabelsAt(map[string]string{"team": "core"}, "sched-1", "hourly-sync", back)
fmt.Fprintln(w, "trigger labels:", len(labels), "tick at", labels[workflow.LabelTickAt], "trigger", labels[workflow.LabelTrigger])
```

Weighted round-robin concurrency (`ConcurrencyRoundRobin`, `CapKeyWeights`). [`FairWeight(concurrency, key)`](ds:block?id=fairweight-a77ybjht) is the key's weight, clamped to `1..MaxKeyWeight` ([`MaxKeyWeight`](ds:block?id=maxkeyweight-wvwmrvcb); 1 without a `Weight` function), and [`FairStride(weight)`](ds:block?id=fairstride-zq442xay) the virtual time one turn of that key costs; [`FairTurn(pass, now, weight)`](ds:block?id=fairturn-usczqc7a) combines them into the run's start tag, and the driver serves waiting runs in start tag order:

<!-- snippet: examples/custom-driver/helpers.go#fair -->
```go
// Round-robin concurrency: a key's weight, then the virtual time one
// of its turns costs. A heavier key's turns cost less, so it gets more.
conc := &workflow.Concurrency{Weight: workflow.KeyWeights(map[string]int{"enterprise": 4}, 1)} // enterprise weighs 4, every other key 1
for _, key := range []string{"enterprise", "free"} {
	weight := workflow.FairWeight(conc, key)
	fmt.Fprintf(w, "key %s weight %d stride %d\n", key, weight, workflow.FairStride(weight))
}
```

Bulk cancel and pauses. [`CancelStatuses(query)`](ds:block?id=cancelstatuses-8fnq9hyv) is what `CancelAll` cancels: the query's statuses, or, when it names none, only runs that have not started. [`RequiresPause(target)`](ds:block?id=requirespause-bpxg35v9) is the capability a pause target needs ([`CheckPause`](ds:block?id=checkpause-9fyvd7wv) calls it and refuses with an `UnsupportedError`):

<!-- snippet: examples/custom-driver/helpers.go#cancel-pause -->
```go
// CancelAll's default statuses (never a run that has started) and the
// capability a pause target needs.
fmt.Fprintln(w, "CancelAll cancels:", workflow.CancelStatuses(workflow.RunQuery{}))
fmt.Fprintln(w, "pause a queue needs:", workflow.RequiresPause(workflow.PauseQueue("bulk")),
	"a workflow needs:", workflow.RequiresPause(workflow.PauseWorkflow("report")))
```

Worker slots (`Step.Slots`, `CapSlotCost`). Call [`CheckSlots(wf, poolSize)`](ds:block?id=checkslots-3nwgkqa3) at `Register`, then take each attempt's [`StepSlots(step)`](ds:block?id=stepslots-am5mgsbq) from one [`NewSlotPool(size)`](ds:block?id=newslotpool-5td9h9hg) between claiming the attempt and running it, and `Release` them after. The pool serves waiters first come, first served, so a heavy step is not starved by a stream of light ones:

<!-- snippet: examples/custom-driver/helpers.go#slots -->
```go
// Step.Slots: an attempt takes its slots from the client's pool
// between claiming and running, and gives them back after. Waiters
// are served first come, first served.
pool := workflow.NewSlotPool(4)                  // the client runs at most 4 slots' worth of work at once
heavy := workflow.Step{Name: "render", Slots: 3} // a heavy step takes 3 of them
if err := pool.Acquire(ctx, workflow.StepSlots(heavy)); err != nil {
	return err
}
fmt.Fprintf(w, "slots in use %d of %d\n", pool.InUse(), pool.Size())
pool.Release(workflow.StepSlots(heavy))
fmt.Fprintln(w, "too many slots:", pool.Acquire(ctx, 5)) // more than the pool holds: refused, never waited for
```

Heartbeats and progress (`CapHeartbeat`, `CapProgress`). A handler calls [`workflow.Heartbeat`](ds:block?id=heartbeat-jrq6m57p) and [`workflow.ReportProgress`](ds:block?id=reportprogress-qtz35tuj); they reach your driver only through what you put on the attempt's context before the middleware chain runs. [`WithHeartbeater`](ds:block?id=withheartbeater-rku3vrw5) carries a `Heartbeater` (adapt a function with [`HeartbeatFunc`](ds:block?id=heartbeatfunc-45wm6xnt)), which resets the attempt's `HeartbeatTimeout` and, on a durable driver, renews its lease. [`WithProgressReporter`](ds:block?id=withprogressreporter-7zw9jr7b) carries a `ProgressReporter` (adapt a function with [`ProgressFunc`](ds:block?id=progressfunc-mgqzzz6x)), which fills `Step` and `At` on the driver's clock and stores the report as `RunInfo.Progress`:

<!-- snippet: examples/custom-driver/helpers.go#reporters -->
```go
// stepContext is what a driver with CapHeartbeat and CapProgress puts on
// each attempt's ctx before the middleware chain runs: a Heartbeater that
// renews the attempt's lease and a ProgressReporter that fills Step and
// At and stores the report with the run. Here both write to a log.
func stepContext(ctx context.Context, step string, now func() time.Time, log *[]string) context.Context {
	ctx = workflow.WithHeartbeater(ctx, workflow.HeartbeatFunc(func(_ context.Context, details []byte) error {
		*log = append(*log, fmt.Sprintf("heartbeat %s %s", step, details))
		return nil
	}))
	return workflow.WithProgressReporter(ctx, workflow.ProgressFunc(func(_ context.Context, p workflow.Progress) error {
		p.Step, p.At = step, now()
		*log = append(*log, fmt.Sprintf("progress %s %.0f%% %q at %s", p.Step, p.Percent, p.Message, p.At.Format(time.TimeOnly)))
		return nil
	}))
}
```

Without them [`Heartbeat`](ds:block?id=heartbeat-jrq6m57p) returns nil and does nothing, since liveness is only a hint, while [`ReportProgress`](ds:block?id=reportprogress-qtz35tuj) returns an `UnsupportedError`: the caller asked for data to be kept, and dropping it silently would be a lie.

Step timeouts (`CapRunTimeout`, `CapHeartbeat`). When an attempt runs past `Step.Timeout` (or the workflow's `Timeout`) or misses its `HeartbeatTimeout`, cancel its context with the exact error [`workflow.TimeoutError(kind, detail)`](ds:block?id=timeouterror-nqztem89) returns for [`ErrStepTimeout`](ds:block?id=errsteptimeout-6b727264) or [`ErrHeartbeatTimeout`](ds:block?id=errheartbeattimeout-t75kucja), not wrapped, so `context.Cause(ctx)` inside the handler matches the sentinel and `ErrTimeout`. Run the handler under `ctx, claimed := workflow.WithTimeoutClaim(ctx)` ([`WithTimeoutClaim`](ds:block?id=withtimeoutclaim-qeyxp3kw)). When the attempt's cause is your own timeout error (compare by identity, since a failing run may cancel its other steps with a `*RunError` that wraps a sibling's timeout), fail the attempt with that timeout whatever the handler returned, unless `claimed()` reports true: then step middleware called [`workflow.ClaimTimeout`](ds:block?id=claimtimeout-62xdkgef) and turned the timeout into its result (a step tolerated by `WithRules` records it as `FailureTimeout`), so keep the handler's result. `Tiny` keeps the handshake for `Step.Timeout` in its `attempt`, although it declares no capability, so its handlers see the same cause they would on a shipped driver:

<!-- snippet: examples/custom-driver/driver.go#timeout-handshake -->
```go
// attempt runs h once. When timeout passes on the injected clock it
// cancels h's ctx with a workflow.TimeoutError, not wrapped, so the
// handler's context.Cause matches workflow.ErrStepTimeout, and fails the
// attempt with it whatever h returned, unless the handler chain claimed
// it (workflow.ClaimTimeout: a step WithRules tolerates keeps its record).
func (t *Tiny) attempt(ctx context.Context, h workflow.Handler, in []byte, timeout time.Duration) ([]byte, error) {
	if timeout <= 0 {
		return h(ctx, in)
	}
	info, _ := workflow.StepInfoFrom(ctx)
	errStep := workflow.TimeoutError(workflow.ErrStepTimeout,
		fmt.Sprintf("tiny: step %q attempt %d after %s", info.Step, info.Attempt, timeout))
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	timer := t.clk.NewTimer(timeout)
	defer timer.Stop()
	go func() {
		select {
		case <-timer.C():
			cancel(errStep)
		case <-ctx.Done():
		}
	}()
	hctx, claimed := workflow.WithTimeoutClaim(ctx)
	out, err := h(hctx, in)
	// Compare by identity: any other cause (the client stopping) is not
	// this attempt's timeout. A late success is never a success.
	switch cause := context.Cause(ctx); {
	case cause == errStep && !claimed():
		return nil, errStep
	case cause != errStep && err == nil && ctx.Err() != nil:
		return nil, cause
	}
	return out, err
}
```

The conformance groups `attemptcause`, `toleratedtimeout` and `storedtimeout` ([`attemptCauseCases`](ds:block?id=workflowtest-attemptcausecases-mcq8ppjn), [`toleratedTimeoutCases`](ds:block?id=workflowtest-toleratedtimeoutcases-8wwwvbmn), [`storedTimeoutCases`](ds:block?id=workflowtest-storedtimeoutcases-b3qx5qe4)) check all of this, and the last one also checks that the run error still matches the sentinel after the run ended (a wait timeout's [`ErrWaitTimeout`](ds:block?id=errwaittimeout-vvzd9zgy) too). A driver without `CapRunTimeout` that keeps this handshake sets `StepTimeoutHandshake` on its [`workflowtest.Harness`](ds:block?id=workflowtest-harness-7uqe9srm), so the step-timeout cases of `attemptcause` and `toleratedtimeout` run on it; the Hatchet driver does, and so does the example's second suite run, limited to those two groups:

<!-- snippet: examples/custom-driver/main_test.go#handshake-suite -->
```go
// TestStepTimeoutHandshake runs the step-timeout cases on Tiny. Tiny
// claims no CapRunTimeout, so TestConformance skips them, but its attempts
// keep the handshake (attempt in driver.go): the harness says so, and the
// cases of attemptcause and toleratedtimeout that need it run.
func TestStepTimeoutHandshake(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	workflowtest.Run(t, workflowtest.Harness{
		StepTimeoutHandshake: true,
		Groups:               func(group string) bool { return group == "attemptcause" || group == "toleratedtimeout" },
		New: func(t *testing.T) workflowtest.Instance {
			clk := clock.NewFake(start)
			return workflowtest.Instance{Client: NewTiny(clk), Clock: clk}
		},
	})
}
```

## Run the conformance suite

[`workflowtest.Run(t, h)`](ds:block?id=workflowtest-run-2dxkjk9h) runs every assertion of the contract against your driver, each as a subtest on a fresh client. Subtests whose capabilities the driver lacks are skipped with the missing capability named, so one suite serves every driver honestly. A [`workflowtest.Harness`](ds:block?id=workflowtest-harness-7uqe9srm) needs only `New`, which returns a [`workflowtest.Instance`](ds:block?id=workflowtest-instance-hareevvd): a fresh, unstarted `Client`, and the `*clock.Fake` it reads (or nil for a driver on real time, such as an engine).

<!-- snippet: examples/custom-driver/main_test.go#conformance -->
```go
// TestConformance holds Tiny to the same suite as every shipped driver.
// Subtests that need a capability Tiny lacks are skipped, naming it; the
// rest must pass.
func TestConformance(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	workflowtest.Run(t, workflowtest.Harness{New: func(t *testing.T) workflowtest.Instance {
		clk := clock.NewFake(start)
		return workflowtest.Instance{Client: NewTiny(clk), Clock: clk}
	}})
}
```

The other [`Harness`](ds:block?id=workflowtest-harness-7uqe9srm) fields tune the suite for a real backend: `Unit`, `Wait`, `Quiet` and `Poll` scale its durations (defaults differ for a fake clock and for real time); `KeyFromLabel` and `WaitKeyFromLabel` supply keys in the engine's expression language; `RateLimitPer` sets a window the engine accepts; `RealCron` runs the cron cases on real time (each waits up to two minutes for a minute boundary); `Prefix` names the suite's workflows on a shared engine; `NewWithQueues` builds clients with a `QueueConfig` list for the queue-limit cases. A durable driver sets [`Instance.Restart`](ds:block?id=workflowtest-instance-hareevvd) (a new client over the same storage, after the old one's `Start` context is cancelled) and `Instance.Crash` (the same without a clean stop, as a killed process), which turn on the survives-a-restart and step-runs-again cases; `Restart` also drives the `shutdownrequeue` group, which checks that a step cut short by a clean stop runs again as the same attempt. A driver with an enqueue-only mode sets `Instance.EnqueueOnly` (an enqueue-only client over the same storage, never started) for the `enqueueonly` group. The groups for these capabilities are `overlap`, `trigger`, `schedhistory`, `enqueueonly`, `errorhistory`, `shutdownrequeue` and `force` (all in [`allGroups`](ds:block?id=workflowtest-allgroups-uk4d9m6p)), each skipped when the driver lacks the capability.

### Run the suite in shares

The suite runs its groups one after another in one test binary. To split it across several binaries, which `go test ./...` runs as parallel processes, set `Groups` on the [`Harness`](ds:block?id=workflowtest-harness-7uqe9srm) to a filter of group names and call `Run` from one small test package per share. [`workflowtest.GroupNames()`](ds:block?id=workflowtest-groupnames-7jhfskgz) lists every group in run order: check your share table against it, since a misspelt group name matches nothing and runs nothing. Give the last share every group the others do not name, so a group added to the suite later still runs. sqlstore does this for SQLite (`sqlstore/enginetest/shards/`): its SQLite engine is pure Go, and it takes process-wide locks on every allocation, so parallel subtests inside one binary waited on each other until lease renewals stalled, while separate processes keep each case as it always was.

The plan and its check, from [examples/driver-conformance](../../examples/driver-conformance/):

<!-- snippet: examples/driver-conformance/main.go#shares -->
```go
// shares names the groups of every share but the last. Put the slowest
// groups of your driver in shares of their own; the last share is every
// group none of these names, so it needs no list.
var shares = [][]string{
	{"lifecycle", "run", "retry", "timeout", "onfailure"},
	{"cron", "misfire", "schedule", "overlap", "trigger"},
}

// shareGroups is the Harness.Groups filter of share i (0-based): the
// groups shares[i] names, or, for the last share (i == len(shares)), every
// group no other share names.
func shareGroups(i int) func(group string) bool {
	if i < len(shares) {
		return func(g string) bool { return slices.Contains(shares[i], g) }
	}
	return func(g string) bool {
		for _, s := range shares {
			if slices.Contains(s, g) {
				return false
			}
		}
		return true
	}
}

// checkShares refuses a plan that names a group the suite does not have
// (a typo would silently run nothing) or names one group twice (it would
// run in two binaries).
func checkShares(suite []string) error {
	seen := map[string]bool{}
	for i, s := range shares {
		for _, g := range s {
			if !slices.Contains(suite, g) {
				return fmt.Errorf("share %d names %q, which is not a conformance group", i+1, g)
			}
			if seen[g] {
				return fmt.Errorf("share %d names %q again", i+1, g)
			}
			seen[g] = true
		}
	}
	return nil
}
```

Each share's package then runs `Run` with its filter; the example runs share 1 on the memory driver:

<!-- snippet: examples/driver-conformance/main_test.go#share -->
```go
// TestConformance runs share 1 of the plan on the memory driver. A driver
// that splits the suite has one small test package per share, each
// calling workflowtest.Run with shareGroups(i), so go test runs the
// shares as parallel processes. This example runs only the first share
// to keep the examples' test run short.
func TestConformance(t *testing.T) {
	workflowtest.Run(t, workflowtest.Harness{
		Groups: shareGroups(0),
		New: func(t *testing.T) workflowtest.Instance {
			clk := clock.NewFake(start)
			return workflowtest.Instance{Client: memory.New(memory.WithClock(clk)), Clock: clk}
		},
	})
}
```

### Prove the core helpers on your driver

[`Run`](ds:block?id=workflowtest-run-2dxkjk9h) covers the `Client` contract. The core's higher-level helpers (child runs and joins, signals and promises, virtual objects, reset, redo and fork, state machines, replay, worker slots) are built on top of a driver, and each has a case function of its own that proves the helper works there. Every one skips the cases that need a capability or operation your driver lacks, naming it, so call them all:

| Case function | Harness | What it proves | Needs |
|---|---|---|---|
| [`RunChildFlows`](ds:block?id=workflowtest-runchildflows-26d5egbe) | [`FlowHarness`](ds:block?id=workflowtest-flowharness-d56mnkx2) | `JoinChildren` (discriminator, cancelling and completing partial joins, a failed join), `CancelChildren` (running and scheduled children), `MapRuns` | `CapSnooze`, `CapIdempotencyKey`, `CapResult`, `CapCancel`, `CapCancelAll`, `CapForceRun`, `CapRunQuery`, `CapDelayedStart` |
| [`RunResetFlows`](ds:block?id=workflowtest-runresetflows-gdes24ha) | `FlowHarness` | `RunResetter`: a failed run re-runs from a step and keeps the outputs before it; an unknown step and a run that is not failed are refused | `RunResetter` |
| [`RunRedoFlows`](ds:block?id=workflowtest-runredoflows-su2chss2) | `FlowHarness` | `RunResetterWith`: a succeeded run reset with `Succeeded` keeps its `RunRef`, carries `LabelRedo` and does not hand its chain on again | the `OpResetSucceeded` operation |
| [`RunForkFlows`](ds:block?id=workflowtest-runforkflows-jm77mfu2) | `FlowHarness` | `RunForker`: a fork starts from the step under a new `RunID` with the outputs before it, gets `LabelForkOf`, `LabelForkStep` (and `LabelRedo` from a succeeded run), and every refusal | the `OpForkRun` operation |
| [`RunSignalFlows`](ds:block?id=workflowtest-runsignalflows-x6z59d2p) | [`SignalHarness`](ds:block?id=workflowtest-signalharness-59st97g3) | broadcast to several waiting runs, `SignalWithStart`, `Update` with `UpdateHandler`, a promise resolved from outside, `ContinueAsNew`, `CancelSteps` | per case |
| [`RunPromiseStore`](ds:block?id=workflowtest-runpromisestore-tgdywy8m) | a `PromiseStore` | the store contract: the first `Settle` wins for every later or concurrent one, `Get` and `Delete`, ids independent | a store of your own |
| [`RunReplayFlows`](ds:block?id=workflowtest-runreplayflows-yjyr3ayu) | [`ReplayHarness`](ds:block?id=workflowtest-replayharness-mqdacwgd) | `NewReplay`: locals kept across a sleep and a promise wait, each recorded call run once, a diverging history fails the run | `CapSnooze` |
| [`RunObjectFlows`](ds:block?id=workflowtest-runobjectflows-86ussnuu) | [`ObjectHarness`](ds:block?id=workflowtest-objectharness-hkns6ws6) | `DefineObject` and `CallObject`: one call per key at a time, each applied once, keys independent, an unknown method fails its call | `CapConcurrencyKey` |
| [`RunMachineFlows`](ds:block?id=workflowtest-runmachineflows-6a9ymgxa) | [`MachineHarness`](ds:block?id=workflowtest-machineharness-aghhe7m6) | `DefineMachine`: guarded transitions and actions in order, hierarchy and parallel states, history, timers per state, events of one key one at a time | `CapConcurrencyKey` with Go key functions; `CapDelayedStart` and `CapIdempotencyKey` for timers |
| [`RunSlotFlows`](ds:block?id=workflowtest-runslotflows-bndr8cke) | [`SlotHarness`](ds:block?id=workflowtest-slotharness-zawhxbmb) | `Step.Slots`: running attempts never hold more than the pool, a heavy step still runs, a step bigger than the pool is refused at `Register` | `CapSlotCost` |

[`RunEscalationFlows`](ds:block?id=workflowtest-runescalationflows-nfcxu96e) with an `EscalationHarness` does the same for signals handled between steps ([escalation](../workflows/escalation.md)).

[`FlowHarness.Start`](ds:block?id=workflowtest-flowharness-d56mnkx2), [`SignalHarness.Start`](ds:block?id=workflowtest-signalharness-59st97g3) and [`ReplayHarness.Start`](ds:block?id=workflowtest-replayharness-mqdacwgd) build a fresh started client with the workflows registered; it should be your driver behind `workflow.Use` with the `Journal` plugin over the driver's durable `JournalStore`, where it has one. `Promises` returns the `PromiseStore` the run and the caller share:

<!-- snippet: examples/driver-conformance/main_test.go#start -->
```go
// startUsed is the Start of FlowHarness, ReplayHarness and SignalHarness:
// a fresh client of the driver behind workflow.Use with the Journal
// plugin (over the driver's durable JournalStore where it has one), the
// workflows registered, started, and stopped when the test ends.
func startUsed(t *testing.T, wfs ...workflow.Workflow) workflow.Client {
	c, err := workflow.Use(memory.New(), workflow.Journal(workflow.NewMemoryJournal()))
	if err != nil {
		t.Fatal(err)
	}
	registerAndStart(t, c, wfs...)
	return c
}

// registerAndStart registers wfs on c and starts it until the test ends.
func registerAndStart(t *testing.T, c workflow.Client, wfs ...workflow.Workflow) {
	for _, wf := range wfs {
		if err := c.Register(wf); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
}

// promises is the PromiseStore the run and the caller share; a durable
// driver returns its own store here.
func promises(*testing.T, workflow.Client) workflow.PromiseStore {
	return workflow.NewMemoryPromises()
}
```

[`ObjectHarness`](ds:block?id=workflowtest-objectharness-hkns6ws6) and [`MachineHarness`](ds:block?id=workflowtest-machineharness-aghhe7m6) take the `JournalStore` the state lives in instead (`Store`), and `MachineHarness.Client` returns the client before `Start`, so a machine with timers can be defined against it. [`SlotHarness.Start`](ds:block?id=workflowtest-slotharness-zawhxbmb) builds a client with a pool of the size the case asks for and returns `Register`'s error, since one case expects a refusal:

<!-- snippet: examples/driver-conformance/main_test.go#flows -->
```go
// TestCoreHelperFlows proves the core's helpers on the driver: children
// and joins, signals and promises, virtual objects, reset, redo and fork,
// state machines and replay. Each case skips what the driver cannot do.
func TestCoreHelperFlows(t *testing.T) {
	flow := workflowtest.FlowHarness{Start: startUsed}
	t.Run("child", func(t *testing.T) { workflowtest.RunChildFlows(t, flow) })
	t.Run("reset", func(t *testing.T) { workflowtest.RunResetFlows(t, flow) })
	t.Run("redo", func(t *testing.T) { workflowtest.RunRedoFlows(t, flow) })
	t.Run("fork", func(t *testing.T) { workflowtest.RunForkFlows(t, flow) })
	t.Run("signal", func(t *testing.T) {
		workflowtest.RunSignalFlows(t, workflowtest.SignalHarness{Start: startUsed, Promises: promises})
	})
	t.Run("promisestore", func(t *testing.T) { workflowtest.RunPromiseStore(t, workflow.NewMemoryPromises()) })
	t.Run("replay", func(t *testing.T) {
		workflowtest.RunReplayFlows(t, workflowtest.ReplayHarness{Start: startUsed, Promises: promises})
	})
	t.Run("object", func(t *testing.T) {
		workflowtest.RunObjectFlows(t, workflowtest.ObjectHarness{
			// Objects keep their state in Store, so the client needs no
			// Journal plugin.
			Start: func(t *testing.T, wfs ...workflow.Workflow) workflow.Client {
				c := memory.New()
				registerAndStart(t, c, wfs...)
				return c
			},
			Store: func(*testing.T) workflow.JournalStore { return workflow.NewMemoryJournal() },
		})
	})
	t.Run("machine", func(t *testing.T) {
		workflowtest.RunMachineFlows(t, workflowtest.MachineHarness{
			// Client comes before Start, so a machine with timers can be
			// defined against the client Start will start.
			Client: func(*testing.T) workflow.Client { return memory.New() },
			Start:  registerAndStart,
			Store:  func(*testing.T, workflow.Client) workflow.JournalStore { return workflow.NewMemoryJournal() },
		})
	})
	t.Run("slot", func(t *testing.T) {
		workflowtest.RunSlotFlows(t, workflowtest.SlotHarness{
			// workers is the pool size the case asks for; a Register
			// refusal is returned, not fatal, since one case expects it.
			Start: func(t *testing.T, workers int, wfs ...workflow.Workflow) (workflow.Client, error) {
				c := memory.New(memory.WithWorkers(workers))
				for _, wf := range wfs {
					if err := c.Register(wf); err != nil {
						return nil, err
					}
				}
				ctx, cancel := context.WithCancel(context.Background())
				t.Cleanup(cancel)
				return c, c.Start(ctx)
			},
		})
	})
}
```

## Try it

```sh
cd examples
go run ./custom-driver
```

The first four lines are `Tiny` running a workflow and refusing a delayed start; the rest is `helpers.go` printing what each shared helper decides:

```text
render got "42 rows"
tiny-1: succeeded
delayed start refused: true
workflow: Job.RunAt unsupported by this driver (missing DelayedStart): job for workflow report
reset failed: true succeeded: false succeeded with Succeeded: true
fork running: false cancelled: true redo of succeeded: true
fork without a step: workflow: invalid spec: ForkRun needs a step
chain after the attempt: [email audit] changed: true
delayed chain job: workflow: Job.RunAt unsupported by this driver (missing DelayedStart): job for workflow later
last missed tick: 12:00:00
trigger labels: 5 tick at 2026-01-05T12:30:00Z trigger manual
key enterprise weight 4 stride 262144
key free weight 1 stride 1048576
CancelAll cancels: [scheduled queued]
pause a queue needs: PauseQueue a workflow needs: PauseWorkflow
slots in use 3 of 4
too many slots: workflow: invalid spec: slot pool: 5 slots asked of 4
heartbeat export row 4000
progress export 40% "4,000 of 10,000 rows" at 09:00:00
without a reporter: workflow: ReportProgress unsupported by this driver: the driver keeps no progress, or ctx is not a step's
```

```sh
go test ./custom-driver -run TestConformance -v
```

45 cases pass (lifecycle, runs and DAG inputs, retries and `StepInfo`, timeouts, `OnFailure`, job retry policies, chains, closures, tenants, versions and the refusal cases) and 248 are skipped, each naming why, across the 76 groups of [`allGroups`](ds:block?id=workflowtest-allgroups-uk4d9m6p) ([`p5Groups`](ds:block?id=workflowtest-p5groups-svs5kb3f), [`p3Groups`](ds:block?id=workflowtest-p3groups-hzv36c8h), [`attemptErrorGroups`](ds:block?id=workflowtest-attempterrorgroups-pckw5r63), `gapGroups`):

```text
    cases_basic.go:164: driver lacks IdempotencyKey
    cases_flow.go:15: driver lacks Cancel
    cases_hook.go:136: driver does not implement workflow.RunNotifier
```

The second suite run, with the handshake declared:

```sh
go test ./custom-driver -run TestStepTimeoutHandshake -v | grep -E '^(--- |    --- |ok)'
```

```text
--- PASS: TestStepTimeoutHandshake (0.01s)
    --- PASS: TestStepTimeoutHandshake/attemptcause (0.00s)
    --- PASS: TestStepTimeoutHandshake/toleratedtimeout (0.01s)
ok  	github.com/ubgo/workflow/examples/custom-driver	0.017s
```

The share plan and the core-helper cases on the memory driver:

```sh
go run ./driver-conformance
```

```text
share 1: lifecycle run retry timeout onfailure
share 2: cron misfire schedule overlap trigger
share 3: every other group (66 of 76)
```

The last share's count follows the suite: 76 groups in [`workflowtest.GroupNames()`](ds:block?id=workflowtest-allgroups-uk4d9m6p) today, 10 of them named by the first two shares.

```sh
go test ./driver-conformance -run 'TestConformance|TestCoreHelperFlows' -v | grep -E '^(--- |    --- |ok)'
```

```text
--- PASS: TestConformance (0.43s)
    --- PASS: TestConformance/lifecycle (0.00s)
    --- PASS: TestConformance/run (0.11s)
    --- PASS: TestConformance/retry (0.21s)
    --- PASS: TestConformance/timeout (0.01s)
    --- PASS: TestConformance/onfailure (0.11s)
--- PASS: TestCoreHelperFlows (2.81s)
    --- PASS: TestCoreHelperFlows/child (0.07s)
    --- PASS: TestCoreHelperFlows/reset (0.01s)
    --- PASS: TestCoreHelperFlows/redo (0.17s)
    --- PASS: TestCoreHelperFlows/fork (0.07s)
    --- PASS: TestCoreHelperFlows/signal (0.13s)
    --- PASS: TestCoreHelperFlows/promisestore (0.00s)
    --- PASS: TestCoreHelperFlows/replay (0.62s)
    --- PASS: TestCoreHelperFlows/object (0.01s)
    --- PASS: TestCoreHelperFlows/machine (1.60s)
    --- PASS: TestCoreHelperFlows/slot (0.13s)
ok  	github.com/ubgo/workflow/examples/driver-conformance	3.249s
```

## Pitfalls

- Declaring a capability you emulate badly is the failure this library exists to prevent. A fake that honoured idempotency keys while the real driver dropped them kept every test green in a production app; the suite catches that only if the capability is declared and the suite runs against the real backend.
- `Capabilities` must not change after construction; the suite checks it across `Start` ([`lifecycleCases`](ds:block?id=workflowtest-lifecyclecases-24esa2ph)).
- Refuse in `Register` or `Start`, not on the first run: a refusal at start-up fails the deploy, a refusal at run time fails a customer's job.
- [`Enqueue`](ds:block?id=client-enqueue-mafr75se) with an idempotency key held by an unfinished run returns that run's `RunRef` and a nil error, not `ErrDuplicate`. The suite accepts either, but apps expect the first.
- A skipped subtest is not a passed one. Read the skip lines: each is a capability you have not declared, and therefore a field your driver refuses.
- `CapConcurrencyLimits` is not "check each key": a run must take all of its buckets in one atomic step, and an admission pass must keep later runs off every key an earlier waiting run needs ([`ConcurrencyLimit`](ds:block?id=concurrencylimit-5anrp8rw)). Checking keys one by one, or admitting runs that skip past a waiting one on a shared key, passes simple tests and starves a run that needs two keys; the `concurrencylimits` conformance group checks both. A driver that cannot do it leaves the capability out, and `CheckCaps` refuses the workflow.
- `CapHoldWhileRunning` is not "decrement the count during a backoff": a step of a parked run that becomes ready must not execute until its run has taken every `HoldWhileRunning` slot back, atomically and at its original place in admission order, and a run that ends while parked must not give those slots back a second time ([`ConcurrencyHold`](ds:block?id=concurrencyhold-k3v8qw2m)). A durable driver must also make the park and a concurrent claim of the run's step exclusive, so two workers claiming at once, or a worker dying while the run parks or rejoins, can never put more than `MaxRuns` runs of a key in execution (the SQL driver serialises both on one row, [`park`](ds:block?id=sql-client-park-x2n6dq8v)). The `concurrencyhold` conformance group ([`concurrencyHoldCases`](ds:block?id=workflowtest-concurrencyholdcases-t8w3vq5n)) checks each of these, with a crash while parked and a lease reclaim right after the rejoin.
- `CapCoalesce` is not a second queue: the run kept pending behind a held key must be admitted like any run once its holder ends (its concurrency keys, every `ConcurrencyLimits` key and its rate limits), and under `CoalesceKeepLast` its keys must come from the payload it runs with, not from the enqueue that stored it. The `coalesce` group checks both.
- `CapCheckpoint` state belongs to the run, not to a slot: a run parked under `HoldWhileRunning` keeps its checkpoints and its next attempt reads them; drop them only when the run ends. A `TxCompleter` completion that ends the run must give back every concurrency slot the run holds, in the same unit of work.
- Never read `time.Now` in driver logic; the suite on a fake clock will hang or fail the timing cases.
- Apply a step's chain edits only after the attempt succeeds. A retried attempt makes a new `ChainEdits` ([`NewChainEdits`](ds:block?id=newchainedits-5sfrvcrs)); applying the edits of a failed attempt would add its jobs once per try.
- A share filter that names a group the suite does not have runs nothing for that name and still passes. Check the table against [`workflowtest.GroupNames()`](ds:block?id=workflowtest-groupnames-7jhfskgz), and let the last share take every group the others do not name.
- The core-helper cases are not part of [`workflowtest.Run`](ds:block?id=workflowtest-run-2dxkjk9h). A driver that runs only `Run` has not proved joins, objects, machines, replay, reset or fork on its backend; call every `Run*Flows` function too.
- [`Heartbeat`](ds:block?id=heartbeat-jrq6m57p) without a `Heartbeater` on the context is a silent no-op, so a driver that claims `CapHeartbeat` but forgets `WithHeartbeater` passes a handler's own tests while its long steps time out. The `heartbeat` and `progress` conformance groups ([`heartbeatCases`](ds:block?id=workflowtest-heartbeatcases-gas246d5), [`progressCases`](ds:block?id=workflowtest-progresscases-4vv5huqs)) catch it.

## Related

- [Capabilities](../concepts/capabilities.md#what-a-driver-declares) and [refusals](../concepts/capabilities.md#refusals)
- [Write step middleware](writing-middleware.md#write-step-middleware)
- [Bring your own store](custom-stores.md#bring-your-own-store)
- The shipped drivers as larger references: [memory](../drivers/memory.md#create-the-client), [SQL](../drivers/sqlstore.md#create-the-client), [Hatchet](../drivers/hatchet.md#create-the-client)
- Example: [custom-driver](../../examples/custom-driver/); the shares and core-helper cases: [driver-conformance](../../examples/driver-conformance/)
- [Test app code with the Fake](../testing/fake.md#forks-and-batches): the Fake follows the same reset and fork rules
