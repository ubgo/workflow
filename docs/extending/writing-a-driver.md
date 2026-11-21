# Write a driver

A driver is any type that implements `workflow.Client`: a queue in Redis, a cloud service, an in-house engine, or a narrower in-process runner. This guide builds `Tiny`, a complete driver of under 300 lines that runs steps as a DAG with retries, timeouts and failure handlers, declares no capability beyond the core contract and refuses everything else, then holds it to the conformance suite every shipped driver runs. The core gives a driver the shared rules as functions (validation, capability checks, step inputs, retry delays, misfire maths), so two drivers cannot disagree on what a spec means.

## Before you start

- You know the [client](../concepts/client.md#the-client-interface) contract and [capabilities](../concepts/capabilities.md#what-a-driver-declares).
- A driver lives in its own package, ideally its own module if it needs a third-party SDK, and imports only `github.com/ubgo/workflow` and its subpackages from this repository.

## What a driver implements

### Step 1 — Declare what it can really do

A capability is a promise the conformance suite will test against your real backend. Declare only what you have proven; the empty set is honest and still runs every plain workflow.

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

No package-level state: everything hangs off the value the app constructs, so two clients in one process never share anything. Every timer comes from an injected `clock.Clock` ([Time](../concepts/time.md#the-clock-interface)), and the constructor opens nothing.

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

`Register` calls `workflow.Validate`, the one shape check, and refuses after `Start`. `Start` calls `workflow.CheckCaps` for every registered workflow, so a workflow that needs a capability you lack fails at start-up with an `UnsupportedError` naming it. The `Start` context is the client's lifetime: cancelling it stops everything. A driver that must refuse a field its backend cannot express (an expression language it does not have, a window size it cannot do) returns `workflow.Unsupported(field, reason)` from `Register` or `Start` too.

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

`workflow.ValidateJob` checks the job's shape, and `workflow.CheckJob` compares what it asks for (`workflow.RequiresJob`: a delayed start, an idempotency key, a queue, a per-job retry policy) with the driver's capabilities. A driver without one returns an `UnsupportedError` naming the field; it never drops the field and runs the job anyway, which is the bug that makes a fake pass while production misbehaves. `workflow.CheckJobLabels` refuses a label under a reserved prefix that the caller's context does not allow; every driver calls it in its public `Enqueue`, and the conformance group `reservedlabels` fails a driver that does not.

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

`Cancel` without `CapCancel` and `Schedule` without `CapCron` return an `UnsupportedError`; `workflow.CheckScheduleCaps` builds the schedule one, naming `Schedule`, `Schedule.Location` or `Schedule.Misfire`. `Unschedule` of an empty ID is a no-op. A `Signal` nobody waits for is not an error.

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

A step starts once all its `After` parents have succeeded, and its input is `workflow.StepInput(payload, parents, outputs)`: the payload, one parent's output, or an object keyed by parent name. When a step fails for good, `OnFailure` runs once with that step's input and the run fails.

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

Each attempt runs with `workflow.WithStepInfo` on its context (so middleware and handlers know the workflow, step, run, attempt and labels), under the step's timeout or the workflow's. Retries follow `workflow.EffectiveRetries(step, wf)` (the step's policy, else the workflow's), `RetryPolicy.Retryable` (not permanent, not cancelled, not excluded by `NoRetryOn`, within `Max`) and `RetryPolicy.NextDelay` (the `DelayFor` answer, else the backoff with its jitter). The loop is in [Time](../concepts/time.md#the-clock-interface), where it serves as the clock example.

The rest of the shared rules, for the capabilities `Tiny` does not have:

| Capability you add | Core helper to call |
|---|---|
| `CapWaitForEvent` | `WaitSpec.ResolveKey(job)` for a run's wait key; keep the `Signal` contract in the `Client` doc (an event sent after the run's enqueue is kept, one run never takes one event twice, one event resumes every matching run) |
| `CapCron`, misfire policies, `CapCronZone` | `cron.Parse` (or a `cron.Parser` option), `Misfire.Due(src, last, now, loc)` for which missed ticks fire, `workflow.IsUTC` |
| `CapRetryJitter`, `CapRetryDelay` | `RetryPolicy.NextDelay`, with a jitter source the app can pin in tests |
| `CapRunFinished` | implement `workflow.RunNotifier`, keeping its delivery contract (before `Start` only, after `Status` shows the end, panics recovered, at least once, every terminal run) |
| batches as one native call | implement `batch.GroupRunner` |

Mark errors from an unreachable backend with `workflow.Unavailable(err)`, so a failover client can tell an outage from a refusal ([Backend outages](../concepts/errors.md#backend-outages)). Accept step middleware with a `WithMiddleware` option and apply it around every step and `OnFailure`, as the shipped drivers do.

### Step 7 — Call the shared helper for each capability you add

Every optional operation and capability has a helper in the core that decides it the way every shipped driver does. Call it at the same point a shipped driver does, rather than writing the rule again: the conformance cases are written against these answers. [examples/custom-driver](../../examples/custom-driver/) calls each one in `helpers.go` and prints what it decides; `Tiny` itself needs none of them, because it declares none of these capabilities.

Resets and forks (`workflow.RunResetter`, `RunResetterWith`, `RunForker`). `ResetStatusOK(status, opts)` says whether a run may be reset (failed always, succeeded only with `ResetOptions.Succeeded`), `ForkStatusOK(status)` whether it may be forked (it has finished), `ForkRedo(status, labels)` whether the fork redoes work that already succeeded (so it gets `LabelRedo`), and `CheckFork(step, opts)` refuses an empty step or a reserved label before the driver looks the run up. Build the fork's labels with `ForkLabels`, which drops the membership labels (batch, parent, schedule, object key...) a fork must not inherit:

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

Chain edits (`workflow.ChainEditor`, from a running step). Make one `NewChainEdits(caps, storedChain)` per attempt and put it on the step's context with `WithChainEditor`; it checks each edit with `ValidateJob` and `CheckJob` against your capabilities and the `MaxChainJobs` cap, and buffers it. Call `Apply(storedChain)` and store the result only when the attempt succeeds, so a failed attempt that edited the chain leaves no trace:

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

Schedules. `LastTick(src, from, to, loc)` is the newest tick in `(from, to]`, which a driver reports as `Tick.At` of a `TickMisfired` outcome; `TriggeredLabelsAt(labels, id, name, at)` is the label set of a run `TriggerSchedule` starts on a driver with `CapTickTime` (the schedule labels, the tick time and `LabelTrigger`; `TriggeredLabels` without the tick time):

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

Weighted round-robin concurrency (`ConcurrencyRoundRobin`, `CapKeyWeights`). `FairWeight(concurrency, key)` is the key's weight, clamped to `1..MaxKeyWeight` (1 without a `Weight` function), and `FairStride(weight)` the virtual time one turn of that key costs; `FairTurn(pass, now, weight)` combines them into the run's start tag, and the driver serves waiting runs in start tag order:

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

Bulk cancel and pauses. `CancelStatuses(query)` is what `CancelAll` cancels: the query's statuses, or, when it names none, only runs that have not started. `RequiresPause(target)` is the capability a pause target needs (`CheckPause` calls it and refuses with an `UnsupportedError`):

<!-- snippet: examples/custom-driver/helpers.go#cancel-pause -->
```go
// CancelAll's default statuses (never a run that has started) and the
// capability a pause target needs.
fmt.Fprintln(w, "CancelAll cancels:", workflow.CancelStatuses(workflow.RunQuery{}))
fmt.Fprintln(w, "pause a queue needs:", workflow.RequiresPause(workflow.PauseQueue("bulk")),
	"a workflow needs:", workflow.RequiresPause(workflow.PauseWorkflow("report")))
```

Worker slots (`Step.Slots`, `CapSlotCost`). Call `CheckSlots(wf, poolSize)` at `Register`, then take each attempt's `StepSlots(step)` from one `NewSlotPool(size)` between claiming the attempt and running it, and `Release` them after. The pool serves waiters first come, first served, so a heavy step is not starved by a stream of light ones:

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

Heartbeats and progress (`CapHeartbeat`, `CapProgress`). A handler calls `workflow.Heartbeat` and `workflow.ReportProgress`; they reach your driver only through what you put on the attempt's context before the middleware chain runs. `WithHeartbeater` carries a `Heartbeater` (adapt a function with `HeartbeatFunc`), which resets the attempt's `HeartbeatTimeout` and, on a durable driver, renews its lease. `WithProgressReporter` carries a `ProgressReporter` (adapt a function with `ProgressFunc`), which fills `Step` and `At` on the driver's clock and stores the report as `RunInfo.Progress`:

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

Without them `Heartbeat` returns nil and does nothing, since liveness is only a hint, while `ReportProgress` returns an `UnsupportedError`: the caller asked for data to be kept, and dropping it silently would be a lie.

## Run the conformance suite

`workflowtest.Run(t, h)` runs every assertion of the contract against your driver, each as a subtest on a fresh client. Subtests whose capabilities the driver lacks are skipped with the missing capability named, so one suite serves every driver honestly. A `workflowtest.Harness` needs only `New`, which returns a `workflowtest.Instance`: a fresh, unstarted `Client`, and the `*clock.Fake` it reads (or nil for a driver on real time, such as an engine).

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

The other `Harness` fields tune the suite for a real backend: `Unit`, `Wait`, `Quiet` and `Poll` scale its durations (defaults differ for a fake clock and for real time); `KeyFromLabel` and `WaitKeyFromLabel` supply keys in the engine's expression language; `RateLimitPer` sets a window the engine accepts; `RealCron` runs the cron cases on real time (each waits up to two minutes for a minute boundary); `Prefix` names the suite's workflows on a shared engine. A durable driver sets `Instance.Restart` (a new client over the same storage, after the old one's `Start` context is cancelled) and `Instance.Crash` (the same without a clean stop, as a killed process), which turn on the survives-a-restart and step-runs-again cases; `Restart` also drives the `shutdownrequeue` group, which checks that a step cut short by a clean stop runs again as the same attempt. A driver with an enqueue-only mode sets `Instance.EnqueueOnly` (an enqueue-only client over the same storage, never started) for the `enqueueonly` group. The groups for this batch's capabilities are `overlap`, `trigger`, `schedhistory`, `enqueueonly`, `errorhistory`, `shutdownrequeue` and `force`, each skipped when the driver lacks the capability.

### Run the suite in shares

The suite runs its groups one after another in one test binary. To split it across several binaries, which `go test ./...` runs as parallel processes, set `Harness.Groups` to a filter of group names and call `Run` from one small test package per share. `workflowtest.GroupNames()` lists every group in run order: check your share table against it, since a misspelt group name matches nothing and runs nothing. Give the last share every group the others do not name, so a group added to the suite later still runs. sqlstore does this for SQLite (`sqlstore/enginetest/shards/`): its SQLite engine is pure Go, and it takes process-wide locks on every allocation, so parallel subtests inside one binary waited on each other until lease renewals stalled, while separate processes keep each case as it always was.

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

`Run` covers the `Client` contract. The core's higher-level helpers (child runs and joins, signals and promises, virtual objects, reset, redo and fork, state machines, replay, worker slots) are built on top of a driver, and each has a case function of its own that proves the helper works there. Every one skips the cases that need a capability or operation your driver lacks, naming it, so call them all:

| Case function | Harness | What it proves | Needs |
|---|---|---|---|
| `RunChildFlows` | `FlowHarness` | `JoinChildren` (discriminator, cancelling and completing partial joins, a failed join), `CancelChildren`, `MapRuns` | `CapSnooze`, `CapIdempotencyKey`, `CapResult`, `CapCancel`, `CapCancelAll`, `CapForceRun`, `CapRunQuery` |
| `RunResetFlows` | `FlowHarness` | `RunResetter`: a failed run re-runs from a step and keeps the outputs before it; an unknown step and a run that is not failed are refused | `RunResetter` |
| `RunRedoFlows` | `FlowHarness` | `RunResetterWith`: a succeeded run reset with `Succeeded` keeps its `RunRef`, carries `LabelRedo` and does not hand its chain on again | the `OpResetSucceeded` operation |
| `RunForkFlows` | `FlowHarness` | `RunForker`: a fork starts from the step under a new `RunID` with the outputs before it, gets `LabelForkOf`, `LabelForkStep` (and `LabelRedo` from a succeeded run), and every refusal | the `OpForkRun` operation |
| `RunSignalFlows` | `SignalHarness` | broadcast to several waiting runs, `SignalWithStart`, `Update` with `UpdateHandler`, a promise resolved from outside, `ContinueAsNew`, `CancelSteps` | per case |
| `RunPromiseStore` | a `PromiseStore` | the store contract: the first `Settle` wins for every later or concurrent one, `Get` and `Delete`, ids independent | a store of your own |
| `RunReplayFlows` | `ReplayHarness` | `NewReplay`: locals kept across a sleep and a promise wait, each recorded call run once, a diverging history fails the run | `CapSnooze` |
| `RunObjectFlows` | `ObjectHarness` | `DefineObject` and `CallObject`: one call per key at a time, each applied once, keys independent, an unknown method fails its call | `CapConcurrencyKey` |
| `RunMachineFlows` | `MachineHarness` | `DefineMachine`: guarded transitions and actions in order, hierarchy and parallel states, history, timers per state, events of one key one at a time | `CapConcurrencyKey` with Go key functions; `CapDelayedStart` and `CapIdempotencyKey` for timers |
| `RunSlotFlows` | `SlotHarness` | `Step.Slots`: running attempts never hold more than the pool, a heavy step still runs, a step bigger than the pool is refused at `Register` | `CapSlotCost` |

`RunEscalationFlows` with an `EscalationHarness` does the same for signals handled between steps ([escalation](../workflows/escalation.md)).

`FlowHarness.Start`, `SignalHarness.Start` and `ReplayHarness.Start` build a fresh started client with the workflows registered; it should be your driver behind `workflow.Use` with the `Journal` plugin over the driver's durable `JournalStore`, where it has one. `Promises` returns the `PromiseStore` the run and the caller share:

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

`ObjectHarness` and `MachineHarness` take the `JournalStore` the state lives in instead (`Store`), and `MachineHarness.Client` returns the client before `Start`, so a machine with timers can be defined against it. `SlotHarness.Start` builds a client with a pool of the size the case asks for and returns `Register`'s error, since one case expects a refusal:

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

43 cases pass (lifecycle, runs and DAG inputs, retries and `StepInfo`, timeouts, `OnFailure`, job retry policies, chains, closures, tenants, versions and the refusal cases) and 188 are skipped, each naming why:

```text
    cases_basic.go:164: driver lacks IdempotencyKey
    cases_flow.go:14: driver lacks Cancel
    cases_hook.go:135: driver does not implement workflow.RunNotifier
```

The share plan and the core-helper cases on the memory driver:

```sh
go run ./driver-conformance
```

```text
share 1: lifecycle run retry timeout onfailure
share 2: cron misfire schedule overlap trigger
share 3: every other group (57 of 67)
```

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
- `Capabilities` must not change after construction; the suite checks it across `Start`.
- Refuse in `Register` or `Start`, not on the first run: a refusal at start-up fails the deploy, a refusal at run time fails a customer's job.
- `Enqueue` with an idempotency key held by an unfinished run returns that run's `RunRef` and a nil error, not `ErrDuplicate`. The suite accepts either, but apps expect the first.
- A skipped subtest is not a passed one. Read the skip lines: each is a capability you have not declared, and therefore a field your driver refuses.
- Never read `time.Now` in driver logic; the suite on a fake clock will hang or fail the timing cases.
- Apply a step's chain edits only after the attempt succeeds. A retried attempt makes a new `ChainEdits`; applying the edits of a failed attempt would add its jobs once per try.
- A share filter that names a group the suite does not have runs nothing for that name and still passes. Check the table against `workflowtest.GroupNames()`, and let the last share take every group the others do not name.
- The core-helper cases are not part of `workflowtest.Run`. A driver that runs only `Run` has not proved joins, objects, machines, replay, reset or fork on its backend; call every `Run*Flows` function too.
- `Heartbeat` without a `Heartbeater` on the context is a silent no-op, so a driver that claims `CapHeartbeat` but forgets `WithHeartbeater` passes a handler's own tests while its long steps time out. The `heartbeat` and `progress` conformance groups catch it.

## Related

- [Capabilities](../concepts/capabilities.md#what-a-driver-declares) and [refusals](../concepts/capabilities.md#refusals)
- [Write step middleware](writing-middleware.md#write-step-middleware)
- [Bring your own store](custom-stores.md#bring-your-own-store)
- The shipped drivers as larger references: [memory](../drivers/memory.md#create-the-client), [SQL](../drivers/sqlstore.md#create-the-client), [Hatchet](../drivers/hatchet.md#create-the-client)
- Example: [custom-driver](../../examples/custom-driver/); the shares and core-helper cases: [driver-conformance](../../examples/driver-conformance/)
- [Test app code with the Fake](../testing/fake.md#forks-and-batches): the Fake follows the same reset and fork rules
