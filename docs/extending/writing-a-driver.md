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
const tinyCaps workflow.Caps = 0
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

func (t *Tiny) Schedule(_ context.Context, s workflow.Schedule) (workflow.ScheduleID, error) {
	if err := workflow.ValidateSchedule(s); err != nil {
		return "", err
	}
	// Without CapCron this always refuses, before or after Start.
	return "", workflow.CheckScheduleCaps(tinyCaps, s)
}

func (t *Tiny) Unschedule(_ context.Context, id workflow.ScheduleID) error {
	if id.IsZero() {
		return nil
	}
	return fmt.Errorf("%w: schedule %q", workflow.ErrNotFound, id)
}

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

## Try it

```sh
cd examples
go run ./custom-driver
```

```text
render got "42 rows"
tiny-1: succeeded
delayed start refused: true
workflow: Job.RunAt unsupported by this driver (missing DelayedStart): job for workflow report
```

```sh
go test ./custom-driver -run TestConformance -v
```

20 subtests pass (lifecycle, runs and DAG inputs, retries and `StepInfo`, timeouts, `OnFailure`, and the three refusal cases) and 44 are skipped, each naming why:

```text
    cases_basic.go:164: driver lacks IdempotencyKey
    cases_flow.go:14: driver lacks Cancel
    cases_hook.go:135: driver does not implement workflow.RunNotifier
```

## Pitfalls

- Declaring a capability you emulate badly is the failure this library exists to prevent. A fake that honoured idempotency keys while the real driver dropped them kept every test green in a production app; the suite catches that only if the capability is declared and the suite runs against the real backend.
- `Capabilities` must not change after construction; the suite checks it across `Start`.
- Refuse in `Register` or `Start`, not on the first run: a refusal at start-up fails the deploy, a refusal at run time fails a customer's job.
- `Enqueue` with an idempotency key held by an unfinished run returns that run's `RunRef` and a nil error, not `ErrDuplicate`. The suite accepts either, but apps expect the first.
- A skipped subtest is not a passed one. Read the skip lines: each is a capability you have not declared, and therefore a field your driver refuses.
- Never read `time.Now` in driver logic; the suite on a fake clock will hang or fail the timing cases.

## Related

- [Capabilities](../concepts/capabilities.md#what-a-driver-declares) and [refusals](../concepts/capabilities.md#refusals)
- [Write step middleware](writing-middleware.md#write-step-middleware)
- [Bring your own store](custom-stores.md#bring-your-own-store)
- The shipped drivers as larger references: [memory](../drivers/memory.md#create-the-client), [SQL](../drivers/sqlstore.md#create-the-client), [Hatchet](../drivers/hatchet.md#create-the-client)
- Example: [custom-driver](../../examples/custom-driver/)
