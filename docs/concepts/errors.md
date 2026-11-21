# Errors

Every error the library returns on purpose wraps one of a few sentinel values, so callers dispatch with `errors.Is` and never on message text. This page lists them, says which call returns each and what to do about it, and covers the marker drivers put on "the backend could not be reached", which is what lets a failover client tell an outage from a bug.

## Before you start

- You know the [client](client.md#the-client-interface) methods.

## Sentinel errors

| Error | Means | Returned by | What to do |
|---|---|---|---|
| `ErrInvalid` | A `Workflow`, `Job`, `Schedule` or option failed validation | `Register`, `Schedule`, `workflow.Validate`, `ValidateSchedule`, `ValidateLabels`, `workflow.Use` (a nil or unnamed plugin), middleware that refuses a job (a reserved label, a missing key) | Fix the spec; the message names the step or field. Never retry |
| `ErrUnsupported` | The driver cannot honour a field or a workflow's needs. Always an `*UnsupportedError` naming the field and the missing capabilities | `Start`, `Register` (Hatchet), `Enqueue`, `Schedule`, `Cancel`, `workflow.Use` (an observer plugin on a client without `RunNotifier`) | Pick a driver that has the capability, or change the spec ([Capabilities](capabilities.md#refusals)) |
| `ErrNotStarted` | A call that needs workers came before `Start` | `Enqueue`, `Signal`, `Schedule` | Start the client first; this is a wiring bug |
| `ErrAlreadyStarted` | `Start` called twice, or `Register` or `OnRunFinished` after `Start` | `Start`, `Register`, `OnRunFinished`, `workflow.Use` with an observer plugin after `Start` | Register everything, then start once |
| `ErrNotFound` | The workflow, run or schedule does not exist | `Enqueue` (workflow not registered on this client), `Status`, `Cancel`, `Unschedule` | Check the name or the stored reference |
| `ErrDuplicate` | An idempotency or uniqueness key collided in a way reported as an error | `middleware.Unique` and `UniqueUntilFinished` (the key is held); the SQL driver when a key kept changing hands under contention | Usually expected: the work is already queued. Drivers report a plain key collision by returning the holder's `RunRef` with a nil error instead |
| `ErrCancelled` | The run was cancelled | The error a cancelled run's steps see, and `RunEvent.Err` of a cancelled run on every driver | Never retried; stop work and return |
| `ErrUnavailable` | The backend could not be reached or did not answer | Drivers, marking connection failures and timeouts ([below](#backend-outages)) | Retry later, or fail over |
| `ErrWrongState` | The run exists but its status does not allow the call | `Retry` of a run that did not fail, `Delete` of an unfinished run ([manage runs](../jobs/manage-runs.md)) | Check `Status` first; cancel before deleting |
| `ErrDiscarded` | A step ended its run with `workflow.Discard`. Also matches `ErrCancelled` | `RunEvent.Err` of a discarded run, `RunInline` | Expected: the work no longer made sense. Not retried, no `OnFailure` |
| `ErrDuplicatePlugin` | Two plugins in one `workflow.Use` call share a name | `workflow.Use` | Rename one plugin |
| `ErrTimeout` | A deadline was missed. Every timeout a driver with `CapRunTimeout` reports also matches one specific sentinel: `ErrStepTimeout`, `ErrWaitTimeout`, `ErrHeartbeatTimeout` or `ErrRunTimeout` ([below](#timeouts)) | The error a timed-out step, wait or run ends with: `RunEvent.Err`, `RunInfo.Error`, `RunInline` | Raise the limit, or make the step faster; `Workflow.OnTimeout` handles it apart from other failures |

The lifecycle and lookup sentinels in action, on the memory driver:

<!-- snippet: examples/capabilities/main.go#sentinels -->
```go
c := memory.New()
if err := c.Register(wf); err != nil {
	return err
}
_, err := c.Enqueue(ctx, workflow.Job{Workflow: "ping"})
fmt.Println("before Start:", errors.Is(err, workflow.ErrNotStarted))

if err := c.Start(ctx); err != nil {
	return err
}
fmt.Println("second Start:", errors.Is(c.Start(ctx), workflow.ErrAlreadyStarted))
fmt.Println("Register after Start:", errors.Is(c.Register(wf), workflow.ErrAlreadyStarted))

_, err = c.Enqueue(ctx, workflow.Job{Workflow: "pnig"})
fmt.Println("unknown workflow:", errors.Is(err, workflow.ErrNotFound))
_, err = c.Status(ctx, workflow.RunRef{RunID: "no-such-run"})
fmt.Println("unknown run:", errors.Is(err, workflow.ErrNotFound))
```

```text
before Start: true
second Start: true
Register after Start: true
unknown workflow: true
unknown run: true
```

The memory driver also shows how a failed run's error names its run ([Run errors](#run-errors)). Three more markers belong to step handlers rather than to calls on the client: `workflow.Permanent(err)` fails a step at once without its remaining retries (`IsPermanent` detects it); `workflow.RetryAfter(err, d)` asks for a specific delay before the next retry under a policy that reads it, and counts as a release rather than an error for `RetryPolicy.MaxErrors` (`IsRelease` detects it); and `workflow.Discard(err)` drops the run without failing it (`IsDiscarded` detects it). They are covered in [retries](../jobs/retries.md#errors-that-must-not-retry) and [limits beyond Max](../jobs/retries.md#limits-beyond-max). Packages add their own sentinels next to their features (for example `middleware.ErrPanic`, `middleware.ErrThrottled`, `sqlstore.ErrAbandoned`, `sqlstore.ErrSchemaTooNew`, `failover.ErrKeyRequired`); the [error reference](../reference/errors.md#errors) lists them all.

### Tell a release from a failure

A release is a failed attempt that is nobody's fault: the partner API answered 429 and named when to come back. Mark it with `workflow.RetryAfter(err, d)` where the step turns a response into an error:

<!-- snippet: examples/release-errors/main.go#mark -->
```go
// callPartner turns an HTTP status into the error a step returns: a 429 is
// a release (RetryAfter with the wait the API asked for), a 5xx a plain
// error, a 4xx a Permanent one that retrying cannot fix.
func callPartner(status int) error {
	switch {
	case status == http.StatusTooManyRequests:
		return workflow.RetryAfter(fmt.Errorf("429 too many requests"), retryAfterWait)
	case status >= http.StatusInternalServerError:
		return errServer
	case status >= http.StatusBadRequest:
		return workflow.Permanent(fmt.Errorf("partner refused the request: %d", status))
	}
	return nil
}
```

`workflow.IsRelease(err)` reports whether the mark is anywhere in the chain, so it survives `fmt.Errorf("...: %w", err)`; `workflow.RetryAfterDelay` reads the wait back. A nil error stays nil, and `Permanent` wins over a release:

<!-- snippet: examples/release-errors/main.go#classify -->
```go
release := callPartner(http.StatusTooManyRequests)
wrapped := fmt.Errorf("sync orders: %w", release) // the mark survives wrapping
d, _ := workflow.RetryAfterDelay(wrapped, 1)
fmt.Fprintln(w, "  429 is a release:", workflow.IsRelease(wrapped), "| wait:", d)
fmt.Fprintln(w, "  503 is a release:", workflow.IsRelease(callPartner(http.StatusServiceUnavailable)))
fmt.Fprintln(w, "  RetryAfter(nil, d) is nil:", workflow.RetryAfter(nil, time.Second) == nil)

// Permanent still wins over a release: the policy does not retry it.
policy := workflow.RetryPolicy{Max: maxAttempts, DelayFor: workflow.RetryAfterDelay}
both := workflow.RetryAfter(workflow.Permanent(errors.New("account closed")), time.Second)
fmt.Fprintln(w, "  RetryAfter(Permanent(err)) retried:", policy.Retryable(both, 1))
```

```text
  429 is a release: true | wait: 10ms
  503 is a release: false
  RetryAfter(nil, d) is nil: true
  RetryAfter(Permanent(err)) retried: false
```

The difference matters to `RetryPolicy.MaxErrors`: a release counts against `Max` only, any other error against both. Drivers keep the count in a `workflow.Attempts` (`Count` grows `N` on every failure and `Errors` only when `IsRelease` is false), and the step below keeps the same counter to print it:

<!-- snippet: examples/release-errors/main.go#policy -->
```go
policy := workflow.RetryPolicy{
	Max:       maxAttempts - 1, // retries after the first attempt
	MaxErrors: maxErrors,       // releases do not count here
	DelayFor:  workflow.RetryAfterDelay,
}
var count workflow.Attempts // the same counter every driver keeps
// step calls the fake API with this attempt's answer and prints the count.
step := func(ctx context.Context, _ []byte) ([]byte, error) {
	info, _ := workflow.StepInfoFrom(ctx)
	err := callPartner(responses[info.Attempt-1])
	if err != nil {
		count = count.Count(err) // N always grows, Errors only when !IsRelease(err)
		fmt.Fprintf(w, "  attempt %d: %v (release: %t) -> attempts %d, errors %d\n",
			info.Attempt, err, workflow.IsRelease(err), count.N, count.Errors)
	}
	return nil, err
}
```

Against answers of 429, 429, 503, 429, 503 the step stops on attempt five, its second real error, although `Max` allows ten attempts. The wait in `RetryAfter` is honoured only under a policy whose `DelayFor` reads it (`RetryAfterDelay`), which needs `workflow.CapRetryDelay`; under any other policy the error still counts as a release but waits the normal backoff.

## Backend outages

A caller sometimes has to tell "the backend is down" from "the backend refused this request": a failover client should move a job to its secondary on the first, never on the second. Drivers wrap errors from an unreachable backend with `workflow.Unavailable(err)`; the result matches `workflow.ErrUnavailable` under `errors.Is` while the original error stays reachable for `errors.Is` and `errors.As`, and `workflow.IsUnavailable(err)` is the short form. `Unavailable(nil)` is nil.

<!-- snippet: examples/capabilities/main.go#unavailable -->
```go
// A driver marks "could not reach the backend" with Unavailable; the
// original error stays reachable for errors.Is and errors.As.
refused := errors.New("dial tcp 10.0.0.5:7077: connection refused")
err = workflow.Unavailable(fmt.Errorf("enqueue: %w", refused))
fmt.Println("outage:", workflow.IsUnavailable(err), errors.Is(err, refused))
fmt.Println("refusal is no outage:", workflow.IsUnavailable(workflow.Unsupported("Job.RunAt", "")))
```

```text
outage: true true
refusal is no outage: false
```

Who marks what:

| Driver | Marked unavailable |
|---|---|
| hatchet | A gRPC `Unavailable` status, a network error from the REST client, an HTTP 502, 503 or 504. The caller's own cancellation or deadline never is |
| sqlstore | Connection failures, as sorted by its error classifier (`DefaultErrorClassifier`, replaceable with `WithErrorClassifier`) ([Database errors](../drivers/sqlstore.md#database-errors)) |
| redis | Network timeouts, connection errors and a closed client; the caller's own cancellation or deadline never is |
| memory | Nothing: there is no backend to lose |

`failover.DefaultClassifier` fails over on errors marked this way, and never on a cancelled caller context, the core sentinels above or a `Permanent` error ([Decide what counts as an outage](../reliability/failover.md#decide-what-counts-as-an-outage)). A driver you write should mark its own outages the same way, so failover works on it unchanged.

## Run errors

The error a failed run ends with (`RunEvent.Err` in a run-finished observer, and the error text in `RunInfo.Error`) is a `*workflow.RunError`: it carries `RunID`, `Workflow`, the `Step` that failed and its `Attempt` as fields, and wraps the step's own error. Code that holds only the error, such as an error tracker's hook or a generic alert, finds the run with `workflow.RunErrorOf(err)` (or `errors.As`) instead of parsing a message. Its `Error()` is the step's message unchanged, so logs and dashboards keep showing what your code said; `Describe()` puts the context in front, for a log line that has nothing else to say which run it was:

<!-- snippet: examples/lifecycle-events/main.go#runerror -->
```go
failures := make(chan string, 1)
if err := c.OnRunFinished(func(_ context.Context, ev workflow.RunEvent) {
	if ev.Status != workflow.StatusFailed {
		return
	}
	re, ok := workflow.RunErrorOf(ev.Err) // which run, workflow, step and attempt failed
	if !ok {
		failures <- "failed: " + ev.Err.Error()
		return
	}
	failures <- fmt.Sprintf("failed: %s\n  step=%s attempt=%d declined=%t\n  Error(): %s",
		re.Describe(), re.Step, re.Attempt, errors.Is(ev.Err, errDeclined), ev.Err)
}); err != nil {
	return err
}
```

```text
failed: workflow "charge" run run-6 step "card" attempt 1: memory: step "card" failed after 1 attempt(s): card declined
  step=card attempt=1 declined=true
  Error(): memory: step "card" failed after 1 attempt(s): card declined
```

Because it wraps the step's error, `errors.Is(err, yourSentinel)` still holds through it on the memory driver. The memory, Redis, SQL and Hatchet drivers build it; the Redis, SQL and Hatchet drivers rebuild the step's error from a stored message, so there `errors.Is` reaches none of the step's sentinels (a forced failure on Redis still matches `ErrForced`). A cancelled or discarded run's error is not a `RunError`: it matches `ErrCancelled` (and `ErrDiscarded`) as before. The example is [`examples/lifecycle-events`](../../examples/lifecycle-events/).

## Timeouts

`ErrTimeout` matches every missed deadline, and each kind has its own sentinel that matches `ErrTimeout` too, so code can test as broadly or as narrowly as it needs:

| Sentinel | Means |
|---|---|
| `ErrStepTimeout` | An attempt ran past `Step.Timeout` (or the workflow's default) |
| `ErrWaitTimeout` | A wait step reached its `Deadline` or `Timeout` with no event |
| `ErrHeartbeatTimeout` | An attempt went `Step.HeartbeatTimeout` without calling `workflow.Heartbeat` |
| `ErrRunTimeout` | The whole run went past `Workflow.RunTimeout`; never retried |

`workflow.IsTimeout(err)` is `errors.Is(err, workflow.ErrTimeout)`. Drivers that declare `CapRunTimeout` build every timeout with `workflow.TimeoutError(kind, detail)`, and a step or wait timeout on the memory and Redis drivers still matches `context.DeadlineExceeded` as well. These hold for the error the driver decides with (which handler runs, whether to retry) and, on the memory driver, for `RunEvent.Err`; a driver that rebuilds a finished run's error from a stored message (Redis) keeps only its text. When the final error is a timeout, `Workflow.OnTimeout` runs instead of `OnFailure` (`workflow.FailureHandler` picks which). How to set each deadline is in [Timeouts](../jobs/timeouts.md).

## Try it

```sh
cd examples
go run ./capabilities
```

The lifecycle sentinels print as shown in [Sentinel errors](#sentinel-errors). For releases and failures:

```sh
cd examples
go run ./release-errors
```

```text
classify:
  429 is a release: true | wait: 10ms
  503 is a release: false
  RetryAfter(nil, d) is nil: true
  RetryAfter(Permanent(err)) retried: false
sync:
  attempt 1: 429 too many requests (release: true) -> attempts 1, errors 0
  attempt 2: 429 too many requests (release: true) -> attempts 2, errors 0
  attempt 3: 503 service unavailable (release: false) -> attempts 3, errors 1
  attempt 4: 429 too many requests (release: true) -> attempts 4, errors 1
  attempt 5: 503 service unavailable (release: false) -> attempts 5, errors 2
  run failed after 2 errors and 3 releases
```

## Pitfalls

- Match with `errors.Is`, never by comparing messages or with `==`: every sentinel arrives wrapped with the field, workflow or key that caused it.
- `ErrDuplicate` is not the normal idempotency answer. A second `Enqueue` with a key held by an unfinished run returns that run's `RunRef` and a nil error; only middleware and rare contention paths report an error.
- On an engine driver, a failed run's `RunEvent.Err` is rebuilt from the stored message, so `errors.Is(ev.Err, yourSentinel)` holds only on in-process drivers. `ErrCancelled` matches on every driver.
- An `ErrUnsupported` at `Start` names the workflow, not the call site that will use it. Read `UnsupportedError.Missing`, or check the workflow with `workflow.Requires` in a test.

## Related

- [Capabilities and refusals](capabilities.md#refusals)
- [Error reference](../reference/errors.md#errors)
- [Timeouts](../jobs/timeouts.md)
- [Lifecycle events](../observability/lifecycle.md)
- [Fail over to a second backend](../reliability/failover.md#fail-over-to-a-second-backend)
- Examples: [capabilities](../../examples/capabilities/), [lifecycle-events](../../examples/lifecycle-events/)
