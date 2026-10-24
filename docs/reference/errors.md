# Errors

Every error the library returns for a condition a caller can act on wraps one of the sentinels below. Match them with `errors.Is`, never on the message text: messages carry details (the field, the workflow, the key) and may change, the sentinels do not. Most errors wrap more than one, for example `failover.ErrKeyRequired` together with `workflow.ErrInvalid`, so a caller can match the specific case or the general one.

## Core sentinels (`workflow`)

| Sentinel | Means | Typical source |
|---|---|---|
| `ErrInvalid` | A `Workflow`, `Job`, `Schedule` or option failed validation | `Validate`, `Register`, `Enqueue` (a missing key under `IdempotencyRequired`, a reserved label), `New` of most packages with a bad argument, `Unique` with a `ttl` of zero or less |
| `ErrUnsupported` | The driver cannot honour a field or a workflow's needs; always returned as an `UnsupportedError` | `Start` (a workflow needing a missing capability), `Enqueue` (a `RunAt` or key without the capability), `Schedule`, middleware that needs a capability (`Debounce`, `UniqueUntilFinished`, `CleanupOnFinish`), `outbox.NewRelay` without `AllowDuplicates` |
| `ErrNotFound` | The workflow, run or schedule does not exist | `Enqueue` of an unregistered workflow, `Status` and `Cancel` of an unknown run, `outbox.Requeue` and `ListDead` with an unknown ID |
| `ErrNotStarted` | A call that needs workers came before `Start`, or after `Stop` | `Enqueue`, `Signal`, `Schedule`, `sqlstore.Client.EnqueueTx` |
| `ErrAlreadyStarted` | `Start` was called twice, or `Register` or a run-finished observer came after `Start` | Every driver, `failover.Client`, `outbox.Relay.Start`, `UniqueUntilFinished` and `CleanupOnFinish` after `Start` |
| `ErrDuplicate` | An idempotency key or uniqueness lock collided in a way reported as an error rather than by returning the existing run | `middleware.Unique` while the key is held; `EnqueueTx` when a key kept changing hands; `Retry` of a run whose key another unfinished run now holds |
| `ErrCancelled` | The run was cancelled; it is what a cancelled run's steps see, and it is never retried | What a cancelled run's steps see; the `Err` of a cancelled run's `RunEvent` matches it on every driver |
| `ErrUnavailable` | The backend could not be reached or did not answer, as opposed to refusing the request | Errors marked with `Unavailable` by the Hatchet driver (a gRPC `Unavailable`, a network error, an HTTP 502, 503 or 504) and by sqlstore (per its error classifier) |
| `ErrDuplicatePlugin` | Two plugins handed to one `Use` call share a name | `workflow.Use` |
| `ErrDiscarded` | A step ended its run with `Discard`; the run's `Err` matches `ErrCancelled` too | The `Err` of a discarded run's `RunEvent` (`StatusDiscarded` with `CapRunStates`), built by `DiscardedError` |
| `ErrWrongState` | The run exists but its status does not allow the call | `RunRetrier.Retry` of a run that did not fail (a discarded run included), `RunDeleter.Delete` of an unfinished run, `RunForcer.ForceComplete` of a finished run that did not fail, `ForceFail` of a finished run |
| `ErrOverlap` | A schedule's overlap policy skips while its previous run is unfinished; built by `OverlapRefusal` | `ScheduleTriggerer.TriggerSchedule` on a schedule with `OverlapSkip`; the admin API answers 409 |
| `ErrForced` | An operator ended the run (`RunForcer`); the cause of an attempt cut short by a force, and the error of a force-failed run (`ForcedFailure(reason)`) | `RunEvent.Err` and `RunInfo.Error` of a run `ForceFail` ended; `context.Cause` in a step a force cancelled |
| `ErrRecycle` | A worker stopped itself at a `Recycle` limit so its supervisor can start a fresh process; always a `*RecycleError` with the reason | `workflow.Serve`, and `StopCause` on every driver with a recycle option |
| `ErrDeferredClosed` | A `Deferred` was already flushed or discarded; it is used once | `Deferred.Add`, through a `DeferEnqueue` client whose context still carries the used `Deferred` |
| `ErrTimeout` | A deadline was missed; every timeout below matches it as well as its own kind, built by `TimeoutError(kind, detail)` and tested with `IsTimeout` | Any timeout a driver with `CapRunTimeout` reports ([Timeouts](../jobs/timeouts.md)) |
| `ErrStepTimeout` | A step attempt ran past `Step.Timeout`; retried like any failure | A step's attempt error, `RunEvent.Err` of a run that ended on it |
| `ErrWaitTimeout` | A wait step's deadline or timeout passed before its event | A wait step's error |
| `ErrHeartbeatTimeout` | A step with `HeartbeatTimeout` stopped beating; retried like any failure | A step's attempt error ([Progress and heartbeats](../jobs/progress.md#catch-a-hung-step)) |
| `ErrRunTimeout` | The whole run passed `Workflow.RunTimeout`; never retried, and `OnTimeout` runs instead of `OnFailure` | `RunEvent.Err` of a run that ran out of time; `context.Cause` in its cancelled attempts |
| `ErrSnoozed` | A step returned `Snooze(d)`; on a driver with `CapSnooze` it runs again after d without spending an attempt, elsewhere it is an ordinary error | `SnoozeDelay` reads the delay ([Retries › Snooze](../jobs/retries.md#snooze-a-step-that-is-not-ready)) |
| `ErrStepCancelled` | An operator withdrew one step with `CancelStep`; the cause of the cancelled attempt, and the run's error when the handler returns one | `StepCanceller.CancelStep`, admin `POST /runs/{id}/steps/{step}/cancel` |
| `ErrPayloadTooLarge` | A job's payload, or a chained job's, is over a limit; always a `*PayloadTooLargeError`, which also matches `ErrInvalid` | `Enqueue` on a driver with a hard cap (`PayloadLimiter`), `memory.WithPayloadLimit`, `middleware.MaxPayload` |
| `ErrScheduleJSON` | A schedule's JSON form could not be written or read: another version, an unknown field, a zone that does not load | `json.Marshal`/`Unmarshal` of a `Schedule`, `UnmarshalSchedule` |
| `ErrLocalCronStarted` | A `LocalCron` entry was added after `Start`, or `Start` was called twice | `LocalCron.Add`, `LocalCron.Start` |
| `ErrNoJournal` | A memoised call ran without the `Journal` plugin or `WithJournal`; permanent | `Memo`, `MapEach`, `Loop`, `ProcessChunks`, `Spawn`, `AwaitChild`, `SetRunData`, `RunData` ([Memoisation](../workflows/memoisation.md)) |
| `ErrLoopLimit` | A `Loop` or `ProcessChunks` reached its limit; permanent | `Loop`, `ProcessChunks` with `MaxChunks` |
| `ErrSecretInPayload` | `SecretGuard` found what looks like a credential in a job; also matches `ErrInvalid`, and the error never prints the value | The `SecretGuard` enqueue middleware ([Secrets in payloads](../reliability/secrets.md)) |
| `ErrNoSecretRef` | A `SecretRef` has no name; permanent | `ResolveSecret` |
| `ErrThrottled` | An `UpstreamLimit` bucket has no room; marked with `RetryAfter` for the wait | `UpstreamLimit.Take` ([Upstream rate limits](../jobs/upstream-limits.md)); not the same variable as `middleware.ErrThrottled` |

## Error types and markers (`workflow`)

| API | What it does |
|---|---|
| `UnsupportedError{Field, Missing, Reason}` | Names what a driver cannot do: the spec field, the missing capabilities (zero when the refusal is not a capability check) and a detail. Matches `ErrUnsupported`; use `errors.As` to read the fields |
| `Unsupported(field, reason)` | Builds an `UnsupportedError` without capabilities |
| `Permanent(err)`, `IsPermanent(err)` | Marks an error that must not be retried: the step fails now and its remaining attempts are skipped. `Permanent(nil)` is nil |
| `Unavailable(err)`, `IsUnavailable(err)` | Marks an error as a backend outage: `errors.Is` matches both `ErrUnavailable` and the original. A failover client fails over on it. `Unavailable(nil)` is nil |
| `Discard(err)`, `IsDiscarded(err)` | Marks an error as "drop this run": no retry, no `OnFailure`, the run ends `discarded` (`cancelled` without `CapRunStates`) with an `Err` matching `ErrDiscarded`. Without `CapDiscard` it acts as `Permanent`. `Discard(nil)` is nil |
| `RetryAfter(err, d)`, `RetryAfterDelay` | Marks an error with the wait the next retry should observe, such as a 429's `Retry-After`. Honoured only under a policy whose `DelayFor` is `RetryAfterDelay` (or reads it); `Permanent` still wins |
| `RunError{RunID, Workflow, Step, Attempt, Err}`, `RunErrorOf(err)` | A failed run's error on drivers that build it: names the run, workflow, step and attempt and unwraps to the step's own error ([Run errors](../concepts/errors.md#run-errors)) |
| `TimeoutError(kind, detail)`, `IsTimeout(err)` | Builds an error matching `ErrTimeout` and one of its kinds; tells any timeout apart from other failures |
| `Snooze(d)`, `SnoozeDelay(err)` | Puts a step back for d without spending an attempt (clamped to `MaxSnooze`, 30 days) and reads the delay back |
| `PayloadTooLargeError{Workflow, Size, Limit, Source}` | A payload over a limit; matches `ErrPayloadTooLarge` and `ErrInvalid`, and `Source` names who set the limit |
| `RunFailedError{Ref, Status, Err}` | What `Await`, `AwaitResult` and `AwaitChild` return for a run that did not succeed; matches `ErrCancelled` for a cancelled or discarded run |

## Package sentinels

| Sentinel | Package | Means |
|---|---|---|
| `ErrKeyRequired` | `failover` | A job without an `IdempotencyKey` on a failover client, which could run it on both backends. Also matches `workflow.ErrInvalid`. Lifted by `AllowWithoutKey` |
| `ErrBlobNotFound` | `middleware` | A `BlobStore` does not hold the key. The claim check step turns it into a `Permanent` error: a blob that is gone does not come back on a retry |
| `ErrDecode` | `middleware` | `DecodeStep` could not open the input: a wrong key, tampered or truncated bytes, or a value that was never encoded. The error never carries the bytes |
| `ErrPanic` | `middleware` | `Recover` turned a panicking step into this error. It retries like any error; add `NoRetryOn: errors.Is(err, ErrPanic)` to stop that |
| `ErrReleased` | `middleware` | `Release` put the step back for a delay without running it. Wrapped in a `workflow.RetryAfter`, so the retry policy brings it back; `RetryPolicy.MaxErrors` does not count it |
| `ErrBuilder` | `cron` | A `cron.Builder` call was invalid; wrapped by every error a builder returns, so a bad frequency call is told apart from a parse error of a hand-written expression |
| `ErrCalendar` | `cron` | A `cron.Calendar` that cannot be written as an expression: a value out of range, or both day fields set |
| `ErrZone` | `cron` | `LoadZone` refused an empty, `"Local"` or unknown zone name; the error names the zone |
| `ErrRunAllStalled` | `memory` | `RunAll` stopped with runs left that only the clock, a resume or a free slot can move ([Run everything now](../testing/run-all.md)) |
| `ErrSignature` | `events` | A webhook's signature does not verify; the webhook handler answers 401 |
| `ErrClosed` | `batch` | `Manager.Add` on a batch that has finished or was cancelled |
| `ErrDispatching` | `batch` | `Manager.Add` or `RetryFailed` while another dispatch of the batch is in progress. Transient: try again |
| `ErrBatchPending` | `batch` | An `AwaitStep` whose batch has not finished; its retry policy polls again. It appears as a failed attempt in run history |
| `ErrBatchFailed` | `batch` | An `AwaitStep` whose batch finished with a failed or cancelled job, unless `AwaitAllowFailures`. Permanent |
| `ErrThrottled` | `middleware` | `ThrottleErrors` returned without calling the step while the key's circuit is open. Retryable on purpose: the retry backoff brings the step back after the cooldown |
| `ErrSchemaTooNew` | `sqlstore` | The database records a sqlstore migration this code does not know: a newer release migrated it. `Migrate` and `Start` refuse it. Returned as a `*SchemaTooNewError` whose `Unknown` lists the migrations |
| `ErrAbandoned` | `sqlstore` | A step attempt failed because its lease expired more than `WithMaxReclaims` times: every worker that took it died. The retry policy treats it like any failure |
| `ErrDriver` | `sqlstore/pgnotify` | `Listen` on a pool whose driver is not the pgx stdlib driver, which alone exposes notifications; the client keeps polling |
| `ErrSchemaTooNew` | `sqlstore/outbox` | The ledger records an outbox migration this code does not know. `Migrate` refuses it |

The two `ErrSchemaTooNew` values are different variables in different packages, as are `workflow.ErrThrottled` and `middleware.ErrThrottled`; match the one of the package you called.

## Matching in practice

Test the specific sentinel first and the general one after: `failover.ErrKeyRequired` before `workflow.ErrInvalid`, `workflow.ErrDuplicate` (already queued: tell the user, do not retry) before a generic failure. For an `ErrUnsupported`, `errors.As` into a `*workflow.UnsupportedError` gives the field and the missing capabilities, which is what an operator needs to change. For an outage, `workflow.IsUnavailable(err)` says the backend is down rather than the request wrong, so retrying later, or letting a failover client handle it, is the right response. The runnable programs show these checks in context: [failover](../../examples/failover/) (`ErrKeyRequired`, `ErrInvalid`, `Unavailable`), [unique-jobs](../../examples/unique-jobs/) (`ErrDuplicate`), [claim-check](../../examples/claim-check/) (`ErrBlobNotFound`), [transactional-enqueue](../../examples/transactional-enqueue/) (`ErrNotFound`) and [outbox](../../examples/outbox/) (`ErrUnsupported`).

## Related

- [Capabilities](capabilities.md): what `UnsupportedError.Missing` names.
- [Retries](../jobs/retries.md): `Permanent`, `RetryAfter` and the retry policy.
- [Failover](../reliability/failover.md#decide-what-counts-as-an-outage): how `ErrUnavailable` decides a failover.
- [Errors in the core contract](../concepts/errors.md).
