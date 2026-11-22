# Errors

Every error the library returns for a condition a caller can act on wraps one of the sentinels below. Match them with `errors.Is`, never on the message text: messages carry details (the field, the workflow, the key) and may change, the sentinels do not. Most errors wrap more than one, for example `failover.ErrKeyRequired` together with `workflow.ErrInvalid` ([`failover.Client.Enqueue`](ds:block?id=failover-client-enqueue-w2azafu4)), so a caller can match the specific case or the general one.

## Core sentinels (`workflow`)

| Sentinel | Means | Typical source |
|---|---|---|
| [`ErrInvalid`](ds:block?id=errinvalid-y3xxpcy2) | A `Workflow`, `Job`, `Schedule` or option failed validation | `Validate`, `Register`, `Enqueue` (a missing key under `IdempotencyRequired`, a reserved label), `New` of most packages with a bad argument, `Unique` with a `ttl` of zero or less |
| [`ErrUnsupported`](ds:block?id=errunsupported-wacxcsch) | The driver cannot honour a field or a workflow's needs; always returned as an `UnsupportedError` | `Start` (a workflow needing a missing capability), `Enqueue` (a `RunAt` or key without the capability), `Schedule`, middleware that needs a capability (`Debounce`, `UniqueUntilFinished`, `CleanupOnFinish`), `outbox.NewRelay` without `AllowDuplicates` |
| [`ErrNotFound`](ds:block?id=errnotfound-5m2hhaq7) | The workflow, run or schedule does not exist | `Enqueue` of an unregistered workflow, `Status` and `Cancel` of an unknown run, `outbox.Requeue` and `ListDead` with an unknown ID |
| [`ErrNotStarted`](ds:block?id=errnotstarted-x6pc2d38) | A call that needs workers came before `Start`, or after `Stop` | `Enqueue`, `Signal`, `Schedule`, `sqlstore.Client.EnqueueTx` |
| [`ErrAlreadyStarted`](ds:block?id=erralreadystarted-prauyy24) | `Start` was called twice, or `Register` or a run-finished observer came after `Start` | Every driver, `failover.Client`, `outbox.Relay.Start`, `UniqueUntilFinished` and `CleanupOnFinish` after `Start` |
| [`ErrDuplicate`](ds:block?id=errduplicate-chfrvqsx) | An idempotency key or uniqueness lock collided in a way reported as an error rather than by returning the existing run | `middleware.Unique` while the key is held; `EnqueueTx` when a key kept changing hands; `Retry` of a run whose key another unfinished run now holds |
| [`ErrCancelled`](ds:block?id=errcancelled-9t9axh67) | The run was cancelled; it is what a cancelled run's steps see, and it is never retried | What a cancelled run's steps see; the `Err` of a cancelled run's `RunEvent` matches it on every driver |
| [`ErrUnavailable`](ds:block?id=errunavailable-982eccbn) | The backend could not be reached or did not answer, as opposed to refusing the request | Errors marked with `Unavailable` by the Hatchet driver (a gRPC `Unavailable`, a network error, an HTTP 502, 503 or 504) and by sqlstore (per its error classifier) |
| [`ErrDuplicatePlugin`](ds:block?id=errduplicateplugin-ce74ermg) | Two plugins handed to one `Use` call share a name | `workflow.Use` |
| [`ErrDiscarded`](ds:block?id=errdiscarded-wcgqzsc5) | A step ended its run with `Discard`; the run's `Err` matches `ErrCancelled` too | The `Err` of a discarded run's `RunEvent` (`StatusDiscarded` with `CapRunStates`), built by `DiscardedError` |
| [`ErrWrongState`](ds:block?id=errwrongstate-7qhmurgh) | The run exists but its status does not allow the call | `RunRetrier.Retry` of a run that did not fail (a discarded run included), `RunDeleter.Delete` of an unfinished run, `RunForcer.ForceComplete` of a finished run that did not fail, `ForceFail` of a finished run |
| [`ErrOverlap`](ds:block?id=erroverlap-e97x6vmy) | A schedule's overlap policy skips while its previous run is unfinished; built by `OverlapRefusal` | `ScheduleTriggerer.TriggerSchedule` on a schedule with `OverlapSkip`; the admin API answers 409 |
| [`ErrForced`](ds:block?id=errforced-2cdhp8xs) | An operator ended the run (`RunForcer`); the cause of an attempt cut short by a force, and the error of a force-failed run (`ForcedFailure(reason)`) | `RunEvent.Err` and `RunInfo.Error` of a run `ForceFail` ended; `context.Cause` in a step a force cancelled |
| [`ErrRecycle`](ds:block?id=errrecycle-smp6ce9f) | A worker stopped itself at a `Recycle` limit so its supervisor can start a fresh process; always a `*RecycleError` with the reason | `workflow.Serve`, and `StopCause` on every driver with a recycle option |
| [`ErrDeferredClosed`](ds:block?id=errdeferredclosed-mcycghsh) | A `Deferred` was already flushed or discarded; it is used once | `Deferred.Add`, through a `DeferEnqueue` client whose context still carries the used `Deferred` |
| [`ErrTimeout`](ds:block?id=errtimeout-w9sszmbd) | A deadline was missed; every timeout below matches it as well as its own kind, built by `TimeoutError(kind, detail)` and tested with `IsTimeout` | Any timeout a driver with `CapRunTimeout` reports ([Timeouts](../jobs/timeouts.md)) |
| [`ErrStepTimeout`](ds:block?id=errsteptimeout-6b727264) | A step attempt ran past `Step.Timeout`; retried like any failure | A step's attempt error, `RunEvent.Err` of a run that ended on it |
| [`ErrWaitTimeout`](ds:block?id=errwaittimeout-vvzd9zgy) | A wait step's deadline or timeout passed before its event | A wait step's error |
| [`ErrHeartbeatTimeout`](ds:block?id=errheartbeattimeout-t75kucja) | A step with `HeartbeatTimeout` stopped beating; retried like any failure | A step's attempt error ([Progress and heartbeats](../jobs/progress.md#catch-a-hung-step)) |
| [`ErrRunTimeout`](ds:block?id=errruntimeout-6cywh9de) | The whole run passed `Workflow.RunTimeout`; never retried, and `OnTimeout` runs instead of `OnFailure` | `RunEvent.Err` of a run that ran out of time; `context.Cause` in its cancelled attempts |
| [`ErrSnoozed`](ds:block?id=errsnoozed-3x74fmpm) | A step returned `Snooze(d)`; on a driver with `CapSnooze` it runs again after d without spending an attempt, elsewhere it is an ordinary error | `SnoozeDelay` reads the delay ([Retries › Snooze](../jobs/retries.md#snooze-a-step-that-is-not-ready)) |
| [`ErrStepCancelled`](ds:block?id=errstepcancelled-bxbppyku) | An operator withdrew one step with `CancelStep`; the cause of the cancelled attempt, and the run's error when the handler returns one | `StepCanceller.CancelStep`, admin `POST /runs/{id}/steps/{step}/cancel` |
| [`ErrPayloadTooLarge`](ds:block?id=errpayloadtoolarge-etx7f76y) | A job's payload, or a chained job's, is over a limit; always a `*PayloadTooLargeError`, which also matches `ErrInvalid` | `Enqueue` on a driver with a hard cap (`PayloadLimiter`), `memory.WithPayloadLimit`, `middleware.MaxPayload` |
| [`ErrScheduleJSON`](ds:block?id=errschedulejson-v6hk7ay2) | A schedule's JSON form could not be written or read: another version, an unknown field, a zone that does not load | `json.Marshal`/`Unmarshal` of a `Schedule`, `UnmarshalSchedule` |
| [`ErrLocalCronStarted`](ds:block?id=errlocalcronstarted-6yxzcj5p) | A `LocalCron` entry was added after `Start`, or `Start` was called twice | `LocalCron.Add`, `LocalCron.Start` |
| [`ErrNoJournal`](ds:block?id=errnojournal-7dxyqv7a) | A memoised call ran without the `Journal` plugin or `WithJournal`; permanent | `Memo`, `MapEach`, `Loop`, `ProcessChunks`, `Spawn`, `AwaitChild`, `SetRunData`, `RunData` ([Memoisation](../workflows/memoisation.md)) |
| [`ErrNoDataStore`](ds:block?id=errnodatastore-j5bgw3ba) | A scoped data helper ran without the `Data` plugin, `WithData` or `ContextWithData`; permanent | `GetData`, `SetData`, `UpdateData`, `DeleteData` ([Share data across runs](../workflows/memoisation.md#share-data-across-runs)) |
| [`ErrDataConflict`](ds:block?id=errdataconflict-vv8g67fu) | A conditional `DataStore.Put` found another version (another writer got there first), or `UpdateData` lost that race [`UpdateDataAttempts`](ds:block?id=updatedataattempts-3544vz5p) times; not permanent | `DataStore.Put`, `UpdateData` |
| [`ErrInboxBusy`](ds:block?id=errinboxbusy-qtu9arf9) | Another worker holds the message's inbox claim (`Claim` found [`InboxBusy`](ds:block?id=inboxbusy-qh2qbbhu); [`InboxClaimed`](ds:block?id=inboxclaimed-pr3yk9gm) and [`InboxDone`](ds:block?id=inboxdone-xysa8a3u) are the other two states); not permanent, the message comes back | [`InboxConsumer.Once`](ds:block?id=inboxconsumer-once-mta79w6e); an inbound webhook with [`Inbox`](ds:block?id=events-webhook-fail-qxvpc8yk) answers 503 ([inbox](../jobs/idempotency.md#drop-redelivered-messages-with-an-inbox)) |
| [`ErrInboxLost`](ds:block?id=errinboxlost-crwssvfu) | The claim's token no longer holds the message (its lease expired and another worker took it over) | `Inbox.Complete`, `InboxConsumer.Once` |
| [`ErrLoopLimit`](ds:block?id=errlooplimit-mtqewctv) | A `Loop` or `ProcessChunks` reached its limit; permanent | `Loop`, `ProcessChunks` with `MaxChunks` |
| [`ErrSecretInPayload`](ds:block?id=errsecretinpayload-nzpdde34) | `SecretGuard` found what looks like a credential in a job; also matches `ErrInvalid`, and the error never prints the value | The `SecretGuard` enqueue middleware ([Secrets in payloads](../reliability/secrets.md)) |
| [`ErrNoSecretRef`](ds:block?id=errnosecretref-xkcq7und) | A `SecretRef` has no name; permanent | `ResolveSecret` |
| [`ErrThrottled`](ds:block?id=errthrottled-ys5p97sd) | An `UpstreamLimit` bucket has no room; marked with `RetryAfter` for the wait | `UpstreamLimit.Take` ([Upstream rate limits](../jobs/upstream-limits.md)); not the same variable as `middleware.ErrThrottled` |
| [`ErrPromiseSettled`](ds:block?id=errpromisesettled-cbm2esnm) | A promise was settled already with a different outcome: the first settlement wins | `ResolvePromise`, `RejectPromise` |
| [`ErrUnhandledEvent`](ds:block?id=errunhandledevent-92z9ny89) | An event no active state handles, under `MachineReject`, or any event sent to a machine instance that reached a top-level final state; matches `ErrInvalid`, permanent | `SendEvent`'s run; `IsUnhandledEvent` recognises it through `AwaitResult`'s `RunFailedError` ([State machines › Unhandled events](../workflows/state-machines.md#unhandled-events)) |
| [`ErrNondeterminism`](ds:block?id=errnondeterminism-4uctguxs) | A replayed handler made a call that differs from its recorded history at that position, or returned with recorded calls left over (`*NondeterminismError` with `Position`, `Recorded`, `Got`); permanent | `NewReplay` ([Replay › Keep the code deterministic](../workflows/replay.md#keep-the-code-deterministic)) |
| [`ErrStepsFailed`](ds:block?id=errstepsfailed-q4yb27zg) | A step before a `Finally` exit handler failed; the run ends with it after the handler ran | A `WithRules` workflow with `Finally` |
| [`ErrSagaAborted`](ds:block?id=errsagaaborted-c3s8tp6k) | The saga started compensating, so no new action runs | `Saga.Do` after `Abort` (in this attempt or an earlier one) |
| [`ErrCompensated`](ds:block?id=errcompensated-4d6vb255) | Every compensation of an aborted saga ran; permanent | `Saga.Abort` |
| [`ErrPollTimeout`](ds:block?id=errpolltimeout-2mzgb5sv) | A `Poll` condition did not hold before `PollSpec.Timeout`; also matches `ErrTimeout` | `Poll` |
| [`ErrApprovalToken`](ds:block?id=errapprovaltoken-ac4jv84z) | A signed approval token is malformed, forged or expired | `ApprovalSigner`, `Approve`, `Reject` |
| [`ErrDepth`](ds:block?id=errdepth-vpx26s3r) | A job would nest deeper than the recursion limit | `RecursionLimit` |
| [`ErrShapeChanged`](ds:block?id=errshapechanged-nukshgy3) | A run was enqueued under another set of steps than the one now registered | `ShapeGuard` |
| [`ErrConstraint`](ds:block?id=errconstraint-57ytu3e7) | An invariant or a pre- or post-condition failed; permanent | `Invariant`, `middleware.Check` |
| [`ErrStreamFull`](ds:block?id=errstreamfull-yqqf883d) | A `MemoryStream` holds its limit of open runs and none can be evicted | `Emit` through a `MemoryStream` |

## Error types and markers (`workflow`)

| API | What it does |
|---|---|
| [`UnsupportedError{Field, Missing, Reason}`](ds:block?id=unsupportederror-46jkqrxt) | Names what a driver cannot do: the spec field, the missing capabilities (zero when the refusal is not a capability check) and a detail. Matches `ErrUnsupported`; use `errors.As` to read the fields |
| [`Unsupported(field, reason)`](ds:block?id=unsupported-4r9efjk8) | Builds an `UnsupportedError` without capabilities |
| [`Permanent(err)`](ds:block?id=permanent-9e6vjm6s), [`IsPermanent(err)`](ds:block?id=ispermanent-pswuapn7) | Marks an error that must not be retried: the step fails now and its remaining attempts are skipped. `Permanent(nil)` is nil |
| [`Unavailable(err)`](ds:block?id=unavailable-wpp9cehg), [`IsUnavailable(err)`](ds:block?id=isunavailable-emny24e5) | Marks an error as a backend outage: `errors.Is` matches both `ErrUnavailable` and the original. A failover client fails over on it. `Unavailable(nil)` is nil |
| [`Discard(err)`](ds:block?id=discard-ttxam8w8), [`IsDiscarded(err)`](ds:block?id=isdiscarded-m5swj98d) | Marks an error as "drop this run": no retry, no `OnFailure`, the run ends `discarded` (`cancelled` without `CapRunStates`) with an `Err` matching `ErrDiscarded`. Without `CapDiscard` it acts as `Permanent`. `Discard(nil)` is nil |
| [`RetryAfter(err, d)`](ds:block?id=retryafter-xck6cdyv), [`RetryAfterDelay`](ds:block?id=retryafterdelay-pgsc6bvn) | Marks an error with the wait the next retry should observe, such as a 429's `Retry-After`. Honoured only under a policy whose `DelayFor` is `RetryAfterDelay` (or reads it); `Permanent` still wins |
| [`RunError{RunID, Workflow, Step, Attempt, Err}`](ds:block?id=runerror-9cdqsecz), [`RunErrorOf(err)`](ds:block?id=runerrorof-43usfvhp) | A failed run's error on drivers that build it: names the run, workflow, step and attempt and unwraps to the step's own error ([Run errors](../concepts/errors.md#run-errors)) |
| [`TimeoutError(kind, detail)`](ds:block?id=timeouterror-nqztem89), [`IsTimeout(err)`](ds:block?id=istimeout-ue449vg3) | Builds an error matching `ErrTimeout` and one of its kinds; tells any timeout apart from other failures |
| [`Snooze(d)`](ds:block?id=snooze-mcp3r4p7), [`SnoozeDelay(err)`](ds:block?id=snoozedelay-hpbukddn) | Puts a step back for d without spending an attempt (clamped to [`MaxSnooze`](ds:block?id=maxsnooze-jzy97vu4), 30 days) and reads the delay back |
| [`PayloadTooLargeError{Workflow, Size, Limit, Source}`](ds:block?id=payloadtoolargeerror-snhqhgrw) | A payload over a limit; matches `ErrPayloadTooLarge` and `ErrInvalid`, and `Source` names who set the limit |
| [`RunFailedError{Ref, Status, Err}`](ds:block?id=runfailederror-nzegsjbf) | What `Await`, `AwaitResult` and `AwaitChild` return for a run that did not succeed; matches `ErrCancelled` for a cancelled or discarded run |
| [`*PromiseRejectedError`](ds:block?id=promiserejectederror-t8d9km2g) | What `AwaitPromise`, `WaitPromise` and `Update` return for a rejected promise |
| [`*JoinFailedError`](ds:block?id=joinfailederror-r52ajzv8) | `JoinChildren` can no longer reach `Need` |
| [`*MapRunsError`](ds:block?id=maprunserror-6agjduvk) | `MapRuns` passed `MaxFailures` |

## Package sentinels

| Sentinel | Package | Means |
|---|---|---|
| [`ErrKeyRequired`](ds:block?id=failover-errkeyrequired-5ywe568r) | `failover` | A job without an `IdempotencyKey` on a failover client, which could run it on both backends. Also matches `workflow.ErrInvalid`. Lifted by `AllowWithoutKey` |
| [`ErrBlobNotFound`](ds:block?id=mw-errblobnotfound-ezj4fu6t) | `middleware` | A `BlobStore` does not hold the key. The claim check step turns it into a `Permanent` error: a blob that is gone does not come back on a retry |
| [`ErrDecode`](ds:block?id=mw-errdecode-4bvj8hk4) | `middleware` | `DecodeStep` could not open the input: a wrong key, tampered or truncated bytes, or a value that was never encoded. The error never carries the bytes |
| [`ErrPanic`](ds:block?id=mw-errpanic-363z3krr) | `middleware` | `Recover` turned a panicking step into this error. It retries like any error; add `NoRetryOn: errors.Is(err, ErrPanic)` to stop that |
| [`ErrReleased`](ds:block?id=mw-errreleased-qtkkhua5) | `middleware` | `Release` put the step back for a delay without running it. Wrapped in a `workflow.RetryAfter`, so the retry policy brings it back; `RetryPolicy.MaxErrors` does not count it |
| [`ErrBuilder`](ds:block?id=cron-errbuilder-bjbhu6mv) | `cron` | A `cron.Builder` call was invalid; wrapped by every error a builder returns, so a bad frequency call is told apart from a parse error of a hand-written expression |
| [`ErrCalendar`](ds:block?id=cron-errcalendar-5cmwqcwg) | `cron` | A `cron.Calendar` that cannot be written as an expression: a value out of range, or both day fields set |
| [`ErrZone`](ds:block?id=cron-errzone-jkqrdnz6) | `cron` | `LoadZone` refused an empty, `"Local"` or unknown zone name; the error names the zone |
| [`ErrRunAllStalled`](ds:block?id=memory-errrunallstalled-gd4qrw79) | `memory` | `RunAll` stopped with runs left that only the clock, a resume or a free slot can move ([Run everything now](../testing/run-all.md)) |
| [`ErrSignature`](ds:block?id=events-errsignature-deeb8maq) | `events` | A webhook's signature does not verify; the webhook handler answers 401 |
| [`ErrClosed`](ds:block?id=batch-errclosed-2uqeatej) | `batch` | `Manager.Add` on a batch that has finished or was cancelled |
| [`ErrDispatching`](ds:block?id=batch-errdispatching-rr7bhna5) | `batch` | `Manager.Add` or `RetryFailed` while another dispatch of the batch is in progress. Transient: try again |
| [`ErrBatchPending`](ds:block?id=batch-errbatchpending-qe5qk678) | `batch` | An `AwaitStep` whose batch has not finished; its retry policy polls again. It appears as a failed attempt in run history |
| [`ErrBatchFailed`](ds:block?id=batch-errbatchfailed-8v9cn2x5) | `batch` | An `AwaitStep` whose batch finished with a failed or cancelled job, unless `AwaitAllowFailures`. Permanent |
| [`ErrThrottled`](ds:block?id=mw-errthrottled-ncrmj7xa) | `middleware` | `ThrottleErrors` returned without calling the step while the key's circuit is open. Retryable on purpose: the retry backoff brings the step back after the cooldown |
| [`ErrExpired`](ds:block?id=mw-errexpired-z9nksyp4) | `middleware` | `Expire` ended a run that started after its expiry (`LabelExpiresAt`), or a step that waited longer than `ExpireMaxWait` for a worker |
| [`ErrQueueFull`](ds:block?id=mw-errqueuefull-6qbdqgbm) | `middleware` | `Bounded` refused an enqueue: the job's queue holds its limit of waiting runs. Backpressure, not an outage: it does not match `workflow.ErrUnavailable`, so failover never sends the job to its secondary |
| [`ErrQuotaExceeded`](ds:block?id=mw-errquotaexceeded-bppgny9j) | `middleware` | `Quota` refused an enqueue: the job's tenant holds its limit of unfinished runs; not an outage either |
| [`ErrPoolBusy`](ds:block?id=mw-errpoolbusy-ze3z4uzz) | `middleware` | Every slot of a `Pool` is taken under `PoolRetryAfter`; retryable, marked with `workflow.RetryAfter` (the default snoozes instead) |
| [`ErrEndpointDisabled`](ds:block?id=mw-errendpointdisabled-tqdkpv29) | `middleware` | `AutoDisable` discarded an attempt while its key is disabled |
| [`ErrFlushNotEnqueued`](ds:block?id=mw-errflushnotenqueued-wyzy7c7x) | `middleware` | `Aggregate` stored an item but could not enqueue the flush; the item is not lost, the group's next trigger enqueues the flush |
| [`ErrUnknownRegion`](ds:block?id=regional-errunknownregion-g9ph2em5) | `regional` | A `Router` named a region the client does not have; matches `workflow.ErrInvalid` too, and the job is never sent to a default region |
| [`ErrNotExpressible`](ds:block?id=schedexport-errnotexpressible-6gamr9aa) | `schedexport` | A format cannot express a part of the schedule; the error is a `*NotExpressibleError` naming `Format` and `Field` |
| [`ErrNoNextFire`](ds:block?id=schedexport-errnonextfire-2pu4m79k) | `schedexport` | A NextFire form found no next fire (`Active` over, last year passed, no tick inside the windows) |
| [`ErrSchemaTooNew`](ds:block?id=sql-errschematoonew-ry9z2uyp) | `sqlstore` | The database records a sqlstore migration this code does not know: a newer release migrated it. `Migrate` and `Start` refuse it. Returned as a `*SchemaTooNewError` whose `Unknown` lists the migrations |
| `ErrAbandoned` | [`sqlstore`](ds:block?id=sql-errabandoned-m79vqdps), [`redis`](ds:block?id=redis-errabandoned-k85tzs7y) | A step attempt failed because its lease expired more than `WithMaxReclaims` times: every worker that took it died. The retry policy treats it like any failure. Matches with `errors.Is` on the run's error after it ended too (`RunEvent.Err`); `RunInfo.Error` is its text |
| [`ErrDriver`](ds:block?id=pgnotify-errdriver-uyq2k4ce) | `sqlstore/pgnotify` | `Listen` on a pool whose driver is not the pgx stdlib driver, which alone exposes notifications; the client keeps polling |
| [`ErrSchemaTooNew`](ds:block?id=outbox-errschematoonew-fqmc6pdh) | `sqlstore/outbox` | The ledger records an outbox migration this code does not know. `Migrate` refuses it |
| [`ErrCancelTooLate`](ds:block?id=hatchet-errcanceltoolate-s9c8yhrj) | `hatchet` | A delayed start fired although `Cancel` deleted it before its due time (Cancel within 15 s of due, [`dueGuard`](ds:block?id=hatchet-dueguard-kj9nm3dg)); Cancel cancelled the run unless it had ended. Also matches `workflow.ErrWrongState` |

The two `ErrSchemaTooNew` values are different variables in different packages, as are `workflow.ErrThrottled` and `middleware.ErrThrottled`; match the one of the package you called.

## Matching in practice

Test the specific sentinel first and the general one after: `failover.ErrKeyRequired` before `workflow.ErrInvalid` (failover's [`Enqueue`](ds:block?id=failover-client-enqueue-w2azafu4) returns both), `workflow.ErrDuplicate` (already queued: tell the user, do not retry) before a generic failure. For an `ErrUnsupported`, `errors.As` into a [`*workflow.UnsupportedError`](ds:block?id=unsupportederror-46jkqrxt) gives the field and the missing capabilities, which is what an operator needs to change. For an outage, [`workflow.IsUnavailable(err)`](ds:block?id=isunavailable-emny24e5) says the backend is down rather than the request wrong, so retrying later, or letting a failover client handle it, is the right response. The runnable programs show these checks in context: [failover](../../examples/failover/) (`ErrKeyRequired`, `ErrInvalid`, `Unavailable`), [unique-jobs](../../examples/unique-jobs/) (`ErrDuplicate`), [claim-check](../../examples/claim-check/) (`ErrBlobNotFound`), [transactional-enqueue](../../examples/transactional-enqueue/) (`ErrNotFound`) and [outbox](../../examples/outbox/) (`ErrUnsupported`).

## Related

- [Capabilities](capabilities.md): what `UnsupportedError.Missing` names.
- [Retries](../jobs/retries.md): `Permanent`, `RetryAfter` and the retry policy.
- [Failover](../reliability/failover.md#decide-what-counts-as-an-outage): how `ErrUnavailable` decides a failover.
- [Errors in the core contract](../concepts/errors.md).
