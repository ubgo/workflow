# Capabilities

A driver declares what it can really do, as a set of capabilities, and the library compares that set with what each workflow, schedule and job asks for. A workflow that needs something the driver lacks is refused when the client starts; a field the driver cannot honour is refused with an error naming it. Nothing is silently dropped or emulated badly, so the same spec either runs the same way on every driver that accepts it, or fails on the spot. This page is how to read those declarations and check against them before anything runs.

## Before you start

- You know the [client](client.md) lifecycle: `Register`, then [`Start`](ds:block?id=client-start-qges69hj).

## What a driver declares

[`Client.Capabilities()`](ds:block?id=client-capabilities-wnp994at) returns a [`workflow.Caps`](ds:block?id=caps-ssth52ga), a set of capabilities that never changes over the life of a client. [`Has(want)`](ds:block?id=caps-has-z5rv5wur) reports whether it holds every capability in `want`; [`Missing(required)`](ds:block?id=caps-missing-yt996zkr) returns the ones it lacks (`IsZero()` when none); [`String()`](ds:block?id=caps-string-rrz2ge6p) joins the names with `|`. A `Caps` is a value with room for [256 capabilities](ds:block?id=capbits-tqwzzyzd): build one with [`workflow.CapsOf(workflow.CapCron, workflow.CapCronZone)`](ds:block?id=capsof-mmp8bkmj) or [`caps.With(...)`](ds:block?id=caps-with-jwfj42qg), drop some with [`caps.Without(...)`](ds:block?id=caps-without-bkp9zq5y), test overlap with [`HasAny`](ds:block?id=caps-hasany-n3gjxxvw) and intersect with [`Intersect`](ds:block?id=caps-intersect-5vawyy93); two sets compare with `==`. [`workflow.AllCaps()`](ds:block?id=allcaps-umeq9egq) is every capability the release declares, and [`workflow.ParseCaps`](ds:block?id=parsecaps-vryjfj24) reads the `String()` form back. In JSON a `Caps` is that string, such as `"Durable|Cron"`; the numeric form of v0.1.x (when `Caps` was a `uint64`) is still read ([`UnmarshalJSON`](ds:block?id=caps-unmarshaljson-dngyx9tw)).

| Capability | What it promises |
|---|---|
| [`CapDurable`](ds:block?id=capdurable-yhjnwexa) | Runs, waits and sleeps survive a process restart |
| [`CapDelayedStart`](ds:block?id=capdelayedstart-qbxsx6hb) | `Job.RunAt` is honoured |
| [`CapWaitForEvent`](ds:block?id=capwaitforevent-2km3dkzy) | Wait steps suspend without holding a worker until a `Signal` or their deadline |
| [`CapDurableSleep`](ds:block?id=capdurablesleep-k5tkkje8) | Sleep steps suspend without holding a worker |
| [`CapConcurrencyKey`](ds:block?id=capconcurrencykey-5amtud9g) | `Workflow.Concurrency` is enforced per key |
| [`CapRateLimit`](ds:block?id=capratelimit-7jktf585) | `Workflow.RateLimits` are enforced |
| [`CapIdempotencyKey`](ds:block?id=capidempotencykey-5wcqqwy8) | `Job.IdempotencyKey` dedups enqueues |
| [`CapCron`](ds:block?id=capcron-twc2ytyn) | `Workflow.Cron` and `Client.Schedule` fire |
| [`CapCancel`](ds:block?id=capcancel-73j3f5s3) | `Client.Cancel` stops a run |
| [`CapRunFinished`](ds:block?id=caprunfinished-7nrsun98) | The client implements `RunNotifier` with guaranteed delivery: every terminal run, at least once |
| [`CapRetryJitter`](ds:block?id=capretryjitter-4dbyewuw) | `Backoff.Jitter` spreads retry delays |
| [`CapRetryDelay`](ds:block?id=capretrydelay-pp5car73) | `RetryPolicy.DelayFor` sets a retry's delay from the error (`RetryAfter`) |
| [`CapMisfireSkip`](ds:block?id=capmisfireskip-g62y4cwh), [`CapMisfireFireOnce`](ds:block?id=capmisfirefireonce-p94vdm89), [`CapMisfireFireAll`](ds:block?id=capmisfirefireall-9zf4spja) | The matching `Misfire` policy is honoured |
| [`CapCronZone`](ds:block?id=capcronzone-4tkwuz9q) | Crons may run in a zone other than UTC, evaluated in that zone across DST |
| [`CapQueue`](ds:block?id=capqueue-47q2gcgz), [`CapQueueOverride`](ds:block?id=capqueueoverride-7tshsxqq) | `Workflow.Queue` and `Job.Queue` route runs to named queues with per-queue worker limits; with the override a run may leave its workflow's queue ([named queues](../jobs/queues.md)) |
| [`CapQueueLimitGlobal`](ds:block?id=capqueuelimitglobal-xqqyp3bq) | `QueueConfig.Limit` caps a queue's in-flight step attempts across every client sharing the store ([named queues](../jobs/queues.md#cap-a-queue-across-every-worker)) |
| [`CapWorkflowVersion`](ds:block?id=capworkflowversion-6gd7k448) | A run records the `Workflow.Version` of the client that enqueued it, and a worker takes only the run versions its definition runs ([rolling deploys](../operations/deploy.md#deploy-a-new-version-while-old-workers-run)) |
| [`CapJobRetry`](ds:block?id=capjobretry-p3zcdugs), [`CapRetryUntil`](ds:block?id=capretryuntil-btnd8y5w), [`CapRetryMaxErrors`](ds:block?id=capretrymaxerrors-pcdbetra) | `Job.Retry`, `RetryPolicy.Until` and `RetryPolicy.MaxErrors` are honoured ([retries](../jobs/retries.md#limits-beyond-max)) |
| [`CapDiscard`](ds:block?id=capdiscard-9fex6gfu) | A step returning `Discard(err)` ends its run without failing it (`discarded` with `CapRunStates`, else `cancelled`); without it `Discard` acts as `Permanent` |
| [`CapDiscardExhausted`](ds:block?id=capdiscardexhausted-vzuqguga) | `RetryPolicy.DiscardOnExhaust` ends a run whose retries ran out `discarded` instead of `failed` ([retries](../jobs/retries.md#quiet-giving-up-discardonexhaust)) |
| [`CapRunQuery`](ds:block?id=caprunquery-7pyv7hv3), [`CapRetryRun`](ds:block?id=capretryrun-qf2ekfmf), [`CapDeleteRun`](ds:block?id=capdeleterun-t4p72gkj), [`CapCancelAll`](ds:block?id=capcancelall-7enkwcsq) | The client implements `RunQuerier`, `RunRetrier`, `RunDeleter` and `BulkCanceller` ([manage runs](../jobs/manage-runs.md)) |
| [`CapPauseWorkflow`](ds:block?id=cappauseworkflow-xe2cngzv), [`CapPauseQueue`](ds:block?id=cappausequeue-5b6wusw5) | The client implements `Pauser` for workflows and everything, and for queues |
| [`CapScheduleWhen`](ds:block?id=capschedulewhen-z3ehkydm), [`CapScheduleWindow`](ds:block?id=capschedulewindow-rn8htvhs) | `Workflow.ScheduleWhen` is asked at every tick; `Schedule.Windows` and `Workflow.CronWindows` are honoured |
| [`CapTickEvents`](ds:block?id=captickevents-gkgp2seh), [`CapScheduleList`](ds:block?id=capschedulelist-3hbmmd6n), [`CapSchedulePause`](ds:block?id=capschedulepause-h24u69mw) | The client implements `TickNotifier`, `ScheduleLister` and `SchedulePauser` |
| [`CapDrain`](ds:block?id=capdrain-durs9rac) | The client implements `Drainer` (`Drain`, `Done`) |
| [`CapDrainReport`](ds:block?id=capdrainreport-fb2tr3jp) | The client implements `ReportingDrainer`: `DrainWithReport` says which attempts a drain cut off ([stop gracefully](client.md#stop-gracefully)) |
| [`CapRunStates`](ds:block?id=caprunstates-qugshrv4) | Runs report `retrying` while a step waits on its backoff and `discarded` after `Discard` ([run states](../jobs/run-states.md)) |
| [`CapChain`](ds:block?id=capchain-2p7k762t) | `Job.Chain` runs jobs one after another, and steps edit the rest with `PrependToChain` and `AppendToChain` ([chains](../workflows/chains.md)) |
| [`CapWorkerScale`](ds:block?id=capworkerscale-7kxc7pt2) | The client implements `WorkerScaler`, so a `BalanceLoop` can move workers between queues ([worker balancing](../operations/balancing.md)) |
| [`CapClosure`](ds:block?id=capclosure-tr6tdrfy) | The client implements `ClosureEnqueuer`: in-process Go functions run as jobs ([closures](../jobs/closures.md)) |
| [`CapLabelPrefix`](ds:block?id=caplabelprefix-3paef4rr) | `RunQuery.LabelPrefixes` matches runs by a label value prefix in `Runs` and `CancelAll` ([search by label](../jobs/manage-runs.md#search-by-label)) |

None of the drivers needs a server to answer, so you can print them all:

<!-- snippet: examples/capabilities/main.go#declare -->
```go
sqlc, err := sqlstore.New(db, sqlb.SQLiteDialect{})
if err != nil {
	return err
}
drivers := []struct {
	name string
	c    workflow.Client
}{
	{"memory", memory.New()},
	{"sqlstore", sqlc},
	{"hatchet", hatchet.New(hatchet.Config{})}, // no connection before Start
}
for _, d := range drivers {
	caps := d.c.Capabilities()
	fmt.Printf("%-8s durable=%-5v %s\n", d.name, caps.Has(workflow.CapDurable), caps)
}
```

```text
memory   durable=false DelayedStart|WaitForEvent|DurableSleep|ConcurrencyKey|RateLimit|IdempotencyKey|Cron|Cancel|RunFinished|RetryJitter|RetryDelay|MisfireSkip|MisfireFireOnce|MisfireFireAll|CronZone|Queue|QueueOverride|JobRetry|RetryUntil|RetryMaxErrors|Discard|RunQuery|RetryRun|DeleteRun|CancelAll|PauseWorkflow|PauseQueue|SchedulePause|ScheduleWhen|ScheduleWindow|TickEvents|ScheduleList|Drain|RunStates|Chain|WorkerScale|Closure|LabelPrefix|OverlapSkip|ScheduleTrigger|ScheduleHistory|ErrorHistory|ForceRun|Snooze|Heartbeat|Tenant|RunTimeout|NodeIdentity|KeyFIFO|DueQuery|Result|CronSeconds|ScheduleActive|ScheduleKeep|TickTime|QueueLimitGlobal|DiscardExhausted|DrainReport|WorkflowVersion|SlotCost|PayloadSearch|WaitOwnRun|RoundRobin|KeyWeights
sqlstore durable=true  Durable|DelayedStart|WaitForEvent|DurableSleep|ConcurrencyKey|RateLimit|IdempotencyKey|Cron|Cancel|RunFinished|RetryJitter|RetryDelay|MisfireSkip|MisfireFireOnce|MisfireFireAll|CronZone|Queue|QueueOverride|JobRetry|RetryUntil|RetryMaxErrors|Discard|RunQuery|RetryRun|DeleteRun|CancelAll|PauseWorkflow|PauseQueue|SchedulePause|ScheduleWhen|ScheduleWindow|TickEvents|ScheduleList|Drain|RunStates|Chain|WorkerScale|LabelPrefix|OverlapSkip|ScheduleTrigger|ScheduleHistory|EnqueueOnly|ErrorHistory|ShutdownRequeue|ForceRun|Snooze|Heartbeat|Tenant|RunTimeout|NodeIdentity|KeyFIFO|DueQuery|Result|CronSeconds|ScheduleActive|ScheduleKeep|TickTime|QueueLimitGlobal|DiscardExhausted|DrainReport|WorkflowVersion|SlotCost|PayloadSearch|WaitOwnRun|RoundRobin|KeyWeights
hatchet  durable=true  Durable|DelayedStart|WaitForEvent|DurableSleep|ConcurrencyKey|RateLimit|IdempotencyKey|Cron|Cancel|Queue|RunQuery|RetryRun|CancelAll|ScheduleList|Drain|ScheduleTrigger|ScheduleHistory|EnqueueOnly|Tenant|KeyFIFO|DueQuery|Result|CronSeconds|TickTime|DrainReport|WaitOwnRun
```

Each declared capability is proven by the [conformance suite](../extending/writing-a-driver.md#run-the-conformance-suite) against the real backend; the Hatchet driver's against a live engine. Durability is never inferred from a workflow ([`Requires`](ds:block?id=requires-6tcjbcfj) never asks for `CapDurable`): the memory driver runs a workflow that waits a week, and losing it on exit is the app's choice of driver. The full matrix, with what each driver refuses beyond capabilities, is in the [capability reference](../reference/capabilities.md#capability-matrix).

## Check a workflow before you run it

[`workflow.Requires(wf)`](ds:block?id=requires-6tcjbcfj) computes what a workflow needs as written, among others: `CapCron` for a `Cron`, `CapCronZone` for a `CronLocation` other than UTC, the misfire capability of its policy, `CapConcurrencyKey`, `CapRateLimit`, `CapIdempotencyKey` for `IdempotencyRequired`, `CapQueue` for a queue other than the default, `CapRunTimeout`, `CapWorkflowVersion`, `CapWaitForEvent` and `CapDurableSleep` for wait and sleep steps, and the retry capabilities of each step's effective policy (a workflow default every step overrides asks for nothing).

<!-- snippet: examples/capabilities/main.go#requires -->
```go
digest := workflow.Workflow{
	Name:         "digest",
	Cron:         []string{"0 9 * * *"},
	CronLocation: kolkata,
	Misfire:      workflow.Misfire{Policy: workflow.MisfireSkip},
	Retry:        workflow.RetryPolicy{Max: 3, Backoff: workflow.Backoff{Initial: time.Second, Factor: 2, Jitter: 0.2}}, // 3 retries from 1s, doubling, 20% jitter (needs RetryJitter)
	Steps: []workflow.Step{{Name: "send", Run: func(ctx context.Context, in []byte) ([]byte, error) {
		return nil, nil
	}}},
}
need := workflow.Requires(digest)
fmt.Println("digest needs:", need)
for _, d := range drivers {
	if missing := d.c.Capabilities().Missing(need); !missing.IsZero() {
		fmt.Printf("  %s lacks %s\n", d.name, missing)
	}
}
```

```text
digest needs: Cron|RetryJitter|MisfireSkip|CronZone
  hatchet lacks RetryJitter|MisfireSkip|CronZone
```

[`workflow.CheckCaps(have, wf)`](ds:block?id=checkcaps-eg44ezuh) is the same check as an error: nil, or an `UnsupportedError` naming the workflow. Drivers call it from `Start` ([memory](ds:block?id=memory-client-start-umc68snk), [SQL](ds:block?id=sql-client-start-ab8ayzsy); the Hatchet driver already at [`Register`](ds:block?id=hatchet-client-register-zg59b2j4)), so a deploy that registers `digest` on Hatchet fails at start-up, before any run. [`workflow.RequiresJob(job)`](ds:block?id=requiresjob-mrfs7vqd) is the per-job counterpart: `CapDelayedStart` for a `RunAt`, `CapIdempotencyKey` for a key (even when the workflow's policy is `IdempotencyOff`, because a key is the caller's request and is never dropped without an error), plus `CapQueue`, `CapTenant`, `CapJobRetry` and `CapChain` for a queue, a tenant, a per-job retry policy and a chain. Drivers check it in `Enqueue` ([`CheckJob`](ds:block?id=checkjob-2rsfu9ma)).

<!-- snippet: examples/capabilities/main.go#job -->
```go
job := workflow.Job{Workflow: "digest", RunAt: time.Date(2026, 12, 1, 9, 0, 0, 0, kolkata), IdempotencyKey: "digest-2026-12-01"}
fmt.Println("job needs:", workflow.RequiresJob(job))
```

```text
job needs: DelayedStart|IdempotencyKey
```

## Check a schedule

A schedule made at run time goes through the same check. [`workflow.RequiresSchedule(s)`](ds:block?id=requiresschedule-wk3hrhfy) is `CapCron`, plus `CapCronZone` for a `Location` other than UTC, the capability of its misfire policy, and one capability each for `Windows`, `Overlap`, `Seconds`, `Active`, `KeepRuns` and `Tenant`; [`workflow.CheckScheduleCaps(have, s)`](ds:block?id=checkschedulecaps-45emsz38) returns an `UnsupportedError` whose `Field` names the cause (`Schedule`, `Schedule.Location`, `Schedule.Misfire`, or the field among those six). Drivers call it from `Client.Schedule`, after `ValidateSchedule`.

<!-- snippet: examples/capabilities/main.go#schedule -->
```go
nightly := workflow.Schedule{Name: "acme-nightly", Workflow: "digest", Cron: "0 2 * * *", Location: kolkata}
fmt.Println("schedule needs:", workflow.RequiresSchedule(nightly))
fmt.Println("on hatchet:", workflow.CheckScheduleCaps(hatchet.New(hatchet.Config{}).Capabilities(), nightly))
fmt.Println("on memory: ", workflow.CheckScheduleCaps(memory.New().Capabilities(), nightly))
```

```text
schedule needs: Cron|CronZone
on hatchet: workflow: Schedule.Location unsupported by this driver (missing CronZone): schedule for workflow digest
on memory:  <nil>
```

## Refusals

A refusal is a [`*workflow.UnsupportedError`](ds:block?id=unsupportederror-46jkqrxt): `Field` is the spec field or feature, `Missing` the capabilities that would be needed (zero when the refusal is not about a capability), `Reason` a detail such as the workflow name. It matches `workflow.ErrUnsupported` under `errors.Is`, and its [message](ds:block?id=unsupportederror-error-yc4b5g68) alone tells an operator what to change. [`workflow.Unsupported(field, reason)`](ds:block?id=unsupported-4r9efjk8) builds one without a capability, for a driver refusing a field it cannot express.

<!-- snippet: examples/capabilities/main.go#refusal -->
```go
err = workflow.CheckCaps(hatchet.New(hatchet.Config{}).Capabilities(), digest)
if ue, ok := errors.AsType[*workflow.UnsupportedError](err); ok {
	fmt.Printf("refused: field=%s missing=%s reason=%q\n", ue.Field, ue.Missing, ue.Reason)
}
fmt.Println("is ErrUnsupported:", errors.Is(err, workflow.ErrUnsupported))

// The Hatchet driver also refuses at Register what its engine cannot
// express, naming the field: here a Go key function, where Hatchet
// takes CEL expressions (workflow.KeyExpr).
perTenant := workflow.Workflow{
	Name:        "sync",
	Concurrency: &workflow.Concurrency{Key: workflow.KeyFunc(func(j workflow.Job) string { return j.Labels["tenant"] }), MaxRuns: 1},
	Steps:       digest.Steps,
}
fmt.Println("hatchet Register:", hatchet.New(hatchet.Config{}).Register(perTenant))
```

```text
refused: field=Workflow missing=RetryJitter|MisfireSkip|CronZone reason="workflow digest"
is ErrUnsupported: true
hatchet Register: workflow: Workflow.Concurrency.Key.Func unsupported by this driver: workflow sync: Hatchet evaluates concurrency keys on the engine; use workflow.KeyExpr with a CEL expression
```

Where each driver refuses:

| Driver | At `Register` | At `Start` | At `Enqueue` and `Schedule` |
|---|---|---|---|
| memory | invalid specs, bad cron expressions ([`Register`](ds:block?id=memory-client-register-ngcr2sck)) | the capability check (it passes, since only `CapDurable`, `CapEnqueueOnly` and `CapShutdownRequeue` are missing and nothing requires them), an expression key (`KeyExpr`) on concurrency, rate limits or a wait ([`Start`](ds:block?id=memory-client-start-umc68snk)) | invalid schedules ([`Schedule`](ds:block?id=memory-client-schedule-vwvtn5w4)) |
| sqlstore | invalid specs, bad cron expressions ([`Register`](ds:block?id=sql-client-register-buvwt9ee)) | the capability check (it has every capability but `CapClosure`: [`caps`](ds:block?id=sql-caps-yu37ef2j)), an expression key (`KeyExpr`) ([`Start`](ds:block?id=sql-client-start-ab8ayzsy)) | invalid schedules ([`Schedule`](ds:block?id=sql-client-schedule-tjaxged6)) |
| hatchet | what Hatchet cannot express ([`checkSupported`](ds:block?id=hatchet-checksupported-733uwgsp): a Go key function on concurrency, since keys are CEL; a non-UTC `CronLocation`; any `Misfire`; cron macros and `@every`; a backoff whose `Initial` is not `Factor` seconds; `Backoff.Jitter`; `DelayFor`; a rate-limit window other than a second, minute, hour, day or week), then the capability check ([`Register`](ds:block?id=hatchet-client-register-zg59b2j4)) | `Config` tunings past the SDK's limits ([`Start`](ds:block?id=hatchet-client-start-mjwjna3p)) | a negative `Job.Priority`, `IdempotencyKey` with `RunAt` on a workflow with a `Concurrency` or past `IdempotencyMaxHold` ([`Enqueue`](ds:block?id=hatchet-client-enqueue-ym8nfcz9)), a `Schedule.Location` other than UTC, any `Schedule.Misfire` ([`Schedule`](ds:block?id=hatchet-client-schedule-ug2ype7f)) |

The Hatchet list is the driver's own documentation (`hatchet/doc.go`), and the [Hatchet guide](../drivers/hatchet.md#what-it-can-do-and-what-it-refuses) explains each.

## Try it

```sh
cd examples
go run ./capabilities
```

The program prints every block above in order, then the error examples of [errors](errors.md) and [labels](labels.md).

## Pitfalls

- A capability check is not the only refusal. `Requires` sees capabilities; a driver may also refuse a field it cannot express (a Go key function on Hatchet, [`checkSupported`](ds:block?id=hatchet-checksupported-733uwgsp); a `KeyExpr` on the [memory](ds:block?id=memory-exprkeys-mrxv22pq), SQL and [Redis](ds:block?id=redis-exprkeys-g3qq6r7w) drivers). Register on the real driver in a test to catch both.
- `CapRunFinished` is about the guarantee, not the method. The Hatchet client implements `OnRunFinished` but does not declare `CapRunFinished` ([`caps`](ds:block?id=hatchet-caps-d6x9fv8z)), because its delivery is best-effort; code that needs every end must check the capability, not only the type assertion.
- [`Caps.Missing`](ds:block?id=caps-missing-yt996zkr) takes the required set, not the available one: `have.Missing(need)`.
- A wrapper declares what it really guarantees. [`failover.Client`](ds:block?id=failover-client-capabilities-cb8n9pqm) declares the intersection of its two backends, so a workflow that runs on each alone may be refused by the pair.
- In app tests, `workflowtest.NewFake(workflowtest.FakeLike(prod))` ([`FakeLike`](ds:block?id=workflowtest-fakelike-my3v73nf)) refuses what the production driver's capabilities refuse, so a test cannot pass with a field production would reject ([Behave like your production driver](../testing/fake.md#behave-like-your-production-driver)).

## Related

- [Capability reference](../reference/capabilities.md#capability-matrix)
- [Choose a driver](../choosing/drivers.md#memory-sql-redis-or-hatchet)
- [Errors](errors.md#sentinel-errors)
- Example: [capabilities](../../examples/capabilities/)
