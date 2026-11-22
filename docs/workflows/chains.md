# Chains of jobs

A workflow's steps are code, fixed when it registers. Sometimes the order of work is only known while it runs: an import step finds a second file to import first, a payment step learns the order also needs a fraud check, a crawler finds more pages. A chain is a list of jobs that run one after another, each starting only once the one before it succeeded. The chain is data stored with the run, so a running step can add jobs to the front or the end of what comes after it. This page covers building a chain, editing it from a step, what happens on failure, and what to do on Hatchet, which cannot edit a run's shape.

## Before you start

- Chains need [`workflow.CapChain`](ds:block?id=capchain-2p7k762t). The [memory](ds:block?id=memory-caps-yxkuq583), [SQL](ds:block?id=sql-batch2caps-9rm4b4ck) and [Redis](ds:block?id=redis-caps-bfgdst9t) drivers declare it; [Hatchet](ds:block?id=hatchet-caps-d6x9fv8z) does not (see [Hatchet: use child runs](#hatchet-use-child-runs)). The rest of what Hatchet refuses is under [Hatchet limitations](../drivers/hatchet.md#limitations).
- Each job in a chain names a registered workflow, as any `Job` does. A chain of one-step workflows is the simplest pipeline; a chained job may itself be a many-step workflow.
- The example is [`examples/chain-edits`](../../examples/chain-edits). Run it with `go run ./chain-edits` from `examples/`.

## Step 1: enqueue a job with a chain

`Job.Chain` lists the jobs that run after this one, in order:

<!-- snippet: examples/chain-edits/main.go#chain -->
```go
_, err := c.Enqueue(ctx, workflow.Job{
	Workflow: "import-file",
	Payload:  []byte("a.csv"),
	// Each job runs after the one before it succeeds.
	Chain: []workflow.Job{
		{Workflow: "notify", Payload: []byte("imported")},
	},
})
if err != nil {
	return err
}
```

```text
chain: import-file(a.csv) -> notify(imported)
```

When the run succeeds the driver enqueues `Chain[0]`, carrying the rest of the chain as its own `Chain`. [`workflow.NextInChain(chain)`](ds:block?id=nextinchain-54sbhgpq) is that rule, and every driver calls it, so a chain flattens the same way everywhere. Each chained job is a full `Job`: its own payload, queue, labels, priority, retry policy and even its own chain.

`Enqueue` validates every chained job ([`ValidateJob`](ds:block?id=validatejob-a7nq3u2t)) and checks it against the driver's capabilities ([`CheckJob`](ds:block?id=checkjob-2rsfu9ma)), so a chain that cannot run is refused at once rather than half way. A chain holds at most [`workflow.MaxChainJobs`](ds:block?id=maxchainjobs-sg4br535) (1000) jobs, nested chains included ([`validateChain`](ds:block?id=validatechain-yz5jdvbx)). [`workflow.CloneJob`](ds:block?id=clonejob-hen2tqys) deep-copies a job and its chain, for code that builds chains from a template.

## Step 2: edit the chain from a running step

Inside a step, [`workflow.PrependToChain(ctx, jobs...)`](ds:block?id=prependtochain-emnak72a) puts jobs in front of the rest of the chain, so `jobs[0]` runs next, and [`workflow.AppendToChain(ctx, jobs...)`](ds:block?id=appendtochain-c4v6fktj) puts them at the end:

<!-- snippet: examples/chain-edits/main.go#edit -->
```go
importFiles := workflow.Workflow{
	Name:  "import-files",
	Retry: workflow.RetryPolicy{Max: 1},
	Steps: []workflow.Step{{Name: "scan", Run: func(ctx context.Context, in []byte) ([]byte, error) {
		ran.add("import-files(" + string(in) + ")")
		// The scan found a second file: import it next, before the
		// rest of the chain, and rebuild the search index at the end.
		if err := workflow.PrependToChain(ctx, workflow.Job{Workflow: "import-file", Payload: []byte("b.csv")}); err != nil {
			return nil, err
		}
		if err := workflow.AppendToChain(ctx, workflow.Job{Workflow: "reindex", Payload: []byte("all")}); err != nil {
			return nil, err
		}
		if importAttempts.Add(1) == 1 {
			// This attempt fails, so its edits are dropped; the retry
			// makes them again and they apply once.
			return nil, errors.New("storage timeout")
		}
		return nil, nil
	}}},
}
```

```text
edited while running (scan tried 2 times): import-files(inbox) -> import-files(inbox) -> import-file(b.csv) -> notify(imported) -> reindex(all)
```

Rules every driver follows:

- **Edits belong to the attempt** ([`ChainEditor`](ds:block?id=chaineditor-76qwdvb8)). They apply only when the step attempt succeeds, atomically with recording that success. A step that fails and is retried makes its edits again on the next attempt, and they apply once, as the example shows.
- **Several prepends stack.** The jobs of the latest `PrependToChain` call run first ([`ChainEdits.Prepend`](ds:block?id=chainedits-prepend-vjmh7rtr)).
- **A bad job is refused at once.** Each added job is validated and checked against the driver's capabilities when the step adds it ([`ChainEdits.Prepend`](ds:block?id=chainedits-prepend-vjmh7rtr)), so the step sees the error and can return it.
- **On the SQL driver the chain survives a crash.** The rest of the chain is stored with the run, the edits are saved with the step's success ([`applyChainEdits`](ds:block?id=sql-client-applychainedits-geebnunb)), and the next job is enqueued in the same transaction that marks the run succeeded ([`handoffChain`](ds:block?id=sql-client-handoffchain-62729cn6)).

Outside a step, or on a driver without `CapChain`, both functions return an `UnsupportedError` naming `CapChain` ([`noChain`](ds:block?id=nochain-q2kw9w9s)):

<!-- snippet: examples/chain-edits/main.go#outside -->
```go
err = workflow.AppendToChain(ctx, workflow.Job{Workflow: "notify"})
```

## When a run fails

Only a succeeded run moves the chain on ([memory](ds:block?id=memory-client-continuechainlocked-gbb7zhe2), [SQL](ds:block?id=sql-client-handoffchain-62729cn6)). A run that fails, is [discarded](../jobs/run-states.md#discarded) or is cancelled ends the chain, and the jobs after it never run:

<!-- snippet: examples/chain-edits/main.go#failed -->
```go
if _, err := c.Enqueue(ctx, workflow.Job{
	Workflow: "charge",
	Payload:  []byte("o-1"),
	Chain:    []workflow.Job{{Workflow: "notify", Payload: []byte("charged")}},
}); err != nil {
	return err
}
```

```text
a failed run ends the chain: charge(o-1) (failed)
```

To react to the failure, give the failing workflow an `OnFailure` step or watch run ends with `OnRunFinished`. Retrying the failed run with `RunRetrier.Retry` resumes the chain if the retried run succeeds.

## Hatchet: use child runs

Hatchet fixes a run's shape when the workflow registers, so the Hatchet driver does not declare `CapChain`. A job with a chain is refused by `Enqueue` (the check is [`CheckJob`](ds:block?id=checkjob-2rsfu9ma)), and `PrependToChain` and `AppendToChain` return `UnsupportedError`:

<!-- snippet: examples/chain-edits/main.go#hatchet -->
```go
job := workflow.Job{Workflow: "import-file", Chain: []workflow.Job{{Workflow: "notify"}}}
err = workflow.CheckJob(hatchet.New(hatchet.Config{}).Capabilities(), job)
```

On Hatchet, a step that discovers more work enqueues it itself: it calls `Enqueue` on the client it captured, with an `IdempotencyKey` built from its run ID ([`workflow.StepInfoFrom(ctx)`](ds:block?id=stepinfofrom-rr9mwh2c)) so a retried step does not start the work twice. For work that must all finish before something else runs, enqueue it as a [batch](../batches/batches.md) and wait with `AwaitStep`.

## Chains, steps and batches

| You have | Use |
|---|---|
| A fixed sequence known when you write the code | One workflow with steps that run `After` each other ([steps](steps.md)) |
| A sequence decided when the job is enqueued, or changed while it runs | `Job.Chain` |
| Many independent jobs, and something to do when all are done | A [batch](../batches/batches.md) |

`batch.Dispatch` refuses a job with a chain, because a batch tracks exactly one run per job ([`checkJobs`](ds:block?id=batch-checkjobs-kjz2x7m5)).

## Try it

```sh
cd examples
go run ./chain-edits
```

```text
chain: import-file(a.csv) -> notify(imported)
edited while running (scan tried 2 times): import-files(inbox) -> import-files(inbox) -> import-file(b.csv) -> notify(imported) -> reindex(all)
a failed run ends the chain: charge(o-1) (failed)
AppendToChain outside a step: true, missing Chain
on hatchet: ErrUnsupported true (workflow: Job.Chain unsupported by this driver (missing Chain): job for workflow import-file)
```

Each line is one part of this page: the chain enqueued in step 1; the scan whose first attempt failed after an edit, so only the second attempt's edits ran (`import-file(b.csv)` in front, `reindex(all)` at the end); the chain that stopped at its failed first job; an edit refused outside a step; and the check that refuses a chain on Hatchet.

## Pitfalls

- **Enqueue middleware runs for the first job only.** The driver enqueues later jobs itself ([memory](ds:block?id=memory-client-continuechainlocked-gbb7zhe2), [SQL](ds:block?id=sql-client-insertchained-x67wtq8j)), as `EnqueueTx` does, so middleware such as `Unique` or `Debounce` does not see them. Payload codecs and the claim check are the exception: they rewrite every chained payload, including jobs a step adds.
- **A chain is not a transaction.** Each job commits on its own. If job three fails, jobs one and two have already done their work; design each job to be safe on its own.
- **Payloads are fixed when the job is added.** A chained job does not receive the previous job's output ([`NextInChain`](ds:block?id=nextinchain-54sbhgpq) copies the stored job as it is). Pass what it needs in its payload, or read it from your own store.
- **On the SQL driver the next workflow must be registered somewhere.** When the client that finishes a run has not registered the next job's workflow, the chain waits in its row until a client that registers it hands it on ([`handoffChain`](ds:block?id=sql-client-handoffchain-62729cn6)). A next job that its workflow refuses (a required idempotency key missing, for example) ends the chain, with the error in the log ([`insertChained`](ds:block?id=sql-client-insertchained-x67wtq8j)).
- **Edits from a failed attempt are dropped** ([`ChainEditor`](ds:block?id=chaineditor-76qwdvb8)). Do not rely on an edit made before an error; the retry must make it again.

## Related

- [Workflows and steps](steps.md): a fixed sequence inside one run.
- [Run states](../jobs/run-states.md): which ends stop a chain.
- [Batches](../batches/batches.md): many jobs at once.
- [Test batches, chains and a subset of workflows](../testing/batches-and-chains.md): assert chains in tests.
