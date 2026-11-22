# Test batches, chains and a subset of workflows

App code that dispatches batches or enqueues a multi-step workflow should be tested without running the jobs. `workflowtest.Fake` records the jobs a `batch.Manager` enqueues through it and [groups them into batches](ds:block?id=workflowtest-fake-batches-beb63n2s), [checks a workflow's step graph](ds:block?id=workflowtest-fake-assertchained-fqr42bgd) as a chain, and can [pass chosen workflows through](ds:block?id=workflowtest-fakeexcept-aubjjne6) to a real client while faking the rest.

## Before you start

- App code that takes a `workflow.Client` and a `*batch.Manager` rather than a concrete driver ([`batch.New`](ds:block?id=batch-new-geazwpek) builds the Manager over any client, the Fake included), as in [Test app code with the Fake](fake.md).
- The runnable program for this page is [examples/batch-tests](../../examples/batch-tests/). Its `main.go` holds the app code; its `main_test.go` holds the tests below.

The app code under test uploads files as one batch and publishes an album through a three-step chain:

<!-- snippet: examples/batch-tests/main.go#app -->
```go
// Gallery is app code: it depends on workflow.Client and a batch.Manager,
// never on a driver, so a test can hand it a Fake.
type Gallery struct {
	Jobs    workflow.Client
	Batches *batch.Manager
}

// Upload resizes every file in one batch and notifies the owner when all
// are done.
func (g *Gallery) Upload(ctx context.Context, owner string, files ...string) (batch.ID, error) {
	b := batch.Batch{Name: "upload", Then: "notify", Labels: map[string]string{"owner": owner}}
	for _, f := range files {
		b.Jobs = append(b.Jobs, workflow.Job{Workflow: "resize", Payload: []byte(f)})
	}
	return g.Batches.Dispatch(ctx, b)
}

// Publish runs the publish chain for an album.
func (g *Gallery) Publish(ctx context.Context, album string) error {
	_, err := g.Jobs.Enqueue(ctx, workflow.Job{Workflow: "publish", Payload: []byte(album)})
	return err
}

// PublishWorkflow is a chain: watermark, then upload to the CDN, then
// announce.
func PublishWorkflow(step workflow.Handler) workflow.Workflow {
	return workflow.Workflow{Name: "publish", Steps: []workflow.Step{
		{Name: "watermark", Run: step},
		{Name: "cdn", After: []string{"watermark"}, Run: step},
		{Name: "announce", After: []string{"cdn"}, Run: step},
	}}
}
```

## Step 1: assert a batch was dispatched

Build the `batch.Manager` over the Fake. It enqueues each job through the Fake with the batch labels ([`Manager.Dispatch`](ds:block?id=batch-manager-dispatch-pdbtg7ks)), so [`fake.Batches()`](ds:block?id=workflowtest-batchesof-c7phrxex) lists each batch with its jobs in index order, including jobs added later with `Manager.Add`:

<!-- snippet: examples/batch-tests/main_test.go#batched -->
```go
func TestUploadDispatchesOneBatch(t *testing.T) {
	ctx := context.Background()
	fake := workflowtest.NewFake()
	if err := Register(fake, resizeOK, noop); err != nil {
		t.Fatal(err)
	}
	if err := fake.Start(ctx); err != nil {
		t.Fatal(err)
	}
	g := &Gallery{Jobs: fake, Batches: batch.New(fake, batch.NewMemoryStore())}

	fake.AssertNothingBatched(t)
	if _, err := g.Upload(ctx, "ada", "a.png", "b.png", "c.png"); err != nil {
		t.Fatal(err)
	}
	fake.AssertBatchCount(t, 1)
	fake.AssertBatched(t,
		workflowtest.BatchSize(3),
		workflowtest.BatchHasJob("resize", workflowtest.HasPayload([]byte("b.png"))),
	)
	fake.AssertNotEnqueued(t, "notify") // nothing ran, so the batch has not finished
}
```

The batch matchers:

| Matcher | Matches a batch |
|---|---|
| [`BatchSize(n)`](ds:block?id=workflowtest-batchsize-vnpqtsa6) | of exactly n jobs |
| [`BatchHasJob(wf, ...JobMatcher)`](ds:block?id=workflowtest-batchhasjob-uppr49yf) | with a job of workflow wf matching every job matcher (`HasPayload`, `HasLabel`, `HasKey` and the rest) |
| [`BatchWhere(desc, fn)`](ds:block?id=workflowtest-batchwhere-p3ekhvzb) | for which fn is true; desc appears in failure messages |

The batch's name and callbacks are kept in the Manager's store, not on its jobs ([`FakeBatch`](ds:block?id=workflowtest-fakebatch-m2ufyk2z)); read them with [`Manager.Progress(ctx, id)`](ds:block?id=batch-manager-progress-etkyu46k) when a test needs them. Nothing runs on the Fake, so a batch never finishes and its callbacks are never enqueued.

## Step 2: assert a chain

A chain in this library is one workflow whose steps each run after the one before. [`fake.AssertChained(t, wf, steps...)`](ds:block?id=workflowtest-fake-assertchained-fqr42bgd) passes when workflow wf is registered with exactly those steps, in declaration order, each after the previous one, and at least one job of wf was enqueued:

<!-- snippet: examples/batch-tests/main_test.go#chained -->
```go
func TestPublishRunsTheChain(t *testing.T) {
	ctx := context.Background()
	fake := workflowtest.NewFake()
	if err := Register(fake, resizeOK, noop); err != nil {
		t.Fatal(err)
	}
	if err := fake.Start(ctx); err != nil {
		t.Fatal(err)
	}
	g := &Gallery{Jobs: fake}
	if err := g.Publish(ctx, "summer"); err != nil {
		t.Fatal(err)
	}
	fake.AssertChained(t, "publish", "watermark", "cdn", "announce")
}
```

A fan-out (two steps after the same parent), a missing step, a different order or an unregistered workflow fails with the reason ([`checkChain`](ds:block?id=workflowtest-checkchain-9dhh5hyg)). [`fake.Workflow(name)`](ds:block?id=workflowtest-fake-workflow-sg3v3jrr) returns the registered definition for any other check.

A [job chain](../workflows/chains.md) is data on the job rather than workflow shape, so it is asserted with a matcher: `fake.AssertEnqueued(t, "import", workflowtest.HasChain("index", "notify"))` passes ([`HasChain`](ds:block?id=workflowtest-haschain-fjhj8q6e)) when an `import` job was enqueued whose `Job.Chain` runs `index` and then `notify`.

<!-- snippet: examples/chain-edits/main_test.go#fake-chain -->
```go
func TestImportEnqueuesItsChain(t *testing.T) {
	ctx := context.Background()
	fake := workflowtest.NewFake()
	for _, name := range []string{"import-file", "notify"} {
		if err := fake.Register(workflow.Workflow{Name: name, Steps: []workflow.Step{{Name: "run", Run: noop}}}); err != nil {
			t.Fatal(err)
		}
	}
	if err := fake.Start(ctx); err != nil {
		t.Fatal(err)
	}
	job := workflow.Job{Workflow: "import-file", Chain: []workflow.Job{{Workflow: "notify"}}}
	if _, err := fake.Enqueue(ctx, job); err != nil {
		t.Fatal(err)
	}
	fake.AssertEnqueued(t, "import-file", workflowtest.HasChain("notify"))
}
```

## Step 3: fake some workflows and run the others

[`workflowtest.FakeExcept(inner, names...)`](ds:block?id=workflowtest-fakeexcept-aubjjne6) passes the named workflows through to a real client and fakes every other one; [`workflowtest.FakeOnly(inner, names...)`](ds:block?id=workflowtest-fakeonly-xp78nstd) fakes only the named ones. Passed-through work is registered, started, enqueued and scheduled on the real client and is not recorded ([`Enqueue`](ds:block?id=workflowtest-fake-enqueue-at6ywx4h)). [`Status`](ds:block?id=workflowtest-fake-status-7afzbsy4) and [`Cancel`](ds:block?id=workflowtest-fake-cancel-qt74g66m) of a run the Fake did not create go to the real client, and [`Signal`](ds:block?id=workflowtest-fake-signal-jwnzmf47) reaches both.

Here the resize jobs run for real on the memory driver and the batch's `Then` callback is faked, so the test sees it recorded once the real jobs end:

<!-- snippet: examples/batch-tests/main_test.go#passthrough -->
```go
// TestUploadNotifiesWhenResizesFinish runs the resize jobs for real on
// the memory driver and fakes everything else, so the test sees the
// batch's Then callback recorded once the real jobs ended.
func TestUploadNotifiesWhenResizesFinish(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fake := workflowtest.NewFake(workflowtest.FakeExcept(memory.New(), "resize"))
	if err := Register(fake, resizeOK, noop); err != nil {
		t.Fatal(err)
	}
	// The Fake reports no run ends, so the Manager polls Status, which
	// the Fake answers from the memory driver for the resize runs.
	m := batch.New(fake, batch.NewMemoryStore(), batch.WithStatusPoll(10*time.Millisecond))
	if err := m.Attach(ctx); err != nil {
		t.Fatal(err)
	}
	if err := fake.Start(ctx); err != nil {
		t.Fatal(err)
	}
	g := &Gallery{Jobs: fake, Batches: m}
	if _, err := g.Upload(ctx, "ada", "a.png", "b.png"); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for len(fake.Enqueued("notify")) == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	fake.AssertEnqueued(t, "notify", workflowtest.HasLabel("owner", "ada"))
	fake.AssertNotEnqueued(t, "resize") // passed through, so not recorded
}
```

The Fake reports no run ends (it [never claims `CapRunFinished`](ds:block?id=workflowtest-fakeunserved-jwdtfx8n)), so the Manager learns of them by polling `Status`, which the Fake answers from the real client for passed-through runs; [`WithStatusPoll`](ds:block?id=batch-withstatuspoll-yu9gvj3a) keeps that quick.

## Try it

```sh
cd examples
go test -v -run 'Upload|Publish' ./batch-tests
```

```text
=== RUN   TestUploadDispatchesOneBatch
--- PASS: TestUploadDispatchesOneBatch (0.00s)
=== RUN   TestPublishRunsTheChain
--- PASS: TestPublishRunsTheChain (0.00s)
=== RUN   TestUploadNotifiesWhenResizesFinish
--- PASS: TestUploadNotifiesWhenResizesFinish (0.02s)
PASS
ok  	github.com/ubgo/workflow/examples/batch-tests	0.030s
```

When an assertion fails it [lists what was recorded](ds:block?id=workflowtest-listbatches-59j5y7tv), for example `no batch with 5 jobs was dispatched; 1 batches:` followed by each batch's ID, size and jobs per workflow.

## Pitfalls

- **`Dispatch` over the Fake needs no `Attach`.** [`Dispatch`](ds:block?id=batch-manager-dispatch-pdbtg7ks) saves the batch and enqueues its labelled jobs; [`Attach`](ds:block?id=batch-manager-attach-xnchrce9) connects the sources that report job ends, so attach only when the test needs the batch to learn of them, as with pass-through.
- **Passed-through jobs are not recorded.** `AssertEnqueued` on a passed-through workflow fails even though it ran ([`Enqueue`](ds:block?id=workflowtest-fake-enqueue-at6ywx4h) hands it to the real client unrecorded); assert on what it caused instead.
- **The Fake's capabilities apply to faked workflows only.** A passed-through workflow is checked by the real client at its own [`Register`](ds:block?id=workflowtest-fake-register-vntnszv5), `Start` and `Enqueue`.
- **`AssertChained` checks declaration order.** A workflow whose steps form the same chain but are declared in another order fails ([`checkChain`](ds:block?id=workflowtest-checkchain-9dhh5hyg) compares the declared step names first); declare chain steps in order.

## Related

- [Test app code with the Fake](fake.md): the Fake's refusals, job assertions and matchers.
- [Test one step](run-step.md): `RunStep` for a handler's release, failure and permanent failure.
- [Test queues, retries and pauses](operations.md): queue, retry-budget, pause, retry and delete assertions.
- [The Recorder](recorder.md): wrap a real client; it has the same batch and chain assertions.
- [Batches](../batches/batches.md): what a batch is and how it finishes.
- Example: [examples/batch-tests](../../examples/batch-tests/).
