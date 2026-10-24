# Test app code with the Fake

App code that enqueues jobs, signals runs or makes schedules should be tested without running anything: the test wants to know what the code asked for, not wait for a worker. `workflowtest.NewFake()` returns a `workflow.Client` that records every call and runs nothing, with assertions to check what was recorded.

Unlike a mock, the Fake refuses what a real driver refuses, so a test cannot pass on the Fake while production fails: `Register` validates the workflow and its cron expressions, `Start` checks every workflow against the claimed capabilities, `Enqueue` before `Start` returns `workflow.ErrNotStarted`, an unregistered workflow is `workflow.ErrNotFound`, a job or schedule needing a missing capability is an `UnsupportedError`, and a workflow with `IdempotencyRequired` refuses a job without a key. Keys dedup as on a real driver: a key held by an unfinished run returns that run's `RunRef` and records nothing.

## Before you start

- App code that takes the `workflow.Client` interface rather than a concrete driver, and a `clock.Clock` rather than calling `time.Now`, so a test can hand it a Fake and a [fake clock](fake-clock.md).
- The runnable program for this page is [examples/app-tests](../../examples/app-tests/). Its `main.go` holds the app code; its `main_test.go` holds the tests below.

The app code under test signs a user up (a welcome email now, a reminder in a day, each with an idempotency key), confirms the address (a signal, and a cancel of the reminder) and schedules a weekly digest:

<!-- snippet: examples/app-tests/main.go#app -->
```go
// Accounts is app code. It depends on the workflow.Client interface and a
// clock.Clock, never on a driver or on time.Now, so a test can swap both.
type Accounts struct {
	Jobs  workflow.Client
	Clock clock.Clock
}

// SignUp sends the welcome email now and a reminder in a day. The keys
// make a double submit enqueue one of each.
func (a *Accounts) SignUp(ctx context.Context, email string) (reminder workflow.RunRef, err error) {
	_, err = a.Jobs.Enqueue(ctx, workflow.Job{
		Workflow:       "welcome-email",
		Payload:        fmt.Appendf(nil, `{"email":%q}`, email),
		IdempotencyKey: "welcome:" + email,
		Labels:         map[string]string{"source": "signup"},
		Priority:       10,
	})
	if err != nil {
		return workflow.RunRef{}, err
	}
	return a.Jobs.Enqueue(ctx, workflow.Job{
		Workflow:       "reminder",
		Payload:        fmt.Appendf(nil, `{"email":%q}`, email),
		IdempotencyKey: "reminder:" + email,
		RunAt:          a.Clock.Now().Add(24 * time.Hour),
	})
}

// Confirm resumes the run waiting for this address's confirmation, and
// drops the reminder.
func (a *Accounts) Confirm(ctx context.Context, email string, reminder workflow.RunRef) error {
	if err := a.Jobs.Signal(ctx, workflow.Event{Name: "email-confirmed", Key: email}); err != nil {
		return err
	}
	return a.Jobs.Cancel(ctx, reminder)
}

// StartDigest schedules the weekly digest for one team.
func (a *Accounts) StartDigest(ctx context.Context, team string) (workflow.ScheduleID, error) {
	return a.Jobs.Schedule(ctx, workflow.Schedule{
		Workflow: "digest",
		Cron:     "0 9 * * MON",
		Location: time.UTC,
		Labels:   map[string]string{"team": team},
	})
}
```

## Assertions

Register the app's workflows on a Fake, start it, run the code, then assert. Every assertion takes a `testing.TB` and reports the recorded calls when it fails:

<!-- snippet: examples/app-tests/main_test.go#assertions -->
```go
func TestSignUpEnqueuesWelcomeAndReminder(t *testing.T) {
	ctx := context.Background()
	fake := workflowtest.NewFake()
	if err := startOn(ctx, fake); err != nil { // Register every workflow, then Start
		t.Fatal(err)
	}
	app := &Accounts{Jobs: fake, Clock: clock.NewFake(start)}

	fake.AssertNothingEnqueued(t)
	if _, err := app.SignUp(ctx, "ada@example.com"); err != nil {
		t.Fatal(err)
	}

	fake.AssertEnqueued(t, "welcome-email",
		workflowtest.HasPayloadJSON(map[string]string{"email": "ada@example.com"}),
		workflowtest.HasKey("welcome:ada@example.com"),
		workflowtest.HasLabel("source", "signup"),
		workflowtest.HasPriority(10))
	fake.AssertEnqueued(t, "reminder", workflowtest.HasRunAt(start.Add(24*time.Hour)))
	fake.AssertNotEnqueued(t, "nudge")
}
```

| Assertion | Fails unless |
|---|---|
| `AssertEnqueued(t, wf, m...)` | At least one job of workflow `wf` matches every matcher |
| `AssertEnqueuedTimes(t, wf, n, m...)` | Exactly `n` jobs of `wf` match every matcher |
| `AssertNotEnqueued(t, wf, m...)` | No job of `wf` matches every matcher |
| `AssertNothingEnqueued(t)` | No job was enqueued at all |
| `AssertScheduled(t, wf)` | A schedule of `wf` was recorded and not unscheduled |
| `AssertSignalled(t, name, key)` | An event with that name and key was signalled |
| `AssertCancelled(t, ref)` | That run was cancelled |
| `AssertClosureEnqueued(t, name)` | A [closure](../jobs/closures.md) with that name was enqueued |

<!-- snippet: examples/app-tests/main_test.go#signals -->
```go
func TestConfirmSignalsAndCancels(t *testing.T) {
	ctx := context.Background()
	app, fake := newApp(t)
	reminder, err := app.SignUp(ctx, "ada@example.com")
	if err != nil {
		t.Fatal(err)
	}
	if err := app.Confirm(ctx, "ada@example.com", reminder); err != nil {
		t.Fatal(err)
	}
	if _, err := app.StartDigest(ctx, "core"); err != nil {
		t.Fatal(err)
	}

	fake.AssertSignalled(t, "email-confirmed", "ada@example.com")
	fake.AssertCancelled(t, reminder)
	fake.AssertScheduled(t, "digest")
	if st, _ := fake.Status(ctx, reminder); st != workflow.StatusCancelled {
		t.Fatalf("reminder status = %s, want cancelled", st)
	}
}
```

## Match jobs

A `JobMatcher` narrows an assertion to jobs with a property; several matchers must all hold. Each has a description that appears in failure messages.

| Matcher | Matches |
|---|---|
| `HasPayload(p)` | The payload, byte for byte |
| `HasPayloadJSON(v)` | A JSON payload equal to `v` once both are decoded, so key order and spacing do not matter; a `v` that does not marshal matches nothing |
| `HasLabel(k, v)` | `Labels[k] == v` |
| `HasKey(key)` | The `IdempotencyKey` |
| `HasRunAt(t)` | A job delayed to exactly `t` |
| `HasPriority(p)` | The `Priority` |
| `HasChain(wf...)` | A job whose [`Job.Chain`](../workflows/chains.md) would run exactly these workflows, in order, nested chains included |
| `Where(desc, fn)` | Any condition of your own, described by `desc` |

<!-- snippet: examples/app-tests/main_test.go#matchers -->
```go
func TestMatchers(t *testing.T) {
	ctx := context.Background()
	app, fake := newApp(t)
	if _, err := app.SignUp(ctx, "ada@example.com"); err != nil {
		t.Fatal(err)
	}

	// HasPayload compares bytes; HasPayloadJSON compares decoded values,
	// so key order and spacing do not matter.
	fake.AssertEnqueued(t, "welcome-email", workflowtest.HasPayload([]byte(`{"email":"ada@example.com"}`)))
	fake.AssertEnqueued(t, "welcome-email", workflowtest.HasPayloadJSON(map[string]any{"email": "ada@example.com"}))
	// Where is the escape hatch; its description shows in failure messages.
	fake.AssertEnqueued(t, "reminder", workflowtest.Where("delayed by a day or more", func(j workflow.Job) bool {
		return !j.RunAt.Before(start.Add(24 * time.Hour))
	}))
}
```

## Inspect what was recorded

When an assertion is not enough, read the records: `Jobs()` returns every recorded job in enqueue order, `Enqueued(wf, m...)` the jobs of one workflow matching the matchers, `Signals()`, `Schedules()` (each a `FakeSchedule` with its `ID`, the `Schedule` and whether it was `Unscheduled`) and `Cancels()` the other calls. Jobs are copied when recorded, so code that reuses its `Job` or payload afterwards cannot change what was recorded. A deduplicated enqueue is not a job: it created no run.

Recorded runs never finish: `Status` reports `StatusScheduled` for a job with `RunAt` and `StatusQueued` otherwise, until `Cancel` or `Finish` moves it. `Finish(ref, status)` moves a run to a terminal status and frees its idempotency key, so a test can model a run that completed:

<!-- snippet: examples/app-tests/main_test.go#inspect -->
```go
func TestDoubleSubmitAndFinish(t *testing.T) {
	ctx := context.Background()
	app, fake := newApp(t)
	reminder, err := app.SignUp(ctx, "ada@example.com")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := app.SignUp(ctx, "ada@example.com"); err != nil { // a double submit
		t.Fatal(err)
	}
	// The keys are held by unfinished runs, so the second call recorded nothing.
	fake.AssertEnqueuedTimes(t, "reminder", 1)
	if got := len(fake.Jobs()); got != 2 {
		t.Fatalf("Jobs() = %d, want 2", got)
	}

	// Finish models the run completing: its key is free again.
	if err := fake.Finish(reminder, workflow.StatusSucceeded); err != nil {
		t.Fatal(err)
	}
	if _, err := app.SignUp(ctx, "ada@example.com"); err != nil {
		t.Fatal(err)
	}
	fake.AssertEnqueuedTimes(t, "reminder", 2)
	if got := fake.Enqueued("reminder", workflowtest.HasKey("reminder:ada@example.com")); len(got) != 2 {
		t.Fatalf("Enqueued(reminder) = %d jobs, want 2", len(got))
	}
}
```

Closures are recorded, not run. `Closures(names...)` returns the recorded `ClosureJob`s in enqueue order, and `RunClosures(ctx)` calls each recorded closure's function once, in order, for a test that wants its side effect; it runs no retries and leaves the recorded statuses alone. `Finish(ref, workflow.StatusDiscarded)` models a run a step dropped with `Discard`.

<!-- snippet: examples/closures/main_test.go#fake-closure -->
```go
func TestWarmUpEnqueuesAClosure(t *testing.T) {
	ctx := context.Background()
	fake := workflowtest.NewFake()
	if err := fake.Start(ctx); err != nil {
		t.Fatal(err)
	}
	warmed := false
	warm := workflow.ClosureJob{Name: "warm-price-cache", Fn: func(context.Context) error { warmed = true; return nil }}
	if _, err := workflow.EnqueueClosure(ctx, fake, warm); err != nil {
		t.Fatal(err)
	}
	fake.AssertClosureEnqueued(t, "warm-price-cache")
	if warmed {
		t.Fatal("the Fake runs nothing until RunClosures")
	}
	if err := fake.RunClosures(ctx); err != nil {
		t.Fatal(err)
	}
	if !warmed {
		t.Error("RunClosures did not call the closure")
	}
}
```

### Operator actions

The Fake implements `ScheduleTriggerer` and `RunForcer` too, so code behind a Run now button or a force action is tested without workers. `TriggerSchedule` enqueues the recorded schedule's job with its input and the trigger labels, through `Enqueue`, so `AssertEnqueued` sees it; it obeys `OverlapSkip` against the run the previous trigger made (end that run with `Finish` or a force). `ForceComplete` and `ForceFail` move a recorded run as a driver would, refusing a finished one with `ErrWrongState`; the Fake runs no chain. `Triggers()` and `Forced()` return the calls, and `AssertTriggered(t, id)` and `AssertForced(t, ref, complete)` check them:

<!-- snippet: examples/schedule-overlap/fake_test.go#fake -->
```go
// TestRunNowButton checks an operator action without workers: the Fake
// records the trigger, enqueues the schedule's job, refuses a second one
// while the first run is unfinished, and records a force.
func TestRunNowButton(t *testing.T) {
	ctx := context.Background()
	fake := workflowtest.NewFake()
	if err := fake.Register(workflow.Workflow{Name: "sync-crm", Steps: []workflow.Step{{Name: "sync", Run: func(context.Context, []byte) ([]byte, error) { return nil, nil }}}}); err != nil {
		t.Fatal(err)
	}
	if err := fake.Start(ctx); err != nil {
		t.Fatal(err)
	}
	id, err := fake.Schedule(ctx, workflow.Schedule{Name: "crm-hourly", Workflow: "sync-crm", Cron: "0 * * * *", Location: time.UTC, Overlap: workflow.OverlapSkip})
	if err != nil {
		t.Fatal(err)
	}

	ref, err := fake.TriggerSchedule(ctx, id) // what the Run now button calls
	if err != nil {
		t.Fatal(err)
	}
	fake.AssertTriggered(t, id)
	fake.AssertEnqueued(t, "sync-crm", workflowtest.HasLabel(workflow.LabelTrigger, workflow.LabelTriggerManual))

	if _, err := fake.TriggerSchedule(ctx, id); !errors.Is(err, workflow.ErrOverlap) {
		t.Fatalf("second trigger: %v, want ErrOverlap", err)
	}
	if err := fake.ForceComplete(ctx, ref); err != nil {
		t.Fatal(err)
	}
	fake.AssertForced(t, ref, true)
	if _, err := fake.TriggerSchedule(ctx, id); err != nil { // the previous run ended
		t.Fatal(err)
	}
}
```

## Behave like your production driver

A default Fake claims the memory driver's capabilities minus the ones it does not serve itself: `CapRunFinished` (it reports no run ends) and the run query, bulk cancel, schedule pause, schedule listing, tick and drain capabilities (it answers those interfaces only by forwarding to a pass-through client). `FakeCaps` and `FakeLike` drop the same set. When production runs on another driver, make the Fake claim that driver's set, so a field production would refuse fails the test too:

- `FakeLike(c)` copies the capabilities of `c`, typically the production client built but not started with test configuration; it reads only `c.Capabilities()`.
- `FakeCaps(caps)` sets them directly. The capabilities the Fake does not serve are always dropped: `CapRunFinished`, because the Fake reports no run ends and does not implement `workflow.RunNotifier`, and the run query, bulk cancel, schedule pause, schedule listing, tick and drain capabilities.
- `FakeKeyExpr()` accepts expression keys (`workflow.KeyExpr`) in concurrency, rate limits and wait keys, as an engine such as Hatchet does. By default the Fake refuses them at `Start`, as the memory driver does.
- `FakeCronParser(p)` checks cron expressions with your parser, matching a production driver built with a custom one (`memory.WithCronParser`).

<!-- snippet: examples/app-tests/main_test.go#like-production -->
```go
func TestRefusedLikeProduction(t *testing.T) {
	ctx := context.Background()

	// FakeLike copies a real client's capabilities; it reads only
	// Capabilities, so an unstarted client built with test settings will do.
	app, _ := newApp(t, workflowtest.FakeLike(memory.New()))
	if _, err := app.SignUp(ctx, "ada@example.com"); err != nil {
		t.Fatal(err)
	}

	// A production driver without delayed starts refuses the reminder, and
	// so does a Fake that claims the same capabilities.
	noDelay := memory.New().Capabilities() &^ workflow.CapDelayedStart
	app, _ = newApp(t, workflowtest.FakeCaps(noDelay))
	_, err := app.SignUp(ctx, "ada@example.com")
	if !errors.Is(err, workflow.ErrUnsupported) {
		t.Fatalf("SignUp on a driver without CapDelayedStart = %v, want ErrUnsupported", err)
	}

	// An expression key is refused at Start, as the memory driver refuses
	// it, unless FakeKeyExpr says the production engine accepts one.
	perTenant := workflow.Workflow{
		Name:        "sync",
		Steps:       []workflow.Step{{Name: "run", Run: noop}},
		Concurrency: &workflow.Concurrency{Key: workflow.KeyExpr("input.tenant"), MaxRuns: 1},
	}
	for _, tc := range []struct {
		opts   []workflowtest.FakeOption
		refuse bool
	}{{nil, true}, {[]workflowtest.FakeOption{workflowtest.FakeKeyExpr()}, false}} {
		fake := workflowtest.NewFake(tc.opts...)
		if err := fake.Register(perTenant); err != nil {
			t.Fatal(err)
		}
		if err := fake.Start(ctx); errors.Is(err, workflow.ErrUnsupported) != tc.refuse {
			t.Fatalf("Start = %v, want refused: %v", err, tc.refuse)
		}
	}
}
```

## Try it

```sh
cd examples
go test ./app-tests/ -run 'TestSignUpEnqueues|TestMatchers|TestDoubleSubmit|TestConfirm|TestRefused' -v
```

```text
=== RUN   TestSignUpEnqueuesWelcomeAndReminder
--- PASS: TestSignUpEnqueuesWelcomeAndReminder (0.00s)
=== RUN   TestMatchers
--- PASS: TestMatchers (0.00s)
=== RUN   TestDoubleSubmitAndFinish
--- PASS: TestDoubleSubmitAndFinish (0.00s)
=== RUN   TestConfirmSignalsAndCancels
--- PASS: TestConfirmSignalsAndCancels (0.00s)
=== RUN   TestRefusedLikeProduction
--- PASS: TestRefusedLikeProduction (0.00s)
PASS
```

`go run ./app-tests` runs the same app code on a Fake and prints what it recorded.

## Pitfalls

- Register the app's workflows and call `Start` before the code under test runs; an enqueue before `Start` is `workflow.ErrNotStarted`, exactly as on a real driver.
- The Fake has no run-finished hook. Code that calls `OnRunFinished` (unique-until-finished, claim check cleanup, batches) needs the memory driver, wrapped in a [Recorder](recorder.md) if the test also checks enqueues.
- A key stays held until the run is cancelled or finished: a test that expects a second enqueue with the same key to be recorded must `Finish` the first run.
- `HasRunAt` compares exactly. Drive the app's clock with a [fake clock](fake-clock.md) so the expected time is known.
- `NewRecorder(nil)`, the older standalone fake, claims every capability and refuses nothing; it is kept only for compatibility. Use `NewFake`.

## Related

- [Record a real client](recorder.md): assertions over a driver that really runs the jobs.
- [Test batches, chains and a subset of workflows](batches-and-chains.md): `AssertBatched`, `AssertChained`, `FakeExcept` and `FakeOnly`.
- [Test one step](run-step.md): `RunStep` for a handler's release, failure and permanent failure.
- [Test queues, retries and pauses](operations.md): `AssertEnqueuedOn`, `HasQueue`, `HasRetry`, `AssertPaused`, `AssertRetried` and `AssertDeleted`.
- [Drive time in tests](fake-clock.md).
- [Capabilities](../reference/capabilities.md): what each driver claims, for `FakeCaps`.
- Example: [examples/app-tests](../../examples/app-tests/), and the shorter [example_fake_test.go](../../workflowtest/example_fake_test.go).
