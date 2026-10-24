# Functions by name

A [closure](closures.md) is quick, but it lives only in the process that enqueued it. When a small piece of work must survive a restart or run on another machine, give the function a name: register it under that name when every worker process starts, and enqueue it by the name with an argument encoded as JSON. The name and the argument can be stored in a database row or sent to an engine, so the run is as durable as any job, on every driver. This page covers defining a function, enqueuing it with a typed argument or by name alone, delaying it, and what happens across a restart.

## Before you start

- A `workflow.Func` is a one-step workflow underneath, so it needs no capability beyond what the driver already has: it runs on the in-memory, SQL, Redis and Hatchet drivers alike. A delay needs `CapDelayedStart`, as for any job.
- The function is registered in code, so every process that runs workers registers the same name, as with any workflow.
- The example is [`examples/functions`](../../examples/functions). Run it with `go run ./functions` from `examples/`.

## Step 1: define the function

`workflow.NewFunc(name, fn, opts...)` names a `func(ctx, In) error`. The argument type `In` is any type that encodes as JSON. The options set what belongs to the definition rather than to one enqueue:

<!-- snippet: examples/functions/main.go#define -->
```go
// welcome is the argument; it travels as JSON, so it survives a restart.
type welcome struct {
	Email string
}

// sendWelcome is built once, at package level or in main, and registered in
// every process that runs workers. The name is what is stored with a run.
func sendWelcome(sent *atomic.Int64, caught chan<- string) workflow.Func[welcome] {
	return workflow.NewFunc("send-welcome",
		func(ctx context.Context, in welcome) error {
			if strings.HasSuffix(in.Email, ".invalid") {
				return workflow.Permanent(errors.New("no such domain")) // no retry
			}
			if sent.Add(1) == 1 {
				return errors.New("mail server busy") // retried under FuncRetry
			}
			return nil // send the mail here
		},
		workflow.FuncRetry(&workflow.RetryPolicy{Max: 2}),
		workflow.FuncTimeout(30*time.Second),
		workflow.FuncCatch(func(ctx context.Context, in welcome) error {
			caught <- in.Email // runs once when a run fails for good
			return nil
		}),
	)
}
```

| Option | Meaning |
|---|---|
| `FuncRetry(*RetryPolicy)` | The retry policy of its one step; without it, no retry |
| `FuncTimeout(d)` | Bounds each attempt; zero means none |
| `FuncQueue(q)` | The default queue of its runs; empty means the default queue |
| `FuncCatch(fn)` | Runs once when a run fails for good, with the decoded argument (the workflow's `OnFailure`) |

The step is named `workflow.FuncStep` (`func`), and the run's workflow name is the function's name, so `Status`, run listings, run-finished observers and step middleware see it like any run.

## Step 2: register and enqueue

`workflow.RegisterFunc(client, f)` registers it (it is `client.Register(f.Workflow())`), and `f.Enqueue(ctx, client, arg)` encodes the argument and enqueues:

<!-- snippet: examples/functions/main.go#register -->
```go
fn := sendWelcome(&sent, caught)
if err := workflow.RegisterFunc(c, fn); err != nil {
	return err
}
if err := c.Start(ctx); err != nil {
	return err
}
if _, err := fn.Enqueue(ctx, c, welcome{Email: "ann@example.com"}); err != nil {
	return err
}
```

```text
send-welcome: succeeded after 2 attempts, step "func"
bad address: failed, catch ran for "eve@example.invalid"
```

The first attempt failed and was retried. The second run returned a `Permanent` error, so it failed without a retry and the catch function ran with its argument.

## Step 3: enqueue by name alone

A caller that does not have the `Func` value (another package, an admin action) builds the job from the name with `workflow.FuncJob(name, arg)`. A name that no one registered on that client is refused the way any unknown workflow is:

<!-- snippet: examples/functions/main.go#by-name -->
```go
job, err := workflow.FuncJob("send-invoice", map[string]string{"Order": "A-1"})
if err != nil {
	return err
}
_, err = c.Enqueue(ctx, job) // nothing registered "send-invoice" here
```

```text
unregistered name: ErrNotFound true
```

An empty name or an argument that does not encode as JSON is refused with `workflow.ErrInvalid` before anything is enqueued.

## Step 4: delay it and survive a restart

`f.Job(arg)` returns the `Job` without enqueuing it, so you can set `RunAt`, `Labels`, `Priority`, `IdempotencyKey`, `Queue` or `Retry` first:

<!-- snippet: examples/functions/main.go#delayed -->
```go
job, err := fn.Job(welcome{Email: "bob@example.com"})
if err != nil {
	return err
}
job.RunAt = clk.Now().Add(time.Hour)
job.IdempotencyKey = "welcome:bob@example.com"
ref, err := first.Enqueue(ctx, job)
if err != nil {
	return err
}
```

In the example the first SQL client stops with the run still waiting. A second client on the same SQLite file registers the same `Func` and starts, and when the hour is up it runs the function:

```text
first process stopped with the run waiting
second process ran send-welcome: succeeded
```

## Closures or functions

| | `EnqueueClosure` | `Func` |
|---|---|---|
| Drivers | In-memory only (`CapClosure`) | Every driver |
| Survives a restart | No | Yes, on a durable driver |
| Runs on another machine | No | Yes, on any worker that registered the name |
| Captures local variables | Yes | No: everything it needs is in the argument |
| Idempotency key, chain | No | Yes, as any job |

## Pitfalls

- **The name is the contract.** Runs are stored under the name, not the code. Renaming a `Func` leaves its waiting runs under the old name, and the SQL driver claims only workflows its client registered, so they wait until some process registers the old name again. Keep the old name registered until its runs drain.
- **The argument is JSON.** Unexported fields are dropped, and a time or a number goes through its JSON form. An argument that no longer decodes after a type change fails the run with a `Permanent` error and no retry; the catch function then gets the zero value of `In`.
- **The enqueuing client must know the name on the SQL driver.** The SQL driver refuses to enqueue a workflow its own client did not register, even when another process would run it, so a web process that only enqueues still calls `RegisterFunc`, and since its client is started, it can pick up runs of that function too.
- **Two functions with one name replace each other**, as two workflows registered under one name do.
- **No captured state.** Values captured by the function are those of the process that runs it, not of the one that enqueued it. Put what the run needs into the argument.

## Related

- [Closures](closures.md): the in-process quick form.
- [Enqueue a job](enqueue.md): the job fields a `Func`'s job carries.
- [Retries](retries.md): the policy `FuncRetry` takes.
- [Delays](delays.md): `RunAt` and cancelling a waiting run.
