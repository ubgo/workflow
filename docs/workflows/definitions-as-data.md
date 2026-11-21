# Definitions as data

A workflow written in Go changes only with a new build. Some teams need the shape of a workflow to change without one: an operator adds a review step, a product team stretches a payment wait from two days to three, a tenant gets its own retry policy, an admin form edits the order of steps. The `dsl` package reads a workflow definition from JSON at run time and builds the same `workflow.Workflow` Go code would. The data names Go handlers the program registered; it never contains code.

## Before you start

- `dsl` is part of the core module and needs nothing beyond the standard library. A built workflow runs on any driver that has the capabilities its steps need, exactly like the Go form: a wait step needs `workflow.CapWaitForEvent`, a sleep step `workflow.CapDurableSleep`.
- Workflows, steps and `After` are explained on [workflows and steps](steps.md); waits on [wait for an event](signals.md).
- The example is [`examples/workflow-json`](../../examples/workflow-json). Run it with `go run ./workflow-json` from `examples/`.

## Write the definition

A definition is a JSON object. Every step that runs code names a handler; waits and sleeps are plain data; durations are Go duration strings such as `"30s"` or `"72h"`:

<!-- snippet: examples/workflow-json/main.go#definition -->
```go
// orderJSON is the definition as an app would keep it in a file, a
// database row or a DataStore entry. Handler names refer to Go code the
// program registers; the document itself holds no code.
const orderJSON = `{
  "name": "order",
  "timeout": "30s",
  "retry": {"max": 2, "backoff": {"initial": "1s", "factor": 2}},
  "steps": [
    {"name": "reserve", "handler": "reserve-stock"},
    {"name": "await-payment", "after": ["reserve"],
     "wait": {"event": "payment.succeeded", "key_label": "order", "timeout": "72h"}},
    {"name": "ship", "after": ["await-payment"], "handler": "ship-parcel",
     "retry": {"max": 5, "backoff": {"initial": "10s", "factor": 2, "max": "5m"}}}
  ]
}`
```

Keep it wherever the app keeps configuration: a file in the repository, a database row, an entry in a [`DataStore`](memoisation.md#share-data-across-runs), an object in a bucket.

## Register the handlers

A `dsl.Registry` maps each name to a `workflow.Handler`. `workflow.Define` gives a typed handler, as in a Go workflow. The zero value works, and so does `dsl.NewRegistry()`; there is no package-level registry, so each program (and each test) has its own:

<!-- snippet: examples/workflow-json/main.go#registry -->
```go
reg := dsl.NewRegistry()
if err := reg.Register("reserve-stock", workflow.Define(func(ctx context.Context, order string) (string, error) {
	return order, nil // reserve the stock here
})); err != nil {
	return err
}
if err := reg.Register("ship-parcel", workflow.Define(func(ctx context.Context, p payment) (string, error) {
	return fmt.Sprintf("shipped %s, paid %d", p.Order, p.Paid), nil
})); err != nil {
	return err
}
```

`Register` refuses an empty name, a nil handler and a name already taken, each with an error wrapping `workflow.ErrInvalid`: two features registering the same name would otherwise change what running definitions do without a word.

The registry is the security boundary. A definition can only call handlers the program registered, so whoever writes definitions can rearrange and retune the program's steps but never run code the program did not ship.

## Load and register

`dsl.Load` parses the document, resolves its handler names, and runs `workflow.Validate`, the check every driver's `Register` runs. Register the result on the client like any workflow:

<!-- snippet: examples/workflow-json/main.go#load -->
```go
wf, err := dsl.Load([]byte(orderJSON), reg) // Parse, then Build against reg
if err != nil {
	return err // wraps workflow.ErrInvalid and names the place
}
c := memory.New()
defer c.Stop()
if err := c.Register(wf); err != nil { // like a workflow written in Go
	return err
}
```

The runs behave as if the workflow were written in Go: here each order waits for the payment of its own order (the wait's `key_label` reads the `order` label of the job), then ships.

## Refusals name the place

Every error from `Parse`, `Build` and `Load` wraps `workflow.ErrInvalid`. One tied to a place in the document is a `*dsl.FieldError` whose `Path` reads like the document, so an admin form can show it next to the field:

<!-- snippet: examples/workflow-json/main.go#refusal -->
```go
_, err = dsl.Load([]byte(`{"name": "order", "steps": [
  {"name": "reserve", "handler": "reserve-stock"},
  {"name": "ship", "after": ["reserve"], "handler": "ship-order"}]}`), reg)
fmt.Println("refused:", err)
```

```text
refused: workflow: invalid spec: dsl: steps[1].handler: no handler is registered as "ship-order" (registered: reserve-stock, ship-parcel)
```

What is refused, and by which call:

| Refusal | Call | Error |
|---|---|---|
| A field the reader does not know (`"handlr"`, or a field of a newer release) | `Parse` | `FieldError` at the field, listing the known names |
| A value of the wrong JSON type (`"after": "x"`, `"version": 1.5`) | `Parse` | `FieldError` at the value |
| Data after the document, or not a JSON object | `Parse` | `FieldError` with an empty path |
| No name, no steps, a step setting none or several of `handler`, `wait` and `sleep` | `Parse` (through `Validate`) | `FieldError` |
| A duration that does not parse (`"10 seconds"`, `"2d"`), a sleep that is not positive | `Parse` (through `Validate`) | `FieldError` |
| A wait without an event, or with both `key` and `key_label`; an unknown `cron_location` | `Parse` (through `Validate`) | `FieldError` |
| A handler or `on_failure` name the registry does not know | `Build` | `FieldError`, listing the registered names |
| A duplicate step name, `after` naming a missing step, a cycle, a value out of range, an unknown strategy | `Build` (through `workflow.Validate`) | the same error the Go form gets |
| A capability the driver lacks (a wait on a driver without `CapWaitForEvent`, a zone on a UTC-only engine) | `Client.Register` | `UnsupportedError`, as for the Go form |

## Store and edit a definition

`dsl.Parse` checks a document without any handler, so a deploy pipeline or an admin form can check a definition before a program loads it. `dsl.Marshal` writes a `Definition` back as indented JSON that `Parse` reads to an equal value:

<!-- snippet: examples/workflow-json/main.go#round-trip -->
```go
def, err := dsl.Parse([]byte(orderJSON))
if err != nil {
	return err
}
stored, err := dsl.Marshal(def) // indented JSON, ready to store
if err != nil {
	return err
}
back, err := dsl.Parse(stored)
if err != nil {
	return err
}
fmt.Println("round trip equal:", reflect.DeepEqual(def, back))
```

A `Definition` built in Go (an admin form filling the struct) goes through `dsl.Build` directly; `Build` runs `Definition.Validate` first, so it is checked the same way.

## Reload without a deploy

Load definitions at start-up and register them before `Start`, like any workflow. To take a changed definition without a deploy, load the new bytes and register them on a fresh client (or restart the workers), since a client refuses `Register` after `Start`. Runs already in flight follow [`Workflow.Version`](../operations/deploy.md#deploy-a-new-version-while-old-workers-run): bump `version` when a change would break them (a step added, removed or renamed, a payload shape changed).

## YAML

YAML is a superset of JSON. The package reads JSON only, so the core module keeps to the standard library; an app that keeps its definitions in YAML converts the document to JSON with the YAML library it already uses and passes the bytes to `dsl.Parse` or `dsl.Load`. The field names are the same in both.

## State machines as data

A [state machine](state-machines.md) can be data too, the way a state-machine language describes states and transitions: `dsl.ParseMachine` reads a `dsl.MachineDefinition` (states with `parent`, `initial`, `parallel`, `final`, `entry` and `exit` actions and `after` timers; transitions with `from`, `event`, `to`, `guard`, `actions`, `history` and `reenter`; `unhandled`, `ordered`, `keep_log` and `retry`), and `dsl.BuildMachine` resolves guard and action names against a `dsl.MachineRegistry[D]` and calls `workflow.DefineMachine`. The data names guards and actions; the program registers the Go functions, typed on its own data type `D`, so the registry stays the security boundary.

```go
reg := dsl.NewMachineRegistry[Ticket]()
_ = reg.Action("note", note)                 // a workflow.MachineAction[Ticket]
_ = reg.Guard("approved-twice", approvedTwice) // a workflow.MachineGuard[Ticket]
wf, err := dsl.LoadMachine(data, reg, dsl.MachineEnv{Store: store, Client: client})
```

```json
{
  "name": "ticket",
  "initial": "open",
  "unhandled": "reject",
  "states": [
    {"name": "open", "after": [{"delay": "48h", "event": "expire"}]},
    {"name": "review", "initial": "first-line", "exit": ["note"]},
    {"name": "first-line", "parent": "review"},
    {"name": "escalated", "parent": "review", "entry": ["note"]},
    {"name": "held"},
    {"name": "closed", "final": true}
  ],
  "transitions": [
    {"from": "open", "event": "assign", "to": "review"},
    {"from": "open", "event": "expire", "to": "closed"},
    {"from": "first-line", "event": "escalate", "to": "escalated"},
    {"from": "review", "event": "close", "to": "closed", "guard": "approved-twice"},
    {"from": "review", "event": "hold", "to": "held"},
    {"from": "held", "event": "resume", "to": "review", "history": "deep"}
  ]
}
```

`dsl.MachineEnv` carries what is not data: the `JournalStore` instances live in, the client timers are sent on, and the clock. Refusals are `*dsl.FieldError`s with their JSON path (`transitions[3].guard`), and `workflow.DefineMachine`'s own checks (an unknown target, a compound state without an initial child) still apply. `dsl.MarshalMachine` writes a definition back. Renaming or removing a state in a stored definition strands the instances that are in it, exactly as in Go: see the state machines page's Pitfalls.

## JSON reference

| Field | Type | Maps to | Notes |
|---|---|---|---|
| `name` | string | `Workflow.Name` | Required. |
| `version` | integer | `Workflow.Version` | Needs `CapWorkflowVersion` when set. |
| `min_version` | integer | `Workflow.MinVersion` | At most `version`. |
| `queue` | string | `Workflow.Queue` | Empty means the default queue. |
| `timeout` | duration | `Workflow.Timeout` | The default per-attempt timeout of every step. |
| `run_timeout` | duration | `Workflow.RunTimeout` | Needs `CapRunTimeout`. |
| `retry` | retry object | `Workflow.Retry` | The default of steps without their own. |
| `concurrency` | concurrency object | `Workflow.Concurrency` | Needs `CapConcurrencyKey`. |
| `steps` | array of step objects | `Workflow.Steps` | At least one. |
| `on_failure` | handler name | `Workflow.OnFailure` | Runs once when a run fails for good. |
| `cron` | array of strings | `Workflow.Cron` | Parsed by the driver at `Register`, in its own dialect. |
| `cron_location` | IANA zone | `Workflow.CronLocation` | Empty means UTC; `Local` is refused. |
| `steps[i].name` | string | `Step.Name` | Required, unique. |
| `steps[i].after` | array of step names | `Step.After` | Parents that must succeed first. |
| `steps[i].handler` | handler name | `Step.Run` | One of `handler`, `wait` and `sleep`. |
| `steps[i].wait` | wait object | `Step.Wait` | One of `handler`, `wait` and `sleep`. |
| `steps[i].sleep` | duration | `Step.Sleep` | Positive. One of `handler`, `wait` and `sleep`. |
| `steps[i].retry` | retry object | `Step.Retry` | `{"max": 0}` means never retry. |
| `steps[i].timeout` | duration | `Step.Timeout` | Per attempt. |
| `wait.event` | string | `WaitSpec.Event` | Required. |
| `wait.key` | string | `WaitSpec.Key` | A fixed key for every run. |
| `wait.key_label` | label name | `WaitSpec.KeyFrom` | The key is the value of this job label, per run. |
| `wait.timeout` | duration | `WaitSpec.Timeout` | Empty means wait forever. |
| `retry.max` | integer | `RetryPolicy.Max` | Retries after the first attempt. |
| `retry.backoff.initial` | duration | `Backoff.Initial` | The first retry's wait. |
| `retry.backoff.factor` | number | `Backoff.Factor` | Below 1 counts as 1. |
| `retry.backoff.max` | duration | `Backoff.Max` | The cap on the wait. |
| `retry.backoff.jitter` | number | `Backoff.Jitter` | In [0, 1]; needs `CapRetryJitter`. |
| `retry.until` | duration | `RetryPolicy.Until` | Needs `CapRetryUntil`. |
| `retry.max_errors` | integer | `RetryPolicy.MaxErrors` | Needs `CapRetryMaxErrors`. |
| `retry.discard_on_exhaust` | boolean | `RetryPolicy.DiscardOnExhaust` | Needs `CapDiscardExhausted`. |
| `concurrency.key_label` | label name | `Concurrency.Key` | The key is the value of this job label; empty means one key for every run. |
| `concurrency.max_runs` | integer | `Concurrency.MaxRuns` | At least 1. |
| `concurrency.strategy` | string | `Concurrency.Strategy` | `queue_newest` (the default), `drop_newest`, `cancel_newest`, `cancel_in_progress`, `round_robin`. |
| `concurrency.fifo` | boolean | `Concurrency.FIFO` | Needs `CapKeyFIFO`. |

A duration is a Go duration string: `"300ms"`, `"30s"`, `"1h30m"`. There are no days; write `"72h"`.

## Pitfalls

- **A handler name is a contract.** Renaming a registered handler breaks every stored definition that names it, at `Build`. Keep the old name registered while definitions move to the new one.
- **Go-only fields have no data form.** `ScheduleWhen`, `NoRetryOn`, `DelayFor`, `KeyExpr`, `OnTimeout` and rate limits are code or engine expressions, so a definition cannot set them. Build the workflow from the definition and set them in Go before `Register`, or write that workflow in Go.
- **A per-run key comes from a label.** `key_label` reads `Job.Labels`, not the payload, so the enqueuer must set the label; a run without it waits on, or is capped under, the empty key.
- **Cron expressions are checked at `Register`, not `Parse`.** The driver's parser owns the dialect, so a mistyped expression passes `Parse` and is refused when the workflow registers.
- **A changed definition does not change runs in flight on its own.** A client registers workflows before `Start`; a new definition takes effect on the client that registers it, and runs started under the old shape follow `Workflow.Version`.
- **The registry is the only gate.** Anyone who can write definitions can call any registered handler with any input the run carries. Register only handlers that are safe to call from any step of any definition, and restrict who can write definitions as you restrict who can deploy.

## Related

- [Workflows and steps](steps.md), [Wait for an event](signals.md), [Sleep inside a workflow](sleep.md).
- [Store a schedule as JSON](../schedules/manage.md#store-a-schedule-as-json): schedules as data.
- [Durable closures by name](../jobs/functions.md): a single Go function enqueued by name.
- Example: [examples/workflow-json](../../examples/workflow-json/).
