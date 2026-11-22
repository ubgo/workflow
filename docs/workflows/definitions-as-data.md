# Definitions as data

A workflow written in Go changes only with a new build. Some teams need the shape of a workflow to change without one: an operator adds a review step, a product team stretches a payment wait from two days to three, a tenant gets its own retry policy, an admin form edits the order of steps. The `dsl` package reads a workflow definition from JSON at run time ([`dsl.Load`](ds:block?id=dsl-load-chpns7f3)) and builds the same `workflow.Workflow` Go code would ([`dsl.Build`](ds:block?id=dsl-build-etfzctan)). The data names Go handlers the program registered; it never contains code.

## Before you start

- `dsl` is part of the core module and needs nothing beyond the standard library. A built workflow runs on any driver that has the capabilities its steps need, exactly like the Go form ([`Build`](ds:block?id=dsl-build-etfzctan) leaves capability checks to `Register`): a wait step needs [`workflow.CapWaitForEvent`](ds:block?id=capwaitforevent-2km3dkzy), a sleep step [`workflow.CapDurableSleep`](ds:block?id=capdurablesleep-k5tkkje8).
- Workflows, steps and `After` are explained on [workflows and steps](steps.md); waits on [wait for an event](signals.md).
- The workflow example is [`examples/workflow-json`](../../examples/workflow-json); the state machine example is [`examples/machine-dsl`](../../examples/machine-dsl). Run them with `go run ./workflow-json` and `go run ./machine-dsl` from `examples/`.
- [State machines as data](#state-machines-as-data) builds on [state machines](state-machines.md): read that page first for states, transitions, guards, actions and history.

## Write the definition

A definition is a JSON object ([`dsl.Definition`](ds:block?id=dsl-definition-m3et9n3c), each step a [`dsl.StepDef`](ds:block?id=dsl-stepdef-uta5kgvn)). Every step that runs code names a handler; waits and sleeps are plain data; durations are Go duration strings such as `"30s"` or `"72h"` ([`duration`](ds:block?id=dsl-duration-qep8j5u5)):

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

Check a document as you write it with [`dsl.Parse`](ds:block?id=dsl-parse-g4vn54w9), which needs no handler. The reader is strict ([`parseDoc`](ds:block?id=dsl-parsedoc-39gj946b)): a misspelled or unknown field, a value of the wrong JSON type or data after the document is refused, and the error is a [`*dsl.FieldError`](ds:block?id=dsl-fielderror-zst5dca3) whose `Path` names the field the way the document reads and whose `Reason` says what is wrong:

<!-- snippet: examples/workflow-json/main.go#check -->
```go
// Parse checks a document without any handler: a typo is refused with
// the JSON path of the field, which a form can show next to it.
_, err := dsl.Parse([]byte(`{"name": "order", "steps": [{"name": "reserve", "handlr": "reserve-stock"}]}`))
var fe *dsl.FieldError
if errors.As(err, &fe) {
	fmt.Printf("checked: %s: %s\n", fe.Path, fe.Reason)
}
```

```text
checked: steps[0].handlr: unknown field (known: after, handler, name, retry, sleep, timeout, wait)
```

An unknown field is refused rather than ignored so that a typo cannot silently drop a setting, and a definition written for a newer release that has more fields fails loudly on an older program instead of running without them.

## Register the handlers

A [`dsl.Registry`](ds:block?id=dsl-registry-aqvqrzp8) maps each name to a `workflow.Handler`. [`workflow.Define`](ds:block?id=define-d3ttbgk3) gives a typed handler, as in a Go workflow. The zero value works, and so does [`dsl.NewRegistry()`](ds:block?id=dsl-newregistry-ub3nydx9); there is no package-level registry, so each program (and each test) has its own:

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

[`Register`](ds:block?id=dsl-registry-register-nqy89s2y) refuses an empty name, a nil handler and a name already taken, each with an error wrapping `workflow.ErrInvalid`: two features registering the same name would otherwise change what running definitions do without a word.

The registry is the security boundary. A definition can only call handlers the program registered ([`resolve`](ds:block?id=dsl-resolve-qt5y6d8c)), so whoever writes definitions can rearrange and retune the program's steps but never run code the program did not ship.

## Load and register

[`dsl.Load`](ds:block?id=dsl-load-chpns7f3) parses the document, resolves its handler names, and runs [`workflow.Validate`](ds:block?id=validate-mnthpzkp), the check every driver's `Register` runs ([`Build`](ds:block?id=dsl-build-etfzctan)). Register the result on the client like any workflow:

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

The runs behave as if the workflow were written in Go: here each order waits for the payment of its own order (the wait's `key_label` reads the `order` label of the job, [`labelKey`](ds:block?id=dsl-labelkey-9hq8uv4m)), then ships.

## Refusals name the place

Every error from `Parse`, `Build` and `Load` wraps `workflow.ErrInvalid`. One tied to a place in the document is a [`*dsl.FieldError`](ds:block?id=dsl-fielderror-zst5dca3) whose `Path` reads like the document, so an admin form can show it next to the field:

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
| A field the reader does not know (`"handlr"`, or a field of a newer release) | [`Parse`](ds:block?id=dsl-parsedoc-39gj946b) | `FieldError` at the field, listing the known names |
| A value of the wrong JSON type (`"after": "x"`, `"version": 1.5`) | [`Parse`](ds:block?id=dsl-parsedoc-39gj946b) | `FieldError` at the value |
| Data after the document, or not a JSON object | [`Parse`](ds:block?id=dsl-parsedoc-39gj946b) | `FieldError` with an empty path |
| No name, no steps, a step setting none or several of `handler`, `wait` and `sleep` (the three [`StepKind`](ds:block?id=stepkind-5k3zmmg5) values [`StepRun`](ds:block?id=steprun-edqhadj2), [`StepWait`](ds:block?id=stepwait-q3hc85tv), [`StepSleep`](ds:block?id=stepsleep-ny78cwqa)) | `Parse` (through [`Validate`](ds:block?id=dsl-stepdef-validate-28zs3f25)) | `FieldError` |
| A duration that does not parse (`"10 seconds"`, `"2d"`), a sleep that is not positive | `Parse` (through [`Validate`](ds:block?id=dsl-stepdef-validate-28zs3f25)) | `FieldError` |
| A wait without an event, or with both `key` and `key_label`; an unknown `cron_location` ([`location`](ds:block?id=dsl-location-bhcn5hdh)) | `Parse` (through [`Validate`](ds:block?id=dsl-definition-validate-3tczvf3z)) | `FieldError` |
| A handler or `on_failure` name the registry does not know | [`Build`](ds:block?id=dsl-resolve-qt5y6d8c) | `FieldError`, listing the registered names |
| A duplicate step name, `after` naming a missing step, a cycle, a value out of range, an unknown strategy | `Build` (through [`workflow.Validate`](ds:block?id=validate-mnthpzkp)) | the same error the Go form gets |
| A capability the driver lacks (a wait on a driver without `CapWaitForEvent`, a zone on a UTC-only engine) | `Client.Register` | `UnsupportedError`, as for the Go form |

## Store and edit a definition

[`dsl.Parse`](ds:block?id=dsl-parse-g4vn54w9) checks a document without any handler, so a deploy pipeline or an admin form can check a definition before a program loads it. [`dsl.Marshal`](ds:block?id=dsl-marshal-h2h9vs2c) writes a `Definition` back as indented JSON that `Parse` reads to an equal value:

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

A `Definition` built in Go (an admin form filling the struct) goes through [`dsl.Build`](ds:block?id=dsl-build-etfzctan) directly; `Build` runs [`Definition.Validate`](ds:block?id=dsl-definition-validate-3tczvf3z) first, so it is checked the same way.

## Reload without a deploy

Load definitions at start-up and register them before `Start`, like any workflow. To take a changed definition without a deploy, load the new bytes and register them on a fresh client (or restart the workers), since a client refuses `Register` after `Start` ([`workflow.ErrAlreadyStarted`](ds:block?id=erralreadystarted-prauyy24)). Runs already in flight follow [`Workflow.Version`](../operations/deploy.md#deploy-a-new-version-while-old-workers-run): bump `version` when a change would break them (a step added, removed or renamed, a payload shape changed).

## YAML

YAML is a superset of JSON. The package reads JSON only ([`parseDoc`](ds:block?id=dsl-parsedoc-39gj946b)), so the core module keeps to the standard library; an app that keeps its definitions in YAML converts the document to JSON with the YAML library it already uses and passes the bytes to [`dsl.Parse`](ds:block?id=dsl-parse-g4vn54w9) or [`dsl.Load`](ds:block?id=dsl-load-chpns7f3). The field names are the same in both.

## State machines as data

A [state machine](state-machines.md) can be data too, the way a state-machine language describes states and transitions. [`dsl.ParseMachine`](ds:block?id=dsl-parsemachine-mngjffec) reads a [`dsl.MachineDefinition`](ds:block?id=dsl-machinedefinition-tmpd4qeb), [`dsl.BuildMachine`](ds:block?id=dsl-buildmachine-pfc8h78c) resolves its guard and action names against a [`dsl.MachineRegistry[D]`](ds:block?id=dsl-machineregistry-k2z5pnpz) and calls [`workflow.DefineMachine`](ds:block?id=definemachine-zgu9ppny), and [`dsl.LoadMachine`](ds:block?id=dsl-loadmachine-hyqt5332) does both. The data names guards and actions; the program registers the Go functions, typed on its own data type `D`, so the registry stays the security boundary. The result is an ordinary machine workflow: [`workflow.SendEvent`](ds:block?id=sendevent-ex33hwx5), [`workflow.StartMachine`](ds:block?id=startmachine-rwxg4f9d), [`workflow.MachineStatus`](ds:block?id=machinestatus-dgbvwcmx) and [`workflow.ForgetMachine`](ds:block?id=forgetmachine-t7tdwu6t) work on it unchanged.

### Write the machine

A [`dsl.MachineDefinition`](ds:block?id=dsl-machinedefinition-tmpd4qeb) lists `states` (each a [`dsl.StateDef`](ds:block?id=dsl-statedef-6whhsty9) with `parent`, `initial`, `parallel`, `final`, `entry` and `exit` action names and `after` timers, each a [`dsl.TimerDef`](ds:block?id=dsl-timerdef-gstrjz2d)) and `transitions` (each a [`dsl.TransitionDef`](ds:block?id=dsl-transitiondef-jjhav7bf) with `from`, `event`, `to`, `guard`, `actions`, `history` and `reenter`), plus `initial`, `unhandled`, `ordered`, `keep_log` and `retry` for the whole machine. The example is a support ticket: open for at most 48 hours, then in review (a compound state whose `escalated` child is compound too), which can be put on hold and resumed where it was, and closed once two approvals arrived:

<!-- snippet: examples/machine-dsl/main.go#definition -->
```go
// ticketJSON is the machine as an app would keep it in a file or a
// database row. Guards and actions are names the program registers; the
// document holds no code.
const ticketJSON = `{
  "name": "ticket",
  "initial": "open",
  "unhandled": "reject",
  "keep_log": 20,
  "states": [
    {"name": "open", "after": [{"delay": "48h", "event": "expire"}]},
    {"name": "review", "initial": "first-line"},
    {"name": "first-line", "parent": "review"},
    {"name": "escalated", "parent": "review", "initial": "triage", "entry": ["note"]},
    {"name": "triage", "parent": "escalated"},
    {"name": "investigating", "parent": "escalated"},
    {"name": "held"},
    {"name": "closed", "final": true, "entry": ["note"]}
  ],
  "transitions": [
    {"from": "open", "event": "assign", "to": "review"},
    {"from": "open", "event": "expire", "to": "closed"},
    {"from": "first-line", "event": "escalate", "to": "escalated"},
    {"from": "triage", "event": "investigate", "to": "investigating"},
    {"from": "review", "event": "approve", "actions": ["approve"]},
    {"from": "review", "event": "close", "to": "closed", "guard": "approved-twice"},
    {"from": "review", "event": "hold", "to": "held"},
    {"from": "held", "event": "resume", "to": "review", "history": "deep"}
  ]
}`
```

The instance's extended state is a Go type of your own, saved as JSON with the configuration ([the event step](ds:block?id=machine-step-vvjdn77u)); guards and actions are typed on it:

<!-- snippet: examples/machine-dsl/main.go#data -->
```go
// Ticket is each instance's extended state, saved as JSON with its
// configuration. The registry's guards and actions are typed on it.
type Ticket struct {
	Approvals int      `json:"approvals"`
	Notes     []string `json:"notes,omitempty"`
}
```

### Register the guards and actions

[`dsl.NewMachineRegistry[D]()`](ds:block?id=dsl-newmachineregistry-hst3eav7) returns an empty registry (the zero value works too). [`Action`](ds:block?id=dsl-machineregistry-action-2d2f5jjj) registers a [`workflow.MachineAction[D]`](ds:block?id=machineaction-tqyyjkvw) and [`Guard`](ds:block?id=dsl-machineregistry-guard-ag7tpere) a [`workflow.MachineGuard[D]`](ds:block?id=machineguard-yfjzyrtr) under a name; an empty name, a nil function or a name already taken is refused with an error wrapping `workflow.ErrInvalid`. Guards and actions have separate names, and the rules of the Go form apply: a guard only reads, an action may change `m.Data` and runs at least once per event.

<!-- snippet: examples/machine-dsl/main.go#registry -->
```go
reg := dsl.NewMachineRegistry[Ticket]()
// An action may change the data; it runs at least once per event.
if err := reg.Action("note", func(_ context.Context, m *workflow.MachineCtx[Ticket]) error {
	m.Data.Notes = append(m.Data.Notes, m.State) // the state being entered
	return nil
}); err != nil {
	return nil, err
}
if err := reg.Action("approve", func(_ context.Context, m *workflow.MachineCtx[Ticket]) error {
	m.Data.Approvals++
	return nil
}); err != nil {
	return nil, err
}
// A guard only reads: it may run more than once per event.
if err := reg.Guard("approved-twice", func(m workflow.MachineCtx[Ticket]) bool {
	return m.Data.Approvals >= requiredApprovals
}); err != nil {
	return nil, err
}
```

### Load and register the machine

[`dsl.MachineEnv`](ds:block?id=dsl-machineenv-wqbuhqmk) carries what is not data: the `JournalStore` instances live in, the `Client` the timers are sent on (required when a state has timers) and the `Clock` (nil means the real clock). [`LoadMachine`](ds:block?id=dsl-loadmachine-hyqt5332) parses the document, resolves every name and runs `DefineMachine`'s checks; register the workflow like any other:

<!-- snippet: examples/machine-dsl/main.go#load -->
```go
env := dsl.MachineEnv{
	Store:  store, // where each instance's record lives
	Client: c,     // sends the timers: delayed runs of the machine
	Clock:  clk,   // times the timers and the log
}
wf, err := dsl.LoadMachine([]byte(ticketJSON), reg, env) // ParseMachine, then BuildMachine
if err != nil {
	return err // wraps workflow.ErrInvalid; a *dsl.FieldError names the place
}
if err := c.Register(wf); err != nil { // like a machine written in Go
	return err
}
```

The ticket then behaves exactly like a machine written in Go. In the run below, `close` is rejected until the `approved-twice` guard passes, `resume` takes deep history back to `investigating`, and the `note` entry action records `escalated` twice because a history entry runs entry actions again ([`enter`](ds:block?id=machinerun-enter-w658d86g)).

### Edit a definition in Go and build it

A `MachineDefinition` is a plain struct, so an admin form or a migration can read one, change it and build it with [`dsl.BuildMachine`](ds:block?id=dsl-buildmachine-pfc8h78c), which runs [`Validate`](ds:block?id=dsl-machinedefinition-validate-w9g8h2br) first and so checks it the same way as `ParseMachine`. The example builds the ticket twice more, under other names, with the `resume` transition's `history` set to `""` (no history) and `"shallow"`:

<!-- snippet: examples/machine-dsl/main.go#edit -->
```go
def, err := dsl.ParseMachine([]byte(ticketJSON))
if err != nil {
	return err
}
resume := slices.IndexFunc(def.Transitions, func(t dsl.TransitionDef) bool {
	return t.From == "held" && t.Event == "resume"
})
for _, v := range variants {
	if v.name == def.Name {
		continue // the loaded machine already has this mode
	}
	variant := def
	variant.Name = v.name
	variant.Transitions = slices.Clone(def.Transitions) // keep def unchanged
	variant.Transitions[resume].History = string(v.history)
	wf, err := dsl.BuildMachine(variant, reg, env) // Validate, resolve names, DefineMachine
	if err != nil {
		return err
	}
	if err := c.Register(wf); err != nil {
		return err
	}
}
```

The same assign, escalate, investigate, hold and resume then shows the three modes side by side ([`addDescendants`](ds:block?id=machinerun-adddescendants-7p23ezrv)): none enters `review` through its initial states, shallow re-enters the child that was active (`escalated`) through its own initial state, deep re-enters the whole configuration:

```text
history modes, after assign, escalate, investigate, hold:
  none    resume enters [first-line]
  shallow resume enters [triage]
  deep    resume enters [investigating]
```

### Timers

A state's `after` entries become [`workflow.StateTimer`](ds:block?id=statetimer-wwkknhcg)s ([`BuildMachine`](ds:block?id=dsl-buildmachine-pfc8h78c)): `{"delay": "48h", "event": "expire"}` sends `expire` to the instance when it has been in `open` for 48 hours. The delay is a Go duration string and must be positive ([`Validate`](ds:block?id=dsl-machinedefinition-validate-w9g8h2br)). The example passes a fake clock in `MachineEnv.Clock` and to `memory.WithClock`, so the 48 hours pass when it moves the clock:

```text
ticket T-2:
> 48 hours pass while it is open
status of T-2: states [closed], done true, approvals 0, notes [closed]
   1 Mon 09:00 (start)     [] -> [open]
   2 Wed 09:00 expire      [open] -> [closed]
```

### Refusals and storage

Refusals are [`*dsl.FieldError`](ds:block?id=dsl-fielderror-zst5dca3)s with their JSON path. [`ParseMachine`](ds:block?id=dsl-parsemachine-mngjffec) refuses what the data form can get wrong ([`Validate`](ds:block?id=dsl-machinedefinition-validate-w9g8h2br), [`historyMode`](ds:block?id=dsl-historymode-bh8makju): an unknown field, a wrong type, an `unhandled` other than `"ignore"` or `"reject"`, a `history` other than `""`, `"shallow"` or `"deep"`, a timer delay that does not parse or is not positive). `BuildMachine` refuses a guard or action name the registry does not know, listing the registered names ([guards](ds:block?id=dsl-machineregistry-guardlookup-epekbcpd), [actions](ds:block?id=dsl-machineregistry-actionlookup-ugtbmzng)):

<!-- snippet: examples/machine-dsl/main.go#refusal -->
```go
bad := strings.Replace(ticketJSON, `"guard": "approved-twice"`, `"guard": "approved-once"`, 1)
_, err = dsl.LoadMachine([]byte(bad), reg, env)
var fe *dsl.FieldError
if errors.As(err, &fe) {
	fmt.Fprintf(w, "refused at %s: %s\n", fe.Path, fe.Reason)
}
```

```text
refused at transitions[5].guard: no guard is registered as "approved-once" (registered: approved-twice)
```

[`workflow.DefineMachine`](ds:block?id=compilemachine-vd5zfvzw)'s own checks still apply after the names resolve (an unknown target or parent, a compound state without an initial child, history into an atomic state, timers without a client): those errors wrap `workflow.ErrInvalid` too, prefixed with the machine's name ([`BuildMachine`](ds:block?id=dsl-buildmachine-pfc8h78c)), but carry no JSON path. [`dsl.MarshalMachine`](ds:block?id=dsl-marshalmachine-zgtjc9q9) writes a definition back as indented JSON that `ParseMachine` reads to an equal value:

<!-- snippet: examples/machine-dsl/main.go#round-trip -->
```go
def, err := dsl.ParseMachine([]byte(ticketJSON))
if err != nil {
	return err
}
stored, err := dsl.MarshalMachine(def) // indented JSON, ready to store
if err != nil {
	return err
}
back, err := dsl.ParseMachine(stored)
if err != nil {
	return err
}
fmt.Fprintln(w, "round trip equal:", reflect.DeepEqual(def, back))
```

Renaming or removing a state in a stored definition strands the instances that are in it, exactly as in Go ([the event step](ds:block?id=machine-step-vvjdn77u)): see the [state machines Pitfalls](state-machines.md#pitfalls).

## Try it

```sh
cd examples
go run ./workflow-json
```

```text
checked: steps[0].handlr: unknown field (known: after, handler, name, retry, sleep, timeout, wait)
loaded "order": reserve (run) await-payment (wait) ship (run)
both orders waiting for payment
A-1: "shipped A-1, paid 40"
A-2: "shipped A-2, paid 25"
refused: workflow: invalid spec: dsl: steps[1].handler: no handler is registered as "ship-order" (registered: reserve-stock, ship-parcel)
round trip equal: true
```

```sh
go run ./machine-dsl
```

```text
ticket T-1:
> assign
  states: [first-line]
> escalate
  states: [triage]
> investigate
  states: [investigating]
> close
  rejected: no transition takes it in the current state
> hold
  states: [held]
> resume
  states: [investigating]
> approve
  states: [investigating]
> approve
  states: [investigating]
> close
  states: [closed]
status of T-1: states [closed], done true, approvals 2, notes [escalated escalated closed]
   2 Mon 09:00 assign      [] -> [first-line]
   3 Mon 09:00 escalate    [first-line] -> [triage]
   4 Mon 09:00 investigate [triage] -> [investigating]
   5 Mon 09:00 hold        [investigating] -> [held]
   6 Mon 09:00 resume      [held] -> [investigating]
   7 Mon 09:00 approve     [investigating] -> [investigating]
   8 Mon 09:00 approve     [investigating] -> [investigating]
   9 Mon 09:00 close       [investigating] -> [closed]
history modes, after assign, escalate, investigate, hold:
  none    resume enters [first-line]
  shallow resume enters [triage]
  deep    resume enters [investigating]
ticket T-2:
> 48 hours pass while it is open
status of T-2: states [closed], done true, approvals 0, notes [closed]
   1 Mon 09:00 (start)     [] -> [open]
   2 Wed 09:00 expire      [open] -> [closed]
refused at transitions[5].guard: no guard is registered as "approved-once" (registered: approved-twice)
round trip equal: true
T-1 after ForgetMachine, found: false
```

The first program checks a misspelled document, runs two orders through the loaded workflow (each waits for its own payment), refuses a definition naming an unregistered handler and round-trips the definition. The second drives a ticket through the loaded machine, compares the three history modes, lets the timer close a second ticket, refuses an unregistered guard, round-trips the definition and deletes the first ticket with `ForgetMachine`.

## JSON reference

| Field | Type | Maps to | Notes |
|---|---|---|---|
| `name` | string | `Workflow.Name` | Required ([`Definition`](ds:block?id=dsl-definition-m3et9n3c), mapped by [`Build`](ds:block?id=dsl-build-etfzctan)). |
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
| `cron_location` | IANA zone | `Workflow.CronLocation` | Empty means UTC; `Local` is refused ([`location`](ds:block?id=dsl-location-bhcn5hdh)). |
| `steps[i].name` | string | `Step.Name` | Required, unique ([`StepDef`](ds:block?id=dsl-stepdef-uta5kgvn), mapped by [`build`](ds:block?id=dsl-stepdef-build-ghrcfsnm)). |
| `steps[i].after` | array of step names | `Step.After` | Parents that must succeed first. |
| `steps[i].handler` | handler name | `Step.Run` | One of `handler`, `wait` and `sleep`: a [`StepRun`](ds:block?id=steprun-edqhadj2) step. |
| `steps[i].wait` | wait object | `Step.Wait` | One of `handler`, `wait` and `sleep`: a [`StepWait`](ds:block?id=stepwait-q3hc85tv) step. |
| `steps[i].sleep` | duration | `Step.Sleep` | Positive. One of `handler`, `wait` and `sleep`: a [`StepSleep`](ds:block?id=stepsleep-ny78cwqa) step. |
| `steps[i].retry` | retry object | `Step.Retry` | `{"max": 0}` means never retry. |
| `steps[i].timeout` | duration | `Step.Timeout` | Per attempt. |
| `wait.event` | string | `WaitSpec.Event` | Required ([`WaitDef`](ds:block?id=dsl-waitdef-4pcz226d)). |
| `wait.key` | string | `WaitSpec.Key` | A fixed key for every run. |
| `wait.key_label` | label name | `WaitSpec.KeyFrom` | The key is the value of this job label, per run. |
| `wait.timeout` | duration | `WaitSpec.Timeout` | Empty means wait forever. |
| `retry.max` | integer | `RetryPolicy.Max` | Retries after the first attempt ([`RetryDef`](ds:block?id=dsl-retrydef-v8rg24uk)). |
| `retry.backoff.initial` | duration | `Backoff.Initial` | The first retry's wait ([`BackoffDef`](ds:block?id=dsl-backoffdef-v5y23xnr)). |
| `retry.backoff.factor` | number | `Backoff.Factor` | Below 1 counts as 1 ([`Backoff.Delay`](ds:block?id=backoff-delay-4u7wxm4a)). |
| `retry.backoff.max` | duration | `Backoff.Max` | The cap on the wait ([`Backoff`](ds:block?id=backoff-3jh78r24)). |
| `retry.backoff.jitter` | number | `Backoff.Jitter` | In [0, 1]; needs `CapRetryJitter`. |
| `retry.until` | duration | `RetryPolicy.Until` | Needs `CapRetryUntil`. |
| `retry.max_errors` | integer | `RetryPolicy.MaxErrors` | Needs `CapRetryMaxErrors`. |
| `retry.discard_on_exhaust` | boolean | `RetryPolicy.DiscardOnExhaust` | Needs `CapDiscardExhausted`. |
| `concurrency.key_label` | label name | `Concurrency.Key` | The key is the value of this job label; empty means one key for every run ([`ConcurrencyDef`](ds:block?id=dsl-concurrencydef-c824mmdq)). |
| `concurrency.max_runs` | integer | `Concurrency.MaxRuns` | At least 1. |
| `concurrency.strategy` | string | `Concurrency.Strategy` | `queue_newest` (the default), `drop_newest`, `cancel_newest`, `cancel_in_progress`, `round_robin` ([`ConcurrencyDef`](ds:block?id=dsl-concurrencydef-c824mmdq)). |
| `concurrency.fifo` | boolean | `Concurrency.FIFO` | Needs `CapKeyFIFO`. |

A duration is a Go duration string ([`duration`](ds:block?id=dsl-duration-qep8j5u5)): `"300ms"`, `"30s"`, `"1h30m"`. There are no days; write `"72h"`.

## Machine JSON reference

| Field | Type | Maps to | Notes |
|---|---|---|---|
| `name` | string | `MachineSpec.Name` | Required; the machine's and its workflow's name, unique on the client ([`MachineDefinition`](ds:block?id=dsl-machinedefinition-tmpd4qeb), mapped by [`BuildMachine`](ds:block?id=dsl-buildmachine-pfc8h78c)). |
| `initial` | state name | `MachineSpec.Initial` | The top-level state an instance starts in. |
| `states` | array of state objects | `MachineSpec.States` | At least one; document order breaks ties between regions and siblings. |
| `transitions` | array of transition objects | `MachineSpec.Transitions` | For one source, earlier transitions are tried first. |
| `unhandled` | string | `MachineSpec.Unhandled` | `ignore` (the default, [`MachineIgnore`](ds:block?id=machineignore-ud9b526j)) or `reject` ([`MachineReject`](ds:block?id=machinereject-z7kxh7ka)). |
| `ordered` | boolean | `MachineSpec.Ordered` | Handle a key's events in the order sent; needs `CapKeyFIFO`. |
| `keep_log` | integer | `MachineSpec.KeepLog` | Events the log keeps; zero means [`workflow.DefaultMachineLog`](ds:block?id=defaultmachinelog-ujan3j33) (100), negative none. |
| `retry` | retry object | `MachineSpec.Retry` | The event runs' retry policy, the same object as a workflow's `retry`. |
| `states[i].name` | string | `MachineState.Name` | Required, unique ([`StateDef`](ds:block?id=dsl-statedef-6whhsty9)). |
| `states[i].parent` | state name | `MachineState.Parent` | Makes the state a child of a compound or parallel state. |
| `states[i].initial` | state name | `MachineState.Initial` | Required for a compound state: the child entered by default. |
| `states[i].parallel` | boolean | `MachineState.Parallel` | Every child (region) is active at once. |
| `states[i].final` | boolean | `MachineState.Final` | A final child raises the parent's done event; a top-level final state ends the instance. |
| `states[i].entry`, `states[i].exit` | array of action names | `MachineState.Entry`, `MachineState.Exit` | Run in order on every entry and exit. |
| `states[i].after` | array of timer objects | `MachineState.After` | Needs `MachineEnv.Client` with `CapDelayedStart` and `CapIdempotencyKey` ([`compileMachine`](ds:block?id=compilemachine-vd5zfvzw)). |
| `after[j].delay` | duration | `StateTimer.Delay` | Positive ([`TimerDef`](ds:block?id=dsl-timerdef-gstrjz2d)). |
| `after[j].event` | string | `StateTimer.Event` | Required; sent to the instance when the delay passes. |
| `transitions[i].from` | state name | `MachineTransition.From` | Also applies in every state below it ([`TransitionDef`](ds:block?id=dsl-transitiondef-jjhav7bf)). |
| `transitions[i].event` | string | `MachineTransition.Event` | A done event is written as [`workflow.DoneEvent(state)`](ds:block?id=doneevent-xdae986h) returns it. |
| `transitions[i].to` | state name | `MachineTransition.To` | Empty makes a targetless transition: its actions run, no state changes. |
| `transitions[i].guard` | guard name | `MachineTransition.Guard` | Empty means always taken. |
| `transitions[i].actions` | array of action names | `MachineTransition.Actions` | Run between the exits and the entries. |
| `transitions[i].history` | string | `MachineTransition.History` | `""` (`HistoryNone`), `shallow` (`HistoryShallow`) or `deep` (`HistoryDeep`), read by [`historyMode`](ds:block?id=dsl-historymode-bh8makju); needs a compound or parallel target. |
| `transitions[i].reenter` | boolean | `MachineTransition.Reenter` | Leave and enter the source again on a self-transition. |

## Pitfalls

- **A handler name is a contract.** Renaming a registered handler breaks every stored definition that names it, at `Build` ([`resolve`](ds:block?id=dsl-resolve-qt5y6d8c)). Keep the old name registered while definitions move to the new one.
- **Go-only fields have no data form.** `ScheduleWhen`, `NoRetryOn`, `DelayFor`, `KeyExpr`, `OnTimeout` and rate limits are code or engine expressions, so a definition cannot set them ([`Definition`](ds:block?id=dsl-definition-m3et9n3c) has no field for them). Build the workflow from the definition and set them in Go before `Register`, or write that workflow in Go.
- **A per-run key comes from a label.** `key_label` reads `Job.Labels`, not the payload ([`labelKey`](ds:block?id=dsl-labelkey-9hq8uv4m)), so the enqueuer must set the label; a run without it waits on, or is capped under, the empty key.
- **Cron expressions are checked at `Register`, not `Parse`.** The driver's parser owns the dialect, so a mistyped expression passes `Parse` ([`Definition.Validate`](ds:block?id=dsl-definition-validate-3tczvf3z) does not read `cron`) and is refused when the workflow registers.
- **A changed definition does not change runs in flight on its own.** A client registers workflows before `Start` ([`ErrAlreadyStarted`](ds:block?id=erralreadystarted-prauyy24) after it); a new definition takes effect on the client that registers it, and runs started under the old shape follow `Workflow.Version`.
- **The registry is the only gate.** Anyone who can write definitions can call any registered handler with any input the run carries ([`resolve`](ds:block?id=dsl-resolve-qt5y6d8c) is the only lookup). Register only handlers that are safe to call from any step of any definition, and restrict who can write definitions as you restrict who can deploy.
- **A guard or action name is a contract too.** Guard and action names are checked at [`BuildMachine`](ds:block?id=dsl-buildmachine-pfc8h78c), not [`ParseMachine`](ds:block?id=dsl-parsemachine-mngjffec), so a document that parses can still be refused when it loads; keep an old name registered while stored machines move off it.
- **Two machines built from one definition need two names.** Registering a second workflow under a name already registered replaces the first without an error ([`Client.Register`](ds:block?id=client-register-2bax7y5t), [the memory driver's](ds:block?id=memory-client-register-ngcr2sck)), so a copy built with another history mode or timer, as in the example, needs its own `name` or it silently takes the original's place; under its own name its instances are separate from the original's ([`objectRun`](ds:block?id=objectrun-5ayyvf97) keys them by machine name).
- **`DefineMachine`'s refusals have no JSON path.** An unknown target, a compound state without `initial` or history into an atomic state passes `ParseMachine` and fails [`BuildMachine`](ds:block?id=dsl-buildmachine-pfc8h78c) with an error naming the machine and the state or transition index at fault ([`compileMachine`](ds:block?id=compilemachine-vd5zfvzw)), not a `*dsl.FieldError`.
- **A machine with timers needs `MachineEnv.Client`.** Without one, `BuildMachine` refuses the definition; with a client lacking `CapDelayedStart` or `CapIdempotencyKey`, it returns an `*UnsupportedError` ([`compileMachine`](ds:block?id=compilemachine-vd5zfvzw)). A nil `MachineEnv.Clock` times the timers with the real clock, so tests pass a fake one to the env and the driver alike.

## Related

- [Workflows and steps](steps.md), [Wait for an event](signals.md), [Sleep inside a workflow](sleep.md).
- [Store a schedule as JSON](../schedules/manage.md#store-a-schedule-as-json): schedules as data.
- [Durable closures by name](../jobs/functions.md): a single Go function enqueued by name.
- [State machines](state-machines.md): the Go form of a machine, its guarantees and its Pitfalls.
- Examples: [examples/workflow-json](../../examples/workflow-json/), [examples/machine-dsl](../../examples/machine-dsl/).
