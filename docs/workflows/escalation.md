# Escalation and signal handlers

A run often needs to hear from someone who knows exactly which run they mean: an operator approving run 42, a child telling its parent that a receipt is over the limit, a payment service answering the order that asked. Every run has an address, its own ID, and a signal sent to that address reaches that run alone. The run takes it in one of two ways: a wait step that blocks until it arrives, or a handler that runs between the run's steps while the run goes on (a BPMN event sub-process).

## Before you start

- A driver with `workflow.CapWaitOwnRun` for a wait on the run's own address: memory, SQL, Redis and Hatchet have it.
- A driver with `workflow.RunSignalReader` for handlers between steps (`workflow.Supports(c, workflow.OpRunSignals)`): memory, SQL and Redis. Hatchet does not have one, so `OnSignals` is refused there.
- [Wait for an event](signals.md) and [child runs](children.md).
- The examples are [`examples/escalation`](../../examples/escalation) (waits, escalation, handlers, and approvals, races and updates by run ID) and [`examples/escalation-wrappers`](../../examples/escalation-wrappers) (the same address behind `regional`, `failover`, `cutover` and `hybrid`, and a plugin that binds its client). Both run on the memory driver. Run them with `go run ./escalation` and `go run ./escalation-wrappers` from `examples/`.

## Signal one run by its ID

**Problem:** a wait step matches events by name and key, and `WaitSpec.KeyFrom` sees the job, not the run, so to reach one run you had to invent a key, store it in the job and keep it next to the run ID.

**Fix:** set `WaitSpec.OwnRun`. The wait takes only the signals addressed to its own run, and the driver resolves that address from the run's ID when the wait begins:

<!-- snippet: examples/escalation/main.go#address -->
```go
order := workflow.Workflow{Name: "order", Steps: []workflow.Step{
	{Name: "await-payment", Wait: &workflow.WaitSpec{Event: "payment", OwnRun: true}},
	{Name: "ship", After: []string{"await-payment"}, Run: func(_ context.Context, in []byte) ([]byte, error) {
		return in, nil
	}},
}}
```

Send with `workflow.SignalRun` and the run's ref:

<!-- snippet: examples/escalation/main.go#send -->
```go
if err := workflow.SignalRun(ctx, c, second, "payment", []byte(`"card 4242"`)); err != nil {
	return err
}
```

`SignalRun` is a `Client.Signal` whose key is `workflow.RunKey(id)`, so the [signal contract](signals.md) holds: a signal sent before the run reaches its wait is kept, and the run takes it once. A plain `Signal` on the same event name, with or without a key, does not reach an `OwnRun` wait. `Key`, `KeyFrom` and `OwnRun` are exclusive; `Register` refuses a driver without `CapWaitOwnRun`.

## Escalate from a child to its parent

**Problem:** a child run finds something its parent must decide (BPMN escalation), but it should not fail or stop to say so, and the parent may be busy waiting for it.

**Fix:** the child calls `workflow.SignalParent` from a step. It sends to the run that spawned it (the `LabelParentRun` that `Spawn`, `AwaitChild` and `JoinChildren` set) and returns; the child goes on. The parent takes the signal with an `OwnRun` wait or an `OnSignals` handler:

<!-- snippet: examples/escalation/main.go#escalate -->
```go
const receiptLimit = 1000 // the most a receipt may claim before a reviewer looks
checkReceipt := workflow.Workflow{Name: "check-receipt", Steps: []workflow.Step{{Name: "check",
	Run: func(ctx context.Context, in []byte) ([]byte, error) {
		var amount int
		if err := json.Unmarshal(in, &amount); err != nil {
			return nil, workflow.Permanent(err)
		}
		if amount > receiptLimit {
			note, _ := json.Marshal(fmt.Sprintf("receipt over limit: %d", amount))
			if err := workflow.SignalParent(ctx, c, "escalation", note); err != nil {
				return nil, err
			}
		}
		return []byte(`"checked"`), nil // the child goes on after escalating
	}}}}
expense := workflow.Workflow{Name: "expense", Steps: []workflow.Step{
	{Name: "check", Run: func(ctx context.Context, in []byte) ([]byte, error) {
		_, err := workflow.Spawn(ctx, c, "receipt", []workflow.Job{{Workflow: "check-receipt", Payload: in}})
		return nil, err
	}},
	{Name: "escalated", After: []string{"check"}, Wait: &workflow.WaitSpec{Event: "escalation", OwnRun: true, Timeout: time.Minute}},
	{Name: "review", After: []string{"escalated"}, Run: func(_ context.Context, in []byte) ([]byte, error) {
		return in, nil
	}},
}}
```

`client` is the client the parent runs on. A run that is no child gets a `Permanent` `ErrInvalid`, so the step does not retry it.

## Handle signals between steps

**Problem:** a wait step stops the run until a signal comes, and a signal that may never come would keep the run open forever. You want the run to go on and react to a signal whenever one arrives: a progress note, a "slow shard" warning, a changed address.

**Fix:** the `workflow.OnSignals` plugin. Before each step attempt of a run of `SignalHandlers.Workflow`, it lists the run's pending signals whose name has a handler, calls each handler in the order the signals were sent, and takes each one its handler accepted. Then the step runs:

<!-- snippet: examples/escalation/main.go#handlers -->
```go
onSlow := workflow.OnSignals(workflow.SignalHandlers{Workflow: "migrate", On: map[string]workflow.SignalHandler{
	"slow": func(_ context.Context, s workflow.RunSignal) error {
		heard.add(string(s.Payload)) // runs before a step of the migration, then the step goes on
		return nil
	},
}})
c, err = workflow.Use(memory.New(), workflow.Journal(workflow.NewMemoryJournal()), onSlow)
```

A step that snoozes is looked at again every poll, so a parent in `JoinChildren`, `AwaitChild` or `Poll` hears its children while it waits. In the example the migration's join hears both slow shards before the join decides.

What a handler returns decides what happens next:

| Handler returns | What happens |
|---|---|
| `nil` | The signal is taken and the step runs (non-interrupting). |
| An error | The step's attempt fails with it and the signal stays; the retry handles it again. |
| A `workflow.Permanent` error | The step fails without a retry, so the run fails (interrupting). |

To end the run early instead, call `workflow.EndRun` (with the `EndRuns` plugin) or `Client.Cancel` from the handler. Signals of names without a handler are left for wait steps or other handlers; a wait step and a handler on one name share one queue, and whichever looks first takes a signal.

`Use` binds the plugin to the client it wraps (the `workflow.ClientBinder` hook) and refuses a client without `RunSignalReader` with an `UnsupportedError`, before anything is registered.

### Bind the client in a plugin

**Problem:** your own plugin needs the driver behind the client (here to read a run's pending signals), and asking the app to pass the client twice, once to `Use` and once to the plugin, invites passing the wrong one.

**Fix:** implement `workflow.ClientBinder` next to `Name`. `Use` calls `BindClient(c)` with the client it wraps, in plugin order and before it collects any middleware, so the plugin keeps what it needs and refuses a client that lacks it. A `BindClient` error fails `Use`, naming the plugin. This gauge counts a run's pending signals before each step and leaves them for wait steps and handlers:

<!-- snippet: examples/escalation-wrappers/main.go#binder -->
```go
// Name implements workflow.Plugin: it names the plugin in Use's refusals.
func (g *gauge) Name() string { return "signal-gauge" }

// BindClient implements workflow.ClientBinder: it keeps the client's RunSignalReader, or refuses a client
// without one, so Use fails before anything is registered.
func (g *gauge) BindClient(c workflow.Client) error {
	r, ok := c.(workflow.RunSignalReader)
	if !ok || !workflow.Supports(c, workflow.OpRunSignals) {
		return workflow.Unsupported("signal-gauge", "the client cannot read a run's signals")
	}
	g.reader = r
	return nil
}

// StepMiddleware implements workflow.StepMiddlewareProvider: it counts
// the run's pending signals before each step.
func (g *gauge) StepMiddleware() []workflow.Middleware {
	return []workflow.Middleware{func(next workflow.Handler) workflow.Handler {
		return func(ctx context.Context, in []byte) ([]byte, error) {
			if info, ok := workflow.StepInfoFrom(ctx); ok {
				sigs, err := g.reader.PendingSignals(ctx, info.RunID, g.names, 0)
				if err != nil {
					return nil, err
				}
				g.log.add(fmt.Sprintf("before %s: %d pending", info.Step, len(sigs)))
			}
			return next(ctx, in)
		}
	}}
}
```

The app passes the plugin to `Use` like any other, here over the regional wrapper:

<!-- snippet: examples/escalation-wrappers/main.go#use-binder -->
```go
c, err := workflow.Use(base, &gauge{names: []string{noteEvent}, log: log})
```

`RunSignalReader` is also yours to call outside any plugin. `PendingSignals` lists the signals addressed to a run that it has not taken, oldest first, for the names you give (a limit of zero means all); `TakeSignal` records that the run took one and reports `false` when someone took it first. Through a wrapper, pass the prefixed `RunID` the ref carries:

<!-- snippet: examples/escalation-wrappers/main.go#read-take -->
```go
reader, ok := c.(workflow.RunSignalReader)
if !ok || !workflow.Supports(c, workflow.OpRunSignals) {
	return errors.New("the client cannot read a run's signals")
}
pending, err := reader.PendingSignals(ctx, ref.RunID, []string{noteEvent}, 0)
if err != nil {
	return err
}
took, err := reader.TakeSignal(ctx, ref.RunID, pending[0].ID)
if err != nil {
	return err
}
again, err := reader.TakeSignal(ctx, ref.RunID, pending[0].ID)
if err != nil {
	return err
}
```

`OnSignals` does exactly this between steps; the program takes `"first"` by hand while the run's first step works, so the gauge sees one signal left before the second step. Use `Supports(c, workflow.OpRunSignals)` and not the type assertion alone: a wrapper may have the methods and still lack a backend that answers them.

## Approvals, races and updates by run ID

The helpers that build wait steps take `OwnRun` too, each with a sender that addresses one run:

| Wait | Sender |
|---|---|
| `ApprovalStep(ApprovalSpec{OwnRun: true})` | `ApproveRun`, `RejectRun` |
| `Race(RaceSpec{OwnRun: true})` | `SignalChoiceRun` |
| A wait with `OwnRun` before an `UpdateHandler` step | `UpdateRun` |

Build the steps with `OwnRun` set. `ApprovalStep` needs exactly one of `Key`, `KeyFrom` and `OwnRun` and refuses the step otherwise; `Race` passes the flag to its wait step (`"<Name>.wait"`); an update is a plain `OwnRun` wait followed by a step whose body is `workflow.UpdateHandler`, which answers through a `PromiseStore` both sides share:

<!-- snippet: examples/escalation/main.go#by-run-steps -->
```go
approval, err := workflow.ApprovalStep(workflow.ApprovalSpec{Name: "approve", OwnRun: true})
if err != nil {
	return err
}
refund := workflow.Workflow{Name: "refund", Steps: []workflow.Step{approval,
	{Name: "pay", After: []string{"approve"}, Run: func(_ context.Context, in []byte) ([]byte, error) {
		d, err := workflow.DecisionOf(in)
		if err != nil {
			return nil, err
		}
		return json.Marshal(fmt.Sprintf("approved=%v by %s", d.Approved, d.By))
	}}}}
race, err := workflow.Race(workflow.RaceSpec{Name: "carrier", Event: "carrier", OwnRun: true, Branches: []workflow.Branch{
	{Name: "express", Run: func(_ context.Context, in []byte) ([]byte, error) { return in, nil }},
	{Name: "ground", Run: func(_ context.Context, in []byte) ([]byte, error) { return in, nil }},
}})
if err != nil {
	return err
}
dispatch := workflow.Workflow{Name: "dispatch", Steps: race}
promises := workflow.NewMemoryPromises() // the update's answer travels through it
cart := workflow.Workflow{Name: "cart", Steps: []workflow.Step{
	{Name: "ask", Wait: &workflow.WaitSpec{Event: "add-item", OwnRun: true}},
	{Name: "answer", After: []string{"ask"}, Run: workflow.UpdateHandler(promises, func(_ context.Context, in []byte) ([]byte, error) {
		return json.Marshal("added " + string(in))
	})},
}}
```

Answer an approval with the run's ref. `ApproveRun` and `RejectRun` send a `workflow.Decision` through `SignalRun`, so each answer reaches only the run it names; the step after the approval reads it with `workflow.DecisionOf`:

<!-- snippet: examples/escalation/main.go#approve-run -->
```go
if err := workflow.ApproveRun(ctx, c, refunds[0], "ana", "receipt checked"); err != nil {
	return err
}
if err := workflow.RejectRun(ctx, c, refunds[1], "bo", "duplicate"); err != nil {
	return err
}
```

Decide a race for one run with `SignalChoiceRun`. The first signal the run takes picks the branch, which receives the payload; a later one is never taken by that run. A signal sent before the run reaches its wait is kept, so the program sends it right after the enqueue:

<!-- snippet: examples/escalation/main.go#choice-run -->
```go
if err := workflow.SignalChoiceRun(ctx, c, ref, "carrier", "express", []byte(`"next day"`)); err != nil {
	return err
}
```

Send an update and wait for the run's answer with `UpdateRun`. It wraps the payload in a `workflow.UpdateRequest` carrying a fresh promise ID, sends it through `SignalRun`, and blocks until the run settles that promise or `ctx` ends, so give it a deadline:

<!-- snippet: examples/escalation/main.go#update-run -->
```go
uctx, stop := context.WithTimeout(ctx, 10*time.Second) // a run that never reaches its wait never answers
defer stop()
answer, err := workflow.UpdateRun(uctx, c, promises, ref, "add-item", []byte("socks"), await)
if err != nil {
	return err
}
```

A `Permanent` error from the update's handler rejects the update: `UpdateRun` returns a `*workflow.PromiseRejectedError`. Any other error goes to the step's retry policy while the caller keeps waiting. The promise is deleted once read.

## Behind wrappers

**Problem:** a driver resolves an `OwnRun` wait to the run ID it handed out, but `regional`, `failover` and `cutover` hand the app a ref prefixed with a region or a backend (`us:`, `primary:` or `secondary:`, `old:` or `new:`), and the `RunID` a step sees carries the prefix too. A key built from the prefixed ID would match no run.

**Fix:** nothing in your code. The three wrappers implement `workflow.RunSignaller`: `SignalRun` hands them the ref, they strip the prefix and call `SignalRun` on the backend that holds the run, so the address matches what its driver resolved. `PendingSignals` and `TakeSignal` read the prefix of the run ID and ask that backend, so `OnSignals` works behind them. `hybrid` hands out its jobs client's refs unprefixed and forwards these calls to the jobs client, where every run lives; the client `Use` returns forwards them to what it wraps. The example runs the same order behind each wrapper, every one over two memory clients:

<!-- snippet: examples/escalation-wrappers/main.go#wrappers -->
```go
// newRegional joins two memory clients as the regions "eu" and "us",
// picking the region from the job's region label ("eu" without one).
func newRegional() (workflow.Client, error) {
	return regional.New(map[string]workflow.Client{"eu": memory.New(), "us": memory.New()},
		regional.ByLabel(regionLabel, "eu"))
}

// wrappers lists the four wrappers, each over two fresh memory clients.
func wrappers() []wrapper {
	return []wrapper{
		{"regional", newRegional},
		{"failover", func() (workflow.Client, error) {
			return failover.New(memory.New(), memory.New(), failover.AllowWithoutKey()), nil
		}},
		{"cutover", func() (workflow.Client, error) { return cutover.New(memory.New(), memory.New()), nil }},
		{"hybrid", func() (workflow.Client, error) { return hybrid.New(memory.New(), memory.New()), nil }},
	}
}
```

The send is the same call as on a bare driver:

<!-- snippet: examples/escalation-wrappers/main.go#wrapped-send -->
```go
// ref.RunID starts with "us:" behind regional, "primary:" behind
// failover and "new:" behind cutover: SignalRun strips the prefix and
// signals the run's own address on the backend that holds it.
if err := workflow.SignalRun(ctx, c, ref, paymentEvent, []byte(`"card 4242"`)); err != nil {
	return err
}
```

A ref without a prefix the wrapper knows (an ID a step read from `StepInfo` of another backend, or a run the old backend held before a cutover) is signalled at the run's address wherever `Signal` goes: every region, or the ones `regional.WithSignalRouter` picks, and both backends of `failover` and `cutover`. `regional` answers `PendingSignals` for a run of no region it knows with no signals.

## Which drivers

| Driver | `OwnRun` wait (`CapWaitOwnRun`) | Handlers between steps (`RunSignalReader`) |
|---|---|---|
| memory | yes | yes |
| SQL (SQLite, Postgres, MySQL) | yes | yes: the signals table and the run's consumed rows, the take under the run's row lock |
| Redis | yes | yes: Lua scripts over the signal sets and the run's consumed markers |
| Hatchet | yes: the durable wait resolves the key from the engine's run ID | no: the engine keeps no record a step could read of which events a run took |

### Prove it on your driver

`workflowtest.RunEscalationFlows` is the suite behind this table. It runs the `OwnRun` wait (including a signal kept until the wait begins), `SignalParent` from a child to a waiting parent and the refusal outside a child, `OnSignals` handlers in send order, escalations heard while the parent joins its children, a handler error retried with its step, the `RunSignalReader` contract (take once, unknown IDs are `ErrNotFound`), `ApproveRun`, `RejectRun`, `SignalChoiceRun` and `UpdateRun` reaching only the run they name, and a handler that fails the run. A case whose capability or `RunSignalReader` the client lacks skips. Your `workflowtest.EscalationHarness.Start` applies the `Journal` plugin and then the suite's plugins with `workflow.Use`, over the driver client itself so `OnSignals` binds its reader, registers the workflows, starts the client and stops it when the test ends. The example runs it over its regional wrapper:

<!-- snippet: examples/escalation-wrappers/main_test.go#suite -->
```go
// TestEscalationFlows runs the run-addressed signal suite over the
// regional client the program builds: OwnRun waits, SignalParent, OnSignals
// handlers, the RunSignalReader contract and ApproveRun, RejectRun,
// SignalChoiceRun and UpdateRun. A case whose capability the client lacks
// skips.
func TestEscalationFlows(t *testing.T) {
	workflowtest.RunEscalationFlows(t, workflowtest.EscalationHarness{
		Start: func(t *testing.T, plugins []workflow.Plugin, wfs ...workflow.Workflow) workflow.Client {
			base, err := newRegional()
			if err != nil {
				t.Fatal(err)
			}
			// The Journal plugin first, then the suite's plugins (OnSignals).
			c, err := workflow.Use(base, append([]workflow.Plugin{workflow.Journal(workflow.NewMemoryJournal())}, plugins...)...)
			if err != nil {
				t.Fatal(err)
			}
			for _, wf := range wfs {
				if err := c.Register(wf); err != nil {
					t.Fatal(err)
				}
			}
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			if err := c.Start(ctx); err != nil {
				t.Fatal(err)
			}
			return c
		},
	})
}
```

The memory, SQL (through `sqlstore/enginetest`) and Redis drivers run it, and so do `regional`, `failover`, `cutover` and `hybrid` over memory clients.

## Try it

```sh
cd examples
go run ./escalation
```

```text
signal a run by its ID:
  second order: paid "card 4242"
  first order: waiting
escalation to the parent:
  parent took: "receipt over limit: 1200"
handlers between steps:
  {"heard":["shard 1 is slow","shard 3 is slow"],"shards":3}
approvals, races and updates by run ID:
  refund 1: "approved=true by ana"
  refund 2: "approved=false by bo"
  dispatch took: "next day"
  cart answered: "added socks"
```

The second order is paid by its ref while the first, waiting on the same event name, still waits. The two refunds wait on the same step and each takes only its own answer.

```sh
go run ./escalation-wrappers
```

```text
signal a run by its ID behind a wrapper:
  regional ref prefix "us"       paid "card 4242", other order waiting
  failover ref prefix "primary"  paid "card 4242", other order waiting
  cutover  ref prefix "new"      paid "card 4242", other order waiting
  hybrid   ref prefix ""         paid "card 4242", other order waiting
a plugin that binds the client:
  pending while working: 2, took "first": true, took it again: false
  gauge before work: 0 pending
  gauge before report: 1 pending
  without RunSignalReader: unsupported true
```

Behind every wrapper the paid order is the second one and the first keeps waiting. `hybrid` refs carry no prefix. The gauge counts the inbox's two notes minus the one the program took; `Use` refuses it on a client that hides `RunSignalReader`.

## Pitfalls

- **Handlers run at step boundaries, not at once.** A run parked in a wait or a sleep handles nothing until its next step begins. Block on a signal with an `OwnRun` wait; keep a snoozing step's `Poll` short when a handler must react quickly.
- **Handlers run at least once.** A worker that dies between a handler and its take, or two steps of one run starting at the same moment, run a handler twice for one signal. Make handlers idempotent; `RunSignal.ID` identifies the signal.
- **A retried step sends again.** `SignalParent` and `SignalRun` from a step that is retried send a second signal. Put an ID in the payload when the parent must count each one once.
- **Every step attempt reads the run's signals.** `OnSignals` costs one read per step attempt of the workflows it serves (one query on SQL, one script on Redis). Name the workflow in `SignalHandlers.Workflow` so other workflows pay nothing.
- **An update needs a deadline.** `UpdateRun` waits until the run settles the reply promise; a run that never reaches its wait never answers. Pass a `ctx` with a timeout, and share a durable `PromiseStore` when the caller and the workers run in different processes.
- **`OwnRun` is one of three.** `ApprovalStep` refuses a spec that sets more or fewer than one of `Key`, `KeyFrom` and `OwnRun`; a wait (including a race's) that sets `OwnRun` with `Key` or `KeyFrom` is refused when it is registered.
- **Your own wrapper must not hide the reader.** A struct that embeds `workflow.Client` hides `RunSignalReader` and `RunSignaller`, so `OnSignals` is refused (the example's `bare` client shows it) and `SignalRun` falls back to a plain `Signal` that cannot strip your prefix. Embed `workflow.Forward` to pass them through, and implement `RunSignaller` if your wrapper prefixes refs.
- **Plugin names are unique per `Use` call.** Two `OnSignals` with one `Workflow`, or two of your binders with one `Name`, fail `Use` with `ErrDuplicatePlugin`.
- **`OnFailure` and `OnTimeout` are wrapped too.** Like every step middleware, the plugin wraps them; a driver that passes them the run's `StepInfo` handles the pending signals before them, and an error there is the failure handler's error, which the driver logs.

## Related

- [Wait for an event](signals.md), [child runs](children.md), [joins](joins.md), [polling and approval](polling-and-approval.md)
- [Regional routing](../operations/regional-routing.md), [failover](../reliability/failover.md), [cutover](../operations/cutover.md), [hybrid placement](../drivers/hybrid.md)
- [Writing a plugin](../extending/writing-a-plugin.md)
- Examples: [`examples/escalation`](../../examples/escalation), [`examples/escalation-wrappers`](../../examples/escalation-wrappers)
