# Multi-choice and races

Two more ways to branch at run time, built of ordinary steps like [`Choice`](branching.md). A multi-choice runs every branch its decision picks and joins exactly those: notify a customer by email and text but not push, because that is what they turned on. A race lets the outside world decide: an offer waits for the customer's reply on any channel, and the first answer to arrive picks the branch.

Use `Choice` when exactly one path runs, `MultiChoice` when any number of paths may run and a later step needs to wait for exactly those, and `Race` when the decision comes from outside the run (a person, a webhook, a deadline).

## Before you start

- `MultiChoice` needs no capability. `Race` needs `CapWaitForEvent` (memory, sqlstore, Redis, Hatchet).
- A local merge that reads what its branches stored uses run data, which needs the [Journal plugin](memoisation.md#turn-on-the-journal).
- The examples are [`examples/flow-patterns`](../../examples/flow-patterns) and, for the mix of "all of" and "any of", [`examples/flow-cancel`](../../examples/flow-cancel). Run them with `go run ./flow-patterns` and `go run ./flow-cancel` from `examples/`.

## Multi-choice

<!-- snippet: examples/flow-patterns/main.go#multichoice -->
```go
steps, err := workflow.MultiChoice(workflow.MultiChoiceSpec{
	Name: "send",
	Decide: func(ctx context.Context, in []byte) ([]string, error) {
		var want map[string]bool
		if err := json.Unmarshal(in, &want); err != nil {
			return nil, workflow.Permanent(err)
		}
		var picked []string
		for _, ch := range []string{"email", "sms", "push"} {
			if want[ch] {
				picked = append(picked, ch)
			}
		}
		return picked, nil
	},
	Branches: []workflow.Branch{{Name: "email", Run: send("email")}, {Name: "sms", Run: send("sms")}, {Name: "push", Run: send("push")}},
})
```

`workflow.MultiChoice` returns a decide step (`<Name>.decide`), one step per branch (`<Name>.<branch>`) and a join named `Name`. The branches not picked end at once without running, so the join waits for exactly the branches taken (a structured synchronizing merge; with one branch taken it is a simple merge). The join's output is a JSON object keyed by branch name holding the outputs of the branches taken. A decision that picks nothing, an unknown branch or one branch twice fails the run (`Permanent`, `ErrInvalid`).

### Merge only the branches that ran

A step that lists several branch steps of a multi-choice (`<Name>.<branch>`) in its `After`, instead of the join `Name`, is a local synchronizing merge: the branches not picked end at once without running, so the merge runs once, right after the last branch that was taken, and never waits for a branch that will not come. Use it when a step needs only some of the branches, or when the branches come from more than one multi-choice. Here the branches of a multi-choice named `page` each store a receipt, and the audit reads the ones that exist:

<!-- snippet: examples/flow-patterns/main.go#merge -->
```go
steps = append(steps, workflow.Step{
	Name:  "audit",
	After: []string{"page.email", "page.sms", "page.push"}, // the branch steps, not the join "page"
	Run: func(ctx context.Context, _ []byte) ([]byte, error) {
		var sent []string
		for _, ch := range []string{"email", "sms", "push"} {
			receipt, ok, err := workflow.RunData[string](ctx, "sent."+ch)
			if err != nil {
				return nil, err
			}
			if ok { // a branch not taken stored nothing
				sent = append(sent, receipt)
			}
		}
		did.add("audit after " + strings.Join(sent, ", "))
		return nil, nil
	},
})
```

Each branch stored its receipt with `workflow.SetRunData(ctx, "sent."+ch, ...)`, one name per branch so parallel branches do not overwrite each other. The merge's input holds each branch step's own record (taken or not), not the plain branch outputs: a merge that needs the outputs follows the join `Name` instead, or reads values the branches stored, as here.

## Race: the first answer wins

<!-- snippet: examples/flow-patterns/main.go#race -->
```go
steps, err := workflow.Race(workflow.RaceSpec{
	Name:  "reply",
	Event: "offer.reply",
	KeyFrom: workflow.KeyFunc(func(j workflow.Job) string {
		var id string
		_ = json.Unmarshal(j.Payload, &id) // the offer ID
		return id
	}),
	Timeout: 72 * time.Hour, // how long the customer has to reply
	Branches: []workflow.Branch{
		{Name: "accept", Run: func(ctx context.Context, in []byte) ([]byte, error) { return []byte(`"order placed"`), nil }},
		{Name: "decline", Run: func(ctx context.Context, in []byte) ([]byte, error) {
			var reason string
			if err := json.Unmarshal(in, &reason); err != nil { // in is the signal's payload
				return nil, workflow.Permanent(err)
			}
			return json.Marshal("offer closed: " + reason)
		}},
	},
})
```

`workflow.Race` returns a wait step (`<Name>.wait`) on the event, keyed like any `WaitSpec`, followed by a choice whose decision is the branch the signal named. Every party that can answer sends `SignalChoice`:

<!-- snippet: examples/flow-patterns/main.go#signal -->
```go
if err := workflow.SignalChoice(ctx, c, "offer.reply", "offer-7", "decline", []byte(`"too expensive"`)); err != nil {
	return err
}
// A later answer loses: the run already took the first one.
if err := workflow.SignalChoice(ctx, c, "offer.reply", "offer-7", "accept", nil); err != nil {
	return err
}
```

The first signal the run takes decides; later ones never reach it. A signal sent before the run reaches its wait is kept, as every signal is, so a quick answer is not lost. Each branch receives the payload its signal carried.

## A default branch at a deadline

A race's `Timeout` fails the run. To take a default branch at a deadline instead (a delayed transition: "expire the listing when nobody answered"), add the default as a branch and enqueue a delayed job that answers for it. It competes like any other party: when someone answered first, its signal simply loses.

<!-- snippet: examples/flow-patterns/main.go#deadline -->
```go
return workflow.Workflow{Name: "listing.deadline", Steps: []workflow.Step{{Name: "expire",
	Run: func(ctx context.Context, in []byte) ([]byte, error) {
		var id string
		if err := json.Unmarshal(in, &id); err != nil { // the listing ID, as the listing run's payload
			return nil, workflow.Permanent(err)
		}
		return nil, workflow.SignalChoice(ctx, c, "listing.answer", id, "expire", nil) // loses quietly if someone answered
	},
}}}
```

Start the deadline job with the run, delayed by the time the run may wait:

<!-- snippet: examples/flow-patterns/main.go#deadline-start -->
```go
ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "listing", Payload: []byte(`"listing-3"`)})
if err != nil {
	return err
}
_, err = c.Enqueue(ctx, workflow.Job{Workflow: "listing.deadline", Payload: []byte(`"listing-3"`), RunAt: time.Now().Add(expireIn)})
if err != nil {
	return err
}
```

The listing's race has a `buy` and an `expire` branch. The deadline job needs `CapDelayedStart` besides the race's `CapWaitForEvent` (memory, sqlstore, Redis, Hatchet).

## Wait for all of, any of, or a mix

- **All of.** Put one wait step per condition and a step whose `After` lists them all: it runs once every event arrived, in any order.
- **Any of.** A race: every condition (a webhook, an operator, a deadline job) sends `SignalChoice` with its own branch, and the first one decides.
- **A mix**, such as "paid and packed, or cancelled": run the "all of" part as a child run whose last step answers the parent's race, and cancel that child from the branch that wins otherwise. The program is [`examples/flow-cancel`](../../examples/flow-cancel), which needs the Journal plugin for `Spawn`.

The child holds one wait per condition, keyed on the order label, and its last step sends the `ship` choice to the parent's race:

<!-- snippet: examples/flow-cancel/main.go#mix-ready -->
```go
return workflow.Workflow{Name: "order.ready", Steps: []workflow.Step{
	{Name: "paid", Wait: &workflow.WaitSpec{Event: "order.paid", KeyFrom: byOrder}},
	{Name: "packed", Wait: &workflow.WaitSpec{Event: "order.packed", KeyFrom: byOrder}},
	{Name: "both", After: []string{"paid", "packed"}, Run: func(ctx context.Context, _ []byte) ([]byte, error) {
		info, _ := workflow.StepInfoFrom(ctx)
		return nil, workflow.SignalChoice(ctx, c, decidedEvent, info.Labels[orderLabel], "ship", nil)
	}},
}}
```

The parent spawns the child in its `watch` step, labelled with the same order ID, then races. The `cancel` branch cancels every unfinished child of `watch`, so a half-met "all of" does not stay waiting forever:

<!-- snippet: examples/flow-cancel/main.go#mix-race -->
```go
watch := workflow.Step{Name: watchStep, Run: func(ctx context.Context, _ []byte) ([]byte, error) {
	info, _ := workflow.StepInfoFrom(ctx)
	_, err := workflow.Spawn(ctx, c, "ready", []workflow.Job{{
		Workflow: "order.ready",
		Labels:   map[string]string{orderLabel: info.Labels[orderLabel]},
	}})
	return nil, err
}}
race, err := workflow.Race(workflow.RaceSpec{
	Name: "decided", After: []string{watchStep}, Event: decidedEvent, KeyFrom: byOrder,
	Branches: []workflow.Branch{
		{Name: "ship", Run: func(context.Context, []byte) ([]byte, error) {
			return json.Marshal("shipped")
		}},
		{Name: "cancel", Run: func(ctx context.Context, _ []byte) ([]byte, error) {
			info, _ := workflow.StepInfoFrom(ctx)
			n, err := workflow.CancelChildren(ctx, c, info.RunID, watchStep) // the half-met "all of"
			if err != nil {
				return nil, err
			}
			return json.Marshal(fmt.Sprintf("cancelled, %d child withdrawn", n))
		}},
	},
})
if err != nil {
	return workflow.Workflow{}, err
}
```

Whoever decides the other way sends the competing choice; the first one the race takes wins and a later one is dropped:

<!-- snippet: examples/flow-cancel/main.go#mix-cancel -->
```go
if err := workflow.SignalChoice(ctx, c, decidedEvent, o.id, "cancel", nil); err != nil {
	return err
}
```

A signal reaches only runs enqueued before it was sent, so start the child before the conditions can happen, or have the senders retry; the program waits until the child is waiting before it sends `order.paid`. `CancelChildren` needs `CapCancelAll` (memory, sqlstore, Redis and Hatchet). Running `go run ./flow-cancel` from `examples/` ends with:

```text
wait for all of, or any of:
  A-1 (order.paid + order.packed): shipped; child succeeded
  A-2 (order.paid): cancelled, 1 child withdrawn; child cancelled
```

## Try it

```sh
cd examples
go run ./flow-patterns
```

Among its output, these lines are this page's: the multi-choice took email and SMS and its join holds both outputs, the race took the first reply (a decline) and ignored the later accept, the audit merged after exactly the two branches taken, and the listing nobody answered took its default branch at the deadline.

```text
multi-choice:
  {"email":"sent by email","sms":"sent by sms"}
race:
  "offer closed: too expensive"
local merge:
  audit after email-receipt-1, sms-receipt-1
race deadline:
  nobody answered: "listing expired"
```

## Pitfalls

- **A race's `Timeout` fails the run** with `ErrWaitTimeout`. For a default branch, see [A default branch at a deadline](#a-default-branch-at-a-deadline).
- **A signal naming no branch of the race fails the run.** Validate the branch name where the answer comes in.
- **A decision that picks nothing, an unknown branch or one branch twice fails the run** (`Permanent`, `ErrInvalid`): return at least one branch, or route the "nothing to do" case to a branch of its own.
- **A signal reaches only runs enqueued before it was sent.** It is kept for a run that is enqueued but has not reached its wait step yet, but a run enqueued after the signal never sees it; that is why the deadline job is enqueued after the run.
- **A local merge reads records, not outputs.** Parse the join's output, or store what the merge needs with `SetRunData` under one name per branch.

## Related

- [Branching with Choice](branching.md), [Signals and waits](signals.md), [Polling and approvals](polling-and-approval.md)
- [Child runs](children.md) and [Joins](joins.md): the "all of" part as a child, `CancelChildren`
- [Trigger rules](trigger-rules.md): steps that run by what their parents did
- [Delayed jobs](../jobs/delays.md): the deadline job's `RunAt`
- Example: [examples/flow-patterns](../../examples/flow-patterns/)
