# Multi-choice and races

Two more ways to branch at run time, built of ordinary steps like [`Choice`](branching.md). A multi-choice runs every branch its decision picks and joins exactly those. A race lets the outside world decide: the first answer to arrive picks the branch.

## Before you start

- `MultiChoice` needs no capability. `Race` needs `CapWaitForEvent` (memory, sqlstore, Redis, Hatchet).
- The example is [`examples/flow-patterns`](../../examples/flow-patterns).

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

A step that lists several branches of a multi-choice in its `After` (instead of the join `Name`) is a local synchronizing merge: the branches not picked end at once without running, so the merge runs once, right after the last branch that was taken, and never waits for a branch that will not come. Use it when a step needs only some of the branches, or when the branches come from more than one multi-choice:

```go
steps = append(steps, workflow.Step{
	Name:  "audit",
	After: []string{"send.email", "send.sms", "send.push"}, // waits only for the branches taken
	Run:   audit,
})
```

The merge's input holds each branch step's own record (taken or not), not the plain branch outputs: a merge that needs the outputs follows the join `Name` instead, or reads values the branches stored with `SetRunData`.

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
	Timeout: 72 * time.Hour,
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

A race's `Timeout` fails the run. To take a default branch at a deadline instead (a delayed transition: "expire the offer when nobody answered in 72 hours"), enqueue a delayed job that answers for that branch. It competes like any other party: when someone answered first, its signal simply loses.

```go
deadline := workflow.Workflow{Name: "offer.deadline", Steps: []workflow.Step{{Name: "expire",
	Run: func(ctx context.Context, in []byte) ([]byte, error) {
		var id string
		if err := json.Unmarshal(in, &id); err != nil { // the offer ID, as the offer run's payload
			return nil, workflow.Permanent(err)
		}
		return nil, workflow.SignalChoice(ctx, c, "offer.reply", id, "expire", nil)
	},
}}}

ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "offer", Payload: []byte(`"offer-7"`)})
// ...
_, err = c.Enqueue(ctx, workflow.Job{Workflow: "offer.deadline", Payload: []byte(`"offer-7"`), RunAt: time.Now().Add(72 * time.Hour)})
```

Add an `expire` branch to the race. The deadline job needs `CapDelayedStart` besides the race's `CapWaitForEvent` (memory, sqlstore, Redis, Hatchet).

## Wait for all of, any of, or a mix

- **All of.** Put one wait step per condition and a step whose `After` lists them all: it runs once every event arrived, in any order.
- **Any of.** A race: every condition (a webhook, an operator, a deadline job) sends `SignalChoice` with its own branch, and the first one decides.
- **A mix**, such as "paid and packed, or cancelled": run the "all of" part as a child run whose last step answers the parent's race, and cancel that child from the branch that wins otherwise.

```go
ready := workflow.Workflow{Name: "order.ready", Steps: []workflow.Step{
	{Name: "paid", Wait: &workflow.WaitSpec{Event: "order.paid", KeyFrom: byOrder}},
	{Name: "packed", Wait: &workflow.WaitSpec{Event: "order.packed", KeyFrom: byOrder}},
	{Name: "both", After: []string{"paid", "packed"}, Run: func(ctx context.Context, _ []byte) ([]byte, error) {
		info, _ := workflow.StepInfoFrom(ctx)
		return nil, workflow.SignalChoice(ctx, c, "order.decided", info.Labels["order"], "ship", nil)
	}},
}}
race, err := workflow.Race(workflow.RaceSpec{
	Name: "decided", After: []string{"watch"}, Event: "order.decided", KeyFrom: byOrder,
	Branches: []workflow.Branch{
		{Name: "ship", Run: ship},
		{Name: "cancel", Run: func(ctx context.Context, _ []byte) ([]byte, error) {
			info, _ := workflow.StepInfoFrom(ctx)
			_, err := workflow.CancelChildren(ctx, c, info.RunID, "watch") // the half-met "all of"
			return nil, err
		}},
	},
})
// The "watch" step before the race spawns order.ready with Spawn, labelled with the order ID.
```

A signal reaches only runs enqueued before it was sent, so start the child before the conditions can happen, or have the senders retry. The child needs `workflow.Spawn` (the Journal plugin) and `CancelChildren` needs `CapCancelAll` (memory, sqlstore, Redis).

## Pitfalls

- A race's `Timeout` fails the run with `ErrWaitTimeout`. For a default branch, see [A default branch at a deadline](#a-default-branch-at-a-deadline).
- A signal naming no branch of the race fails the run.
