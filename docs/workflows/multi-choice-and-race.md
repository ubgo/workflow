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

## Pitfalls

- A race's `Timeout` fails the run with `ErrWaitTimeout`. For a default branch at a deadline, enqueue a delayed job that sends `SignalChoice` for it: it simply loses when someone answered first.
- A signal naming no branch of the race fails the run.
