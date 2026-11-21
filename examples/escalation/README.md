# escalation

Signals addressed to one run by its ID, on the in-memory driver. An order waits with `WaitSpec.OwnRun` and only `workflow.SignalRun` for that order resumes it. An expense report spawns a receipt check that escalates to it with `workflow.SignalParent` and goes on. A migration joins three shards while a `workflow.OnSignals` handler hears the slow ones between its steps. Two refunds wait on an approval step with `ApprovalSpec.OwnRun` and each takes only its own answer from `workflow.ApproveRun` or `workflow.RejectRun`; a dispatch race is decided for one run with `workflow.SignalChoiceRun`; a cart answers an update sent to it alone with `workflow.UpdateRun`.

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

## The code

A wait that only signals addressed to its own run resume:

<!-- snippet: examples/escalation/main.go#address -->
```go
order := workflow.Workflow{Name: "order", Steps: []workflow.Step{
	{Name: "await-payment", Wait: &workflow.WaitSpec{Event: "payment", OwnRun: true}},
	{Name: "ship", After: []string{"await-payment"}, Run: func(_ context.Context, in []byte) ([]byte, error) {
		return in, nil
	}},
}}
```

Send to one run by its ref:

<!-- snippet: examples/escalation/main.go#send -->
```go
if err := workflow.SignalRun(ctx, c, second, "payment", []byte(`"card 4242"`)); err != nil {
	return err
}
```

A child escalates to its parent and goes on; the parent waits on its own address:

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

A handler that runs between the migration's steps, including every look of its join:

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

Approval, race and update steps that wait on the run's own address:

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

Answers addressed to one run each:

<!-- snippet: examples/escalation/main.go#approve-run -->
```go
if err := workflow.ApproveRun(ctx, c, refunds[0], "ana", "receipt checked"); err != nil {
	return err
}
if err := workflow.RejectRun(ctx, c, refunds[1], "bo", "duplicate"); err != nil {
	return err
}
```

<!-- snippet: examples/escalation/main.go#choice-run -->
```go
if err := workflow.SignalChoiceRun(ctx, c, ref, "carrier", "express", []byte(`"next day"`)); err != nil {
	return err
}
```

<!-- snippet: examples/escalation/main.go#update-run -->
```go
uctx, stop := context.WithTimeout(ctx, 10*time.Second) // a run that never reaches its wait never answers
defer stop()
answer, err := workflow.UpdateRun(uctx, c, promises, ref, "add-item", []byte("socks"), await)
if err != nil {
	return err
}
```

The same address behind the client wrappers is [escalation-wrappers](../escalation-wrappers).

The guide: [Escalation and signal handlers](../../docs/workflows/escalation.md).
