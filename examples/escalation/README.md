# escalation

Signals addressed to one run by its ID, on the in-memory driver. An order waits with `WaitSpec.OwnRun` and only `workflow.SignalRun` for that order resumes it. An expense report spawns a receipt check that escalates to it with `workflow.SignalParent` and goes on. A migration joins three shards while a `workflow.OnSignals` handler hears the slow ones between its steps.

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
checkReceipt := workflow.Workflow{Name: "check-receipt", Steps: []workflow.Step{{Name: "check",
	Run: func(ctx context.Context, in []byte) ([]byte, error) {
		var amount int
		if err := json.Unmarshal(in, &amount); err != nil {
			return nil, workflow.Permanent(err)
		}
		if amount > 1000 {
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
c, err := workflow.Use(memory.New(), workflow.Journal(workflow.NewMemoryJournal()), onSlow)
```

The guide: [Escalation and signal handlers](../../docs/workflows/escalation.md).
