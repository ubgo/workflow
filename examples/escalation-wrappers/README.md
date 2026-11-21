# escalation-wrappers

Run-addressed signals behind the client wrappers, every backend the in-memory driver. Two orders wait on their own address (`WaitSpec.OwnRun`) behind `regional`, `failover`, `cutover` and `hybrid`; `workflow.SignalRun` pays the second by the prefixed ref the wrapper returned, and the first keeps waiting. A plugin with the `workflow.ClientBinder` hook keeps the client's `workflow.RunSignalReader` and counts an inbox run's pending signals before each step, while the program reads them with `PendingSignals` and takes one with `TakeSignal` through the regional wrapper. `workflow.Use` refuses the plugin on a client that hides the reader. The test also runs `workflowtest.RunEscalationFlows` over the regional wrapper.

```sh
cd examples
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

## The code

The four wrappers, each over two memory clients:

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

The send is the same call as on a bare driver; the wrapper strips its prefix:

<!-- snippet: examples/escalation-wrappers/main.go#wrapped-send -->
```go
// ref.RunID starts with "us:" behind regional, "primary:" behind
// failover and "new:" behind cutover: SignalRun strips the prefix and
// signals the run's own address on the backend that holds it.
if err := workflow.SignalRun(ctx, c, ref, paymentEvent, []byte(`"card 4242"`)); err != nil {
	return err
}
```

A plugin that binds the client it wraps:

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

<!-- snippet: examples/escalation-wrappers/main.go#use-binder -->
```go
c, err := workflow.Use(base, &gauge{names: []string{noteEvent}, log: log})
```

Reading and taking a run's signals by hand:

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

The run-addressed signal suite over the same regional client:

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

The guide: [Escalation and signal handlers](../../docs/workflows/escalation.md#behind-wrappers).
