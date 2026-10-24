# discard-exhausted

Two rules about retries, on the in-memory driver. Which policy a step gets: `Job.Retry` beats `Step.Retry` beats `Workflow.Retry` beats the default of no retries. And how a run ends when it runs out of retries: `failed` by default (an operator must look, `OnFailure` runs), or `discarded` with no `OnFailure` when the policy sets `RetryPolicy.DiscardOnExhaust` (giving up is routine). A `Permanent` error is discarded the same way when the option is on.

```sh
cd examples
go run ./discard-exhausted
```

```text
workflow policy, DiscardOnExhaust: discarded after 4 attempt(s), OnFailure ran 0, ErrDiscarded true
workflow policy, default ending:   failed    after 4 attempt(s), OnFailure ran 1, ErrDiscarded false
step policy beats workflow:        discarded after 2 attempt(s), OnFailure ran 0, ErrDiscarded true
Job.Retry beats step policy:       failed    after 1 attempt(s), OnFailure ran 0, ErrDiscarded false
Permanent with DiscardOnExhaust:   discarded after 1 attempt(s), OnFailure ran 0, ErrDiscarded true
```

## The code

Four workflows: a quiet one, a loud one, a step that carries its own policy, and a permanent failure:

<!-- snippet: examples/discard-exhausted/main.go#policies -->
```go
// Workflow.Retry is the default for steps that set none: 3 retries.
// "welcome-mail" gives up quietly; "invoice-mail" keeps the default
// ending (failed, OnFailure pages someone).
welcome := workflow.Workflow{
	Name:      "welcome-mail",
	Retry:     workflow.RetryPolicy{Max: 3, DiscardOnExhaust: true},
	Steps:     []workflow.Step{{Name: "send", Run: failing(errDown)}},
	OnFailure: onFailure,
}
invoice := workflow.Workflow{
	Name:      "invoice-mail",
	Retry:     workflow.RetryPolicy{Max: 3},
	Steps:     []workflow.Step{{Name: "send", Run: failing(errDown)}},
	OnFailure: onFailure,
}
// A step's own Retry beats the workflow's, and RetriesNone() is "never".
stepOwn := workflow.Workflow{
	Name:  "step-own",
	Retry: workflow.RetryPolicy{Max: 5},
	Steps: []workflow.Step{{Name: "send", Run: failing(errDown),
		Retry: &workflow.RetryPolicy{Max: 1, DiscardOnExhaust: true}}},
}
// A Permanent error stops at once; with the option on it is discarded
// instead of failed.
permanent := workflow.Workflow{
	Name:  "permanent",
	Retry: workflow.RetryPolicy{Max: 5, DiscardOnExhaust: true},
	Steps: []workflow.Step{{Name: "send", Run: failing(workflow.Permanent(errors.New("mailbox does not exist")))}},
}
```

One dispatch overriding the step's policy:

<!-- snippet: examples/discard-exhausted/main.go#job -->
```go
// Job.Retry beats both: this dispatch gets no retries and the default ending.
if err := report("Job.Retry beats step policy", workflow.Job{Workflow: "step-own", Retry: &workflow.RetryPolicy{Max: 0}}); err != nil {
	return err
}
```

`main_test.go` runs the program and compares its whole output with the block above.

## Guides

- [Retries: which policy a step gets](../../docs/jobs/retries.md#which-policy-a-step-gets)
- [Retries: quiet giving up, DiscardOnExhaust](../../docs/jobs/retries.md#quiet-giving-up-discardonexhaust)
- [Run states: the state machine](../../docs/jobs/run-states.md#the-state-machine)
