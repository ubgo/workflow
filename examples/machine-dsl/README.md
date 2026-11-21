# machine-dsl

A durable state machine whose definition is data. A JSON document lists the states of a support ticket (open with a 48-hour timer, a compound review state whose escalated child is compound too, held, and a final closed state) and its transitions, with the guard and actions named by string. The program registers the Go guard and actions in a `dsl.MachineRegistry[Ticket]`, loads the document with `dsl.LoadMachine` and registers the result on the in-memory driver like a machine written in Go.

It drives ticket T-1 through the machine (a `close` the `approved-twice` guard rejects, a hold and a deep-history resume, two approvals, then `close`), and prints its status and log. It edits the parsed definition in Go and builds it twice more with `dsl.BuildMachine`, with no history and with shallow history, so one hold and resume shows all three history modes. It lets the 48-hour timer close ticket T-2 on a fake clock, shows a definition naming an unregistered guard refused with its JSON path, round-trips the definition through `dsl.MarshalMachine` and `dsl.ParseMachine`, and deletes T-1 with `workflow.ForgetMachine`.

```sh
cd examples
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

`dsl.ParseMachine` reads and checks a document strictly, `dsl.BuildMachine` resolves its guard and action names against the registry and calls `workflow.DefineMachine`, and `dsl.LoadMachine` does both. `dsl.MachineEnv` carries what is not data: the journal store, the client that sends timers and the clock. Every refusal wraps `workflow.ErrInvalid`; one tied to a place in the document is a `*dsl.FieldError` whose `Path` reads like the document. The registry is the security boundary: a definition can only call guards and actions the program registered.

The guide that copies this program is [state machines as data](../../docs/workflows/definitions-as-data.md#state-machines-as-data); the Go form is on [state machines](../../docs/workflows/state-machines.md). `go test ./machine-dsl` checks every line.
