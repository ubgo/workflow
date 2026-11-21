# flow-boundary

Three ways to act on a step around its work, on the in-memory driver: a non-interrupting timer (`workflow.Boundary`) that sends a reminder while a slow review carries on, an interrupting deadline whose timeout a trigger rule sends to a fallback step, and a setup and teardown pair where the setup stores the environment it created as run data (`workflow.SetRunData`) and a Finally step (`RulesSpec.Finally`) reads it back (`workflow.RunData`) and deletes it whether the test suite passed or failed.

```sh
cd examples
go run ./flow-boundary
```

```text
boundary timer:
  quick review: "approved"
    ran: file, review
  slow review: "approved"
    ran: file, reminder: claim-slow still in "review" after 20ms, review
interrupting deadline:
  "cached price 9.90 (price failed: timeout)"
setup and teardown:
  green suite: run succeeded
    ran: create env-42, test, delete env-42
  broken suite: run failed
    ran: create env-42, test, delete env-42
```

The quick review ends before the timer, so it gets no reminder. The broken suite's run ends failed although the teardown succeeded: a Finally step keeps the run's status honest.

The guide that copies this program is [trigger rules and exit handlers](../../docs/workflows/trigger-rules.md). `go test ./flow-boundary` checks every line of the output.
