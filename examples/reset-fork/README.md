# reset-fork

Redo work that already succeeded, two ways, on the in-memory driver: reset a succeeded run in place from the step whose bug was fixed, and fork the run into a new run that starts from a later step with the outputs of the steps before it. The `Then` rule that told the team about the first report does not fire again for either redo.

```sh
cd examples
go run ./reset-fork
```

```text
first run: 41
reset in place: true 42
fork: true 42 fork-of true redo true
extract ran 1 time(s), notify 1
```

The reset kept the run's ID and re-ran `transform` and `publish` with the fix; `extract` kept its output. The fork is a new run that ran `publish` only, carries `workflow.LabelForkOf` (the original's RunID) and `workflow.LabelRedo`, since the original had succeeded. `notify` ran once: `Then` skips runs carrying `LabelRedo` unless `workflow.ThenAfterRedo()` is passed.

The guide that copies this program is [manage runs](../../docs/jobs/manage-runs.md#reset-a-run-that-succeeded). `go test ./reset-fork` checks every line.
