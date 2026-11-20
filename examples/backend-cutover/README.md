# backend-cutover

Moving an app from one backend to another while it keeps running, with package `cutover`, on two in-memory drivers standing in for the database the app is leaving and the one it moves to. A new job lands on the new backend with a `new:` ref, a ref stored before the cutover still answers, `Runs` lists both backends, the schedule the app made on the old backend moves to the new one, and `Remaining` falls to zero once the old backend's last run has finished.

```sh
cd examples
go run ./backend-cutover
```

```text
-- New work goes to the new backend
enqueued: new:run-2
status of run-2: running
status of new:run-2: running
runs: new:run-2, old:run-2
-- Move the schedules
schedule "nightly" on old:schedule-4
moved "nightly" from old:schedule-4 to new:schedule-4
schedule "nightly" on new:schedule-4
-- Retire the old backend
unfinished runs on the old backend: 1
after the export finished: 0 - the app can drop the old backend
```

## The code

One client over both backends:

<!-- snippet: examples/backend-cutover/main.go#client -->
```go
oldBackend, newBackend := memory.New(), memory.New()
c := cutover.New(oldBackend, newBackend)   // schedules go to the new backend
if err := c.Register(export); err != nil { // on both: the old one finishes what it holds
	return err
}
if err := c.Start(ctx); err != nil {
	return err
}
```

Moving the schedules the app made on the old backend:

<!-- snippet: examples/backend-cutover/main.go#move -->
```go
inputs := map[string][]byte{"nightly": []byte(`"nightly"`)} // a ScheduleInfo carries no Input: the app supplies it
moved, err := c.MoveSchedules(ctx, func(info workflow.ScheduleInfo) ([]byte, error) {
	return inputs[info.Name], nil
})
if err != nil {
	return err
}
for _, m := range moved {
	fmt.Printf("moved %q from %s to %s\n", m.Name, m.From, m.To)
}
```

The guide that copies this program is [backend cutover](../../docs/operations/cutover.md). `go test ./backend-cutover` checks every line.
