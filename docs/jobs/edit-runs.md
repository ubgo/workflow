# Edit a scheduled run

A reminder is scheduled for tomorrow with the wrong address. Cancelling it and enqueuing a new one loses its RunRef, its place in the admin and its idempotency key; editing it keeps them. `workflow.RunEditor` changes a delayed run's payload, priority and start before it runs.

## Before you start

- `RunEditor` is an optional interface with no capability bit, like `RunRescheduler`: check with a type assertion. The memory, SQL and Redis drivers have it; Hatchet does not (its scheduled runs cannot be edited), and a wrapper over a client without it returns an `UnsupportedError`.
- The example is [`examples/edit-run`](../../examples/edit-run). Run it with `go run ./edit-run` from `examples/`.

## Edit the run

<!-- snippet: examples/edit-run/main.go#edit -->
```go
editor, ok := workflow.Client(c).(workflow.RunEditor)
if !ok {
	return errors.New("this driver cannot edit runs")
}
payload := []byte(`{"to":"new@example.com"}`)
priority := 10
now := time.Now()
ref, err = editor.EditRun(ctx, ref, workflow.JobEdit{Payload: &payload, Priority: &priority, RunAt: &now})
if err != nil {
	return err // ErrWrongState once the run has started
}
```

A `workflow.JobEdit` field left nil is not changed: `Payload` replaces the payload (the driver's payload limit applies), `Priority` replaces `Job.Priority`, `RunAt` moves the start as `Reschedule` does (a past time means now). The run keeps its RunRef, labels, idempotency key and queue. A concurrency or rate key computed from the payload is computed again from the new one.

## From the admin

The admin handler serves `POST /runs/{id}/edit` (add `?scheduled=1` when `{id}` is a ScheduledID, as for every run route) with a body of `{"payload": <JSON>, "priority": n, "run_at": "RFC 3339"}`, each field optional, behind the action `admin.ActionEditRun`; the `Authorizer` sees the edit in `Resource.Edit`, so it can allow a new time and refuse a new payload. `Admin.EditRun` is the same operation in Go (`admin.OpEditRun`).

## Pitfalls

- Only a run in `StatusScheduled` can be edited: a run that was admitted may already be running on another node, so any other status returns `ErrWrongState`, and so does an edit that races the run's due time and loses. An unknown ref returns `ErrNotFound`, an edit that changes nothing `ErrInvalid`.
- Labels, queue and idempotency key cannot be edited; cancel and enqueue again for that.
