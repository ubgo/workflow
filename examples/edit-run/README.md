# edit-run

`workflow.RunEditor` on the in-memory driver: a reminder scheduled for tomorrow gets a new address and a higher priority and is moved to now before it runs; editing it after it ran is refused with `ErrWrongState`. A second reminder is then edited through the admin module: `Admin.EditRun` raises its priority, and `POST /runs/{id}/edit` (with `?scheduled=1`, since a delayed run not yet started has only a `ScheduledID`) runs behind an `Authorizer` that lets support staff move the run but answers 403 when they change its payload; a lead may, and a run that already ran answers 409.

```sh
cd examples
go run ./edit-run
```

```text
sent to {"to":"new@example.com"}
editing it after it ran: true
admin edit: priority 20 status scheduled
POST /runs/{id}/edit: support moves it 200 support readdresses it 403 lead readdresses it 200 a finished run 409
after the HTTP edits: due in 24h0m0s more, priority 20 same ref true
```

The guide that copies this program is [edit a scheduled run](../../docs/jobs/edit-runs.md).
