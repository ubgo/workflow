# edit-run

`workflow.RunEditor` on the in-memory driver: a reminder scheduled for tomorrow gets a new address and a higher priority and is moved to now before it runs; editing it after it ran is refused with `ErrWrongState`.

```sh
cd examples
go run ./edit-run
```

```text
sent to {"to":"new@example.com"}
editing it after it ran: true
```

The guide that copies this program is [edit a scheduled run](../../docs/jobs/edit-runs.md).
