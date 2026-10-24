# named-queues

Named queues on the in-memory driver: `Workflow.Queue` as a workflow's default queue, `Job.Queue` to send one run elsewhere, `memory.WithQueues` to give each queue a priority and a worker cap, and `Pause` / `Resume` on one queue while the others keep running. One worker serves every queue, so the order the steps run in is the order the queues hand them out.

```sh
cd examples
go run ./named-queues
```

```text
build-report hold on reports
send-email password-reset on urgent
send-email newsletter on mail
build-report monthly on reports
build-report weekly on reports
digest while mail is paused: queued
send-email digest on mail
enqueue to a queue this client does not serve: workflow: invalid spec: queue "sms" is not served by this client (WithQueues); the run would never start
```

The first report holds the only worker while three more runs queue up. When it ends, the `urgent` queue (priority 10) goes first, then `mail` (5), then `reports`. With `mail` paused, the weekly report still runs and the digest waits in `queued` until `Resume`.

The guide that copies this program is [named queues](../../docs/jobs/queues.md). `go test ./named-queues` checks every line.
