# Event log, replay and cancel on event

Three problems sit next to a bus. The receipt mailer was down for an hour, so the receipts of that hour's `invoice.issued` events failed and must be sent again, but publishing the same event IDs again starts nothing, because the bus deduplicates by ID. A customer checks out, so the reminder runs for that cart must stop. An order is cancelled, so its fulfilment must stop and a refund must start instead. The event log with `Bus.Replay` solves the first; a cancel subscription solves the other two.

## Before you start

- The log and replay run on every driver. A replay repeated with the same ID starts nothing twice while its runs are unfinished on every driver, and also after they finished on a client with `workflow.RunQuerier` (`CapRunQuery`: memory, SQL, Redis).
- A cancel subscription needs `workflow.CapCancelAll` (memory, SQL, Redis; not Hatchet); `SubscribeCancel` refuses a client without it with an `*workflow.UnsupportedError`. A divert also needs `CapRunQuery`, which every driver with `CapCancelAll` has.
- The example is [`examples/events-more`](../../examples/events-more). Run it with `go run ./events-more` from `examples/`.

## Keep a log

`events.WithLog(log)` makes `Publish` append every message it accepts to an `events.EventLog` before delivering it, whether or not any subscription matches. `events.WithClock` sets the clock that stamps each entry:

<!-- snippet: examples/events-more/main.go#log -->
```go
log := events.NewMemoryLog() // in-process; a durable EventLog for a fleet
b, err := events.New(c, events.WithLog(log), events.WithClock(clk))
if err != nil {
	return err
}
```

`EventLog` is two methods, `Append(ctx, m, at)` and `Range(ctx, from, to, fn)`, so the log can live where your data does (a table, an object store). `events.NewMemoryLog()` is the in-process default: lost on restart, one per process, with `Len` and `Prune(before)` for retention. Messages `Publish` refuses (no name, reserved labels, a version newer than current) are not logged, and an `Append` error fails the publish before anything is delivered, so every delivered message is in the log. The log keeps the message as published, before upcasting, so a replay after a deploy upcasts with the new code.

## Replay logged events

`Bus.Replay(ctx, events.ReplayOptions{...})` offers each logged message in `[From, To)` that passes `Filter` to the named subscriptions (all of them when `Subscriptions` is empty), exactly as `Publish` would: event name, the subscription's `Filter`, upcasting, its `Job`.

<!-- snippet: examples/events-more/main.go#replay -->
```go
from := time.Date(2026, 11, 20, 10, 0, 0, 0, time.UTC) // the hour the receipt mailer was down
r, err := b.Replay(ctx, events.ReplayOptions{
	ID:            "mailer-outage-2026-11-20",
	From:          from,
	To:            from.Add(2 * time.Hour),
	Subscriptions: []string{"receipt"},
})
if err != nil {
	return err
}
```

```text
replay:
  log holds 3 messages; replayed 2 to receipt: ["inv_2" "inv_3"]
  same replay ID again: 2 of 2 already started, new receipt runs 0
```

Each replayed run carries the idempotency key `event/<subscription>/<id>/replay/<replay ID>` and the label `events.LabelReplay`, so the original delivery's key does not swallow it even while the original run is unfinished. Replaying again with the same `ID` starts nothing twice: the key holds while the replayed run is unfinished, and on a client with run queries `Replay` first looks for a run labelled with the message, subscription and replay ID in any status and reports it as `Existing`. Leave `ID` empty for a new one each call. A message ID logged twice (a retried publish) is delivered once.

`ReplayResult` has the replay `ID`, how many distinct `Messages` matched, and one `ReplayDelivery` per message and subscription with its `Ref`, `Existing` and `Err`. Failures are per delivery and joined into the returned error, so calling `Replay` again with the same ID retries only those. `Replay` refuses, with `ErrInvalid`, a bus without a log, `From` not before `To`, and an unknown subscription name.

## Cancel runs on an event

`Bus.SubscribeCancel(events.CancelSubscription{...})` cancels, for every message of `Event`, every unfinished run whose label `Label` equals the message's `Key`, through `workflow.BulkCanceller.CancelAll`, across workflows:

<!-- snippet: examples/events-more/main.go#cancel -->
```go
err = b.SubscribeCancel(events.CancelSubscription{
	Name:  "stop-cart-reminders",
	Event: "cart.checked-out",
	Label: "cart", // cancels every unfinished run labelled cart=<the message's Key>
})
if err != nil {
	return err
}
```

<!-- snippet: examples/events-more/main.go#cancel-publish -->
```go
p, err := b.Publish(ctx, events.Message{ID: "chk_1", Name: "cart.checked-out", Key: "c1"})
if err != nil {
	return err
}
fmt.Println("cancel on event:")
fmt.Printf("  %s cancelled %d run(s)\n", p.Cancellations[0].Subscription, p.Cancellations[0].Cancelled)
```

```text
cancel on event:
  stop-cart-reminders cancelled 1 run(s)
  cart c1 reminder: cancelled; cart c2 reminder: running
```

`Key` reads the label value from the message when it is not `m.Key`; a message for which it is empty cancels nothing and its `Cancellation` holds `ErrInvalid`, since an empty value would match every run carrying the label empty. `Workflow` narrows the cancel to one workflow, and `Statuses` to some statuses; the default, `events.UnfinishedStatuses`, includes running and waiting runs, so a running reminder is cut short, unlike `CancelAll`'s own default of runs not yet started. `Filter` works as on a subscription. `Published.Cancellations` reports each cancel subscription's `Cancelled` count and error.

`Publish` runs every cancel before it starts any run, so the runs a message starts are never cancelled by the same message, even when they carry the label. Cancel subscriptions share the name space of subscriptions, and `Replay` never runs them: cancelling later would hit runs that did not exist when the event happened.

## Divert to another workflow

With `Divert` set, the cancel subscription is an external trigger exception: after the cancel succeeds it starts a run of `Divert` with the message's payload, so the process leaves its normal path for the exception path:

<!-- snippet: examples/events-more/main.go#divert -->
```go
err = b.SubscribeCancel(events.CancelSubscription{
	Name:   "cancel-to-refund",
	Event:  "order.cancelled",
	Label:  "order",
	Divert: "refund", // after the cancel, start refund with the message's payload
})
if err != nil {
	return err
}
```

```text
divert:
  cancelled 1 fulfilment run(s); refund ran with {"order":42}
  republish: cancelled 0, same refund run true, refund runs 1
```

`DivertJob` builds the job instead (another payload, a queue, `RunAt`). The divert run carries the bus's labels and the key `event/<name>/<id>`, like a subscription's run. It starts whether the cancel found runs or not, because a publish retried after a crash between the cancel and the divert finds nothing left to cancel and must still divert; it does not start when the cancel failed, so a republish retries both. A message whose divert run already exists, in any status, was handled: `Publish` finds that run and neither cancels nor diverts again, which keeps a republish from cancelling the divert run itself when it carries the cancelled label. `Cancellation.Diverted` is the divert run.

## Pitfalls

- `MemoryLog` lives in one process. A fleet where every replica publishes needs one durable `EventLog` they share, or each replica replays only its own messages.
- A replay without run queries is deduplicated only while its runs are unfinished; on such a client keep the replay ID per incident and do not repeat it after the runs finished.
- Cancelling is by label, so the runs to stop must carry it: label them at enqueue (`Labels: {"order": "42"}`), or publish the event with that label so subscriptions copy it onto their runs.
- `SubscribeCancel` refuses a terminal status in `Statuses` and a subscription without `Name`, `Event` or `Label`, with `ErrInvalid`.
