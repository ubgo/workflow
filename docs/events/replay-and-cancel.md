# Event log, replay and cancel on event

Three problems sit next to a bus. The receipt mailer was down for an hour, so the receipts of that hour's `invoice.issued` events failed and must be sent again, but publishing the same event IDs again starts nothing, because the bus deduplicates by ID ([`deliver`](ds:block?id=events-bus-deliver-zntu7tc7)). A customer checks out, so the reminder runs for that cart must stop. An order is cancelled, so its fulfilment must stop and a refund must start instead. The event log with [`Bus.Replay`](ds:block?id=events-bus-replay-r7vbdtbv) solves the first; a cancel subscription ([`CancelSubscription`](ds:block?id=events-cancelsubscription-eqr493yk)) solves the other two.

## Before you start

- The log and replay run on every driver. A replay repeated with the same ID starts nothing twice while its runs are unfinished on every driver, and also after they finished on a client with `workflow.RunQuerier` ([`Bus.Replay`](ds:block?id=events-bus-replay-r7vbdtbv); `CapRunQuery`: [memory](ds:block?id=memory-caps-yxkuq583), [SQL](ds:block?id=sql-paritycaps-e5y5vqhk), [Redis](ds:block?id=redis-caps-bfgdst9t) and [Hatchet](ds:block?id=hatchet-caps-d6x9fv8z)).
- A cancel subscription needs `workflow.CapCancelAll` ([memory](ds:block?id=memory-caps-yxkuq583), [SQL](ds:block?id=sql-paritycaps-e5y5vqhk), [Redis](ds:block?id=redis-caps-bfgdst9t), [Hatchet](ds:block?id=hatchet-caps-d6x9fv8z)); [`SubscribeCancel`](ds:block?id=events-bus-subscribecancel-6bxys433) refuses a client without it with an `*workflow.UnsupportedError`. A divert also needs `CapRunQuery`, which every driver with `CapCancelAll` has. On Hatchet set `Statuses` yourself: its `CancelAll` refuses `StatusRetrying` ([`checkRunStates`](ds:block?id=hatchet-checkrunstates-d62jh9cp)), which the default [`events.UnfinishedStatuses`](ds:block?id=events-unfinishedstatuses-h9ta9rw2) holds (see Pitfalls).
- The examples are [`examples/events-more`](../../examples/events-more) (the log, a replay, a cancel and a divert) and [`examples/events-signal`](../../examples/events-signal) (`ReplayResult` with a failed delivery, `Cancellation` and `UnfinishedStatuses`), both on the in-memory driver. Run them with `go run ./events-more` and `go run ./events-signal` from `examples/`.

## Keep a log

[`events.WithLog(log)`](ds:block?id=events-withlog-dwyx7ymc) makes `Publish` append every message it accepts to an [`events.EventLog`](ds:block?id=events-eventlog-qhc9zkht) before delivering it, whether or not any subscription matches. [`events.WithClock`](ds:block?id=events-withclock-jm3ye43j) sets the clock that stamps each entry:

<!-- snippet: examples/events-more/main.go#log -->
```go
log := events.NewMemoryLog() // in-process; a durable EventLog for a fleet
b, err := events.New(c, events.WithLog(log), events.WithClock(clk))
if err != nil {
	return err
}
```

[`EventLog`](ds:block?id=events-eventlog-qhc9zkht) is two methods, `Append(ctx, m, at)` and `Range(ctx, from, to, fn)`, so the log can live where your data does (a table, an object store). [`events.NewMemoryLog()`](ds:block?id=events-newmemorylog-ejg663z5) is the in-process default ([`MemoryLog`](ds:block?id=events-memorylog-cmrq93ya)): lost on restart, one per process, with `Len` and [`Prune(before)`](ds:block?id=events-memorylog-prune-rapr8x3v) for retention. Messages `Publish` refuses (no name, reserved labels, a version newer than current) are not logged, and an `Append` error fails the publish before anything is delivered, so every delivered message is in the log ([`Publish`](ds:block?id=events-bus-publish-k8u3yru9)). The log keeps the message as published, before upcasting, so a replay after a deploy upcasts with the new code.

## Replay logged events

[`Bus.Replay(ctx, events.ReplayOptions{...})`](ds:block?id=events-bus-replay-r7vbdtbv) ([`ReplayOptions`](ds:block?id=events-replayoptions-gqq7vh9d)) offers each logged message in `[From, To)` that passes `Filter` to the named subscriptions (all of them when `Subscriptions` is empty), exactly as `Publish` would: event name, the subscription's `Filter`, upcasting, its `Job`.

<!-- snippet: examples/events-more/main.go#replay -->
```go
from := time.Date(2026, 11, 20, 10, 0, 0, 0, time.UTC) // the hour the receipt mailer was down
const outage = 2 * time.Hour                           // how long it stayed down
r, err := b.Replay(ctx, events.ReplayOptions{
	ID:            "mailer-outage-2026-11-20",
	From:          from,
	To:            from.Add(outage),
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

Each replayed run carries the idempotency key `event/<subscription>/<id>/replay/<replay ID>` and the label [`events.LabelReplay`](ds:block?id=events-labelreplay-49dxzaum) ([`deliver`](ds:block?id=events-bus-deliver-zntu7tc7)), so the original delivery's key does not swallow it even while the original run is unfinished. Replaying again with the same `ID` starts nothing twice: the key holds while the replayed run is unfinished, and on a client with run queries `Replay` first looks for a run labelled with the message, subscription and replay ID in any status and reports it as `Existing` ([`replayOne`](ds:block?id=events-bus-replayone-fvpqjh34)). Leave `ID` empty for a new one each call. A message ID logged twice (a retried publish) is delivered once.

[`Replay`](ds:block?id=events-bus-replay-r7vbdtbv) refuses, with `ErrInvalid`, a bus without a log, `From` not before `To`, an unknown subscription name and a cancel subscription's name.

## Read what a replay did

[`Bus.Replay`](ds:block?id=events-bus-replay-r7vbdtbv) returns an [`events.ReplayResult`](ds:block?id=events-replayresult-6pdnbqpp) even when some deliveries failed:

| Field | Meaning |
|---|---|
| `ID` | The replay ID, the one given or the one the bus made |
| `Messages` | How many distinct message IDs in the range passed `Filter` |
| `Deliveries` | One [`events.ReplayDelivery`](ds:block?id=events-replaydelivery-9ww3w5f4) per message and matching subscription, in log order, then subscription order |

Each [`ReplayDelivery`](ds:block?id=events-replaydelivery-9ww3w5f4) has the `MessageID`, the `Subscription`, the run's `Ref`, `Existing` (an earlier replay with this ID already started that run, found and returned as `Ref`, and nothing was enqueued) and `Err`. A failure is per delivery: it lands in that delivery's `Err`, is joined into the error [`Replay`](ds:block?id=events-bus-replay-r7vbdtbv) returns, and the replay goes on with the rest. Calling `Replay` again with the same ID then starts only what failed, because everything else is found as `Existing`.

Here the ledger subscription's `Job` fails while the ledger is down. The replay delivers every receipt and reports both ledger deliveries as failed:

<!-- snippet: examples/events-signal/main.go#replay-result -->
```go
r, err := bus.Replay(ctx, events.ReplayOptions{ID: "backfill-1"}) // zero From and To: the whole log
fmt.Fprintf(p.w, "  replay %s: %d messages, %d deliveries, error: %v\n", r.ID, r.Messages, len(r.Deliveries), err != nil)
for _, d := range r.Deliveries {
	fmt.Fprintf(p.w, "    %s -> %-7s existing=%-5v failed=%v\n", d.MessageID, d.Subscription, d.Existing, d.Err != nil)
}
```

With the ledger back, the same replay ID finds the receipts as `Existing` and starts only the two ledger runs:

<!-- snippet: examples/events-signal/main.go#replay-again -->
```go
again, err := bus.Replay(ctx, events.ReplayOptions{ID: "backfill-1"}) // retries only what failed
if err != nil {
	return err
}
for _, d := range again.Deliveries {
	fmt.Fprintf(p.w, "    %s -> %-7s existing=%-5v failed=%v\n", d.MessageID, d.Subscription, d.Existing, d.Err != nil)
}
```

```text
replay result:
  replay backfill-1: 2 messages, 4 deliveries, error: true
    inv_1 -> receipt existing=false failed=false
    inv_1 -> ledger  existing=false failed=true
    inv_2 -> receipt existing=false failed=false
    inv_2 -> ledger  existing=false failed=true
  the ledger is back; the same replay ID again:
    inv_1 -> receipt existing=true  failed=false
    inv_1 -> ledger  existing=false failed=false
    inv_2 -> receipt existing=true  failed=false
    inv_2 -> ledger  existing=false failed=false
```

Check the result, not only the error: a nil error with an empty `Deliveries` means the range or `Filter` matched nothing, and `Messages` counts what matched before any subscription's own `Filter` ([`ReplayResult`](ds:block?id=events-replayresult-6pdnbqpp)).

## Cancel runs on an event

[`Bus.SubscribeCancel(events.CancelSubscription{...})`](ds:block?id=events-bus-subscribecancel-6bxys433) cancels, for every message of `Event`, every unfinished run whose label `Label` equals the message's `Key`, through `workflow.BulkCanceller.CancelAll`, across workflows ([`runCancels`](ds:block?id=events-bus-runcancels-w2bhnty2)):

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

`Workflow` narrows the cancel to one workflow, and `Statuses` to some statuses ([`cancelQuery`](ds:block?id=events-cancelquery-ndzcm36d)); the default, [`events.UnfinishedStatuses`](ds:block?id=events-unfinishedstatuses-h9ta9rw2), includes running and waiting runs, so a running reminder is cut short, unlike `CancelAll`'s own default of runs not yet started. `Filter` works as on a subscription.

## Read what a cancel did

[`Published.Cancellations`](ds:block?id=events-published-p4vqqdu7) has one [`events.Cancellation`](ds:block?id=events-cancellation-5uvt9g2f) per cancel subscription the message matched, in the order they were subscribed:

| Field | Meaning |
|---|---|
| `Subscription` | The cancel subscription's name |
| `Cancelled` | How many runs `CancelAll` cancelled |
| `Diverted` | The divert run (see below); zero without `Divert` or when the cancel failed |
| `Err` | The cancel's or the divert's error, also joined into the error `Publish` returns |

[`Key`](ds:block?id=events-cancelsubscription-eqr493yk) reads the label value from the message when it is not `m.Key`. A message for which it is empty cancels nothing ([`runCancels`](ds:block?id=events-bus-runcancels-w2bhnty2)): its `Cancellation` holds an error matching `ErrInvalid`, since an empty value would match every run carrying the label empty, and `Publish` returns that error too. The subscribers' runs of that message are still started.

<!-- snippet: examples/events-signal/main.go#cancellation -->
```go
err = bus.SubscribeCancel(events.CancelSubscription{
	Name:  "stop-reminders",
	Event: evCheckedOut,
	Label: labelCart,
	Key:   func(m events.Message) string { return m.Labels[labelCart] }, // not m.Key
})
if err != nil {
	return err
}
pub, err := bus.Publish(ctx, events.Message{ID: "chk_1", Name: evCheckedOut, Labels: map[string]string{labelCart: "c1"}})
if err != nil {
	return err
}
cn := pub.Cancellations[0]
fmt.Fprintf(p.w, "  %s: cancelled %d, diverted %v, err %v\n", cn.Subscription, cn.Cancelled, !cn.Diverted.IsZero(), cn.Err)
```

<!-- snippet: examples/events-signal/main.go#cancellation-nokey -->
```go
pub, err = bus.Publish(ctx, events.Message{ID: "chk_2", Name: evCheckedOut}) // no cart label
fmt.Fprintf(p.w, "  no key: cancelled %d, Cancellation.Err is ErrInvalid: %v, Publish error: %v\n",
	pub.Cancellations[0].Cancelled, errors.Is(pub.Cancellations[0].Err, workflow.ErrInvalid), err != nil)
```

```text
cancellation:
  default statuses: scheduled, queued, running, waiting, retrying
  stop-reminders: cancelled 1, diverted false, err <nil>
  no key: cancelled 0, Cancellation.Err is ErrInvalid: true, Publish error: true
```

[`Publish`](ds:block?id=events-bus-publish-k8u3yru9) runs every cancel before it starts any run, so the runs a message starts are never cancelled by the same message, even when they carry the label. Cancel subscriptions share the name space of subscriptions ([`SubscribeCancel`](ds:block?id=events-bus-subscribecancel-6bxys433)), and [`Replay`](ds:block?id=events-bus-replay-r7vbdtbv) never runs them: cancelling later would hit runs that did not exist when the event happened.

## Divert to another workflow

With `Divert` set, the cancel subscription ([`CancelSubscription`](ds:block?id=events-cancelsubscription-eqr493yk)) is an external trigger exception: after the cancel succeeds it starts a run of `Divert` with the message's payload, so the process leaves its normal path for the exception path:

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

`DivertJob` builds the job instead (another payload, a queue, `RunAt`). The divert run carries the bus's labels and the key `event/<name>/<id>`, like a subscription's run. It starts whether the cancel found runs or not, because a publish retried after a crash between the cancel and the divert finds nothing left to cancel and must still divert; it does not start when the cancel failed, so a republish retries both. A message whose divert run already exists, in any status, was handled: `Publish` finds that run ([`findDivert`](ds:block?id=events-bus-finddivert-ecbdgdn8)) and neither cancels nor diverts again, which keeps a republish from cancelling the divert run itself when it carries the cancelled label. `Cancellation.Diverted` is the divert run.

## Try it

```sh
cd examples
go run ./events-more
```

The log, replay, cancel and divert parts:

```text
replay:
  log holds 3 messages; replayed 2 to receipt: ["inv_2" "inv_3"]
  same replay ID again: 2 of 2 already started, new receipt runs 0
cancel on event:
  stop-cart-reminders cancelled 1 run(s)
  cart c1 reminder: cancelled; cart c2 reminder: running
divert:
  cancelled 1 fulfilment run(s); refund ran with {"order":42}
  republish: cancelled 0, same refund run true, refund runs 1
```

The replay result and cancellation parts come from the second program:

```sh
go run ./events-signal
```

Its `replay result:` and `cancellation:` sections are the ones shown in [Read what a replay did](#read-what-a-replay-did) and [Read what a cancel did](#read-what-a-cancel-did).

## Pitfalls

- [`MemoryLog`](ds:block?id=events-memorylog-cmrq93ya) lives in one process. A fleet where every replica publishes needs one durable `EventLog` they share, or each replica replays only its own messages.
- A replay without run queries is deduplicated only while its runs are unfinished ([`replayOne`](ds:block?id=events-bus-replayone-fvpqjh34)); on such a client keep the replay ID per incident and do not repeat it after the runs finished.
- Cancelling is by label ([`cancelQuery`](ds:block?id=events-cancelquery-ndzcm36d)), so the runs to stop must carry it: label them at enqueue (`Labels: {"order": "42"}`), or publish the event with that label so subscriptions copy it onto their runs.
- [`SubscribeCancel`](ds:block?id=events-bus-subscribecancel-6bxys433) refuses a terminal status in `Statuses` ([`ValidateCancelQuery`](ds:block?id=validatecancelquery-ewgpn2hh)) and a subscription without `Name`, `Event` or `Label`, with `ErrInvalid`.
- On Hatchet, a cancel subscription without `Statuses` is accepted but fails on every publish: the default [`UnfinishedStatuses`](ds:block?id=events-unfinishedstatuses-h9ta9rw2) holds `StatusRetrying`, which Hatchet's `CancelAll` refuses with an `UnsupportedError` naming `CapRunStates` ([`checkRunStates`](ds:block?id=hatchet-checkrunstates-d62jh9cp)). Give it `Statuses: []workflow.Status{workflow.StatusScheduled, workflow.StatusQueued, workflow.StatusRunning, workflow.StatusWaiting}`; a run in retry backoff reads queued there, so it is still cancelled.
- A cancel subscription's error fails the `Publish` call even though the subscribers' runs were started ([`Publish`](ds:block?id=events-bus-publish-k8u3yru9)). Read `Published.Deliveries` and `Published.Cancellations` before treating the error as "nothing happened", and publish again with the same ID: the deliveries are deduplicated and only the cancel is retried.
- Replayed runs are new runs ([`Replay`](ds:block?id=events-bus-replay-r7vbdtbv) uses its own idempotency key): they run the subscriber's workflow again, so the workflow must be safe to run twice for one event (a receipt mailer that checks whether it already sent one).

## Related

- [Publish and subscribe](publish-subscribe.md), [CloudEvents and payload versions](cloudevents.md), [Choreography](choreography.md), [Events](README.md)
- [Manage runs](../jobs/manage-runs.md), [Idempotency keys](../jobs/idempotency.md)
- Examples: [examples/events-more](../../examples/events-more/), [examples/events-signal](../../examples/events-signal/)
