# consumer-inbox

Idempotent consumption: a message delivered more than once (an at-least-once broker, a webhook sender's retry, an outbox relay after a crash) is processed once per consumer. An `InboxConsumer` over the in-memory inbox drops two redeliveries of one message and lets a message whose first attempt failed run again; sqlstore's `Inbox.MarkTx` records a message inside the app's own transaction on SQLite, so four concurrent deliveries write one order; an inbound webhook with an `Inbox` answers a redelivered ID with 200 and starts one run.

```sh
cd examples
go run ./consumer-inbox
```

```text
consumer:
  msg-1 ran: true
  msg-1 ran: false
  msg-1 ran: false
  receipts sent: 1
  msg-2 first attempt: smtp down
  msg-2 redelivered, ran: true <nil>
transactional, four concurrent deliveries:
  orders written: 1
webhook:
  status: 200
  status: 200
  status: 200
  runs started: 1
```

The guide is [Idempotency keys › Drop redelivered messages with an inbox](../../docs/jobs/idempotency.md#drop-redelivered-messages-with-an-inbox).
