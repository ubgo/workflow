# Payload codecs

A codec turns the bytes your app hands the library into the bytes an engine stores, and back. With one in place a queue table, a Hatchet dashboard or a database backup holds only sealed payloads, while every handler still receives plain input. The stock codec is AES-GCM encryption with key rotation; compression or a KMS envelope plug in through the same two-method interface.

## Before you start

- Any driver and the `middleware` package (core module, standard library crypto).
- A key of 16, 24 or 32 bytes (AES-128, -192 or -256), loaded from your secret store. The key belongs to the app; the library never stores it.
- The same codec on both sides: the client that enqueues and every worker that runs steps.
- The runnable program for this page is [examples/encrypted-payloads](../../examples/encrypted-payloads/).

## Transform payloads

A `middleware.Codec` has `Encode(plain) ([]byte, error)` and `Decode(encoded) ([]byte, error)`, and `Decode(Encode(b))` must equal `b`. The middleware wraps the codec's output in an envelope, a JSON string `"wfenc1:<base64>"`, so the sealed value survives every place a driver moves it, including the JSON object a step with several parents receives. An empty payload or output is never sealed, so a job with no input needs no key.

There are two halves, and both are needed:

- The client half seals. `middleware.WithCodec(c, codec, mws...)` returns a `Client` that seals `Job.Payload` on `Enqueue`, `Event.Payload` on `Signal` (it becomes a wait step's output, which a child step decodes) and `Schedule.Input` on `Schedule` (each fired run's payload). `middleware.EncodeEnqueue(codec)` is the enqueue middleware alone, for an app that only ever enqueues; prefer `WithCodec`.
- The worker half opens. `middleware.DecodeStep(codec)` is step middleware: it opens each step's input before the handler sees it, whether that is the job payload, one parent's output or the object of several parents, and seals the step's output afterwards, so a child step's input is sealed like a payload and every byte the engine stores stays sealed.

On the worker, `DecodeStep` goes in the driver's middleware; here a two-step workflow whose first output travels sealed to the second step:

<!-- snippet: examples/encrypted-payloads/main.go#worker -->
```go
c := memory.New(memory.WithMiddleware(middleware.DecodeStep(rotated)))
err = c.Register(workflow.Workflow{
	Name: "charge",
	Steps: []workflow.Step{
		{Name: "authorize", Run: workflow.Define(func(ctx context.Context, o order) (string, error) {
			return "auth-" + o.ID, nil // sealed again by DecodeStep before the engine keeps it
		})},
		{Name: "capture", After: []string{"authorize"}, Run: workflow.Define(func(ctx context.Context, auth string) (string, error) {
			fmt.Fprintln(out, "capture got", auth)
			return "", nil
		})},
	},
})
if err != nil {
	return err
}
```

On the client, `WithCodec`. Its extra middleware runs after sealing, so the example's `stored` sees exactly what the driver keeps: a sealed payload, the card number nowhere in it:

<!-- snippet: examples/encrypted-payloads/main.go#client -->
```go
// stored runs after sealing, so it sees exactly what the driver keeps.
stored := func(next workflow.EnqueueFunc) workflow.EnqueueFunc {
	return func(ctx context.Context, job workflow.Job) (workflow.RunRef, error) {
		fmt.Fprintf(out, "driver stores a sealed payload: %v, card visible: %v\n",
			strings.HasPrefix(string(job.Payload), `"wfenc1:`), bytes.Contains(job.Payload, []byte("4242")))
		return next(ctx, job)
	}
}
app := middleware.WithCodec(c, rotated, stored)
ref, err := app.Enqueue(ctx, workflow.Job{Workflow: "charge", Payload: []byte(`{"ID":"o-1","Card":"4242"}`)})
if err != nil {
	return err
}
```

The codec is closed by default. Input that does not open, or that was never sealed, fails the step with a permanent error wrapping `middleware.ErrDecode`: retrying the same bytes with the same key cannot succeed, and accepting plaintext would make the codec optional without anyone noticing. A job enqueued on the bare client, past `WithCodec`, fails that way:

<!-- snippet: examples/encrypted-payloads/main.go#plaintext -->
```go
ref, err = c.Enqueue(ctx, workflow.Job{Workflow: "charge", Payload: []byte(`{"ID":"o-3"}`)}) // not through WithCodec
if err != nil {
	return err
}
```

The error never carries the bytes themselves.

## Encrypt payloads

`middleware.Encrypt(key, oldKeys...)` returns an AES-GCM `Codec`. Each `Encode` draws a fresh random nonce and writes nonce, ciphertext and tag, so equal payloads never encrypt to equal bytes; `Decode` authenticates before returning anything, so a wrong key or a changed byte is an error, never garbage. A key of the wrong length fails with an error wrapping `workflow.ErrInvalid`.

`oldKeys` rotate the key: `Encode` always uses `key`, while `Decode` tries `key` first and then each old key in order, so payloads sealed before the rotation still open. The sealed format is the same for every key (no key ID is written), so rotating needs no change to stored runs.

<!-- snippet: examples/encrypted-payloads/main.go#codec -->
```go
sealing, err := middleware.Encrypt(oldKey)
if err != nil {
	return err
}
// After the rotation: seal with newKey, still open what oldKey sealed.
rotated, err := middleware.Encrypt(newKey, oldKey)
if err != nil {
	return err
}
```

A rotation in three steps: deploy every worker with `Encrypt(newKey, oldKey)` (they open both, and seal new step outputs with the new key); switch every enqueuing client to the same codec; drop `oldKey` once no run, delayed job or schedule sealed with it can still be read. In the example, a job sealed by a client still on the old key runs fine on the rotated worker, and a worker that dropped the old key too early fails that run for good with `ErrDecode`.

## Try it

```sh
cd examples
go run ./encrypted-payloads
```

```text
driver stores a sealed payload: true, card visible: false
capture got auth-o-1
sealed with the new key: succeeded, decode error: false
capture got auth-o-2
sealed with the old key: succeeded, decode error: false
plaintext: failed, decode error: true
old key dropped: failed, decode error: true, permanent: true
```

`go test ./encrypted-payloads` runs the same program and checks every line.

## Pitfalls

- **Both halves, every time, and no plaintext in flight.** `WithCodec` without `DecodeStep` on the workers hands handlers sealed bytes; `DecodeStep` refuses every payload that was never sealed, including runs enqueued before the codec was turned on. Turn a codec on with no runs in flight (drain the queue first), or those runs fail with `ErrDecode`.
- **`Signal` must go through `WithCodec` too.** A wait step's output is the event payload, and `DecodeStep` on its child refuses one that was never sealed.
- **Schedules keep the input they were sealed with.** `Schedule.Input` is sealed once, when `Schedule` is called, and every fire reuses it; keep an old key until every schedule sealed with it has been re-declared, or its fires fail with `ErrDecode`.
- **Payload-keyed middleware belongs outside the codec.** Sealed payloads differ on every enqueue (fresh nonce), so `Unique`, `Debounce` or `Route` keyed on payload content must sit in a `WrapClient` around the `WithCodec` client, not in its extra middleware.
- **A decode failure is permanent.** Retries are not spent on it, so a missing key shows up as failed runs at once; watch for `ErrDecode` after a deploy.
- **Labels are not sealed.** A codec covers payloads, outputs, event payloads and schedule inputs; job labels travel in the clear and should not hold secrets.

## Related

- [Step middleware](step-middleware.md) and [enqueue middleware](enqueue-middleware.md): where the two halves sit.
- [Claim check](../reliability/claim-check.md): move big payloads out of the engine, before or after a codec.
- [Wait for an event](../workflows/signals.md#wait-for-an-event): the event payloads `WithCodec` seals.
- Example: [examples/encrypted-payloads](../../examples/encrypted-payloads/).
