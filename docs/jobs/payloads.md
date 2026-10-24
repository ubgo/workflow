# Typed payloads

A job's payload is bytes, and every handler and caller that touches it repeats the same JSON encode and decode. This page covers doing that once with Go types: building a job from a struct, decoding a payload or a result back into one, capping how big a payload may be before it reaches the driver, and changing a payload's shape between releases while runs written by the old release are still waiting.

## Before you start

- Typed jobs, decoding and the app's own size cap (`middleware.MaxPayload`) are plain functions and enqueue middleware, so they work on every driver. Payload versioning is enqueue middleware plus a step handler, so it works on every driver too.
- A driver's own hard cap is reported through the optional `workflow.PayloadLimiter` interface: the memory, Redis and SQL drivers have `WithPayloadLimit`, and Hatchet has `Config.PayloadLimit` (1.5 MiB by default).
- The example is [`examples/typed-payloads`](../../examples/typed-payloads). It runs on the memory driver, so its output never varies. Run it with `go run ./typed-payloads` from `examples/`.
- Typed step handlers come from `workflow.Define`, covered in [Workflows and steps](../workflows/steps.md#typed-handlers-with-define).

## Build a job from a struct

Declare the payload as a type, next to the workflow that reads it:

<!-- snippet: examples/typed-payloads/main.go#types -->
```go
// Welcome is the payload of the "send-welcome" workflow.
type Welcome struct {
	UserID string `json:"user_id"`
	Plan   string `json:"plan"`
}

// Receipt is what its step returns.
type Receipt struct {
	MessageID string `json:"message_id"`
}
```

`workflow.JobOf(workflow, v)` returns a `Job` for that workflow whose `Payload` is `v` encoded as JSON, and an error wrapping `workflow.ErrInvalid` when `v` cannot be encoded. Everything else on the job (`IdempotencyKey`, `RunAt`, `Labels`, `Tenant`) is set on the returned value as usual. On the other side, the step's handler is a `workflow.Define` over the same type, so it receives the decoded struct:

<!-- snippet: examples/typed-payloads/main.go#handler -->
```go
sendWelcome := workflow.Define(func(_ context.Context, in Welcome) (Receipt, error) {
	return Receipt{MessageID: "msg-" + in.UserID}, nil // in is already decoded
})
```

## Decode a payload or a result

`workflow.DecodePayload[T](raw)` decodes JSON bytes into a `T`: a step's input, `RunInfo.Payload`, or a run's [result](results.md). Here a caller enqueues the typed job, waits for its result and decodes it into the step's return type:

<!-- snippet: examples/typed-payloads/main.go#job-of -->
```go
job, err := workflow.JobOf("send-welcome", Welcome{UserID: "u-7", Plan: "pro"})
if err != nil {
	return err
}
job.IdempotencyKey = "welcome:u-7" // the rest of the Job is set as usual
ref, err := c.Enqueue(ctx, job)
if err != nil {
	return err
}
raw, err := workflow.AwaitResult(ctx, c, ref, wait)
if err != nil {
	return err
}
receipt, err := workflow.DecodePayload[Receipt](raw)
if err != nil {
	return err
}
```

Empty input decodes to the zero `T`. A decode failure is returned as `workflow.Permanent`, since decoding the same bytes again cannot succeed: inside a handler, returning it fails the step without retries.

## Cap the payload size

A queue is a poor place for a 5 MB document: an engine refuses messages over its limit, a database row grows, and every retry copies the bytes again. There are two caps, and both refuse with a `*workflow.PayloadTooLargeError`, which names the workflow, the size, the limit and whose limit it was (`Source`), and matches both `workflow.ErrPayloadTooLarge` and `workflow.ErrInvalid` (the same bytes will never fit).

**The app's cap.** `middleware.MaxPayload(limit)` is enqueue middleware that refuses a job whose payload, or the payload of any job in its `Chain`, is over `limit` bytes, before the driver sees it:

<!-- snippet: examples/typed-payloads/main.go#max-payload -->
```go
app := middleware.WrapClient(c, middleware.MaxPayload(1024)) // the app's own cap
big := workflow.Job{Workflow: "send-welcome", Payload: []byte(`"` + strings.Repeat("x", 2000) + `"`)}
_, err = app.Enqueue(ctx, big)
var tooBig *workflow.PayloadTooLargeError
if errors.As(err, &tooBig) {
	fmt.Fprintf(out, "  %d bytes over the %s limit of %d\n", tooBig.Size, tooBig.Source, tooBig.Limit)
}
```

Pick the app's cap below the driver's, so an oversized job fails at the caller with a clear error rather than deep in a broker. A `limit` of zero or less refuses every enqueue with `ErrInvalid`, so a missing setting never means "no limit" by accident.

**The driver's cap.** A driver with a hard limit implements `workflow.PayloadLimiter`, whose `PayloadLimit()` returns the cap in bytes (zero means none), and checks every enqueue against it with `workflow.CheckPayload`. The memory driver has no limit by default; `memory.WithPayloadLimit(n)` gives it one, so a test on the memory driver catches a payload production would refuse:

<!-- snippet: examples/typed-payloads/main.go#driver-limit -->
```go
c := memory.New(memory.WithClock(clk), memory.WithPayloadLimit(4096)) // what production's engine takes
```

Code that sizes its work against the real number (a batch splitter, the claim check) reads it through the interface; every wrapper in the library forwards it:

<!-- snippet: examples/typed-payloads/main.go#limiter -->
```go
if l, ok := any(c).(workflow.PayloadLimiter); ok {
	fmt.Fprintf(out, "  driver limit: %d bytes\n", l.PayloadLimit())
}
```

The Redis driver's default and ceiling is 512 MiB, the most Redis stores in one value, and `redis.WithPayloadLimit(n)` lowers it. For payloads that are big by nature, move them out of the queue with the [claim check](../reliability/claim-check.md) and put `MaxPayload` after it in the chain, so only what really travels is measured.

## Change a payload's shape between releases

A delayed job, a retry or a scheduled run can be read by a newer release than the one that wrote it. Renaming a field breaks those runs: the new handler decodes the old bytes into the new struct and gets empty fields. Payload versioning keeps both readable. Every job is stamped with the version of the shape that wrote it, and the handler upcasts older shapes to the current one before decoding.

Give each old shape an `workflow.Upcaster` that turns version n into version n+1, and build the handler with `workflow.DefineVersioned(current, upcasters, fn)`. Here release 1 sent the full name in one field, and release 2 splits it:

<!-- snippet: examples/typed-payloads/main.go#versions -->
```go
// GreetV1 is how release 1 wrote the "greet" payload.
type GreetV1 struct {
	Name string `json:"name"` // "Ann Lee"
}

// GreetV2 is the shape this code reads: the name split in two.
type GreetV2 struct {
	First string `json:"first"`
	Last  string `json:"last"`
}

// upcasters turn each old shape into the next one: 1 into 2.
var upcasters = map[int]workflow.Upcaster{
	1: func(raw []byte) ([]byte, error) {
		var v1 GreetV1
		if err := json.Unmarshal(raw, &v1); err != nil {
			return nil, err
		}
		first, last, _ := strings.Cut(v1.Name, " ")
		return json.Marshal(GreetV2{First: first, Last: last})
	},
}

// greet reads every version: older payloads are upcast to 2 first.
var greet = workflow.DefineVersioned(2, upcasters, func(_ context.Context, in GreetV2) (string, error) {
	return "Hello " + in.First + " " + in.Last, nil
})
```

Stamp the version at enqueue with `middleware.PayloadVersion(current)`. It writes the reserved label `workflow.LabelPayloadVersion` (`ubgo.workflow/payload-version`) on a copy of the job's labels. In the example two wrappers stand in for the old release, still running somewhere, and the new one:

<!-- snippet: examples/typed-payloads/main.go#stamp -->
```go
release1 := middleware.WrapClient(c, middleware.PayloadVersion(1)) // the old release, still enqueuing
release2 := middleware.WrapClient(c, middleware.PayloadVersion(2)) // this release
```

When the step runs, `DefineVersioned` reads the version from the run's labels (`workflow.PayloadVersionOf`; no label counts as version 0, the oldest), applies `ups[from]`, `ups[from+1]` and so on up to `current` (`workflow.Upcast`), then decodes into the handler's type. The program's output shows the version-1 payload and the version-2 payload greeted by the same handler.

The rules, all enforced:

- A job that already carries a version (a retry, a job forwarded from elsewhere) keeps it; `PayloadVersion` does not stamp over it.
- A job carrying a version newer than `current` is refused at enqueue with `ErrInvalid`, since this release could not read it.
- At run time a missing upcaster in the chain, a version newer than the handler's, or an upcaster's own error is `Permanent`: the step fails without retries, because the same bytes would fail again.
- The label is reserved: like every label under `ubgo.workflow/`, a driver refuses it on a job that did not pass through `PayloadVersion`, so a version cannot arrive by accident from a request's labels.

## Try it

```sh
cd examples
go run ./typed-payloads
```

```text
typed:
  payload {"user_id":"u-7","plan":"pro"}
  receipt msg-u-7
  bad payload: permanent true
size limits:
  2002 bytes over the middleware.MaxPayload limit of 1024
  ErrPayloadTooLarge true, ErrInvalid true
  5002 bytes over the memory.WithPayloadLimit limit of 4096
  driver limit: 4096 bytes
versions:
  version 1 {"name":"Ann Lee"} -> "Hello Ann Lee"
  version 2 {"first":"Bo","last":"Chen"} -> "Hello Bo Chen"
  version 3 sent to release 2: ErrInvalid true
```

`go test ./typed-payloads` checks every line.

## Pitfalls

- **`DefineVersioned` is for steps that receive the run's payload.** A step with `After` receives a parent's output, which has no version; use plain `Define` there.
- **Versioning needs the middleware on every enqueue path.** A job enqueued straight on the driver, or by a release that predates the middleware, carries no label and is read as version 0. With the example's upcasters such a job fails for good, since there is no upcaster from 0 to 1: give version 0 one, or call the first shape version 0.
- **Keep old upcasters until no old run can be waiting.** A delayed job a year out, or a run kept for retry, still needs the chain from its version.
- **`MaxPayload` measures what it sees.** Put it after a claim check or a compressing codec; before them it measures bytes that will never travel.
- **A size limit of zero is not "no limit" on the middleware.** `MaxPayload(0)` refuses everything; leave the middleware out instead.
- **`JobOf` encodes with `encoding/json`.** Unexported fields are dropped and a `json:"-"` field is never sent, exactly as `json.Marshal` does.

## Related

- [Enqueue a job](enqueue.md): the `Job` fields.
- [Workflows and steps › Typed handlers with Define](../workflows/steps.md#typed-handlers-with-define).
- [Results and awaiting a run](results.md): decode a run's result.
- [Claim check](../reliability/claim-check.md): big payloads in a blob store.
- [Enqueue middleware](../middleware/enqueue-middleware.md): chain order.
- Example: [examples/typed-payloads](../../examples/typed-payloads/).
