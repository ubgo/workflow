# typed-payloads

Payloads as Go types, on the in-memory driver: a job built from a struct with `JobOf` and its result decoded with `DecodePayload`, a payload that does not decode (a permanent error), the app's size cap (`middleware.MaxPayload`) and the driver's (`memory.WithPayloadLimit`, read through `PayloadLimiter`), and a payload whose shape changed between releases, stamped by `middleware.PayloadVersion` and read by one `DefineVersioned` handler through an upcaster.

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

## The code

The payload and result types:

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

A typed handler:

<!-- snippet: examples/typed-payloads/main.go#handler -->
```go
sendWelcome := workflow.Define(func(_ context.Context, in Welcome) (Receipt, error) {
	return Receipt{MessageID: "msg-" + in.UserID}, nil // in is already decoded
})
```

A job from a struct, its result awaited and decoded:

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

The app's own size cap:

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

The driver's cap, as production's engine would set it:

<!-- snippet: examples/typed-payloads/main.go#driver-limit -->
```go
c := memory.New(memory.WithClock(clk), memory.WithPayloadLimit(4096)) // what production's engine takes
```

Reading the driver's cap:

<!-- snippet: examples/typed-payloads/main.go#limiter -->
```go
if l, ok := any(c).(workflow.PayloadLimiter); ok {
	fmt.Fprintf(out, "  driver limit: %d bytes\n", l.PayloadLimit())
}
```

Two payload versions, an upcaster and a versioned handler:

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

The old release and the new one, each stamping its version:

<!-- snippet: examples/typed-payloads/main.go#stamp -->
```go
release1 := middleware.WrapClient(c, middleware.PayloadVersion(1)) // the old release, still enqueuing
release2 := middleware.WrapClient(c, middleware.PayloadVersion(2)) // this release
```

`main_test.go` runs the program and compares its whole output with the block above.

## Guides

- [Typed payloads](../../docs/jobs/payloads.md)
- [Typed payloads: cap the payload size](../../docs/jobs/payloads.md#cap-the-payload-size)
- [Typed payloads: change a payload's shape between releases](../../docs/jobs/payloads.md#change-a-payloads-shape-between-releases)
