# CloudEvents and payload versions

A payment provider, a SaaS webhook or another team's service sends events in the CloudEvents 1.0 envelope: an `id`, a `source`, a `type`, an optional `subject` and `time`, the payload in `data`, and extension attributes beside them, either as `ce-*` HTTP headers (binary mode) or as one JSON document (structured mode). `events.CloudEvent` reads and writes that envelope with the standard library and maps it onto a bus `Message`, so an incoming CloudEvent starts the same runs a local `Publish` would. The second half of this page covers what happens when the payload's shape changes: a message carries a version, and the bus upcasts an old one to the current shape before any subscriber sees it.

## Before you start

- Everything here is in the `events` package and runs on every driver; the bus needs `workflow.CapIdempotencyKey` as always (see [Publish and subscribe](publish-subscribe.md)).
- Only the JSON event format is read and written, in HTTP binary and structured mode. Batch mode (`application/cloudevents-batch+json`) and the other formats (Avro, Protobuf) are refused with `workflow.ErrInvalid`.
- The example is [`examples/events-more`](../../examples/events-more). Run it with `go run ./events-more` from `examples/`.

## Send a CloudEvent over HTTP

`CloudEvent.WriteHTTP(h)` writes binary mode: each attribute as a `ce-<name>` header (percent-encoded as the HTTP binding requires), `DataContentType` as `Content-Type`, and returns `Data` as the body. `WriteHTTPStructured(h)` writes structured mode instead: `Content-Type: application/cloudevents+json` and the whole event as the body.

<!-- snippet: examples/events-more/main.go#cloudevents-send -->
```go
ce := events.CloudEvent{SpecVersion: events.CloudEventsSpecVersion, ID: "evt_1", Source: "https://shop.example/orders",
	Type: "order.paid", Subject: "42", DataContentType: "application/json",
	Data: []byte(`{"order":42,"cents":1999,"currency":"EUR"}`), Extensions: map[string]string{"tenant": "acme"}}
h := http.Header{}
body, err := ce.WriteHTTP(h) // binary mode: ce-* headers, Data as the body
if err != nil {
	return err
}
req := httptest.NewRequest(http.MethodPost, "/events", bytes.NewReader(body))
req.Header = h
```

Both refuse an event that fails `Validate`: no `ID`, `Source` or `Type`, a `SpecVersion` other than `"1.0"` (`events.CloudEventsSpecVersion`), a `Source` that is not a URI-reference, a `DataSchema` that is not an absolute URI, a `DataContentType` that is not a media type, or an extension name that is not lower-case ASCII letters and digits or that reuses a core attribute's name.

## Receive one and publish it

`events.ReadHTTP(r)` reads either mode: structured when `Content-Type` is `application/cloudevents+json`, binary when a `ce-specversion` header is present, and `ErrInvalid` for anything else. `events.ToMessage` turns the event into a bus message:

<!-- snippet: examples/events-more/main.go#cloudevents-receive -->
```go
got, err := events.ReadHTTP(req) // binary or structured mode; refusals wrap ErrInvalid
if err != nil {
	return err
}
m := events.ToMessage(got) // ID, Type→Name, Subject→Key, Data→Payload, extensions→Labels
p, err := b.Publish(ctx, m)
if err != nil {
	return err
}
```

The mapping, both ways:

| CloudEvent | Message | Rule |
|---|---|---|
| `ID` | `ID` | The bus deduplicates by it, so a sender's retry starts nothing new |
| `Type` | `Name` | What subscriptions match on |
| `Subject` | `Key` | The key a waiting run matches under `WithSignal`, and what a cancel subscription reads by default |
| `Data` | `Payload` | As bytes, unchanged |
| `Extensions` | `Labels` | `ToMessage` copies every extension to the label of the same name. `FromMessage` copies only labels whose key is a valid extension name (`events.ValidExtensionName`: lower-case letters and digits, not a core attribute's name); `tenant-id` or `order.channel` have no CloudEvents spelling and are dropped, so name a label that must cross over like an extension |
| `Source`, `Time`, `DataContentType`, `DataSchema` | none | `ToMessage` drops them; read them from the event before publishing if you need one. `FromMessage(m, source)` takes the source and leaves the others unset |

`ReadHTTP` reads at most `events.DefaultWebhookMaxBody` (1 MiB); wrap `r.Body` in `http.MaxBytesReader` for a smaller cap. It does not authenticate the sender: put it behind the same verification you would use for a webhook (see [Inbound webhooks](webhooks.md)).

## Write the JSON format

`json.Marshal` on a `CloudEvent` writes the JSON format with attributes in a fixed order and extensions sorted, so one event always encodes to the same bytes. The data member follows the content type: JSON data as a JSON value under `data`, text (`text/*`, XML) as a string under `data`, anything else base64 under `data_base64`. With an empty `DataContentType` the format reads `data` as JSON, so non-JSON bytes go out as `data_base64`.

<!-- snippet: examples/events-more/main.go#cloudevents-json -->
```go
out, err := json.Marshal(events.FromMessage(m, "https://shop.example/orders"))
```

```text
structured: {"specversion":"1.0","id":"evt_1","source":"https://shop.example/orders","type":"order.paid","subject":"42","tenant":"acme","data":{"order":42,"cents":1999,"currency":"EUR"}}
```

`json.Unmarshal` reads the format back and validates it. Extension values that arrive as JSON numbers or booleans are kept as their literal text (`5`, `true`); objects and arrays are refused, and `data` together with `data_base64` is refused.

## Version an event's payload

The payload of `order.paid` started as `{"amount": 500}`, became `{"cents": 500}`, and later gained a `currency`. Publishers on the old code keep sending the old shape for a while, and a replayed message from last month has it too. `events.WithUpcasters(event, current, ups)` gives the event a current version and the steps to reach it; `ups[n]` turns version `n` into `n+1`, the same `workflow.Upcaster` that `workflow.DefineVersioned` uses for a workflow's own payload:

<!-- snippet: examples/events-more/main.go#versions -->
```go
b, err := events.New(c, events.WithUpcasters("order.paid", 2, map[int]workflow.Upcaster{
	0: func(raw []byte) ([]byte, error) { // v0 -> v1: "amount" is now "cents"
		return bytes.Replace(raw, []byte(`"amount"`), []byte(`"cents"`), 1), nil
	},
	1: func(raw []byte) ([]byte, error) { // v1 -> v2: add the currency, EUR before v2
		return bytes.Replace(raw, []byte(`}`), []byte(`,"currency":"EUR"}`), 1), nil
	},
}))
if err != nil {
	return err
}
```

A message carries its version in `Message.Version`; zero is the oldest version, the one a publisher that never set it wrote. `Publish` upcasts a lower version to the current one before any subscription's `Filter` or `Job`, any cancel subscription, any divert or any signal sees it, and sets `Version` to current. A version above current, published by newer code than this process runs, is refused with `ErrInvalid` before anything is logged or started:

<!-- snippet: examples/events-more/main.go#versions-publish -->
```go
old := events.Message{ID: "evt_v0", Name: "order.paid", Version: 0, Payload: []byte(`{"order":7,"amount":500}`)}
p, err := b.Publish(ctx, old) // the receipt run gets {"order":7,"cents":500,"currency":"EUR"}
if err != nil {
	return err
}
_, future := b.Publish(ctx, events.Message{ID: "evt_v3", Name: "order.paid", Version: 3})
```

```text
versions:
  v0 message reached the receipt run as {"order":7,"cents":500,"currency":"EUR"}
  v3 message refused: ErrInvalid=true
```

`Bus.CurrentVersion(event)` reports an event's current version. An event without upcasters ignores `Version`.

## Pitfalls

- A CloudEvent carries no `Message.Version`. When the sender puts the version in `dataschema` (`https://shop.example/schemas/order-paid/v2`) or an extension, set `m.Version` from it after `ToMessage`.
- An upcaster that fails, or a gap in `ups`, fails the publish with `ErrInvalid` after the message is logged (with `WithLog`), so a deploy that fixes the upcaster can [replay](replay-and-cancel.md#replay-logged-events) it.
- `New` refuses an empty event name, a negative current version, an upcaster keyed outside `[0, current)` or nil, and two `WithUpcasters` for one event.
- The run gets the upcast payload but no `workflow.LabelPayloadVersion`: a workflow's payload version is the workflow's own (`middleware.PayloadVersion`).
- `ReadHTTP` refuses a request that is in neither mode; a plain JSON webhook without `ce-` headers belongs to [`NewWebhook`](webhooks.md).
