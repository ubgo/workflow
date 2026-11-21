# workflow-json

A workflow whose definition is data. It first checks a document with a misspelled field with `dsl.Parse`, which names the field by its JSON path. A JSON document names the steps, their order, a wait for an event, retries and timeouts, and the Go handlers the steps run, by name. The program registers those handlers in a `dsl.Registry`, loads the document with `dsl.Load`, registers the result on the in-memory driver like any workflow written in Go, and runs two orders through it: each waits for the payment of its own order (the wait's key comes from the `order` label), then ships. It then shows a definition naming a handler nobody registered being refused with its JSON path, and a `Marshal` and `Parse` round trip.

```sh
cd examples
go run ./workflow-json
```

```text
checked: steps[0].handlr: unknown field (known: after, handler, name, retry, sleep, timeout, wait)
loaded "order": reserve (run) await-payment (wait) ship (run)
both orders waiting for payment
A-1: "shipped A-1, paid 40"
A-2: "shipped A-2, paid 25"
refused: workflow: invalid spec: dsl: steps[1].handler: no handler is registered as "ship-order" (registered: reserve-stock, ship-parcel)
round trip equal: true
```

`dsl.Parse` reads and checks a document without any handler, `dsl.Build` resolves its handler names against a `dsl.Registry` and runs `workflow.Validate`, and `dsl.Load` does both. Every refusal wraps `workflow.ErrInvalid`; one tied to a place in the document is a `*dsl.FieldError` whose `Path` reads like the document. The registry is the security boundary: a definition can only call handlers the program registered.

The guide that copies this program is [definitions as data](../../docs/workflows/definitions-as-data.md). `go test ./workflow-json` checks every line.
