# client-wrapper

A `workflow.Client` wrapper written by hand: `audited` logs every `Enqueue` and passes it on. Because it embeds `workflow.Forward{Inner: inner}` beside the `Client` it wraps, it still implements every optional interface of the client inside (`RunQuerier`, `RunRetrier`, `RunDeleter`, `BulkCanceller`, `Pauser`, `SchedulePauser`, `ScheduleLister`, `TickNotifier`, `Drainer`), so code that finds them by type assertion keeps working. The same wrapper without `Forward` hides them, and `Forward` over a client that lacks one refuses with an `UnsupportedError` naming the capability. It runs on the memory driver.

```sh
cd examples
go run ./client-wrapper
```

```text
audit: enqueue email
audited: RunQuerier=true Pauser=true
runs through the wrapper: 1 succeeded
paused through the wrapper: email
bare: RunQuerier=false, although the driver has CapRunQuery=true
Forward over bare: ErrUnsupported=true missing=true
```

`Forward` does not cover `RunNotifier` (`OnRunFinished`): a wrapper that needs it implements it itself. The clients `workflow.Use`, `middleware.WrapClient` and `middleware.WithCodec` return already forward every optional interface; write a wrapper like this one only when none of them fits.

## The code

- `wrapper`: the wrapper type, its constructor and the one method it changes.
- `use`: the optional interfaces found through the wrapper.
- `bare`: what a wrapper without `Forward` hides.
- `refuse`: `Forward` over a client that lacks an interface.

The guide [The Client interface](../../docs/concepts/client.md#the-client-interface) copies its code from the regions marked `snippet:`. `main_test.go` runs the same program and checks every line above.
