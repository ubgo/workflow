# Claim check

A queue or an engine caps how big a payload may be: an SQS message, a Hatchet input row, a database column you would rather keep small. The claim check pattern stores a payload above a size threshold in a blob store and sends a short reference through the queue in its place; the step that runs resolves the reference back into the original bytes before the handler sees them. [`middleware.ClaimCheck`](ds:block?id=mw-claimcheck-2w4r29tw) does this for job payloads and for step outputs, so a large result passed from one step to the next leaves the queue too, and it deletes a run's blobs once the run has finished.

Handlers do not change: they receive exactly the bytes they would receive without a claim check.

## Before you start

- A [`BlobStore`](ds:block?id=mw-blobstore-qhytdhk9) every process that enqueues or runs steps can reach. [`middleware.MemoryBlobStore`](ds:block?id=mw-memoryblobstore-aygfdxck) is the in-process one for tests and single-process apps; for S3, a filesystem or a database table, implement the three-method interface (adapters live outside the core module).
- A threshold below your queue's cap.
- The runnable program for this page is [examples/claim-check](../../examples/claim-check/).

## Move big payloads out of the queue

[`middleware.NewClaimCheck(store, threshold)`](ds:block?id=mw-newclaimcheck-vbjnm6gt) returns a `ClaimCheck`, which has three halves to wire:

- [`Enqueue()`](ds:block?id=mw-claimcheck-enqueue-xdp7mwr4) is enqueue middleware. It stamps a fresh random token in the [`ClaimCheckLabel`](ds:block?id=mw-claimchecklabel-y2nauzvc) label (`ubgo.workflow/claim-check`) of every job, large or not, and stores a `Job.Payload` longer than the threshold under `<token>/payload`, replacing it with a reference.
- [`Step()`](ds:block?id=mw-claimcheck-step-vezvse3j) is step middleware. Before the handler runs it resolves every reference in the input that belongs to the run; after the handler, it stores an output longer than the threshold and returns a reference, so the child step's input is a reference too.
- `CleanupOnFinish(c)` deletes the run's blobs when the run finishes (next section).

On the memory driver the step half goes in `memory.WithMiddleware` and the enqueue half in `middleware.WrapClient`. The example adds a step middleware of its own, `raw`, in front of the claim check so it can print what travelled through the queue:

<!-- snippet: examples/claim-check/main.go#wire -->
```go
store := middleware.NewMemoryBlobStore() // S3, a filesystem or a table in production
cc, err := middleware.NewClaimCheck(store, threshold)
if err != nil {
	return err
}
// The step half resolves references before the handler runs and
// claims a large output after it.
mc := memory.New(memory.WithClock(clk), memory.WithMiddleware(raw, cc.Step()))
// The enqueue half stamps the run's token and claims a large payload.
c := middleware.WrapClient(mc, cc.Enqueue())
```

A payload of 511 bytes then reaches the queue as a reference, the `load` handler gets all 511 bytes, its 200-byte output is stored, and `summarise` receives a reference that resolves to those 200 bytes:

<!-- snippet: examples/claim-check/main.go#enqueue -->
```go
big := fmt.Appendf(nil, `{"rows":%q}`, strings.Repeat("x", 500))
if _, err := c.Enqueue(ctx, workflow.Job{Workflow: "report", Payload: big}); err != nil {
	return err
}
```

A reference is resolved only when its key is under the token of the run it runs in ([`parseClaimRef`](ds:block?id=mw-parseclaimref-udepr64k)). The token is drawn after the app built the payload, so app data that merely looks like a reference, or a forged one naming another run's blob, passes through untouched and is never fetched ([`TestClaimCheck_LookalikeNotResolved`](ds:block?id=mw-testclaimcheck-lookalikenotresolved-heg3pqw8)). A step outside a run that `Enqueue` stamped (no `StepInfo`, or no token label) runs untouched ([`runClaimToken`](ds:block?id=mw-runclaimtoken-qtta8r5c)).

[`WithClaimKeys(fn)`](ds:block?id=mw-withclaimkeys-kgxzzy73) replaces the random key source (128 random bits in hex by default, [`claimTokenBytes`](ds:block?id=mw-claimtokenbytes-deg4dsf8)). It must return values that are unique and unguessable: a token another run can predict lets a forged reference reach that run's blobs.

With a codec, claim after sealing and resolve before opening, so the blob store only ever holds sealed bytes: pass `cc.Enqueue()` as an extra middleware of `middleware.WithCodec(c, codec, cc.Enqueue())`, and put the step half outside the decoder, `middleware.Chain(cc.Step(), middleware.DecodeStep(codec))` ([codecs](../middleware/codecs.md); [`TestClaimCheck_WithCodec`](ds:block?id=mw-testclaimcheck-withcodec-q8m9pkha) proves the store then holds only sealed bytes).

## Delete blobs when the run finishes

A blob must outlive every retry of its step, so it cannot be deleted when the step succeeds. [`cc.CleanupOnFinish(c)`](ds:block?id=mw-claimcheck-cleanuponfinish-n9fn72cs) registers a run-finished observer on `c` that deletes the run's blobs once the run is terminal, whatever its status. It registers an observer, so call it before `Start`:

<!-- snippet: examples/claim-check/main.go#cleanup -->
```go
// Before Start: deletes every blob under the run's token once the run
// is terminal (MemoryBlobStore is a BlobPrefixDeleter).
if err := cc.CleanupOnFinish(mc); err != nil {
	return err
}
```

With a store that implements the optional [`BlobPrefixDeleter`](ds:block?id=mw-blobprefixdeleter-sre4kb37) (`DeletePrefix`), it removes everything under the run's token, step outputs included; [`MemoryBlobStore`](ds:block?id=mw-memoryblobstore-deleteprefix-akarnjr5) does. Without it, only the payload is deleted and step outputs are left to the store's own expiry ([`TestClaimCheck_CleanupOnFinish`](ds:block?id=mw-testclaimcheck-cleanuponfinish-4as3jn9h)).

A blob that is gone when its step runs (an expiry that came too soon, a delete by hand) fails the step with a `Permanent` error wrapping [`middleware.ErrBlobNotFound`](ds:block?id=mw-errblobnotfound-ezj4fu6t): the blob will not come back on a retry, so the step's retry policy is skipped ([`TestClaimCheck_MissingBlobIsPermanent`](ds:block?id=mw-testclaimcheck-missingblobispermanent-vxjgsrbt)). Any other store error fails the attempt and leaves the retry to the policy ([`ClaimCheck.Step`](ds:block?id=mw-claimcheck-step-vezvse3j)). In the example the `late` workflow allows three retries, its blob is deleted before it runs, and it fails after one attempt.

## Try it

```sh
cd examples
go run ./claim-check
```

```text
-- Move big payloads out of the queue
load: queue carried a reference
load: handler got 511 bytes
summarise: queue carried a reference
summarise: handler got 200 bytes; blobs held: 2
run: succeeded token label set: true
-- Delete blobs when the run finishes
blobs held after the run: 0
use: queue carried a reference
late run: failed ErrBlobNotFound: true attempts: 1
```

## Pitfalls

- [`NewClaimCheck`](ds:block?id=mw-newclaimcheck-vbjnm6gt) with a nil store or a threshold of zero or less is `workflow.ErrInvalid`. A payload of exactly the threshold stays inline; only longer ones are stored ([`TestClaimCheck_Threshold`](ds:block?id=mw-testclaimcheck-threshold-c7zsq6u6)).
- [`CleanupOnFinish`](ds:block?id=mw-claimcheck-cleanuponfinish-n9fn72cs) after `Start` returns the driver's refusal of a late observer, `workflow.ErrAlreadyStarted`. On a client without `workflow.RunNotifier` it returns an `UnsupportedError` naming `CapRunFinished`; there, and for every event a best-effort driver (Hatchet) loses, the store's own expiry is the only cleanup, so set one longer than the longest run. See [Hatchet limitations](../drivers/hatchet.md#limitations).
- When the driver refuses an enqueue, the payload blob is kept ([`ClaimCheck.Enqueue`](ds:block?id=mw-claimcheck-enqueue-xdp7mwr4)): an error does not prove the run was not created. A blob whose run never existed, or a job deduplicated onto an older run by its `IdempotencyKey`, is an orphan only the store's expiry removes.
- Signal payloads and `Schedule` inputs stay inline, and runs fired by a `Schedule` or a declared `Cron` carry no token, so their payload and outputs stay inline whatever their size ([`ClaimCheck`](ds:block?id=mw-claimcheck-2w4r29tw)).
- A run replayed by hand on an engine after it finished finds its blobs gone.
- `ClaimCheck.Enqueue` owns `ubgo.workflow/claim-check`. A job arriving with it, or with any other label under `ubgo.workflow/` except the two unique labels, is refused with `workflow.ErrInvalid` ([`ClaimCheck.prepare`](ds:block?id=mw-claimcheck-prepare-dyrwb8kw), [`TestClaimCheck_OwnsItsLabel`](ds:block?id=mw-testclaimcheck-ownsitslabel-s27c8a6w)). A job enqueued straight on the driver, bypassing the middleware, is not claimed.
- A key from `WithClaimKeys` that is empty or contains `/` fails the enqueue or step with `workflow.ErrInvalid` ([`validClaimKey`](ds:block?id=mw-validclaimkey-6737rm3n), [`TestClaimCheck_Config`](ds:block?id=mw-testclaimcheck-config-qxhardrn)), because `/` separates a token from its keys.
- The blob store must be shared by every process: the step that resolves a reference may run on another machine than the enqueue that stored it ([`BlobStore`](ds:block?id=mw-blobstore-qhytdhk9)). `MemoryBlobStore` works only when one process does both.

## Related

- [Codecs](../middleware/codecs.md): encrypt payloads, and the order to combine both.
- [Step middleware](../middleware/step-middleware.md) and [enqueue middleware](../middleware/enqueue-middleware.md).
- [React when a run finishes](../observability/run-finished.md): the hook the cleanup uses.
- [Custom stores](../extending/custom-stores.md): writing a `BlobStore`.
- Example: [examples/claim-check](../../examples/claim-check/).
