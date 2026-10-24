# claim-check

Move payloads and step outputs above a 64-byte threshold out of the queue with `middleware.NewClaimCheck` and `MemoryBlobStore`: a 511-byte payload and a 200-byte step output travel as references while the handlers get the full bytes, `CleanupOnFinish` deletes the run's blobs when it finishes, and a blob that is gone before its step runs fails the step once with `ErrBlobNotFound`, without the retries its policy allows.

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

The guide [Claim check](../../docs/reliability/claim-check.md) explains each step. `main_test.go` runs the same program and checks every line above.
