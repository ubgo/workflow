# release-errors

How a step tells a release from a failure, on the in-memory driver: `workflow.RetryAfter` marks a 429 as a release with the wait the API named, `workflow.IsRelease` finds the mark through any wrapping, `RetryAfterDelay` reads the wait back, and `Permanent` still wins over a release. A `sync` workflow then meets a partner API that answers 429, 429, 503, 429, 503: with `Max: 9` and `MaxErrors: 2` the step stops on its second real error, because the three releases count against `Max` only (`workflow.Attempts` is the counter every driver keeps).

```sh
cd examples
go run ./release-errors
```

```text
classify:
  429 is a release: true | wait: 10ms
  503 is a release: false
  RetryAfter(nil, d) is nil: true
  RetryAfter(Permanent(err)) retried: false
sync:
  attempt 1: 429 too many requests (release: true) -> attempts 1, errors 0
  attempt 2: 429 too many requests (release: true) -> attempts 2, errors 0
  attempt 3: 503 service unavailable (release: false) -> attempts 3, errors 1
  attempt 4: 429 too many requests (release: true) -> attempts 4, errors 1
  attempt 5: 503 service unavailable (release: false) -> attempts 5, errors 2
  run failed after 2 errors and 3 releases
```

The guide that copies this program is [errors: tell a release from a failure](../../docs/concepts/errors.md#tell-a-release-from-a-failure).
