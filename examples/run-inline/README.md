# run-inline

`workflow.RunInline` runs a workflow once in the caller's goroutine: no client, no `Start`, no worker. The steps run one at a time in dependency order with their retries, timeouts and the middleware you pass, which suits a command-line tool, a one-off script or a unit test of a whole workflow.

```sh
cd examples
go run ./run-inline
```

```text
step fetch attempt 1: err=CSV host timed out
step fetch attempt 2: err=<nil>
step store attempt 1: err=<nil>
store output: "stored ["ada","grace"]"
attempts: fetch 2, store 1; backoff waits [1s]
a wait step without InlineEvents is refused: true
wait output: "by ada"
```

`fetch` fails once and is retried after its one-second backoff, which `InlineSleep` records instead of sleeping. A wait step needs its events up front through `InlineEvents`; without them the call is refused before anything runs.

The guide that copies this program is [run a workflow inline](../../docs/jobs/run-inline.md). `go test ./run-inline` checks every line.
