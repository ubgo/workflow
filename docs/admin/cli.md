# The workflow command

`workflow` is a command line client for the [admin JSON API](http-api.md): list and inspect runs, retry, cancel and delete them one at a time or by filter, move a delayed run, withdraw one step, pause and resume, list and trigger schedules, wait for a queue to drain, and read run-end history, health and the online nodes, from a terminal or a deploy script. It talks HTTP only and imports nothing beyond the standard library, not even the library's core, so installing it pulls no driver, SDK or database code. Every command can print the API's JSON instead of a table, and every destructive one can preview first.

## Before you start

- An app that mounts `admin.NewHandler` ([Admin JSON API](http-api.md)) and an `Identify` that accepts something a script can send: a bearer token, or a header such as a session cookie.
- Install the stand-alone binary: `go install github.com/ubgo/workflow/cli/cmd/workflow@latest`. It is the `github.com/ubgo/workflow/cli` module (Go 1.24 or later).
- The example is [`examples/admin-cli`](../../examples/admin-cli). It starts the admin handler over a memory client on an `httptest` server and drives the CLI against it. Run it with `go run ./admin-cli` from `examples/`.

## Connect

Point it at the mount point and authenticate the way your `Identify` expects. Flags win over environment variables, and the global flags go before or after the command:

| Flag | Environment variable | What it sends |
|---|---|---|
| `--url URL` | `WORKFLOW_ADMIN_URL` | Where the app mounted the handler, such as `https://app.example.com/admin` |
| `--token T` | `WORKFLOW_ADMIN_TOKEN` | `Authorization: Bearer T`; never printed, not even in an error or in `help` |
| `--header 'K: V'` | `WORKFLOW_ADMIN_HEADER` (one header) | Any header, repeatable: a session cookie, a tenant header |
| `--json` | | Print the API's JSON answer, indented, instead of a table |
| `--timeout D` | | An overall time limit for the command (none by default; each request is bounded at 30 seconds) |

```sh
export WORKFLOW_ADMIN_URL=https://app.example.com/admin
export WORKFLOW_ADMIN_TOKEN=...   # from your secret store
workflow failures --workflow send-email
workflow --header 'Cookie: session=...' inspect 01J9Z...
```

## Commands

| Command | What it does | Flags |
|---|---|---|
| `caps` | Every admin operation and whether the server's driver supports it, plus whether output and metrics are wired | |
| `runs` | List runs | `--workflow`, `--queue`, `--tenant`, `--status` (comma-separated or repeated), `--label k=v` (a value ending in `*` is a prefix, repeatable), `--from`, `--to` (RFC 3339), `--limit` (default 50), `--cursor`, `--count` |
| `failures`, `discarded` | List failed or discarded runs | The `runs` flags without `--status` |
| `inspect RUN` | One run, its attempt errors and, with `--output`, captured output | `--scheduled` (RUN is a ScheduledID), `--output` |
| `retry RUN`, `retry --all` | Retry a failed run, or every failed run the filter flags match | `--dry-run`, `--scheduled`, the filter flags (`--tenant` included), `--max` |
| `cancel RUN`, `cancel --all` | Cancel a run, or every run the filter flags match | `--dry-run`, `--scheduled`, the filter flags |
| `delete RUN`, `delete --all` | Forget a finished run, or every finished run the filter flags match | `--dry-run`, `--scheduled`, the filter flags, `--max` |
| `reschedule RUN` | Move a delayed run that has not started | `--at` (RFC 3339, required), `--scheduled`, `--dry-run` |
| `cancel-step RUN STEP` | Withdraw one step of a run ([Admin operations › Step 3](operations.md#step-3-act-on-runs)) | `--scheduled`, `--dry-run` |
| `pause`, `resume` | Pause or resume one workflow, one queue or everything | Exactly one of `--workflow`, `--queue`, `--everything`; `--dry-run` |
| `pauses` | The active pauses | |
| `queues` | Depth and oldest wait per queue, with workers and running attempts where the driver scales workers | |
| `schedules`, `schedule ID` | List schedules with their tenant (`-` when unset) and next fire, or show one, with a tenant row | |
| `trigger ID` | Run a schedule now | `--dry-run` |
| `drain` | Wait until no run is queued, running or retrying | `--queue` (comma-separated or repeated; default every queue), `--wait` (default 10m), `--interval` (default 2s) |
| `metrics` | Run ends per step by status, with totals | `--window` (default one hour), `--step`, `--from`, `--to`, `--workflow` |
| `health` | Live, ready and each check | |
| `nodes` | The online nodes | |

`workflow help` lists them; `workflow help <command>` shows a command's flags. Flags may follow positional arguments (`workflow retry r-1 --dry-run`), and `--` ends the flags.

## Preview before you act

`--dry-run` reads and changes nothing. For one run it fetches the run and says whether the action would be accepted; with `--all` it asks the server for a dry run with the same filter, so the count and the sample are the server's own:

```text
$ workflow retry --all --workflow send-email --dry-run
dry run: would retry 3 runs; nothing changed
  run-8
  run-5
  run-2
exit 0
$ workflow retry --all --workflow send-email
matched 3, retried 3, skipped 0 (changed meanwhile)
exit 0
```

A bulk retry or delete acts on at most the server's bound (`admin.WithMaxBulk`, 1,000 by default) or `--max`; when more match it says so, and running the same command again does the next batch. `delete --all` with no filter is refused by the server: list the statuses to mean every finished run ([Admin operations › Act on many runs at once](operations.md#act-on-many-runs-at-once)). `trigger --dry-run` shows the schedule that would run and warns when its overlap policy would refuse the trigger. For the single-run commands the dry run reads the run and exits 1 when the action would be refused: `reschedule --dry-run` unless the run is still delayed (`scheduled`), `cancel-step --dry-run` when the run has finished. `pause --dry-run` and `resume --dry-run` read the active pauses, say whether the target is paused and how many pauses are active, and write nothing.

## Script with it

`--json` prints the API's own answer, so a script reads it with `jq` and the shape is the one documented in [Admin JSON API](http-api.md#routes). The exit code is `0` when the command did what it was asked, `1` when the API refused or failed (a 401, a 403, a 409, a timeout) or a check failed, and `2` for a wrong command line:

```text
$ workflow health
CHECK    OK   DETAIL
live     yes  
ready    yes  
workers  yes  0 of 4 busy
exit 0
$ workflow failures
workflow: admin API: HTTP 401 unauthorized: authentication required
exit 1
$ workflow retyr
workflow: unknown command "retyr"
run 'workflow help' for usage
exit 2
```

`health` exits 1 when the client is not live or not ready, and `drain` exits 1 when runs are still queued or running after `--wait`, so a deploy script can gate on either:

```sh
workflow pause --queue emails
workflow drain --queue emails --wait 10m && ./deploy-email-workers.sh
workflow resume --queue emails
```

`drain` counts runs that are queued, running or retrying; delayed runs and runs waiting for an event are not waited for, since either can wait for days.

## Embed it in your binary

`cli.Run(ctx, args, cli.Options{...})` is the whole CLI as a function that returns the exit code, so an app can ship it as a subcommand of its own binary with its own default URL and auth. `Options` takes `Stdout` and `Stderr`, `Getenv` (nil reads no environment; pass `os.Getenv` or your own lookup), `HTTPClient` (default: a 30-second timeout), `Now` and `Sleep` for `drain`'s polling, and `Name` for the usage text. `cli.Main()` is the stand-alone binary's body.

<!-- snippet: examples/admin-cli/main.go#embed -->
```go
env := map[string]string{
	cli.EnvURL:   srv.URL + "/admin", // where the app mounted admin.NewHandler
	cli.EnvToken: opsToken,           // sent as a bearer token, never printed
}
workflowCmd := func(args ...string) int {
	return cli.Run(ctx, args, cli.Options{
		Stdout: out,
		Stderr: out,
		Getenv: func(k string) string { return env[k] },
		Name:   "workflow",
	})
}
```

## Try it

```sh
cd examples
go run ./admin-cli
```

It prints each command, its output and its exit code: the bulk retry previewed and done, `health`, `nodes --json`, a `drain` of an empty queue, a refused token (exit 1) and an unknown command (exit 2). `go test ./admin-cli` checks every line.

## Pitfalls

- **Keep the token out of shell history.** Set `WORKFLOW_ADMIN_TOKEN` from a secret store or a file rather than typing `--token`.
- **Times in tables are local.** Tables print times in the terminal's zone, to the second; `--json` keeps the API's RFC 3339 times.
- **The CLI checks only shape.** It validates label and time formats; status names, queue names and what the driver supports are the server's to refuse, and its refusal is printed with exit 1.
- **`--all` without filters means every match.** `retry --all` with no filter retries every failed run, up to the bound; preview with `--dry-run` first.
- **It is as allowed as its token.** Every call goes through your `Authorize`; give automation a token whose role allows only what the script does.

## Related

- [Admin JSON API](http-api.md): the routes the CLI calls.
- [Admin operations](operations.md): the same operations from Go.
- [Admin web UI](ui.md).
- Example: [examples/admin-cli](../../examples/admin-cli/).
