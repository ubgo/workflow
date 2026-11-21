# The workflow command

`workflow` is a command line client for the [admin JSON API](http-api.md): list and inspect runs, retry, cancel and delete them one at a time or by filter, move a delayed run, withdraw one step, pause and resume, list and trigger schedules, wait for a queue to drain, and read run-end history, health and the online nodes, from a terminal or a deploy script. It talks HTTP only and imports nothing beyond the standard library, not even the library's core, so installing it pulls no driver, SDK or database code. Every command can print the API's JSON instead of a table, and every destructive one can preview first.

## Before you start

- An app that mounts `admin.NewHandler` ([Admin JSON API](http-api.md)) and an `Identify` that accepts something a script can send: a bearer token, or a header such as a session cookie.
- Install the stand-alone binary: `go install github.com/ubgo/workflow/cli/cmd/workflow@latest`. It is the `github.com/ubgo/workflow/cli` module (Go 1.24 or later).
- The example is [`examples/admin-cli`](../../examples/admin-cli). It starts the admin handler over a memory client on an `httptest` server and drives the CLI against it. Run it with `go run ./admin-cli` from `examples/`. [`examples/cli-tour`](../../examples/cli-tour) runs every command below once against the same kind of server, embedded as a subcommand of an app's binary.

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

An app that embeds the CLI hands it the environment through `cli.Options.Getenv`, keyed by the constants `cli.EnvURL`, `cli.EnvToken` and `cli.EnvHeader`; the tour reads them from a map:

<!-- snippet: examples/cli-tour/main.go#env -->
```go
env := map[string]string{
	cli.EnvURL:   srv.URL + "/admin", // where the app mounted admin.NewHandler
	cli.EnvToken: opsToken,           // sent as a bearer token, never printed
}
getenv := func(k string) string { return env[k] } // the CLI reads env here instead of os.Getenv
```

For one command it clears the token and sets `cli.EnvHeader` to `Cookie: session=s-ops`, which its `Identify` accepts as well: the `pauses` line of the tour.

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
| `reset RUN` | Re-run a failed run in place from a step; the steps before it keep their outputs ([Re-run from a step or fork a run](#re-run-from-a-step-or-fork-a-run)) | `--step` (required), `--succeeded` (also a run that succeeded), `--dry-run` |
| `fork RUN` | Start a new run from a finished one at a step; the original is not changed | `--step` (required), `--key` (the fork's idempotency key), `--label k=v` (repeatable), `--dry-run` |
| `signal-waiting NAME` | Deliver an event only to the runs waiting for it now, and say how many resumed ([Signal the runs waiting now](#signal-the-runs-waiting-now)) | `--key`, `--payload` (JSON), `--dry-run` |
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

A bulk retry or delete acts on at most the server's bound (`admin.WithMaxBulk`, 1,000 by default) or `--max`; when more match it says so, and running the same command again does the next batch. `delete --all` with no filter is refused by the server: list the statuses to mean every finished run ([Admin operations › Act on many runs at once](operations.md#act-on-many-runs-at-once)). `trigger --dry-run` shows the schedule that would run and warns when its overlap policy would refuse the trigger. For the single-run commands the dry run reads the run and exits 1 when the action would be refused: `reschedule --dry-run` unless the run is still delayed (`scheduled`), `cancel-step --dry-run` when the run has finished, `reset --dry-run` unless the run failed (or succeeded, with `--succeeded`), `fork --dry-run` unless the run has finished. `signal-waiting --dry-run` reads the server's capabilities and exits 1 when it would refuse the call. `pause --dry-run` and `resume --dry-run` read the active pauses, say whether the target is paused and how many pauses are active, and write nothing.

## Re-run from a step or fork a run

When a run failed, or succeeded with a wrong result, because of something upstream of the step that failed (bad data fixed at the source, a transform deployed with a fix), `retry` is not enough: it resumes at the step that failed. `reset` re-runs the run in place from the step you name, and `fork` starts a new run from it, leaving the original as it was ([Manage runs › Re-run a failed run from an earlier step](../jobs/manage-runs.md#re-run-a-failed-run-from-an-earlier-step)):

```text
$ workflow reset run-7 --step transform --dry-run
dry run: would reset run run-7 (etl, failed) from step transform; nothing changed
exit 0
$ workflow reset run-7 --step transform
reset run-7 from step transform
exit 0
$ workflow reset run-9 --step transform
workflow: admin API: HTTP 409 conflict: workflow: run is in the wrong state for this call: run run-9 is succeeded; only a failed run can be reset (ResetOptions.Succeeded, the CLI's reset --succeeded, also resets a succeeded run)
exit 1
$ workflow reset run-9 --step transform --succeeded
reset run-9 from step transform
exit 0
$ workflow fork run-9 --step publish --key replay-run-9 --label ticket=OPS-7
forked run-9 from step publish as run run-12
exit 0
$ workflow fork run-9 --step publish --key replay-run-9
key replay-run-9 is held by run run-12; nothing started
exit 0
```

`--succeeded` is the explicit opt-in for redoing work that already succeeded: the run gets the `ubgo.workflow/redo` label and its chain is not handed on again ([Manage runs › Reset a run that succeeded](../jobs/manage-runs.md#reset-a-run-that-succeeded)). A fork keeps the original's workflow, payload, tenant, queue and priority, gets a new run ID and the `ubgo.workflow/fork-of` and `ubgo.workflow/fork-step` labels, and never copies the original's idempotency key: `--key` gives it its own, and while an unfinished run holds that key the command prints that run and starts nothing ([Manage runs › Fork a run into a new run](../jobs/manage-runs.md#fork-a-run-into-a-new-run)). `--label` adds app labels; a reserved prefix is refused with a 400. `caps` shows whether the server's driver does each (`reset_run`, `reset_succeeded`, `fork_run`).

## Signal the runs waiting now

`signal-waiting` sends an event that only the runs already waiting for it receive; a run that reaches its wait step a moment later never sees it, where a plain signal is kept for it. Use it for hints whose loss is harmless, such as a "refresh now" nudge ([Signals › Deliver only to runs waiting now](../workflows/signals.md#deliver-only-to-runs-waiting-now)):

```text
$ workflow signal-waiting cache.refresh --key eu --payload '{"reason":"deploy"}'
signalled cache.refresh: 2 waiting runs resumed
exit 0
```

Zero resumed is not an error. The payload must be JSON (a usage error, exit 2, otherwise). A caller the server confines to one tenant is refused with a 403: an event has no tenant and would reach every tenant's waiting runs ([Per-tenant dashboard](tenants.md)).

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

The codes are the constants `cli.ExitOK`, `cli.ExitError` and `cli.ExitUsage`, so Go code around `cli.Run` names them instead of repeating numbers:

<!-- snippet: examples/cli-tour/main.go#exit-codes -->
```go
// exitNames names the CLI's exit codes, so a script's log says what
// happened: 0 done, 1 refused or failed (a 4xx or 5xx answer, a failed
// health check, a drain that timed out), 2 a wrong command line.
var exitNames = map[int]string{
	cli.ExitOK:    "cli.ExitOK",
	cli.ExitError: "cli.ExitError",
	cli.ExitUsage: "cli.ExitUsage",
}
```

An API refusal is printed as `admin API: HTTP <status> <code>: <message>`, the text of a `*cli.APIError` (`Status`, `Code`, `Message`), with the codes of [Admin JSON API › Step 3](http-api.md#step-3-handle-the-error-codes).

`health` exits 1 when the client is not live or not ready, and `drain` exits 1 when runs are still queued or running after `--wait`, so a deploy script can gate on either:

```sh
workflow pause --queue emails
workflow drain --queue emails --wait 10m && ./deploy-email-workers.sh
workflow resume --queue emails
```

`drain` counts runs that are queued, running or retrying; delayed runs and runs waiting for an event are not waited for, since either can wait for days.

## Embed it in your binary

`cli.Run(ctx, args, cli.Options{...})` is the whole CLI as a function that returns the exit code, so an app can ship it as a subcommand of its own binary with its own default URL and auth. `Options` takes `Stdout` and `Stderr`, `Getenv` (nil reads no environment; pass `os.Getenv` or your own lookup), `HTTPClient` (default: a 30-second timeout), `Now` and `Sleep` for `drain`'s polling, and `Name` for the usage text. `cli.Main()` is the stand-alone binary's body: it calls `Run` with the process's arguments, `os.Getenv`, a context cancelled on an interrupt, and exits with the code.

To ship it inside your own binary, call `cli.Run` from a subcommand and give the URL a default of your own, so `myapp admin failures` works with only a token set:

<!-- snippet: examples/cli-tour/main.go#subcommand -->
```go
// defaultAdminURL is where this app serves its admin API when
// WORKFLOW_ADMIN_URL is not set.
const defaultAdminURL = "http://127.0.0.1:8080/admin"

// runApp is an app binary's main with the admin CLI as its "admin"
// subcommand: "myapp admin failures" runs "workflow failures" against
// the app's own admin API. It returns the exit code for os.Exit.
func runApp(ctx context.Context, args []string, stdout, stderr io.Writer, getenv func(string) string) int {
	if len(args) == 0 || args[0] != "admin" {
		fmt.Fprintln(stderr, "usage: myapp admin <command> [flags]")
		return cli.ExitUsage
	}
	return cli.Run(ctx, args[1:], cli.Options{
		Stdout: stdout,
		Stderr: stderr,
		Getenv: func(k string) string {
			if v := getenv(k); v != "" {
				return v
			}
			if k == cli.EnvURL {
				return defaultAdminURL
			}
			return ""
		},
		Name: "myapp admin",
	})
}
```

The usage text then says `myapp admin` wherever it would say `workflow`, as the tour's last line shows.

<!-- snippet: examples/admin-cli/main.go#embed -->
```go
env := map[string]string{
	cli.EnvURL:   srv.URL + "/admin", // where the app mounted admin.NewHandler
	cli.EnvToken: opsToken,           // sent as a bearer token, never printed
}
workflowCmd := func(args ...string) int {
	return cli.Run(ctx, args, cli.Options{
		Stdout: os.Stdout,
		Stderr: os.Stdout,
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

Every command once, through the `runApp` subcommand above, against a memory client with a fake clock: two failed `etl` runs, three `send-email` runs (one failed), a discarded `cleanup` run, four `cache-refresh` runs waiting for their region's event, a schedule, and later a delayed run:

```sh
go run ./cli-tour
```

```text
## read
$ myapp admin caps
OPERATION         SUPPORTED  MISSING
runs              yes        
run               yes        
failures          yes        
discarded         yes        
queues            yes        
retry             yes        
delete            yes        
cancel            yes        
cancel_all        yes        
pause             yes        
schedules         yes        
schedule_pause    yes        
schedule_trigger  yes        
force             yes        
retry_all         yes        
delete_all        yes        
nodes             yes        
health            yes        
reschedule        yes        
cancel_step       yes        
edit_run          yes        
reset_run         yes        
reset_succeeded   yes        
fork_run          yes        
signal_waiting    yes        
output            yes        
metrics           yes        
exit 0 cli.ExitOK
$ myapp admin runs --workflow etl --count
RUN    WORKFLOW  QUEUE    STATUS  ATT  ENQUEUED             FINISHED             ERROR
run-6  etl       default  failed  2    2026-10-14 09:00:00  2026-10-14 09:00:00  memory: step "transform" failed after 1 attempt(s): transform: bad date in row 7
run-3  etl       default  failed  2    2026-10-14 09:00:00  2026-10-14 09:00:00  memory: step "transform" failed after 1 attempt(s): transform: bad date in row 7
2 of 2
exit 0 cli.ExitOK
$ myapp admin failures
RUN     WORKFLOW    QUEUE    STATUS  ATT  ENQUEUED             FINISHED             ERROR
run-12  send-email  emails   failed  1    2026-10-14 09:00:00  2026-10-14 09:00:00  memory: step "send" failed after 1 attempt(s): smtp: connection refused
run-6   etl         default  failed  2    2026-10-14 09:00:00  2026-10-14 09:00:00  memory: step "transform" failed after 1 attempt(s): transform: bad date in row 7
run-3   etl         default  failed  2    2026-10-14 09:00:00  2026-10-14 09:00:00  memory: step "transform" failed after 1 attempt(s): transform: bad date in row 7
exit 0 cli.ExitOK
$ myapp admin discarded
RUN     WORKFLOW  QUEUE    STATUS     ATT  ENQUEUED             FINISHED             ERROR
run-18  cleanup   default  discarded  1    2026-10-14 09:00:00  2026-10-14 09:00:00  workflow: run cancelled: workflow: run discarded: step "sweep": nothing to clean
exit 0 cli.ExitOK
$ myapp admin inspect run-6 --output
run          run-6
workflow     etl
queue        default
status       failed
attempts     2
priority     0
enqueued     2026-10-14 09:00:00
started      2026-10-14 09:00:00
finished     2026-10-14 09:00:00
failed step  transform
error        memory: step "transform" failed after 1 attempt(s): transform: bad date in row 7
label        team=data-eng

failed attempts:
ATTEMPT  STEP       AT                   ERROR
1        transform  2026-10-14 09:00:00  transform: bad date in row 7

output of extract #1 (ok)
== 2026-10-14T09:00:00Z etl/extract attempt 1 [run-6] ok
2026-10-14T09:00:00Z INFO extracted rows=42


output of transform #1 (error: transform: bad date in row 7)
== 2026-10-14T09:00:00Z etl/transform attempt 1 [run-6] error: transform: bad date in row 7
2026-10-14T09:00:00Z ERROR bad date column row=7

exit 0 cli.ExitOK
## act on many runs
$ myapp admin retry --all --workflow send-email --dry-run
dry run: would retry 1 runs; nothing changed
  run-12
exit 0 cli.ExitOK
$ myapp admin cancel --all --workflow cache-refresh --status waiting --dry-run
dry run: would cancel 4 runs; nothing changed
  run-27
  run-25
  run-23
  run-21
exit 0 cli.ExitOK
$ myapp admin delete --all --workflow send-email --status succeeded --dry-run
dry run: would delete 2 runs; nothing changed
  run-15
  run-9
exit 0 cli.ExitOK
## act on one run
$ myapp admin retry run-12
retried run-12 as run-12
exit 0 cli.ExitOK
$ myapp admin cancel run-23
cancelled run-23
exit 0 cli.ExitOK
$ myapp admin delete run-9
deleted run-9
exit 0 cli.ExitOK
$ myapp admin cancel-step run-25 await
cancelled step await of run-25
exit 0 cli.ExitOK
$ myapp admin reset run-3 --step transform
reset run-3 from step transform
exit 0 cli.ExitOK
$ myapp admin fork run-3 --step publish --key replay-etl --label ticket=OPS-7
forked run-3 from step publish as run run-36
exit 0 cli.ExitOK
$ myapp admin signal-waiting cache.refresh --key eu --payload '{"reason":"deploy"}'
signalled cache.refresh: 1 waiting runs resumed
exit 0 cli.ExitOK
$ myapp admin reschedule delayed-41 --scheduled --at 2026-10-14T11:00:00Z
rescheduled scheduled:delayed-41 to 2026-10-14 11:00:00
exit 0 cli.ExitOK
## pauses, queues and schedules
$ myapp admin pause --queue emails
paused queue emails
exit 0 cli.ExitOK
$ myapp admin pauses
queue emails
exit 0 cli.ExitOK
$ myapp admin resume --queue emails
resumed queue emails
exit 0 cli.ExitOK
$ myapp admin queues
QUEUE    DEPTH  OLDEST WAIT  WORKERS  RUNNING
default  0      -            2        0
emails   0      -            1        0
exit 0 cli.ExitOK
$ myapp admin schedules
ID          NAME     WORKFLOW        TENANT  CRON       ZONE  NEXT                 STATE
schedule-1  nightly  nightly-report  -       0 2 * * *  UTC   2026-10-15 02:00:00  active
exit 0 cli.ExitOK
$ myapp admin schedule schedule-1
id        schedule-1
name      nightly
workflow  nightly-report
cron      0 2 * * *
zone      UTC
overlap   allow
state     active
next      2026-10-15 02:00:00
exit 0 cli.ExitOK
$ myapp admin trigger schedule-1 --dry-run
dry run: would run schedule schedule-1 (workflow nightly-report) now; nothing enqueued
exit 0 cli.ExitOK
$ myapp admin trigger schedule-1
triggered schedule-1 as run run-43
exit 0 cli.ExitOK
## charts and system
$ myapp admin drain --wait 1m
drained after 0s
exit 0 cli.ExitOK
$ myapp admin metrics --from 2026-10-14T09:00:00Z --to 2026-10-14T09:10:00Z --step 5m
AT                   SUCCEEDED  FAILED  CANCELLED  DISCARDED
2026-10-14 09:00:00  8          4       1          1
2026-10-14 09:05:00  0          0       0          0
total                8          4       1          1
data since 2026-10-14 09:00:00
exit 0 cli.ExitOK
$ myapp admin health
CHECK    OK   DETAIL
live     yes  
ready    yes  
workers  yes  0 of 2 busy
exit 0 cli.ExitOK
$ myapp admin nodes
NODE      MODE  QUEUES          WORKERS  RUNNING  LAST SEEN
worker-1  full  default,emails  2        0        2026-10-14 09:00:00
exit 0 cli.ExitOK
## exit codes
$ myapp admin reset run-3 --step transform
myapp admin: admin API: HTTP 409 conflict: workflow: run is in the wrong state for this call: run run-3 is succeeded; only a failed run can be reset (ResetOptions.Succeeded, the CLI's reset --succeeded, also resets a succeeded run)
exit 1 cli.ExitError
$ myapp admin reschedule delayed-41 --scheduled
myapp admin: --at "": want an RFC 3339 time such as 2026-10-15T09:00:00Z
run 'myapp admin help' for usage
exit 2 cli.ExitUsage
```

`go test ./cli-tour` checks every line, and fails when the CLI gains a command the tour does not run.

## Pitfalls

- **Keep the token out of shell history.** Set `WORKFLOW_ADMIN_TOKEN` from a secret store or a file rather than typing `--token`.
- **Times in tables are local.** Tables print times in the terminal's zone, to the second; `--json` keeps the API's RFC 3339 times. (The tour fixes the zone at UTC so its output is the same everywhere.)
- **A delayed run needs `--scheduled`.** Until it starts its ID is a `ScheduledID`, as `reschedule` shows in the tour; without the flag the server answers 404.
- **There is no `edit` command.** Editing a delayed run is `POST /runs/{id}/edit` or `Admin.EditRun` ([Admin operations › Step 3](operations.md#step-3-act-on-runs)).
- **The CLI checks only shape.** It validates label and time formats; status names, queue names and what the driver supports are the server's to refuse, and its refusal is printed with exit 1.
- **`--all` without filters means every match.** `retry --all` with no filter retries every failed run, up to the bound; preview with `--dry-run` first.
- **`reset` and `fork` do the work again.** The named step and every step after it run again, side effects included; preview with `--dry-run` and pick the latest step that fixes the result.
- **It is as allowed as its token.** Every call goes through your `Authorize`; give automation a token whose role allows only what the script does.

## Related

- [Admin JSON API](http-api.md): the routes the CLI calls.
- [Admin operations](operations.md): the same operations from Go.
- [Admin web UI](ui.md).
- Examples: [examples/admin-cli](../../examples/admin-cli/), [examples/cli-tour](../../examples/cli-tour/) (every command).
