# cli-tour

Every command of the `workflow` command line client run once against a real admin JSON API. An `httptest` server runs `admin.NewHandler` over a memory client with a fake clock, and the program calls `cli.Run` the way an app embeds the CLI as a subcommand of its own binary (`myapp admin <command>`, with its own default URL). It reads `cli.EnvURL` and `cli.EnvToken`, switches to a session cookie through `cli.EnvHeader` for one command, and ends with a refused reset (exit 1, `cli.ExitError`) and a missing flag (exit 2, `cli.ExitUsage`). Table times print in the terminal's zone, so the program fixes the zone at UTC to keep the output the same on every machine. `main_test.go` also fails when the CLI gains a command the tour does not run.

```sh
cd examples
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

The guide that copies this program is [The workflow command](../../docs/admin/cli.md). `go test ./cli-tour` checks every line.
