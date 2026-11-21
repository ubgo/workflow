# failover-observer

The parts of a failover client an operator tunes and watches, on two in-memory backends with a fake clock for the cool-off: which errors count as an outage (`DefaultClassifier`, `StrictClassifier` and a classifier of the app's own through `WithClassifier`), an observer that logs every `failover.Event` and counts them per `EventKind` while `WithCoolOff` skips the primary, `ResetRun`, `EditRun` and `SignalWaiting` on runs that sit on either backend (with `workflow.Supports` asking first), and `WithSchedulesOnBoth`.

```sh
cd examples
go run ./failover-observer
```

```text
-- Decide what counts as an outage
  marked Unavailable default true  strict true  ours true
  connection refused default true  strict false ours true
  unknown error      default true  strict false ours false
  invalid job        default false strict false ours false
  permanent          default false strict false ours false
-- Watch failovers
approve a-1 -> primary
-- the primary goes down
  event failed_over workflow=report at=09:00:00 err="dial primary: connection refused"
report r-1 -> secondary
  event failed_over workflow=remind at=09:00:00 err="dial primary: connection refused"
  event cool_off_started workflow=remind at=09:00:00 until=09:01:00
remind m-1 -> secondary
  event primary_skipped workflow=approve at=09:00:00
approve a-2 -> secondary
-- the primary is back and the cool-off runs out
  event cool_off_ended workflow=approve at=09:01:00
approve a-3 -> primary
  count failed_over: 2
  count cool_off_started: 1
  count primary_skipped: 1
  count cool_off_ended: 1
-- Reset, edit and signal runs on either backend
report on the secondary is failed
  supports reset-run: true
  supports edit-run: true
  supports signal-waiting: true
reset in place: true succeeded
edited remind on the secondary sent to "new@example.com"
signal-waiting resumed 3 run(s)
  approve on the primary is succeeded
  approve on the secondary is succeeded
  approve on the primary is succeeded
  with a bare secondary, supports signal-waiting: false
  SignalWaiting refused: true
-- Schedules on both backends
schedule copies: primary, secondary
Schedule returned an ID for: both
```

The app's classifier fails over on what a driver marked `Unavailable` and on a refused connection, and returns every other unknown error to the caller, where the default would have failed over. The primary fails two enqueues in a row, which starts a one-minute cool-off: the third job goes straight to the secondary without trying the primary. After the minute the primary takes a job again and the cool-off ends. The reset and the edit reach the secondary because the refs carry its prefix; `SignalWaiting` resumes the waiting runs of both backends. A secondary that hides the optional interfaces makes `Supports` false and the call an `ErrUnsupported`.

The guide that copies this program is [failover](../../docs/reliability/failover.md#decide-what-counts-as-an-outage). `go test ./failover-observer` checks every line.
