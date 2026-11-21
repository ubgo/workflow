# admin-alerts

Queue alerts with `admin.Monitor` on the memory driver, with fake clocks so every time is fixed. A paused queue holds three runs: `Check` raises `admin.AlertQueueDepth`, and eleven minutes later `admin.AlertQueueWait`. `NewMonitor` refuses a threshold that checks nothing and two thresholds for one queue. Then `Monitor.Run` polls every 30 seconds: a poll while the run store is down goes to `admin.WithErrorHandler` and the next poll still runs, `admin.WithRepeat` sends the depth alert again fifteen minutes after the first, and both alerts resolve once the queue is resumed and drains.

```sh
cd examples
go run ./admin-alerts
```

```text
-- Check polls once
09:00:00 ALERT queue_depth emails: depth 3 > 2
09:11:00 ALERT queue_wait emails: oldest wait 11m0s > 10m0s
a threshold that checks nothing: true
two thresholds for one queue: true
-- Run polls every interval
09:11:30 monitor error: run store: connection refused
09:15:00 ALERT queue_depth emails: depth 3 > 2
09:16:00 resolved queue_depth emails: depth 0 <= 2
09:16:00 resolved queue_wait emails: oldest wait 0s <= 10m0s
Run returned: context canceled
```

The guide that copies this program is [Queue alerts](../../docs/admin/alerts.md). `go test ./admin-alerts` checks every line.
