# failover-schedules

Where a failover client keeps its schedules. By default `Client.Schedule` puts a schedule on the primary alone: it is listed once, each tick fires once, and no tick fires while the primary is down. With `failover.WithSchedulesOnBoth()` the schedule is made on both backends: `Schedule` returns one ID that stands for both copies, the list shows each copy with its backend's prefix, a tick fires on both while both are up, and pausing the returned ID pauses both copies. Use it only for scheduled work that tolerates running twice per tick. Both backends are the memory driver on one fake clock.

```sh
cd examples
go run ./failover-schedules
```

```text
-- default: schedules on the primary only
Schedule returned an ID for: primary
  listed on primary
09:00 fired on: primary
  primary paused: true
-- WithSchedulesOnBoth: schedules on both backends
Schedule returned an ID for: both
  listed on primary
  listed on secondary
09:00 fired on: primary, secondary
  primary paused: true
  secondary paused: true
```

## The code

- `new`: the failover client, with or without `WithSchedulesOnBoth`.
- `schedule`: one schedule, and the list with each copy's backend.
- `pause`: pausing every copy through the ID `Schedule` returned.

The guide [Failover](../../docs/reliability/failover.md#schedules) copies its code from the regions marked `snippet:`. `main_test.go` runs the same program and checks every line above.
