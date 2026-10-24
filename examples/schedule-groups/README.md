# schedule-groups

Several schedules that share options, built by one Go helper instead of a group type: `officeHours` returns a `workflow.Schedule` with the shared zone (Berlin), a 09:00 to 18:00 window, the skip misfire policy and a `group` label. The label finds the group in the schedule list, filters the runs its ticks started (fired runs carry their schedule's labels), and pauses every schedule of the group at once, while a schedule built without the helper keeps running. It runs on the memory driver with a fake clock.

```sh
cd examples
go run ./schedule-groups
```

```text
-- the group in the schedule list
  nightly       -            next 02:00 CET
  digest-hourly office-hours next 09:00 CET
  orders-hourly office-hours next 09:00 CET
-- 09:00: both schedules of the group fire
runs of the group: 2
-- pause the group
  nightly       -            next 02:00 CET
  digest-hourly office-hours paused
  orders-hourly office-hours paused
```

## The code

- `helper`: the function that gives every schedule of the group the same options.
- `schedule`: two schedules from the helper, one without it.
- `list`: the schedule list with each schedule's group.
- `runs`: the runs of the group, found by its label.
- `pause`: pausing every schedule that carries the group label.

The guide [Manage schedules](../../docs/schedules/manage.md#group-schedules) copies its code from the regions marked `snippet:`. `main_test.go` runs the same program and checks every line above.
