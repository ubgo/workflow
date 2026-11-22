# Misfire policies

A cron tick misfires when nothing was running to fire it (a deploy, a crash, a laptop asleep) or when the scheduler reaches it late. What should happen when the scheduler comes back depends on the work: a 09:00 digest must not go out at 14:00, a "sync now" should run once to catch up, and a daily report should run once for every day it missed. A misfire policy says which, per workflow or per schedule.

## Before you start

- Each policy is a capability: [`workflow.CapMisfireSkip`](ds:block?id=capmisfireskip-g62y4cwh), [`CapMisfireFireOnce`](ds:block?id=capmisfirefireonce-p94vdm89) and [`CapMisfireFireAll`](ds:block?id=capmisfirefireall-9zf4spja) (see [the check below](#check-that-the-driver-honours-the-policy)). The [memory](ds:block?id=memory-caps-yxkuq583), [SQL](ds:block?id=sql-caps-yu37ef2j) and [Redis](ds:block?id=redis-caps-bfgdst9t) drivers honour all three; [Hatchet](ds:block?id=hatchet-caps-d6x9fv8z) none (it [never catches up a missed tick](ds:block?id=hatchet-misfirereason-eythccvb)).
- The example is [`examples/cron-schedules`](../../examples/cron-schedules). Its misfire part pauses three hourly schedules for five and a half hours in one jump of a fake clock. Run it with `go run ./cron-schedules` from `examples/`.
- Cron basics are on [cron schedules](cron.md).

## Misfire policies

Set `Workflow.Misfire` (for `Workflow.Cron`) or `Schedule.Misfire` (for `Client.Schedule`) to a [`workflow.Misfire`](ds:block?id=misfire-hwbgv2py), whose `Policy` is a [`workflow.MisfirePolicy`](ds:block?id=misfirepolicy-pupp8x7w):

| Policy | What fires after a pause | Options |
|---|---|---|
| `MisfireSkip` | Nothing late: a tick fires only if the scheduler reaches it within [`workflow.MisfireGrace`](ds:block?id=misfiregrace-ruw2h32f) (one minute) of its time | none |
| `MisfireFireOnce` | The [newest missed tick, once](ds:block?id=misfire-due-5butt4h7); older ones are dropped | `Within`: drop it if it is older than this |
| `MisfireFireAll` | [Every missed tick, oldest first](ds:block?id=misfire-due-5butt4h7), at most `Max` of them | `Max` (required, at least 1), `Within` |

[`Within`](ds:block?id=misfire-hwbgv2py) is a starting deadline: a missed tick reached more than `Within` after its time does not fire. Zero means no age limit. [`Max`](ds:block?id=misfire-due-5butt4h7) keeps a backfill bounded: after a week down, an hourly report with `Max: 24` fires the newest 24 ticks, not 168.

<!-- snippet: examples/cron-schedules/main.go#misfire -->
```go
policies := map[string]workflow.Misfire{
	"skip":      {Policy: workflow.MisfireSkip},                            // fire nothing late
	"fire-once": {Policy: workflow.MisfireFireOnce, Within: 6 * time.Hour}, // the newest missed tick, if under 6h old
	"fire-all":  {Policy: workflow.MisfireFireAll, Max: 3},                 // up to 3 missed ticks, oldest first
}
```

The zero `Misfire` is the driver's own default, documented in its package and needing no capability. On the [memory](ds:block?id=memory-effectivemisfire-cz2fnaea), [SQL](ds:block?id=sql-client-fireschedules-9tkj9xrk) and [Redis](ds:block?id=redis-effectivemisfire-ued3ypmw) drivers the default is fire-once with no age limit: a tick missed while nothing ran fires once on return, and older missed ticks are not replayed. On Hatchet [a tick missed while the engine was down is dropped](ds:block?id=hatchet-misfirereason-eythccvb).

[Validation](ds:block?id=validatemisfire-mna4wuqq) catches the combinations that make no sense, with an error matching `workflow.ErrInvalid`: `Within` with `MisfireSkip` (it uses `MisfireGrace`), `Max` with any policy but `MisfireFireAll`, `MisfireFireAll` without `Max`, a negative `Within`, and a `Workflow.Misfire` on a workflow with no `Cron` ([`validatePolicies`](ds:block?id=validatepolicies-x88m4hwu)).

## Check that the driver honours the policy

Each policy is its own capability bit ([`misfireCaps`](ds:block?id=misfirecaps-e5ug8m9f)), because drivers differ: the [memory](ds:block?id=memory-caps-yxkuq583), [SQL](ds:block?id=sql-caps-yu37ef2j) and [Redis](ds:block?id=redis-caps-bfgdst9t) drivers honour all three, [Hatchet](ds:block?id=hatchet-caps-d6x9fv8z) none. A driver refuses a policy it lacks when the workflow is registered or the schedule is made, so an app that picks its driver at run time checks the bits it needs up front, where the error can name them:

<!-- snippet: examples/cron-schedules/main.go#misfire-caps -->
```go
need := workflow.CapsOf(workflow.CapMisfireSkip, workflow.CapMisfireFireOnce, workflow.CapMisfireFireAll)
if missing := c.Capabilities().Missing(need); !missing.IsZero() {
	return fmt.Errorf("the driver cannot honour the misfire policies %s", missing)
}
```

| Policy | Capability |
|---|---|
| `MisfireSkip` | `workflow.CapMisfireSkip` |
| `MisfireFireOnce` | `workflow.CapMisfireFireOnce` |
| `MisfireFireAll` | `workflow.CapMisfireFireAll` |
| zero `Misfire` (the driver's default) | none ([`misfireCaps`](ds:block?id=misfirecaps-e5ug8m9f)) |

[`workflow.RequiresSchedule(s)`](ds:block?id=requiresschedule-wk3hrhfy) returns every capability a schedule needs, its misfire bit included, and [`workflow.CheckScheduleCaps(caps, s)`](ds:block?id=checkschedulecaps-45emsz38) is the check every driver runs in `Schedule`: an `*workflow.UnsupportedError` matching `workflow.ErrUnsupported`, with the field (`Schedule.Misfire`) and the missing bit. Run it against a driver's capabilities before scheduling, for a settings form that should not offer a policy the driver cannot keep:

<!-- snippet: examples/cron-schedules/main.go#misfire-check -->
```go
catchUp := workflow.Schedule{Workflow: "hourly", Cron: "0 * * * *", Location: time.UTC, Misfire: policies["fire-all"]}
fmt.Println("  fire-all needs", workflow.RequiresSchedule(catchUp))
h := hatchet.New(hatchet.Config{}) // never catches up a missed tick
err := workflow.CheckScheduleCaps(h.Capabilities(), catchUp)
fmt.Println("  hatchet:", errors.Is(err, workflow.ErrUnsupported), err)
```

```text
  fire-all needs Cron|MisfireFireAll
  hatchet: true workflow: Schedule.Misfire unsupported by this driver (missing MisfireFireAll): schedule for workflow hourly
```

## Compute missed ticks yourself

[`Misfire.Due(src, last, now, loc)`](ds:block?id=misfire-due-5butt4h7) is the one rule every driver applies: given when the scheduler last caught up (`last`) and the instant it woke (`now`), it returns the ticks in `(last, now]` that the policy fires, oldest first. It returns nil for the zero policy, so a driver applies its own default instead. If you write a driver or a scheduler of your own, call it rather than re-deriving the rules:

<!-- snippet: examples/cron-schedules/main.go#due -->
```go
hourly, _ := cron.Parse("0 * * * *")
last, now := monday, monday.Add(5*time.Hour+30*time.Minute) // after the five-hour pause
for _, name := range names {
	var ticks []string
	for _, t := range policies[name].Due(hourly, last, now, time.UTC) {
		ticks = append(ticks, t.Format("15:04"))
	}
	fmt.Printf("  Due %-9s [%s]\n", name, strings.Join(ticks, " "))
}
```

`src` is a [`workflow.TickSource`](ds:block?id=ticksource-8kbat78x), the one method the core needs from a schedule: `Between(from, to, loc, limit)`. `cron.Schedule` satisfies it, so `cron.Parse` output can be passed directly; the core module does not import the parser. `Due` asks for ticks [in chunks](ds:block?id=tickscanchunk-jppjg98q), so a long outage of a frequent cron costs bounded memory however many ticks it missed.

## Try it

```sh
cd examples
go run ./cron-schedules
```

The misfire part, after a pause from 00:00 to 05:30 of three hourly schedules:

```text
misfire after a five-hour pause:
  skip      0 run(s)
  fire-once 1 run(s)
  fire-all  3 run(s)
  Due skip      []
  Due fire-once [05:00]
  Due fire-all  [03:00 04:00 05:00]
  fire-all needs Cron|MisfireFireAll
  hatchet: true workflow: Schedule.Misfire unsupported by this driver (missing MisfireFireAll): schedule for workflow hourly
```

The 05:00 tick is 30 minutes old when the scheduler wakes, past the one-minute grace, so `skip` fires nothing; `fire-once` fires the 05:00 tick; `fire-all` fires the newest three of the five missed ticks. The last two lines are the capability check: the fire-all schedule needs `CapMisfireFireAll`, which Hatchet lacks. `go test ./cron-schedules` checks every line.

## Pitfalls

- **Hatchet refuses every explicit policy.** The Hatchet driver ([`checkScheduleMisfire`](ds:block?id=hatchet-checkschedulemisfire-ucbwxfy2)) returns an error matching `workflow.ErrUnsupported` (field `Workflow.Misfire` or `Schedule.Misfire`) for any non-zero `Misfire`, because the engine never catches up a missed tick and has no lateness window. Leave `Misfire` zero for workflows that run on Hatchet. See [Hatchet limitations](../drivers/hatchet.md#limitations).
- **A fired-late run starts now, but is for an earlier tick.** Each catch-up fire is an ordinary run with the schedule's input; under `MisfireFireAll` the runs arrive oldest first, all at about the same wall time. On a driver with [`workflow.CapTickTime`](ds:block?id=capticktime-6gn5wpmj) each carries its own tick, so read [`StepInfo.TickAt()`](ds:block?id=stepinfo-tickat-rt2h9tgz) rather than the clock (see [which tick a run is for](cron.md#which-tick-a-run-is-for)); for work where only the newest tick matters, skip the older ones with [`middleware.LatestOnly`](latest-only.md).
- **"Missed" is measured from the last catch-up, in the scheduler's process.** On the memory driver that is since `Start` or the `Schedule` call; ticks from before the process started are not counted. On the SQL driver it is [the schedule's stored next fire time](ds:block?id=sql-client-fireschedules-9tkj9xrk), so ticks missed while no client ran are counted.
- **Dropped ticks are reported, not silent.** On a driver with `workflow.CapTickEvents`, the ticks a policy drops reach [tick observers and schedule hooks](hooks.md) as one [`workflow.TickMisfired`](ds:block?id=tickmisfired-gynura72) tick with `Missed` (how many) and `At` (the newest dropped). [`Misfire.Plan(src, last, now, loc)`](ds:block?id=misfire-plan-knwft3x6) is `Due` plus that count, for a driver or a test.
- **A paused tick is not a missed tick.** Ticks that fall due while a schedule is [paused](manage.md#pause-a-schedule) [never fire](ds:block?id=sql-client-fireschedules-9tkj9xrk), not even on resume, whatever the policy; the policy only decides about ticks nobody was awake to fire.
- **Skip is not "never late".** A tick reached within [`MisfireGrace`](ds:block?id=misfiregrace-ruw2h32f) (one minute) still fires; a scheduler wakes a little after a tick's time even when nothing was missed.

## Related

- [Cron schedules](cron.md), [time zones](time-zones.md), [latest-only runs](latest-only.md)
- [Schedule hooks and pings](hooks.md): `TickMisfired` events; [Manage schedules](manage.md): pause and resume
- [Capabilities](../concepts/capabilities.md)
- [Writing a driver](../extending/writing-a-driver.md)
- Example: [`examples/cron-schedules`](../../examples/cron-schedules)
