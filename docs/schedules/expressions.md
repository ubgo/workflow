# Cron expression tools

Five cron fields cannot say "09:00 on the last day of the month", "the weekday nearest the 15th" or "the first Monday". A thousand tenants each given `0 * * * *` all start at minute 0 and hit the database in the same second. A settings page that shows `30 8 * * MON#1` should also say what it means. The `cron` package has three tools for this: an opt-in extended dialect with the `L`, `W` and `#` day items, [`cron.Hash`](ds:block?id=cron-hash-xrapuzt3) to spread schedules written alike, and [`cron.Describe`](ds:block?id=cron-describe-e5rw59ey) for an English description.

## Before you start

- The extended dialect ([`cron.ParseExtended`](ds:block?id=cron-parseextended-tbb99h8z), [`cron.Extended`](ds:block?id=cron-extended-b3hq94dx)) is a `cron.Parser`, used only by a driver built with it: the memory driver ([`memory.WithCronParser`](ds:block?id=memory-withcronparser-jnqd5hex)), the SQL driver ([`sqlstore.WithCronParser`](ds:block?id=sql-withcronparser-dqejw86q)) and the Redis driver ([`redis.WithCronParser`](ds:block?id=redis-withcronparser-vkv8aegq)). [Hatchet](ds:block?id=hatchet-checkcron-s2s37tr5) parses expressions on its engine and has no such option, so a schedule written in the extended dialect does not run there. See [Hatchet limitations](../drivers/hatchet.md#limitations).
- [`cron.Hash`](ds:block?id=cron-hash-xrapuzt3) returns a standard five-field expression, so its output runs on every driver, Hatchet included. [`cron.Describe`](ds:block?id=cron-describe-e5rw59ey) is a pure function.
- The example is [`examples/schedule-tools`](../../examples/schedule-tools). Run it with `go run ./schedule-tools` from `examples/`.
- Cron basics and the standard syntax are on [cron schedules](cron.md).

## The last day, the nearest weekday, the n-th weekday

[`cron.ParseExtended`](ds:block?id=cron-parseextended-tbb99h8z) reads every standard expression, macros and `@every` included, plus these day items ([day of month](ds:block?id=cron-splitdom-bur6fbx6), [day of week](ds:block?id=cron-splitdow-9sq56utt)):

| Field | Item | Fires on |
|---|---|---|
| day of month | `L` | the last day of the month |
| day of month | `L-n` | n days before the last day (`L-1` is the day before the last; n 0-30) |
| day of month | `nW` | the weekday (Monday to Friday) nearest day n (1-31), never crossing into another month: a Saturday 1st fires on Monday the 3rd |
| day of month | `LW` | the last weekday of the month |
| day of week | `d#n` | the n-th weekday d of the month (`MON#1`, `5#3`; n 1-5) |
| day of week | `dL` | the last weekday d of the month (`FRIL`, `5L`) |

A month that lacks the day (`L-30` in February, `31W` in June, `THU#5` in most months) [does not fire](ds:block?id=cron-extspec-daymatches-8gz3bb2u). Special items mix with ordinary ones in a list (`1,15,L`), and a field holding one counts as restricted for the Vixie rule: `0 0 L * FRI` fires on the last day of every month and on every Friday. The fire times follow the same [DST rules](time-zones.md#daylight-saving-time) as every expression: a fixed time that falls in a spring-forward gap on the second Sunday of March fires once, at the end of the gap.

<!-- snippet: examples/schedule-tools/main.go#extended -->
```go
s, err := cron.ParseExtended(expr) // L, L-n, nW, LW, d#n and dL on top of the five fields
if err != nil {
	return err
}
next := s.Between(monday, monday.AddDate(0, 3, 0), time.UTC, 3)
```

The dialect is opt-in per driver. Pass `cron.Extended` where the driver takes a parser:

<!-- snippet: examples/schedule-tools/main.go#extended-driver -->
```go
c, fc, err := fakeClient(ctx, monday, []workflow.Workflow{rec.workflow("month-end")},
	memory.WithCronParser(cron.Extended)) // opt-in: this driver now reads L, W and #
if err != nil {
	return err
}
defer c.Stop()
_, err = c.Schedule(ctx, workflow.Schedule{Name: "month-end", Workflow: "month-end", Cron: "0 9 L * *", Location: time.UTC})
```

An expression without a special item [parses to exactly what `cron.Parse` returns](ds:block?id=cron-parseextended-tbb99h8z), so switching a driver to `cron.Extended` changes no existing schedule. Every error is a `*cron.ParseError` naming the field (`day-of-month`, `day-of-week`).

## Spread schedules written alike with H

[`cron.Hash(expr, seed)`](ds:block?id=cron-hash-xrapuzt3) rewrites the hashed items of an expression into fixed numbers derived from the seed ([FNV-1a](ds:block?id=cron-hashvalue-n5gtxsfn)), and returns a standard expression. `H` is one value in the field's range, `H(a-b)` one value in a-b, `H/n` every n-th value from an offset below n, and `H(a-b)/n` the same within a-b. In day of month `H` [stays within 1-28](ds:block?id=cron-hashdommax-begnkwpt), so every month has the day.

<!-- snippet: examples/schedule-tools/main.go#hash -->
```go
expr, err := cron.Hash("H H(0-5) * * *", tenant+"/nightly-export") // once a night, 00:00-05:59
if err != nil {
	return err
}
```

Pass a stable seed, such as the tenant and the job name: the same seed gives the same expression on every call, process and node, and different seeds spread evenly over the range. Hash once where the schedule is declared and store the result; the stored schedule is then a plain expression every driver reads.

## Describe an expression

[`cron.Describe(expr)`](ds:block?id=cron-describe-e5rw59ey) returns an English description of a five-field expression, a macro or `@every`, and reads the extended day items too. [`cron.DescribeSeconds`](ds:block?id=cron-describeseconds-vjkqt6sr) does the same for the six-field [seconds dialect](cron.md#a-seconds-field).

<!-- snippet: examples/schedule-tools/main.go#describe -->
```go
text, err := cron.Describe(expr) // "0 9 * * MON-FRI" is "At 09:00, Monday through Friday"
if err != nil {
	return err
}
```

When both day fields are restricted the [description says "or"](ds:block?id=cron-describedays-mn5tyxmv), because cron fires on either: `0 0 1 * MON` is "At 00:00, on day 1 of the month or on Monday". Times are the schedule's wall clock, 24-hour, with no zone named, since the expression carries none; show the schedule's `Location` beside it.

## Try it

```sh
cd examples
go run ./schedule-tools
```

The first three sections of its output:

```text
extended:
  0 9 L * *      [Sat Oct 31 09:00 Mon Nov 30 09:00 Thu Dec 31 09:00]
  0 9 15W * *    [Thu Oct 15 09:00 Mon Nov 16 09:00 Tue Dec 15 09:00]
  0 18 * * FRIL  [Fri Oct 30 18:00 Fri Nov 27 18:00 Fri Dec 25 18:00]
  0 9 * * MON#1  [Mon Nov 2 09:00 Mon Dec 7 09:00 Mon Jan 4 09:00]
  memory with cron.Extended fired for Oct 31 09:00; the default parser refuses L: true
hashed:
  acme     42 3 * * *
  globex   7 4 * * *
  initech  16 1 * * *
describe:
  0 9 * * MON-FRI  At 09:00, Monday through Friday
  */15 * * * *     Every 15 minutes
  5 * * * *        At minute 5 past every hour
  0 0 L * *        At 00:00, on the last day of the month
  30 8 * * MON#1   At 08:30, on the first Monday of the month
  @every 1h30m     Every 1 hour and 30 minutes
  */10 * * * * *   Every 10 seconds
```

## Pitfalls

- **The extended dialect is per driver, not per schedule.** A stored schedule with `L` fires only on clients built with `cron.Extended`; on the SQL and Redis drivers every client of one database must use the same parser, since each one fires stored schedules. A client with the default parser [refuses the expression at `Schedule`](ds:block?id=memory-client-schedule-vwvtn5w4).
- **A bare `L` in day of week is [refused](ds:block?id=cron-splitdow-9sq56utt).** Some dialects read it as Saturday; write `SAT` or `6`, or `dL` for the last such weekday.
- **`nW` [never leaves the month](ds:block?id=cron-nearestweekday-fgn3shjq).** `1W` on a Saturday the 1st fires on Monday the 3rd, not on Friday of the month before; `31W` on a Sunday fires on Friday the 29th.
- **Hash with a stable seed.** A seed that changes between deploys (a random value, a pod name) [moves the schedule](ds:block?id=cron-hashfield-fy2xgcma) on every deploy. Use the schedule's or the tenant's name.
- **`H` in a step can still collide.** `H/15` gives one of 15 offsets; with more schedules than offsets some share one, as they would by hand.
- **[Describe](ds:block?id=cron-describe-e5rw59ey) names no zone.** "At 09:00" is 09:00 on the schedule's `Location` wall clock.

## Related

- [Cron schedules](cron.md) (the standard syntax, other dialects, the seconds field)
- [Backfill and schedule lifetimes](backfill.md) (backfill, limited and one-shot schedules, startup runs, calendars, jitter)
- [Time zones and DST](time-zones.md)
- Example: [`examples/schedule-tools`](../../examples/schedule-tools)
