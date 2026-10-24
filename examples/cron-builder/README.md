# cron-builder

Cron expressions from named frequencies: `cron.DailyAt("9:00").Weekdays()`, `cron.EveryMinutes(15)`, `cron.WeeklyOn(time.Friday, "17:30")` and the rest. The program prints each built expression with its next fire, the inputs the builder refuses, and a built expression driving a workflow's cron on the in-memory driver with a fake clock, so nothing needs to be installed and the output never varies.

```sh
cd examples
go run ./cron-builder
```

```text
frequencies:
  @every 30s         next Mon 5 Jan 00:00:30
  */15 * * * 1-5     next Mon 5 Jan 00:15:00
  17 * * * *         next Mon 5 Jan 00:17:00
  0 */6 * * *        next Mon 5 Jan 06:00:00
  0 9 * * 1-5        next Mon 5 Jan 09:00:00
  0 1,13 * * *       next Mon 5 Jan 01:00:00
  30 17 * * 5        next Fri 9 Jan 17:30:00
  0 8 15 * *         next Thu 15 Jan 08:00:00
  0 0 1 1,4,7,10 *   next Wed 1 Apr 00:00:00
  0 12 1 6 *         next Mon 1 Jun 12:00:00
refusals:
  ErrBuilder=true cron: invalid builder: EveryMinutes(7): n must divide 60 (1, 2, 3, 4, 5, 6, 10, 12, 15, 20, 30)
  ErrBuilder=true cron: invalid builder: At("13:00"): only a daily or coarser frequency has a time of day to set
  ErrBuilder=true cron: invalid builder: Days(): the frequency also fixes the day of the month, and cron would fire when either day matches
  ErrBuilder=true cron: invalid builder: YearlyOn(February, 30): day must be 1-29
  ErrBuilder=true cron: invalid builder: At("9:5"): want "HH:MM" on a 24-hour clock, e.g. "13:00" or "9:05"
on a workflow:
  Mon 5 Jan 09:30, Tue 6 Jan 09:30, Wed 7 Jan 09:30, Thu 8 Jan 09:30, Fri 9 Jan 09:30, Mon 12 Jan 09:30
```

## The code

Frequencies, each parsed and asked for its next fire:

<!-- snippet: examples/cron-builder/main.go#frequencies -->
```go
frequencies := []cron.Builder{
	cron.EverySeconds(30),
	cron.EveryMinutes(15).Weekdays(),
	cron.HourlyAt(17),
	cron.EveryHours(6),
	cron.DailyAt("9:00").Weekdays(),
	cron.TwiceDaily(1, 13),
	cron.WeeklyOn(time.Friday, "17:30"),
	cron.MonthlyOn(15, "8:00"),
	cron.Quarterly(),
	cron.YearlyOn(time.June, 1, "12:00"),
}
for _, f := range frequencies {
	s, err := f.Schedule() // the expression, parsed
	if err != nil {
		return err
	}
	next := s.Next(monday, time.UTC)
	fmt.Fprintf(out, "  %-18s next %s\n", s.String(), next.Format("Mon 2 Jan 15:04:05"))
}
```

What the builder refuses, and why:

<!-- snippet: examples/cron-builder/main.go#refusals -->
```go
for _, f := range []cron.Builder{
	cron.EveryMinutes(7),                     // */7 would fire at :56 and :00
	cron.Hourly().At("13:00"),                // At would drop the repetition
	cron.Monthly().Days(time.Monday),         // cron fires when EITHER day matches
	cron.YearlyOn(time.February, 30, "9:00"), // never fires
	cron.DailyAt("9:5"),                      // minutes take two digits
} {
	_, err := f.Expr()
	fmt.Fprintf(out, "  ErrBuilder=%t %v\n", errors.Is(err, cron.ErrBuilder), err)
}
```

A built expression as a workflow's cron:

<!-- snippet: examples/cron-builder/main.go#workflow -->
```go
err := c.Register(workflow.Workflow{
	Name:         "standup-reminder",
	Cron:         []string{cron.DailyAt("9:30").Weekdays().MustExpr()}, // "30 9 * * 1-5"
	CronLocation: time.UTC,
	Steps: []workflow.Step{{Name: "remind", Run: func(context.Context, []byte) ([]byte, error) {
		fired <- clk.Now()
		return nil, nil
	}}},
})
```

`main_test.go` runs the program and compares its whole output with the block above.

## Guides

- [Cron schedules: build an expression from a frequency](../../docs/schedules/cron.md#build-an-expression-from-a-frequency)
