package workflow

import "github.com/ubgo/workflow/cron"

// ParseCron parses a schedule's expression the way every driver must:
// seconds true (Schedule.Seconds, Workflow.CronSeconds) reads six fields
// with cron.ParseSeconds; otherwise custom parses it when set (a driver's
// WithCronParser dialect), else cron.Parse. The seconds flag wins over a
// custom dialect because it is part of the schedule's stored meaning,
// while a custom parser is one process's choice.
func ParseCron(expr string, seconds bool, custom cron.Parser) (cron.Schedule, error) {
	switch {
	case seconds:
		return cron.ParseSeconds(expr)
	case custom != nil:
		return custom(expr)
	}
	return cron.Parse(expr)
}
