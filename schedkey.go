package workflow

import (
	"strings"
	"time"
)

// HeldPolicy says what TriggerSchedule does when the idempotency key of a
// schedule (Schedule.IdempotencyKey) is held by an unfinished run. It is
// the choice UniqueConfig.ReturnExisting gives enqueue middleware: answer
// with the existing run, or refuse with ErrDuplicate.
type HeldPolicy string

// The held-key policies of a schedule's manual trigger.
const (
	// HeldReturnExisting returns the holder's RunRef and a nil error,
	// marking the EnqueueOutcome a duplicate, exactly as Enqueue answers a
	// held key. It is the default; the empty value means it too.
	HeldReturnExisting HeldPolicy = "return-existing"
	// HeldRefuse returns an error wrapping ErrDuplicate and starts
	// nothing, so an operator sees that the work is already running.
	HeldRefuse HeldPolicy = "refuse"
)

// Valid reports whether p is a known policy or empty.
func (p HeldPolicy) Valid() bool {
	return p == "" || p == HeldReturnExisting || p == HeldRefuse
}

// The placeholders of a schedule key template. Each is replaced by a
// value of the fire time: the tick's time for a cron fire, the driver's
// clock for TriggerSchedule.
const (
	// KeyFire is replaced by the fire time in UTC, RFC 3339 to the second
	// ("2026-11-28T09:00:00Z"): one key per tick.
	KeyFire = "{fire}"
	// KeyDay, KeyWeek, KeyMonth, KeyQuarter and KeyYear are replaced by
	// PeriodKey of the fire time on the schedule's Location ("2026-11-28",
	// "2026-W48", "2026-11", "2026-Q4", "2026"): one key per calendar
	// period, so a manual trigger on the same day as a daily tick carries
	// the tick's key.
	KeyDay     = "{day}"
	KeyWeek    = "{week}"
	KeyMonth   = "{month}"
	KeyQuarter = "{quarter}"
	KeyYear    = "{year}"
)

// keyPeriods maps each period placeholder to its Period.
var keyPeriods = map[string]Period{
	KeyDay: PeriodDay, KeyWeek: PeriodWeek, KeyMonth: PeriodMonth, KeyQuarter: PeriodQuarter, KeyYear: PeriodYear,
}

// ExpandScheduleKey returns the idempotency key template expands to for a
// fire at time fire, periods read on loc's wall clock. Text outside braces
// is kept as written, so a template without placeholders is a fixed key.
// An unknown or unclosed placeholder, an empty template and a nil or
// time.Local loc are refused with ErrInvalid. Drivers call it for every
// run a schedule fires; an app can call it to compute the key a fire
// carries (to enqueue the same work by hand under it).
func ExpandScheduleKey(template string, fire time.Time, loc *time.Location) (string, error) {
	if template == "" {
		return "", invalidf("schedule key template is empty")
	}
	if loc == nil || loc == time.Local {
		return "", invalidf("schedule key needs an explicit location, not nil or time.Local")
	}
	var b strings.Builder
	rest := template
	for {
		open := strings.IndexByte(rest, '{')
		if open < 0 {
			b.WriteString(rest)
			break
		}
		b.WriteString(rest[:open])
		end := strings.IndexByte(rest[open:], '}')
		if end < 0 {
			return "", invalidf("schedule key template %q: unclosed placeholder", template)
		}
		ph := rest[open : open+end+1]
		switch p, ok := keyPeriods[ph]; {
		case ph == KeyFire:
			b.WriteString(fire.UTC().Format(time.RFC3339))
		case ok:
			k, err := PeriodKey(fire, p, loc)
			if err != nil {
				return "", err
			}
			b.WriteString(k)
		default:
			return "", invalidf("schedule key template %q: unknown placeholder %s (use %s, %s, %s, %s, %s or %s)",
				template, ph, KeyFire, KeyDay, KeyWeek, KeyMonth, KeyQuarter, KeyYear)
		}
		rest = rest[open+end+1:]
	}
	return b.String(), nil
}

// validateScheduleKey checks a schedule's key template and held policy.
func validateScheduleKey(s Schedule) error {
	if !s.OnHeld.Valid() {
		return invalidf("unknown OnHeld %q", s.OnHeld)
	}
	if s.IdempotencyKey == "" {
		if s.OnHeld != "" {
			return invalidf("OnHeld is set without IdempotencyKey")
		}
		return nil
	}
	_, err := ExpandScheduleKey(s.IdempotencyKey, time.Unix(0, 0), time.UTC)
	return err
}
