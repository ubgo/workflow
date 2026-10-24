package workflow

import (
	"fmt"
	"time"
)

// Period is a calendar period for PeriodKey.
type Period string

// The periods. Week is the ISO 8601 week (Monday first), so the last days
// of December can belong to week 1 of the next year.
const (
	PeriodDay     Period = "day"
	PeriodWeek    Period = "week"
	PeriodMonth   Period = "month"
	PeriodQuarter Period = "quarter"
	PeriodYear    Period = "year"
)

// PeriodKey names the period of p that t falls in, on loc's wall clock:
// "2026-10-14", "2026-W42", "2026-10", "2026-Q4", "2026". Every instant
// of one period gets the same key, so a key built from it is stable
// across retries, nodes and late fires. loc is required and time.Local is
// refused, as for Schedule.Location: a period boundary depends on the
// zone, and the host's zone differs from machine to machine. Errors wrap
// ErrInvalid.
func PeriodKey(t time.Time, p Period, loc *time.Location) (string, error) {
	if loc == nil || loc == time.Local {
		return "", invalidf("period key needs an explicit location, not nil or time.Local")
	}
	t = t.In(loc)
	switch p {
	case PeriodDay:
		return t.Format(time.DateOnly), nil
	case PeriodWeek:
		y, w := t.ISOWeek()
		return fmt.Sprintf("%04d-W%02d", y, w), nil
	case PeriodMonth:
		return t.Format("2006-01"), nil
	case PeriodQuarter:
		return fmt.Sprintf("%04d-Q%d", t.Year(), (int(t.Month())-1)/3+1), nil
	case PeriodYear:
		return fmt.Sprintf("%04d", t.Year()), nil
	}
	return "", invalidf("unknown period %q", p)
}

// OncePerPeriod returns job with an IdempotencyKey of
// "period/<scope>/<PeriodKey>", for work that must happen once per scope
// per period (SA-02: charge customer 42 once a month): every enqueue of
// the same scope in the same period, from any node and any number of
// schedule fires, carries the same key. It needs CapIdempotencyKey. A job
// that already has a key is refused (ErrInvalid) rather than overwritten.
//
// What the key does and does not cover: a driver dedups on it while the
// run holding it is unfinished (Client.Enqueue). To keep a later enqueue
// in the same period from running again after that run succeeded, also
// pass the same key to the payment provider's own idempotency key (the
// period key alone, PeriodKey, is the stable value for that), and
// memoise the charge with Memo so a retried step does not charge twice.
func OncePerPeriod(job Job, scope string, p Period, t time.Time, loc *time.Location) (Job, error) {
	if scope == "" {
		return job, invalidf("once per period: scope is empty")
	}
	if job.IdempotencyKey != "" {
		return job, invalidf("once per period: job already has IdempotencyKey %q", job.IdempotencyKey)
	}
	k, err := PeriodKey(t, p, loc)
	if err != nil {
		return job, err
	}
	job.IdempotencyKey = "period/" + scope + "/" + k
	return job, nil
}
