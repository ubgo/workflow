package workflow

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/ubgo/workflow/clock"
)

// ErrPollTimeout matches the error Poll ends with when its condition did
// not hold before PollSpec.Timeout; it also matches ErrTimeout.
//
//ds:def id=errpolltimeout-2mzgb5sv owner=@khanakia stability=stable
var ErrPollTimeout = fmt.Errorf("%w: poll", ErrTimeout)

// PollSpec says how often Poll looks and for how long.
//
//ds:def id=pollspec-rbwpje2v owner=@khanakia stability=stable
type PollSpec struct {
	// Every is the wait after the first look that found nothing; zero
	// means one second.
	Every time.Duration

	// Factor grows the wait after each empty look (1 or less keeps it
	// fixed); Max caps it (zero means no cap).
	Factor float64
	Max    time.Duration

	// Next, when set, replaces Every, Factor and Max: the wait after
	// empty look n (1 for the first).
	Next func(n int) time.Duration

	// Timeout ends the polling with ErrPollTimeout (Permanent) once this
	// long has passed since the first look; zero polls until the run's
	// own limits stop it.
	Timeout time.Duration

	// Clock is the time source; nil means clock.Real.
	Clock clock.Clock
}

// pollState is what Poll keeps in the journal between looks.
type pollState struct {
	Start time.Time `json:"start"`
	Looks int       `json:"looks"`
}

// Wait returns the wait after empty look n (1 for the first).
func (s PollSpec) Wait(n int) time.Duration {
	if s.Next != nil {
		return s.Next(n)
	}
	d := s.Every
	if d <= 0 {
		d = time.Second
	}
	for i := 1; i < n && s.Factor > 1; i++ {
		d = time.Duration(float64(d) * s.Factor)
		if s.Max > 0 && d >= s.Max {
			break
		}
	}
	if s.Max > 0 && d > s.Max {
		d = s.Max
	}
	return d
}

// Poll is a monitor loop inside a step (WP-06, WP-02, ET-04): it calls
// check, and while check reports not done it ends the attempt with a
// Snooze for the next wait, so no worker is held between looks and no
// retry is spent. When check reports done, Poll returns its value. A
// check error is returned as is (the step's retry policy applies). The
// start time and the number of looks live in the journal under name, so
// the interval grows and the timeout holds across looks, retries and
// process restarts (with a durable JournalStore).
//
// It needs the Journal plugin and a driver with CapSnooze (memory,
// sqlstore, Redis); on a driver without it the snooze spends an attempt.
// Use it for a sensor (wait until a file or partition exists), for an
// external job started by an earlier step (poll its status), or for any
// "until" condition.
//
//ds:def id=poll-48rh3xt3 owner=@khanakia stability=stable
func Poll[T any](ctx context.Context, name string, spec PollSpec, check func(ctx context.Context) (T, bool, error)) (T, error) {
	var zero T
	j, err := journalFrom(ctx)
	if err != nil {
		return zero, err
	}
	key, err := j.claim("poll/" + name)
	if err != nil {
		return zero, err
	}
	clk := spec.Clock
	if clk == nil {
		clk = clock.Real{}
	}
	now := clk.Now()
	var st pollState
	raw, ok, err := j.store.Load(ctx, j.run, key)
	if err != nil {
		return zero, fmt.Errorf("workflow: poll %q: load: %w", name, err)
	}
	if ok {
		if err := json.Unmarshal(raw, &st); err != nil {
			return zero, Permanent(fmt.Errorf("workflow: poll %q: stored state does not decode: %w", name, err))
		}
	} else {
		st.Start = now
	}
	v, done, err := check(ctx)
	if err != nil {
		return zero, err
	}
	if done {
		return v, nil
	}
	if spec.Timeout > 0 && now.Sub(st.Start) >= spec.Timeout {
		return zero, Permanent(fmt.Errorf("%w: %q gave up after %s and %s looks", ErrPollTimeout, name, spec.Timeout, strconv.Itoa(st.Looks+1)))
	}
	st.Looks++
	b, err := json.Marshal(st)
	if err != nil {
		return zero, err
	}
	if err := j.store.Save(ctx, j.run, key, b); err != nil {
		return zero, fmt.Errorf("workflow: poll %q: save: %w", name, err)
	}
	d := spec.Wait(st.Looks)
	if spec.Timeout > 0 {
		if left := st.Start.Add(spec.Timeout).Sub(now); d > left {
			d = left
		}
	}
	return zero, Snooze(d)
}

// SleepUntil is a durable sleep to an absolute time inside a step (DE-03):
// before t it returns a Snooze for the time left, so the step comes back
// at t holding no worker and spending no attempt; at or after t it
// returns nil. Return its error from the handler when it is not nil:
//
//	if err := workflow.SleepUntil(clk, renewal); err != nil {
//		return nil, err
//	}
//
// It needs CapSnooze (memory, sqlstore, Redis). A wait longer than
// MaxSnooze takes several snoozes; each comes back, finds t still ahead
// and snoozes again. Code above the call runs on every return, so keep
// the call at the top of the step or put earlier work in Memo.
//
//ds:def id=sleepuntil-6vncwvvy owner=@khanakia stability=stable
func SleepUntil(clk clock.Clock, t time.Time) error {
	if clk == nil {
		clk = clock.Real{}
	}
	if d := t.Sub(clk.Now()); d > 0 {
		return Snooze(d)
	}
	return nil
}
