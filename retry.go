package workflow

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"time"
)

// DefaultRetryMax is the number of retries a step gets when no policy at
// any level (Job.Retry, Step.Retry, Workflow.Retry) says otherwise: none.
// It is the last link of the precedence chain EffectiveRetriesFor resolves,
// and the Max of the zero RetryPolicy.
const DefaultRetryMax = 0

// RetryPolicy says how often and how soon a failed step is tried again.
// Its zero value means no retries (Max is DefaultRetryMax).
//
// A policy reaches a step from four places; the first that is set wins and
// the others are ignored whole (policies are never merged field by field):
//
//  1. Job.Retry, chosen per dispatch, for every step of that run;
//  2. Step.Retry (RetriesNone() means never retry);
//  3. Workflow.Retry, the default for steps whose Retry is nil;
//  4. the zero policy, DefaultRetryMax retries.
//
// EffectiveRetriesFor is the one function that applies this order, and
// every driver calls it, so no two drivers can resolve a step's policy
// differently.
type RetryPolicy struct {
	// Max is the number of retries after the first attempt, so a step runs
	// at most Max+1 times. The value that applies is the one of the policy
	// EffectiveRetriesFor resolves: job over step over workflow over
	// DefaultRetryMax.
	Max int

	// Backoff is the wait between attempts. Zero means retry at once.
	Backoff Backoff

	// NoRetryOn marks errors that must not be retried. Returning
	// Permanent(err) from the handler does the same without a policy.
	NoRetryOn func(error) bool

	// DelayFor sets the wait before a retry from the error itself, such
	// as a 429's Retry-After: it returns the delay and true, or false to
	// fall back to Backoff. retry is 1 for the first retry. The delay is
	// used as returned (no jitter; a negative one is zero). Nil means
	// Backoff alone. RetryAfterDelay is the stock implementation, reading
	// errors wrapped by RetryAfter. Needs CapRetryDelay.
	DelayFor func(err error, retry int) (time.Duration, bool)

	// Until stops retrying once a retry would start more than Until after
	// the step's first attempt started. It wins
	// over Max: a step with Max 100 and Until one hour stops at the hour.
	// Zero means no deadline. Needs CapRetryUntil.
	Until time.Duration

	// MaxErrors fails the step for good on its MaxErrors-th error, even
	// when Max allows more attempts. A release
	// (an error marked by RetryAfter, see IsRelease) counts against Max
	// only; any other error counts against both. Zero means no cap. Needs
	// CapRetryMaxErrors.
	MaxErrors int

	// DiscardOnExhaust ends the run StatusDiscarded (Err matching
	// ErrDiscarded and ErrCancelled, no OnFailure, no chain) instead of
	// StatusFailed when this policy stops retrying a failed attempt:
	// attempts ran out (Max), MaxErrors or Until stopped it, NoRetryOn
	// excluded the error, or the handler returned Permanent. Use it for
	// work whose giving up is routine (a notification nobody needs after
	// five tries) so failed stays a signal that needs a person. The default
	// (false) keeps the run failed. An explicit Discard and ErrCancelled are
	// not affected. Needs CapDiscardExhausted, which also needs
	// CapRunStates to report the status.
	DiscardOnExhaust bool
}

// RetriesNone returns a policy that never retries, for a step that must
// not inherit the workflow default. It returns a fresh value each call so
// no caller can change it for another.
func RetriesNone() *RetryPolicy { return &RetryPolicy{Max: DefaultRetryMax} }

// Retryable reports whether err, after the given number of attempts (1
// for the first), may be tried again: not permanent, not discarded, not
// excluded by NoRetryOn, and within Max. It ignores Until and MaxErrors,
// which need more than an attempt count; drivers call Next.
func (p RetryPolicy) Retryable(err error, attempts int) bool {
	if err == nil || IsPermanent(err) || IsDiscarded(err) || errors.Is(err, ErrCancelled) {
		return false
	}
	if p.NoRetryOn != nil && p.NoRetryOn(err) {
		return false
	}
	return attempts <= p.Max
}

// NextDelay is the wait before retry number retry (1 for the first) after
// err: DelayFor's answer when it gives one, else the Backoff delay with
// its jitter drawn from rnd, a source of uniform values in [0, 1) (nil
// means math/rand/v2). Every driver that retries calls it, so no two
// drivers can disagree on the rule.
func (p RetryPolicy) NextDelay(err error, retry int, rnd func() float64) time.Duration {
	if p.DelayFor != nil {
		if d, ok := p.DelayFor(err, retry); ok {
			return max(d, 0)
		}
	}
	if p.Backoff.Jitter <= 0 {
		return p.Backoff.Delay(retry)
	}
	if rnd == nil {
		rnd = rand.Float64
	}
	return p.Backoff.JitteredDelay(retry, rnd())
}

// Attempts is what a driver knows about one step's attempts when one of
// them fails.
type Attempts struct {
	// N is the number of attempts made, including the one that just
	// failed: 1 after the first.
	N int

	// Errors is how many of those attempts failed with an error that was
	// not a release (IsRelease false), including the one that just failed
	// when it is such an error.
	Errors int

	// First is when the step's first attempt started, on the driver's
	// clock. Zero disables Until.
	First time.Time
}

// Count returns a after one more failed attempt with err: N grows by one,
// and Errors too unless err is a release. Drivers keep an Attempts per
// step and call it on every failure, so no two drivers count differently.
func (a Attempts) Count(err error) Attempts {
	a.N++
	if !IsRelease(err) {
		a.Errors++
	}
	return a
}

// IsRelease reports whether err carries a RetryAfter delay: a release,
// which counts against RetryPolicy.Max but not against
// MaxErrors.
func IsRelease(err error) bool {
	var r *retryAfterError
	return errors.As(err, &r)
}

// Next is the one retry decision every driver makes when a step attempt
// fails with err: whether to try again and, if so, after how long. It
// applies, in order:
//
//   - Retryable(err, a.N): nil, Permanent, Discard, ErrCancelled and
//     NoRetryOn stop it, and so does a.N beyond Max;
//   - MaxErrors: a.Errors reaching it stops it;
//   - the delay, NextDelay(err, a.N, rnd);
//   - Until: a retry starting after a.First + Until (now + delay) stops
//     it.
//
// now is the driver's clock. It is a pure function of its arguments
// (with a pinned rnd), so a test can pin every branch.
func (p RetryPolicy) Next(err error, a Attempts, now time.Time, rnd func() float64) (delay time.Duration, retry bool) {
	if !p.Retryable(err, a.N) {
		return 0, false
	}
	if p.MaxErrors > 0 && a.Errors >= p.MaxErrors {
		return 0, false
	}
	delay = p.NextDelay(err, a.N, rnd)
	if p.Until > 0 && !a.First.IsZero() && now.Add(delay).After(a.First.Add(p.Until)) {
		return 0, false
	}
	return delay, true
}

// Backoff is an exponential wait: Initial, then times Factor per attempt,
// capped at Max, optionally shortened by a random Jitter.
type Backoff struct {
	Initial time.Duration
	Factor  float64 // below 1 is treated as 1 (constant backoff)
	Max     time.Duration

	// Jitter spreads retries that failed together (an outage failing
	// thousands of runs at once) so they do not all retry at the same
	// instant: a delay d becomes a uniform value in [d*(1-Jitter), d].
	// It is a fraction in [0, 1]; zero means no jitter (the exact delay,
	// as before). Needs CapRetryJitter.
	Jitter float64
}

// Delay returns the wait before retry number n (1 for the first retry).
func (b Backoff) Delay(n int) time.Duration {
	if b.Initial <= 0 || n < 1 {
		return 0
	}
	f := b.Factor
	if f < 1 {
		f = 1
	}
	d := float64(b.Initial)
	for i := 1; i < n; i++ {
		d *= f
		if b.Max > 0 && d >= float64(b.Max) {
			return b.Max
		}
	}
	if b.Max > 0 && d > float64(b.Max) {
		return b.Max
	}
	return time.Duration(d)
}

// JitteredDelay is Delay(n) shortened by Jitter for the uniform draw u in
// [0, 1): u = 0 gives the full delay, u near 1 the shortest. The draw is
// an argument, not read here, so the result is a pure function a test can
// pin.
func (b Backoff) JitteredDelay(n int, u float64) time.Duration {
	d := b.Delay(n)
	j := min(max(b.Jitter, 0), 1)
	u = min(max(u, 0), 1)
	return d - time.Duration(float64(d)*j*u)
}

// retryAfterError carries the delay a failed attempt asked for.
type retryAfterError struct {
	err error
	d   time.Duration
}

func (r *retryAfterError) Error() string { return r.err.Error() }
func (r *retryAfterError) Unwrap() error { return r.err }

// RetryAfter marks err with the wait the next retry should observe, such
// as the Retry-After of a 429. It is honoured only under a policy whose
// DelayFor reads it (RetryAfterDelay); under any other policy the error
// retries on the normal Backoff, on every driver. Permanent still wins:
// RetryAfter(Permanent(err), d) is not retried. RetryAfter(nil, d) is nil
// and a negative d counts as zero.
func RetryAfter(err error, d time.Duration) error {
	if err == nil {
		return nil
	}
	return &retryAfterError{err: err, d: max(d, 0)}
}

// RetryAfterDelay is the stock RetryPolicy.DelayFor: the delay of the
// outermost RetryAfter in err's chain, or false when there is none.
func RetryAfterDelay(err error, _ int) (time.Duration, bool) {
	var r *retryAfterError
	if errors.As(err, &r) {
		return r.d, true
	}
	return 0, false
}

// EffectiveRetries resolves the policy a driver must apply to step inside
// wf when no per-dispatch override exists: the step's own Retry when set,
// else the workflow default. It is EffectiveRetriesFor with an empty Job,
// for callers that decide at declaration time (a driver that registers
// retries with an engine); a driver that sees runs calls
// EffectiveRetriesFor.
func EffectiveRetries(step Step, wf Workflow) RetryPolicy {
	return EffectiveRetriesFor(step, wf, Job{})
}

// EffectiveRetriesFor is the one place the retry precedence lives, job over
// step over workflow over default: job.Retry when set (a per-dispatch
// override wins over every declared policy), else step.Retry, else
// wf.Retry, whose zero value is the default of DefaultRetryMax retries.
// The winning policy is returned whole; nothing is merged. Every driver
// that retries (memory, sqlstore, redis, RunInline) calls it with the
// run's own Job, so a driver that ignored Job.Retry on a step would be a
// bug the conformance suite catches.
func EffectiveRetriesFor(step Step, wf Workflow, job Job) RetryPolicy {
	switch {
	case job.Retry != nil:
		return *job.Retry
	case step.Retry != nil:
		return *step.Retry
	}
	return wf.Retry
}

// ExhaustedError is what a driver ends a run with when policy stopped
// retrying a step's failed attempt number attempts with err and
// policy.DiscardOnExhaust is set: the returned error is a
// Discard of err (IsDiscarded), so EndStatus maps it to StatusDiscarded and
// DiscardedError builds the run's Err from it. With the option off, or when
// err already is a discard or a cancellation (those end the run their own
// way), it returns nil and the driver fails the run as before. Every driver
// calls it at the point Next says stop, so none decides differently.
func (p RetryPolicy) ExhaustedError(attempts int, err error) error {
	if !p.DiscardOnExhaust || err == nil || IsDiscarded(err) || errors.Is(err, ErrCancelled) {
		return nil
	}
	return Discard(fmt.Errorf("gave up after %d attempt(s): %w", attempts, err))
}
