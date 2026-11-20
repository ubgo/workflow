package workflow

import (
	"slices"
	"time"
)

// RetryLadder returns a RetryPolicy.DelayFor that waits an explicit list
// of delays instead of an exponential curve: delays[0] before the first
// retry, delays[1] before the second, and the last delay again for every
// retry past the end of the list, so a policy whose Max is larger than
// the ladder keeps retrying at the last step. It is the schedule webhook
// senders publish ("after 5s, 5m, 30m, 2h, 5h, 10h, then every 10h").
//
// The delay is the same whatever the error; a negative delay is zero. An
// empty ladder answers false, so the policy's Backoff applies. The list
// is copied, so changing the caller's slice later changes nothing.
//
// It needs CapRetryDelay (memory, sqlstore and Redis declare it); a
// driver without it (Hatchet) refuses a policy with DelayFor set, never
// ignoring it. LadderPolicy builds the whole policy.
func RetryLadder(delays ...time.Duration) func(err error, retry int) (time.Duration, bool) {
	ladder := slices.Clone(delays)
	return func(_ error, retry int) (time.Duration, bool) {
		if len(ladder) == 0 {
			return 0, false
		}
		i := min(max(retry, 1), len(ladder)) - 1
		return max(ladder[i], 0), true
	}
}

// LadderPolicy is a RetryPolicy that retries once per ladder step: Max is
// len(delays) and DelayFor is RetryLadder(delays...), so a step runs at
// most len(delays)+1 times and the last retry waits the last delay. Set
// Max higher on the returned policy to keep retrying at the last delay;
// the other fields (NoRetryOn, Until, MaxErrors, DiscardOnExhaust) are
// zero and may be set too. Needs CapRetryDelay, like RetryLadder.
func LadderPolicy(delays ...time.Duration) RetryPolicy {
	return RetryPolicy{Max: len(delays), DelayFor: RetryLadder(delays...)}
}
