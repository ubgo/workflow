package workflow

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/ubgo/workflow/clock"
)

// ErrThrottled means an upstream's rate limit has no room for the call
// now. UpstreamLimit.Take returns it marked with RetryAfter, so a step
// that returns it is retried once the bucket has room on a workflow whose
// RetryPolicy.DelayFor is RetryAfterDelay, and on its normal backoff
// otherwise.
var ErrThrottled = errors.New("workflow: upstream rate limit reached")

// BucketSpec is a leaky bucket: it holds at most Capacity units of cost
// and drains LeakPerSecond units every second. A call of cost c fits when
// the level plus c is at most Capacity. Shopify's REST limit is Capacity
// 40, LeakPerSecond 2 with cost 1 per call; its GraphQL limit is cost
// based (Capacity 1000, LeakPerSecond 50, cost per query).
type BucketSpec struct {
	Capacity      float64
	LeakPerSecond float64
}

func (s BucketSpec) validate() error {
	if !(s.Capacity > 0) || !(s.LeakPerSecond > 0) || math.IsInf(s.Capacity, 0) || math.IsInf(s.LeakPerSecond, 0) {
		return invalidf("bucket needs a positive, finite Capacity and LeakPerSecond (got %v, %v)", s.Capacity, s.LeakPerSecond)
	}
	return nil
}

// BucketStore holds bucket levels per key (one key per credential or
// shop). MemoryBuckets keeps them in this process; a shared store (Redis,
// SQL) makes several workers draw from one bucket, which is what an
// upstream limit per credential needs once more than one process calls
// it. Implementations apply the leak themselves, from the now they are
// given, and must be safe for concurrent use; Take must check and add in
// one atomic step.
type BucketStore interface {
	// Take adds cost to key's level and returns zero when it fits;
	// otherwise it adds nothing and returns how long until it would fit.
	Take(ctx context.Context, key string, spec BucketSpec, cost float64, now time.Time) (wait time.Duration, err error)

	// Observe sets key's level to what the upstream reported (Shopify's
	// "32/40" call-limit header, a GraphQL throttle status), so the
	// bucket follows the upstream's own count, including calls made by
	// other apps on the same credential.
	Observe(ctx context.Context, key string, spec BucketSpec, level float64, now time.Time) error
}

// MemoryBuckets is an in-process BucketStore. The zero value is ready to
// use.
type MemoryBuckets struct {
	mu      sync.Mutex
	buckets map[string]bucketLevel
}

type bucketLevel struct {
	level float64
	at    time.Time
}

// drained returns b's level at now after leaking since b.at.
func (b bucketLevel) drained(spec BucketSpec, now time.Time) float64 {
	if elapsed := now.Sub(b.at).Seconds(); elapsed > 0 {
		return max(b.level-elapsed*spec.LeakPerSecond, 0)
	}
	return b.level
}

// Take implements BucketStore.
func (m *MemoryBuckets) Take(_ context.Context, key string, spec BucketSpec, cost float64, now time.Time) (time.Duration, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.buckets == nil {
		m.buckets = map[string]bucketLevel{}
	}
	level := m.buckets[key].drained(spec, now)
	if over := level + cost - spec.Capacity; over > 0 {
		m.buckets[key] = bucketLevel{level: level, at: now}
		return time.Duration(math.Ceil(over / spec.LeakPerSecond * float64(time.Second))), nil
	}
	m.buckets[key] = bucketLevel{level: level + cost, at: now}
	return 0, nil
}

// Observe implements BucketStore.
func (m *MemoryBuckets) Observe(_ context.Context, key string, spec BucketSpec, level float64, now time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.buckets == nil {
		m.buckets = map[string]bucketLevel{}
	}
	m.buckets[key] = bucketLevel{level: min(max(level, 0), spec.Capacity), at: now}
	return nil
}

// UpstreamLimit is a leaky-bucket, cost-based rate limit on calls a step
// makes to an outside API (PS-02), one bucket per key. Unlike
// Workflow.RateLimits, which counts run starts, it counts what the
// upstream counts: the cost of each call, against a bucket that drains
// over time. Build it with NewUpstreamLimit.
type UpstreamLimit struct {
	store BucketStore
	spec  BucketSpec
	clk   clock.Clock
}

// NewUpstreamLimit returns a limit over store with spec, reading time
// from clk. A nil store means a new MemoryBuckets; a nil clk means
// clock.Real. It returns an error wrapping ErrInvalid when spec is not
// positive and finite.
func NewUpstreamLimit(store BucketStore, spec BucketSpec, clk clock.Clock) (*UpstreamLimit, error) {
	if err := spec.validate(); err != nil {
		return nil, err
	}
	if store == nil {
		store = &MemoryBuckets{}
	}
	if clk == nil {
		clk = clock.Real{}
	}
	return &UpstreamLimit{store: store, spec: spec, clk: clk}, nil
}

// Take reserves cost units in key's bucket. It returns nil when they fit;
// otherwise RetryAfter(ErrThrottled, wait), which a step returns as it is
// so the attempt is retried once the bucket has room (see ErrThrottled).
// A cost above the bucket's capacity can never fit and is a Permanent
// error wrapping ErrInvalid; a negative cost is refused the same way.
func (u *UpstreamLimit) Take(ctx context.Context, key string, cost float64) error {
	wait, err := u.Wait(ctx, key, cost)
	if err != nil {
		return err
	}
	if wait > 0 {
		return RetryAfter(fmt.Errorf("%w: key %q, room in %s", ErrThrottled, key, wait), wait)
	}
	return nil
}

// TakeOrSnooze is Take for a driver with CapSnooze: when the bucket has
// no room it returns Snooze(wait), which puts the step back without
// spending an attempt, since a spent upstream limit is "not yet", not a
// failure. On a driver without CapSnooze a snooze is an ordinary error and
// spends an attempt; use Take there.
func (u *UpstreamLimit) TakeOrSnooze(ctx context.Context, key string, cost float64) error {
	wait, err := u.Wait(ctx, key, cost)
	if err != nil {
		return err
	}
	if wait > 0 {
		return Snooze(wait)
	}
	return nil
}

// Wait is Take without the error: it reserves cost and returns zero, or
// reserves nothing and returns how long until cost fits, for a caller
// that would rather sleep in the step than retry it.
func (u *UpstreamLimit) Wait(ctx context.Context, key string, cost float64) (time.Duration, error) {
	if cost < 0 || cost > u.spec.Capacity || math.IsNaN(cost) {
		return 0, Permanent(invalidf("upstream limit: cost %v is outside 0..%v", cost, u.spec.Capacity))
	}
	return u.store.Take(ctx, key, u.spec, cost, u.clk.Now())
}

// Observe records the level the upstream reported for key, so the next
// Take decides from the upstream's count. Call it with each response's
// rate-limit header.
func (u *UpstreamLimit) Observe(ctx context.Context, key string, level float64) error {
	return u.store.Observe(ctx, key, u.spec, level, u.clk.Now())
}
