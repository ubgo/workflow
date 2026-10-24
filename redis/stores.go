package redis

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/middleware"
)

// Stores for state that would otherwise live in one process: a step
// journal (workflow.JournalStore), leaky buckets (workflow.BucketStore)
// and debounce bursts (middleware.DebounceStore). Each keeps its keys
// under a prefix's hash tag, like the driver, and each atomic decision is
// one Lua script.

// storeBase checks rdb and prefix and returns the hash-tagged key base.
func storeBase(rdb goredis.UniversalClient, prefix string) (string, error) {
	if rdb == nil {
		return "", fmt.Errorf("%w: redis: nil Redis client", workflow.ErrInvalid)
	}
	if !prefixPattern.MatchString(prefix) {
		return "", fmt.Errorf("%w: redis: prefix %q must match %s", workflow.ErrInvalid, prefix, prefixPattern)
	}
	return "{" + prefix + "}", nil
}

// JournalStore is a workflow.JournalStore in Redis: one hash per run
// ({prefix}:journal:<run>, a field per memo key), so a step's memoised
// results survive a crash and are seen by whichever worker resumes the
// run. With a TTL each Save also sets the hash to expire that long (wall
// time, measured by Redis) after its last write, a safety net for runs
// whose Forget never comes; zero keeps entries until Forget.
type JournalStore struct {
	rdb  goredis.UniversalClient
	base string
	ttl  time.Duration
}

// NewJournalStore returns a JournalStore over rdb under prefix, whose
// entries expire ttl after a run's last Save (zero: never).
func NewJournalStore(rdb goredis.UniversalClient, prefix string, ttl time.Duration) (*JournalStore, error) {
	base, err := storeBase(rdb, prefix)
	if err != nil {
		return nil, err
	}
	if ttl < 0 {
		return nil, fmt.Errorf("%w: redis: journal ttl %s is negative", workflow.ErrInvalid, ttl)
	}
	return &JournalStore{rdb: rdb, base: base, ttl: ttl}, nil
}

func (j *JournalStore) key(run workflow.RunID) string { return j.base + ":journal:" + string(run) }

// Load returns the value saved under key for run.
func (j *JournalStore) Load(ctx context.Context, run workflow.RunID, key string) ([]byte, bool, error) {
	v, err := j.rdb.HGet(ctx, j.key(run), key).Bytes()
	if err == goredis.Nil {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("redis: journal load: %w", classify(err))
	}
	return v, true, nil
}

// Save stores val under key for run, replacing what was there.
func (j *JournalStore) Save(ctx context.Context, run workflow.RunID, key string, val []byte) error {
	pipe := j.rdb.TxPipeline()
	pipe.HSet(ctx, j.key(run), key, val)
	if j.ttl > 0 {
		pipe.PExpire(ctx, j.key(run), j.ttl)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("redis: journal save: %w", classify(err))
	}
	return nil
}

// Forget removes every entry of run.
func (j *JournalStore) Forget(ctx context.Context, run workflow.RunID) error {
	if err := j.rdb.Del(ctx, j.key(run)).Err(); err != nil {
		return fmt.Errorf("redis: journal forget: %w", classify(err))
	}
	return nil
}

// BucketStore is a workflow.BucketStore in Redis: one hash per bucket key
// ({prefix}:bucket:<key>: its level and when it was last set), so every
// worker calling one upstream draws from one bucket. Take drains the
// level by the leak since then and admits or refuses the cost in one
// script, from the now the caller passes (never Redis's clock), so a fake
// clock drives it. Times are stored in microseconds, which a Lua number
// holds exactly. An idle bucket's key expires once it would have drained
// to empty, plus a minute.
type BucketStore struct {
	rdb  goredis.UniversalClient
	base string
}

// NewBucketStore returns a BucketStore over rdb under prefix.
func NewBucketStore(rdb goredis.UniversalClient, prefix string) (*BucketStore, error) {
	base, err := storeBase(rdb, prefix)
	if err != nil {
		return nil, err
	}
	return &BucketStore{rdb: rdb, base: base}, nil
}

// takeScript is MemoryBuckets.Take in one step. KEYS bucket; ARGV
// capacity, leak per second, cost, now (us), expiry (ms), mode ('take' or
// 'set': set the level to cost). Returns the wait in ns (0: admitted).
var takeScript = goredis.NewScript(`
local cap, leak, cost, now = tonumber(ARGV[1]), tonumber(ARGV[2]), tonumber(ARGV[3]), tonumber(ARGV[4])
local h = redis.call('HMGET', KEYS[1], 'level', 'at')
local level = tonumber(h[1] or '0') or 0
local at = tonumber(h[2] or '0') or 0
if ARGV[6] == 'set' then
  level = math.min(math.max(cost, 0), cap)
else
  if h[1] and now > at then level = math.max(level - (now - at) / 1e6 * leak, 0) end
  local over = level + cost - cap
  if over > 0 then
    redis.call('HSET', KEYS[1], 'level', string.format('%.17g', level), 'at', string.format('%d', now))
    redis.call('PEXPIRE', KEYS[1], ARGV[5])
    return math.ceil(over / leak * 1e9)
  end
  level = level + cost
end
redis.call('HSET', KEYS[1], 'level', string.format('%.17g', level), 'at', string.format('%d', now))
redis.call('PEXPIRE', KEYS[1], ARGV[5])
return 0
`)

func (b *BucketStore) run(ctx context.Context, key string, spec workflow.BucketSpec, v float64, now time.Time, mode string) (int64, error) {
	if !(spec.Capacity > 0) || !(spec.LeakPerSecond > 0) || math.IsInf(spec.Capacity, 0) || math.IsInf(spec.LeakPerSecond, 0) {
		return 0, fmt.Errorf("%w: redis: bucket needs a positive, finite Capacity and LeakPerSecond", workflow.ErrInvalid)
	}
	drain := time.Duration((spec.Capacity + math.Max(v, 0)) / spec.LeakPerSecond * float64(time.Second))
	expiry := (drain + time.Minute).Milliseconds()
	f := func(x float64) string { return strconv.FormatFloat(x, 'g', -1, 64) }
	ns, err := takeScript.Run(ctx, b.rdb, []string{b.base + ":bucket:" + key},
		f(spec.Capacity), f(spec.LeakPerSecond), f(v), now.UnixMicro(), expiry, mode).Int64()
	if err != nil {
		return 0, fmt.Errorf("redis: bucket: %w", classify(err))
	}
	return ns, nil
}

// Take adds cost to key's level and returns zero when it fits; otherwise
// it adds nothing and returns how long until it would fit.
func (b *BucketStore) Take(ctx context.Context, key string, spec workflow.BucketSpec, cost float64, now time.Time) (time.Duration, error) {
	ns, err := b.run(ctx, key, spec, cost, now, "take")
	return time.Duration(ns), err
}

// Observe sets key's level to what the upstream reported, clamped to
// [0, Capacity].
func (b *BucketStore) Observe(ctx context.Context, key string, spec workflow.BucketSpec, level float64, now time.Time) error {
	_, err := b.run(ctx, key, spec, level, now, "set")
	return err
}

// DebounceStore is a middleware.DebounceStore in Redis: one hash per
// debounce key ({prefix}:debounce:<key>: the burst's first enqueue, its
// run time and its pending run), so a key enqueued from several processes
// is debounced once. Swap is one script, so of two processes swapping at
// once each gets back a different previous burst. A burst's key expires
// an hour of wall time after its RunAt (at least an hour after the swap),
// when its run has long started and nothing can cancel it any more.
type DebounceStore struct {
	rdb  goredis.UniversalClient
	base string
}

// NewDebounceStore returns a DebounceStore over rdb under prefix.
func NewDebounceStore(rdb goredis.UniversalClient, prefix string) (*DebounceStore, error) {
	base, err := storeBase(rdb, prefix)
	if err != nil {
		return nil, err
	}
	return &DebounceStore{rdb: rdb, base: base}, nil
}

// debounceKeep is how long a burst outlives its RunAt.
const debounceKeep = time.Hour

func (d *DebounceStore) key(k string) string { return d.base + ":debounce:" + k }

// swapScript replaces a burst and returns the one it replaced. KEYS burst;
// ARGV first (ns), run at (ns), run id, scheduled id, expiry (ms). Returns
// {} or {first, run at, run id, scheduled id}.
var swapScript = goredis.NewScript(`
local old = redis.call('HMGET', KEYS[1], 'first', 'at', 'rid', 'sid')
redis.call('HSET', KEYS[1], 'first', ARGV[1], 'at', ARGV[2], 'rid', ARGV[3], 'sid', ARGV[4])
redis.call('PEXPIRE', KEYS[1], ARGV[5])
if not old[1] then return {} end
return old
`)

func burstOf(first, at, rid, sid string) middleware.DebounceBurst {
	return middleware.DebounceBurst{First: nanos(first), RunAt: nanos(at),
		Ref: workflow.RunRef{RunID: workflow.RunID(rid), ScheduledID: workflow.ScheduledID(sid)}}
}

// Get returns key's burst, or found false when there is none.
func (d *DebounceStore) Get(ctx context.Context, key string) (middleware.DebounceBurst, bool, error) {
	v, err := d.rdb.HMGet(ctx, d.key(key), "first", "at", "rid", "sid").Result()
	if err != nil {
		return middleware.DebounceBurst{}, false, fmt.Errorf("redis: debounce get: %w", classify(err))
	}
	if v[0] == nil {
		return middleware.DebounceBurst{}, false, nil
	}
	return burstOf(str(v[0]), str(v[1]), str(v[2]), str(v[3])), true, nil
}

// Swap stores b as key's burst and returns the burst it replaced.
func (d *DebounceStore) Swap(ctx context.Context, key string, b middleware.DebounceBurst) (middleware.DebounceBurst, bool, error) {
	ttl := max(time.Until(b.RunAt), 0) + debounceKeep
	ns := func(t time.Time) int64 {
		if t.IsZero() {
			return 0
		}
		return t.UnixNano()
	}
	res, err := swapScript.Run(ctx, d.rdb, []string{d.key(key)}, ns(b.First), ns(b.RunAt),
		string(b.Ref.RunID), string(b.Ref.ScheduledID), ttl.Milliseconds()).Slice()
	if err != nil {
		return middleware.DebounceBurst{}, false, fmt.Errorf("redis: debounce swap: %w", classify(err))
	}
	if len(res) != 4 {
		return middleware.DebounceBurst{}, false, nil
	}
	return burstOf(str(res[0]), str(res[1]), str(res[2]), str(res[3])), true, nil
}

// JournalStore returns a JournalStore over this client's Redis and prefix
// with no expiry.
func (c *Client) JournalStore() *JournalStore { return &JournalStore{rdb: c.rdb, base: c.base} }

// BucketStore returns a BucketStore over this client's Redis and prefix.
func (c *Client) BucketStore() *BucketStore { return &BucketStore{rdb: c.rdb, base: c.base} }

// DebounceStore returns a DebounceStore over this client's Redis and
// prefix.
func (c *Client) DebounceStore() *DebounceStore { return &DebounceStore{rdb: c.rdb, base: c.base} }

var (
	_ workflow.JournalStore    = (*JournalStore)(nil)
	_ workflow.BucketStore     = (*BucketStore)(nil)
	_ middleware.DebounceStore = (*DebounceStore)(nil)
)
