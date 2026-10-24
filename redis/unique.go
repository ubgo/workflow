package redis

import (
	"context"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/ubgo/workflow"
)

// UniqueStore keeps the locks of the middleware package's Unique,
// UniqueUntilFinished and UniqueWith in Redis, so every process that
// enqueues shares them: it implements middleware.UniqueStore and
// middleware.UniqueHolders (the holder's run, for UniqueConfig.States and
// ReturnExisting). Each lock is the hash {prefix}:uniq:<key> (its owner
// and run) with a Redis expiry of its ttl, so a lock's ttl is wall time
// measured by Redis, and a lock nobody releases disappears on its own.
// Acquire, Release and SetHolder are each one script, so two processes
// racing on one key get one lock.
type UniqueStore struct {
	rdb  goredis.UniversalClient
	base string
}

// NewUniqueStore returns a UniqueStore over rdb whose keys start with
// prefix (WithPrefix's rules; a Client of the same prefix shares the
// hash tag, so on a Redis Cluster the locks live beside its runs).
func NewUniqueStore(rdb goredis.UniversalClient, prefix string) (*UniqueStore, error) {
	if rdb == nil {
		return nil, fmt.Errorf("%w: redis: nil Redis client", workflow.ErrInvalid)
	}
	if !prefixPattern.MatchString(prefix) {
		return nil, fmt.Errorf("%w: redis: prefix %q must match %s", workflow.ErrInvalid, prefix, prefixPattern)
	}
	return &UniqueStore{rdb: rdb, base: "{" + prefix + "}"}, nil
}

// UniqueStore returns a UniqueStore over this client's Redis and prefix.
func (c *Client) UniqueStore() *UniqueStore { return &UniqueStore{rdb: c.rdb, base: c.base} }

func (u *UniqueStore) key(k string) string { return u.base + ":uniq:" + k }

// acquireScript takes a free lock. KEYS lock; ARGV owner, ttl (ms).
var acquireScript = goredis.NewScript(`
if redis.call('EXISTS', KEYS[1]) == 1 then return 0 end
redis.call('HSET', KEYS[1], 'owner', ARGV[1])
redis.call('PEXPIRE', KEYS[1], ARGV[2])
return 1
`)

// ownedScript runs only while owner holds the lock: 'del' deletes it,
// 'set' records the holder's run. KEYS lock; ARGV owner, op, run id,
// scheduled id.
var ownedScript = goredis.NewScript(`
if redis.call('HGET', KEYS[1], 'owner') ~= ARGV[1] then return 0 end
if ARGV[2] == 'del' then
  redis.call('DEL', KEYS[1])
else
  redis.call('HSET', KEYS[1], 'rid', ARGV[3], 'sid', ARGV[4])
end
return 1
`)

// Acquire takes key for owner for ttl and reports true, or reports false
// while another holder has it.
func (u *UniqueStore) Acquire(ctx context.Context, key, owner string, ttl time.Duration) (bool, error) {
	if ttl <= 0 {
		return false, fmt.Errorf("%w: redis: unique lock %q needs a positive ttl", workflow.ErrInvalid, key)
	}
	ms := max(ttl.Milliseconds(), 1)
	n, err := acquireScript.Run(ctx, u.rdb, []string{u.key(key)}, owner, ms).Int()
	if err != nil {
		return false, fmt.Errorf("redis: acquire unique lock: %w", classify(err))
	}
	return n == 1, nil
}

// Release frees key if owner holds it; otherwise it does nothing.
func (u *UniqueStore) Release(ctx context.Context, key, owner string) error {
	if err := ownedScript.Run(ctx, u.rdb, []string{u.key(key)}, owner, "del", "", "").Err(); err != nil {
		return fmt.Errorf("redis: release unique lock: %w", classify(err))
	}
	return nil
}

// SetHolder records ref as the run of key while owner holds it.
func (u *UniqueStore) SetHolder(ctx context.Context, key, owner string, ref workflow.RunRef) error {
	if err := ownedScript.Run(ctx, u.rdb, []string{u.key(key)}, owner, "set", string(ref.RunID), string(ref.ScheduledID)).Err(); err != nil {
		return fmt.Errorf("redis: record unique lock holder: %w", classify(err))
	}
	return nil
}

// Holder reports the owner and run of a held key whose run was recorded.
func (u *UniqueStore) Holder(ctx context.Context, key string) (string, workflow.RunRef, bool, error) {
	v, err := u.rdb.HMGet(ctx, u.key(key), "owner", "rid", "sid").Result()
	if err != nil {
		return "", workflow.RunRef{}, false, fmt.Errorf("redis: read unique lock: %w", classify(err))
	}
	ref := workflow.RunRef{RunID: workflow.RunID(str(v[1])), ScheduledID: workflow.ScheduledID(str(v[2]))}
	if v[0] == nil || ref.IsZero() {
		return "", workflow.RunRef{}, false, nil
	}
	return str(v[0]), ref, true, nil
}
