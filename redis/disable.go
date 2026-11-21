package redis

import (
	"context"
	"fmt"

	goredis "github.com/redis/go-redis/v9"
	"github.com/ubgo/workflow/middleware"
)

// DisableStore is a middleware.DisableStore in Redis (WH-02): one hash per
// key ({prefix}:disable:<key>: its count of consecutive failures and its
// switch), so middleware.AutoDisable counts the failures of every worker
// sharing the Redis and an endpoint switched off by one is off for all of
// them, until Enable. Fail is one script, so of any number of concurrent
// Fails exactly one reports newly true. A key that never failed has no
// hash; Enable deletes it. Hashes carry no expiry: a disabled key stays
// disabled until Enable, and a key's count stays until a success resets
// it.
type DisableStore struct {
	rdb  goredis.UniversalClient
	base string
}

var _ middleware.DisableStore = (*DisableStore)(nil)

// NewDisableStore returns a DisableStore over rdb under prefix.
func NewDisableStore(rdb goredis.UniversalClient, prefix string) (*DisableStore, error) {
	base, err := storeBase(rdb, prefix)
	if err != nil {
		return nil, err
	}
	return &DisableStore{rdb: rdb, base: base}, nil
}

// DisableStore returns a DisableStore over this client's Redis and prefix.
func (c *Client) DisableStore() *DisableStore { return &DisableStore{rdb: c.rdb, base: c.base} }

func (d *DisableStore) key(k string) string { return d.base + ":disable:" + k }

// disableFailScript counts a failure and maybe switches the key off.
// KEYS key; ARGV after. Returns 1 when this call switched it off, else 0.
var disableFailScript = goredis.NewScript(`
local n = redis.call('HINCRBY', KEYS[1], 'fails', 1)
if redis.call('HGET', KEYS[1], 'off') == '1' then return 0 end
if n >= tonumber(ARGV[1]) then
  redis.call('HSET', KEYS[1], 'off', '1')
  return 1
end
return 0
`)

// disableSucceedScript resets the count of a key that has a hash. KEYS
// key.
var disableSucceedScript = goredis.NewScript(`
if redis.call('EXISTS', KEYS[1]) == 1 then redis.call('HSET', KEYS[1], 'fails', 0) end
return 1
`)

// Disabled implements middleware.DisableStore.
func (d *DisableStore) Disabled(ctx context.Context, key string) (bool, error) {
	v, err := d.rdb.HGet(ctx, d.key(key), "off").Result()
	if err == goredis.Nil {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("redis: disable read: %w", classify(err))
	}
	return v == "1", nil
}

// Fail implements middleware.DisableStore in one script.
func (d *DisableStore) Fail(ctx context.Context, key string, after int) (bool, error) {
	n, err := disableFailScript.Run(ctx, d.rdb, []string{d.key(key)}, after).Int64()
	if err != nil {
		return false, fmt.Errorf("redis: disable fail: %w", classify(err))
	}
	return n == 1, nil
}

// Succeed implements middleware.DisableStore: it resets the count of a key
// that has failed and leaves its switch as it is.
func (d *DisableStore) Succeed(ctx context.Context, key string) error {
	if err := disableSucceedScript.Run(ctx, d.rdb, []string{d.key(key)}).Err(); err != nil {
		return fmt.Errorf("redis: disable succeed: %w", classify(err))
	}
	return nil
}

// Enable implements middleware.DisableStore: it deletes the key's hash.
func (d *DisableStore) Enable(ctx context.Context, key string) error {
	if err := d.rdb.Del(ctx, d.key(key)).Err(); err != nil {
		return fmt.Errorf("redis: disable enable: %w", classify(err))
	}
	return nil
}
