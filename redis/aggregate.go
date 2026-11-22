package redis

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/middleware"
)

// AggregateStore is a middleware.AggregateStore in Redis: one list per
// group ({prefix}:aggregate:<group>), each element the time Append was
// given (Unix nanoseconds in decimal) and a space in front of the item, so
// middleware.Aggregate collects a group's items from every process sharing
// the Redis and every worker's flush run takes from the same list. Append
// and Take are one script each, so they are atomic per group: the count
// Append returns is exact, and of two Takes at once each gets different
// items and together they miss none. Items carry no expiry: a pending
// item stays until a Take removes it, and the list goes with its last
// item. The time is the caller's now, never Redis's clock.
//
//ds:def id=redis-aggregatestore-mudq4qgz owner=@khanakia stability=stable
type AggregateStore struct {
	rdb  goredis.UniversalClient
	base string
}

// NewAggregateStore returns an AggregateStore over rdb under prefix.
//
//ds:def id=redis-newaggregatestore-z6ddgfxu owner=@khanakia stability=stable
func NewAggregateStore(rdb goredis.UniversalClient, prefix string) (*AggregateStore, error) {
	base, err := storeBase(rdb, prefix)
	if err != nil {
		return nil, err
	}
	return &AggregateStore{rdb: rdb, base: base}, nil
}

// AggregateStore returns an AggregateStore over this client's Redis and
// prefix.
func (c *Client) AggregateStore() *AggregateStore { return &AggregateStore{rdb: c.rdb, base: c.base} }

var _ middleware.AggregateStore = (*AggregateStore)(nil)

func (a *AggregateStore) key(group string) string { return a.base + ":aggregate:" + group }

// aggAppendScript pushes one element and returns the list's length and its
// oldest element. KEYS list; ARGV element.
var aggAppendScript = goredis.NewScript(`
local n = redis.call('RPUSH', KEYS[1], ARGV[1])
return {n, redis.call('LINDEX', KEYS[1], 0)}
`)

// aggTakeScript removes and returns up to ARGV[1] of the oldest elements (0:
// all). KEYS list.
var aggTakeScript = goredis.NewScript(`
local limit = tonumber(ARGV[1])
local items = redis.call('LRANGE', KEYS[1], 0, limit - 1)
if limit <= 0 or #items < limit then
  redis.call('DEL', KEYS[1])
else
  redis.call('LTRIM', KEYS[1], limit, -1)
end
return items
`)

// Append implements middleware.AggregateStore.
func (a *AggregateStore) Append(ctx context.Context, group string, item []byte, now time.Time) (int, time.Time, error) {
	elem := append(strconv.AppendInt(nil, now.UnixNano(), 10), ' ')
	res, err := aggAppendScript.Run(ctx, a.rdb, []string{a.key(group)}, append(elem, item...)).Slice()
	if err != nil {
		return 0, time.Time{}, fmt.Errorf("redis: aggregate append: %w", classify(err))
	}
	n, _ := res[0].(int64)
	at, _, err := splitAggregate(str(res[1]))
	if err != nil {
		return 0, time.Time{}, err
	}
	return int(n), at, nil
}

// Take implements middleware.AggregateStore.
func (a *AggregateStore) Take(ctx context.Context, group string, limit int) ([][]byte, error) {
	res, err := aggTakeScript.Run(ctx, a.rdb, []string{a.key(group)}, max(limit, 0)).StringSlice()
	if err != nil && err != goredis.Nil {
		return nil, fmt.Errorf("redis: aggregate take: %w", classify(err))
	}
	out := make([][]byte, 0, len(res))
	for _, e := range res {
		_, item, err := splitAggregate(e)
		if err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, nil
}

// splitAggregate splits a list element into its time and its item.
func splitAggregate(e string) (time.Time, []byte, error) {
	at, item, ok := bytes.Cut([]byte(e), []byte{' '})
	n, err := strconv.ParseInt(string(at), 10, 64)
	if !ok || err != nil {
		return time.Time{}, nil, fmt.Errorf("%w: redis: aggregate element was not written by this store", workflow.ErrInvalid)
	}
	return time.Unix(0, n).UTC(), item, nil
}
