package redis

import (
	"context"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/ubgo/workflow"
)

// PromiseStore is a workflow.PromiseStore in Redis: each settled promise
// is one string key ({prefix}:promise:<id>) written with SET NX, so of two
// processes settling one promise at once exactly one stores its value and
// both read the stored one. With a TTL a value expires that long after it
// was stored (wall time, measured by Redis), a safety net for promises
// nobody reads; zero keeps it until Delete.
type PromiseStore struct {
	rdb  goredis.UniversalClient
	base string
	ttl  time.Duration
}

var _ workflow.PromiseStore = (*PromiseStore)(nil)

// NewPromiseStore returns a PromiseStore over rdb under prefix whose
// values expire ttl after they are stored (zero: never).
//
//ds:def id=redis-newpromisestore-v66xmp89 owner=@khanakia stability=stable
func NewPromiseStore(rdb goredis.UniversalClient, prefix string, ttl time.Duration) (*PromiseStore, error) {
	base, err := storeBase(rdb, prefix)
	if err != nil {
		return nil, err
	}
	if ttl < 0 {
		return nil, fmt.Errorf("%w: redis: promise ttl %s is negative", workflow.ErrInvalid, ttl)
	}
	return &PromiseStore{rdb: rdb, base: base, ttl: ttl}, nil
}

// PromiseStore returns a PromiseStore over this client's Redis and prefix
// with no expiry.
//
//ds:def id=redis-client-promisestore-r626cn6b owner=@khanakia stability=stable
func (c *Client) PromiseStore() *PromiseStore { return &PromiseStore{rdb: c.rdb, base: c.base} }

func (p *PromiseStore) key(id string) string { return p.base + ":promise:" + id }

// Settle implements workflow.PromiseStore.
//
//ds:def id=redis-promisestore-settle-tctztsg7 owner=@khanakia stability=stable
func (p *PromiseStore) Settle(ctx context.Context, id string, val []byte) ([]byte, bool, error) {
	if val == nil {
		val = []byte{}
	}
	// SET NX GET returns the old value when there was one and stores
	// nothing; Nil means this call stored val.
	old, err := p.rdb.SetArgs(ctx, p.key(id), val, goredis.SetArgs{Mode: "NX", Get: true, TTL: p.ttl}).Bytes()
	if err == goredis.Nil {
		return append([]byte{}, val...), true, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("redis: promise settle: %w", classify(err))
	}
	return old, false, nil
}

// Get implements workflow.PromiseStore.
func (p *PromiseStore) Get(ctx context.Context, id string) ([]byte, bool, error) {
	v, err := p.rdb.Get(ctx, p.key(id)).Bytes()
	if err == goredis.Nil {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("redis: promise get: %w", classify(err))
	}
	return v, true, nil
}

// Delete implements workflow.PromiseStore.
func (p *PromiseStore) Delete(ctx context.Context, id string) error {
	if err := p.rdb.Del(ctx, p.key(id)).Err(); err != nil {
		return fmt.Errorf("redis: promise delete: %w", classify(err))
	}
	return nil
}
