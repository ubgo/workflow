package main

import (
	"context"
	"sync"
	"time"

	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/middleware"
)

// snippet:store
// KV is the slice of a key-value service a unique store needs: an atomic
// "set if absent, with expiry" and a "delete if the value is still mine".
// Redis gives both (SET key value NX PX ttl, and a compare-and-delete
// script); so does a SQL table with a primary key.
type KV interface {
	SetNX(ctx context.Context, key, value string, ttl time.Duration) (bool, error)
	DeleteIfEquals(ctx context.Context, key, value string) error
}

// kvUnique is a middleware.UniqueStore over any KV. Acquire is one atomic
// call, so two processes enqueueing the same key at once cannot both win,
// and Release deletes only the owner's own lock, so a late run-finished
// event for an old run never frees a newer job's lock.
type kvUnique struct {
	kv     KV
	prefix string
}

var _ middleware.UniqueStore = kvUnique{}

func (s kvUnique) Acquire(ctx context.Context, key, owner string, ttl time.Duration) (bool, error) {
	return s.kv.SetNX(ctx, s.prefix+key, owner, ttl)
}

func (s kvUnique) Release(ctx context.Context, key, owner string) error {
	return s.kv.DeleteIfEquals(ctx, s.prefix+key, owner)
}

// snippet:end

// memKV is an in-process KV for this example and its test; production
// passes a client of a service every process shares.
type memKV struct {
	clk clock.Clock
	mu  sync.Mutex
	m   map[string]memEntry
}

type memEntry struct {
	value   string
	expires time.Time
}

func newMemKV(clk clock.Clock) *memKV { return &memKV{clk: clk, m: map[string]memEntry{}} }

func (k *memKV) SetNX(_ context.Context, key, value string, ttl time.Duration) (bool, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	now := k.clk.Now()
	if e, ok := k.m[key]; ok && now.Before(e.expires) {
		return false, nil
	}
	k.m[key] = memEntry{value: value, expires: now.Add(ttl)}
	return true, nil
}

func (k *memKV) DeleteIfEquals(_ context.Context, key, value string) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	if e, ok := k.m[key]; ok && e.value == value {
		delete(k.m, key)
	}
	return nil
}
