package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"os"

	goredis "github.com/redis/go-redis/v9"
	"github.com/ubgo/workflow"
	wfredis "github.com/ubgo/workflow/redis"
)

// newRedis runs the Redis driver under a key prefix made for this run
// (bench_<random>; the driver writes keys as {bench_<random>}:...), whose
// keys are deleted after it.
func newRedis(addr string) system {
	s := &wfSystem{}
	var (
		rdb    *goredis.Client
		prefix string
	)
	s.newClient = func(_ context.Context, conc int) (workflow.Client, error) {
		var b [4]byte
		if _, err := rand.Read(b[:]); err != nil {
			return nil, err
		}
		prefix = "bench_" + hex.EncodeToString(b[:])
		rdb = goredis.NewClient(&goredis.Options{Addr: addr, Password: os.Getenv(envRedisPassword)})
		return wfredis.New(rdb, wfredis.WithPrefix(prefix), wfredis.WithWorkers(conc))
	}
	s.newProducer = func(context.Context) (workflow.Client, error) {
		return wfredis.New(rdb, wfredis.WithPrefix(prefix), wfredis.WithEnqueueOnly())
	}
	s.cleanup = func(ctx context.Context) error {
		if rdb == nil {
			return nil
		}
		return errors.Join(deleteKeys(ctx, rdb, "{"+prefix+"}*"), rdb.Close())
	}
	return s
}

// envRedisPassword names the Redis password (empty: no auth).
const envRedisPassword = "REDIS_PASSWORD"

// deleteKeys removes every key matching pattern, scanning in pages so a
// large keyspace never blocks the server.
func deleteKeys(ctx context.Context, rdb *goredis.Client, pattern string) error {
	iter := rdb.Scan(ctx, 0, pattern, 1000).Iterator()
	var batch []string
	for iter.Next(ctx) {
		batch = append(batch, iter.Val())
		if len(batch) == 1000 {
			if err := rdb.Del(ctx, batch...).Err(); err != nil {
				return err
			}
			batch = batch[:0]
		}
	}
	if err := iter.Err(); err != nil {
		return err
	}
	if len(batch) > 0 {
		return rdb.Del(ctx, batch...).Err()
	}
	return nil
}
