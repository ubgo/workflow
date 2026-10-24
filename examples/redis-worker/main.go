// Command redis-worker runs jobs and a cron schedule on the Redis driver:
// what it declares and refuses (checked without a server), then, with
// REDIS_URL set, a web client that only enqueues, two workers sharing one
// prefix, a job run once across them, a retried step, and a schedule
// triggered by hand. Every key it writes is under a fresh prefix, deleted
// at the end.
//
// It is the program behind docs/drivers/redis.md: the guide copies its
// regions (marked "snippet:") and task docs:snippets fails when they
// drift.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/redis"
)

func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "redis-worker:", err)
		os.Exit(1)
	}
}

// envURL names the Redis to run against, such as redis://localhost:7012/0.
const envURL = "REDIS_URL"

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context, out io.Writer) error {
	if err := offline(out); err != nil {
		return err
	}
	url := os.Getenv(envURL)
	if url == "" {
		fmt.Fprintln(out, envURL, "is not set: the live run is skipped")
		return nil
	}
	return live(ctx, out, url)
}

// offline shows the refusals; building a client opens no connection.
func offline(out io.Writer) error {
	c, err := redis.New(goredis.NewClient(&goredis.Options{Addr: "127.0.0.1:1"}))
	if err != nil {
		return err
	}
	caps := c.Capabilities()
	fmt.Fprintln(out, "durable:", caps.Has(workflow.CapDurable), "cron:", caps.Has(workflow.CapCron), "overlap skip:", caps.Has(workflow.CapOverlapSkip))
	// snippet:refused
	// A closure is a Go value that cannot be stored in Redis, so the
	// driver does not declare CapClosure and has no EnqueueFunc.
	_, isClosure := any(c).(workflow.ClosureEnqueuer)
	fmt.Fprintln(out, "closures:", caps.Has(workflow.CapClosure), isClosure)
	// snippet:end
	return nil
}

func live(ctx context.Context, out io.Writer, url string) error {
	opts, err := goredis.ParseURL(url)
	if err != nil {
		return err
	}
	// snippet:client
	rdb := goredis.NewClient(opts) // the package is named redis too, hence goredis
	defer rdb.Close()
	prefix := "example-" + strconv.FormatInt(time.Now().UnixNano(), 36)
	newWorker := func() (*redis.Client, error) {
		return redis.New(rdb,
			redis.WithPrefix(prefix), // clients that share a prefix share every run and schedule
			redis.WithPollInterval(50*time.Millisecond),
		)
	}
	// snippet:end
	defer func() { _ = deletePrefix(context.Background(), rdb, prefix) }()

	var sends, attempts atomic.Int32
	email := workflow.Workflow{
		Name:  "send-email",
		Retry: workflow.RetryPolicy{Max: 2, Backoff: workflow.Backoff{Initial: 50 * time.Millisecond}},
		Steps: []workflow.Step{{Name: "send", Run: func(context.Context, []byte) ([]byte, error) {
			if attempts.Add(1) == 1 {
				return nil, errors.New("smtp 451")
			}
			sends.Add(1)
			return nil, nil
		}}},
	}
	report := workflow.Workflow{Name: "report", Steps: []workflow.Step{{Name: "build", Run: func(context.Context, []byte) ([]byte, error) { return nil, nil }}}}

	wctx, stop := context.WithCancel(ctx)
	defer stop()
	var workers []*redis.Client
	for range 2 {
		w, err := newWorker()
		if err != nil {
			return err
		}
		for _, wf := range []workflow.Workflow{email, report} {
			if err := w.Register(wf); err != nil {
				return err
			}
		}
		if err := w.Start(wctx); err != nil {
			return err
		}
		workers = append(workers, w)
	}

	// snippet:web
	web, err := redis.New(rdb, redis.WithPrefix(prefix), redis.WithEnqueueOnly())
	if err != nil {
		return err
	}
	ref, err := web.Enqueue(ctx, workflow.Job{Workflow: "send-email", IdempotencyKey: "welcome:u-7"})
	if err != nil {
		return err
	}
	again, err := web.Enqueue(ctx, workflow.Job{Workflow: "send-email", IdempotencyKey: "welcome:u-7"})
	// snippet:end
	if err != nil {
		return err
	}
	fmt.Fprintln(out, "same key, same run:", again.RunID == ref.RunID)
	if err := waitEnd(ctx, web, ref); err != nil {
		return err
	}
	page, err := web.Runs(ctx, workflow.RunQuery{Ref: ref})
	if err != nil {
		return err
	}
	r := page.Runs[0]
	fmt.Fprintf(out, "send-email %s after %d attempts, sent %d time(s), errors kept %d\n", r.Status, r.Attempts, sends.Load(), len(r.Errors))

	// snippet:trigger
	id, err := workers[0].Schedule(ctx, workflow.Schedule{Name: "nightly", Workflow: "report", Cron: "0 2 * * *", Location: time.UTC, Overlap: workflow.OverlapSkip})
	if err != nil {
		return err
	}
	run, err := workers[1].TriggerSchedule(ctx, id) // any client of the prefix
	if err != nil {
		return err
	}
	// snippet:end
	if err := waitEnd(ctx, web, run); err != nil {
		return err
	}
	infos, err := web.Schedules(ctx)
	if err != nil {
		return err
	}
	for _, s := range infos {
		fmt.Fprintf(out, "schedule %s: last run %s, next fire %s\n", s.Name, s.Last.Status, s.Next.Format("15:04 MST"))
	}
	stop()
	for _, w := range workers {
		w.Stop()
	}
	return nil
}

// waitEnd polls until the run is terminal.
func waitEnd(ctx context.Context, c workflow.Client, ref workflow.RunRef) error {
	deadline := time.Now().Add(10 * time.Second)
	for {
		st, err := c.Status(ctx, ref)
		if err != nil {
			return err
		}
		if st.IsTerminal() {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("run %s still %s after 10s", ref.RunID, st)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// deletePrefix removes every key of the example's prefix.
func deletePrefix(ctx context.Context, rdb *goredis.Client, prefix string) error {
	iter := rdb.Scan(ctx, 0, "{"+prefix+"}*", 500).Iterator()
	for iter.Next(ctx) {
		if err := rdb.Del(ctx, iter.Val()).Err(); err != nil {
			return err
		}
	}
	return iter.Err()
}
