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
	"os"
	"strconv"
	"sync/atomic"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/redis"
)

// main runs the example and exits non-zero on error.
func main() {
	if err := run(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, "redis-worker:", err)
		os.Exit(1)
	}
}

// envURL names the Redis to run against, such as redis://localhost:7012/0.
const envURL = "REDIS_URL"

const (
	// prefixBase writes the prefix's timestamp in base 36: short, and only
	// letters and digits, so it is safe in a key.
	prefixBase = 36
	// workerCount is how many workers share the prefix: two, so the run
	// once across them and the trigger from another worker are visible.
	workerCount = 2
	// sendRetries lets the send step retry twice after its first attempt;
	// it fails only once, so the run ends after two attempts.
	sendRetries = 2
	// retryBackoff is the wait before a retry: short so the demo is fast.
	retryBackoff = 50 * time.Millisecond
	// endTimeout bounds waitEnd, and endPoll is how often it reads the run.
	endTimeout = 10 * time.Second
	endPoll    = 20 * time.Millisecond
	// scanBatch is the SCAN count hint when the prefix's keys are deleted.
	scanBatch = 500
)

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context) error {
	if err := offline(); err != nil {
		return err
	}
	url := os.Getenv(envURL)
	if url == "" {
		fmt.Println(envURL, "is not set: the live run is skipped")
		return nil
	}
	return live(ctx, url)
}

// offline shows the refusals; building a client opens no connection.
func offline() error {
	c, err := redis.New(goredis.NewClient(&goredis.Options{Addr: "127.0.0.1:1"}))
	if err != nil {
		return err
	}
	caps := c.Capabilities()
	fmt.Println("durable:", caps.Has(workflow.CapDurable), "cron:", caps.Has(workflow.CapCron), "overlap skip:", caps.Has(workflow.CapOverlapSkip))
	// snippet:refused
	// A closure is a Go value that cannot be stored in Redis, so the
	// driver does not declare CapClosure and has no EnqueueFunc.
	_, isClosure := any(c).(workflow.ClosureEnqueuer)
	fmt.Println("closures:", caps.Has(workflow.CapClosure), isClosure)
	// snippet:end
	return nil
}

// live runs the web client, the two workers and the schedule against the
// Redis at url, all under a fresh prefix that it deletes at the end.
func live(ctx context.Context, url string) (err error) {
	opts, err := goredis.ParseURL(url)
	if err != nil {
		return err
	}
	// snippet:client
	rdb := goredis.NewClient(opts) // the package is named redis too, hence goredis
	defer func() { err = errors.Join(err, rdb.Close()) }()
	prefix := "example-" + strconv.FormatInt(time.Now().UnixNano(), prefixBase)
	// newWorker builds one worker client on the shared prefix.
	newWorker := func() (*redis.Client, error) {
		return redis.New(rdb,
			redis.WithPrefix(prefix),                    // clients that share a prefix share every run and schedule
			redis.WithPollInterval(50*time.Millisecond), // short for the demo; how often a worker looks for due runs
		)
	}
	// snippet:end
	defer func() { _ = deletePrefix(context.Background(), rdb, prefix) }()

	var sends, attempts atomic.Int32
	email := workflow.Workflow{
		Name:  "send-email",
		Retry: workflow.RetryPolicy{Max: sendRetries, Backoff: workflow.Backoff{Initial: retryBackoff}},
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
	for range workerCount {
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
	fmt.Println("same key, same run:", again.RunID == ref.RunID)
	if err := waitEnd(ctx, web, ref); err != nil {
		return err
	}
	page, err := web.Runs(ctx, workflow.RunQuery{Ref: ref})
	if err != nil {
		return err
	}
	r := page.Runs[0]
	fmt.Printf("send-email %s after %d attempts, sent %d time(s), errors kept %d\n", r.Status, r.Attempts, sends.Load(), len(r.Errors))

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
		fmt.Printf("schedule %s: last run %s, next fire %s\n", s.Name, s.Last.Status, s.Next.Format("15:04 MST"))
	}
	stop()
	for _, w := range workers {
		w.Stop()
	}
	return nil
}

// waitEnd polls until the run is terminal.
func waitEnd(ctx context.Context, c workflow.Client, ref workflow.RunRef) error {
	deadline := time.Now().Add(endTimeout)
	for {
		st, err := c.Status(ctx, ref)
		if err != nil {
			return err
		}
		if st.IsTerminal() {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("run %s still %s after %s", ref.RunID, st, endTimeout)
		}
		time.Sleep(endPoll)
	}
}

// deletePrefix removes every key of the example's prefix.
func deletePrefix(ctx context.Context, rdb *goredis.Client, prefix string) error {
	iter := rdb.Scan(ctx, 0, "{"+prefix+"}*", scanBatch).Iterator()
	for iter.Next(ctx) {
		if err := rdb.Del(ctx, iter.Val()).Err(); err != nil {
			return err
		}
	}
	return iter.Err()
}
