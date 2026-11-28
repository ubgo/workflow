// Command redis-parity walks through what the Redis driver does at the
// level of the SQL driver: what it declares and refuses (checked without a
// server), then, with REDIS_URL set, every option of a worker, a durable
// wait for a signal, a concurrency key, a rate limit, a named queue, a
// workflow pause, a run-finished observer, a chain, a schedule condition,
// a clean stop that hands a run to another worker, and the shared stores
// built on their own. Every key it writes is under a fresh prefix, deleted
// at the end.
//
// It is the program behind the parity sections of docs/drivers/redis.md:
// the guide copies its regions (marked "snippet:") and task docs:snippets
// fails when they drift.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/cron"
	"github.com/ubgo/workflow/redis"
)

// envURL names the Redis to run against, such as redis://localhost:7012/0;
// the same variable examples/redis-worker reads.
const envURL = "REDIS_URL"

const (
	// runTimeout bounds the wait for one run, generous for a slow CI host.
	runTimeout = 15 * time.Second
	// statusPoll is how often waitStatus re-reads a run's status.
	statusPoll = 20 * time.Millisecond
	// holdCheck is how long the program watches a run it expects to stay
	// held (by a rate limit or a pause) before it reads the status.
	holdCheck = 500 * time.Millisecond
	// pollEvery is the workers' fallback poll; a published message wakes
	// them at once, so this only bounds a missed wake-up.
	pollEvery = 50 * time.Millisecond
	// leaseFor is how long a claim lives without renewal; renewed every
	// third of it, so a dead worker's run is taken again within this.
	leaseFor = 10 * time.Second
	// maxReclaims is how many lease expiries in a row a run survives with
	// no step finishing before it fails with redis.ErrAbandoned.
	maxReclaims = 3
	// stepWork is how long the per-customer step takes, long enough for
	// two runs of one key to overlap if the key did not hold them apart.
	stepWork = 50 * time.Millisecond
	// smsPerHour is the rate limit of the sms workflow.
	smsPerHour = 2
	// tickWatch bounds how long the schedule-condition part waits for one
	// skipped and one fired tick of a schedule that ticks every second.
	tickWatch = 10 * time.Second
	// unreachable is an address nothing listens on: building a client
	// opens no connection, so the offline part needs no server.
	unreachable = "127.0.0.1:1"
	// prefixBase writes the prefix's timestamp in base 36, the shortest
	// digits strconv offers, so every key stays short.
	prefixBase = 36
	// tickBuffer holds the heartbeat's ticks until the program reads them;
	// the observer drops a tick rather than block the scheduler.
	tickBuffer = 16
	// invoiceRuns is how many runs of one customer the key test starts:
	// enough that two clients would overlap them without the key.
	invoiceRuns = 3
	// chainSteps is how many workflows the chain runs: extract, then load.
	chainSteps = 2
	// scanBatch is how many keys one SCAN round trip asks for at cleanup.
	scanBatch = 500
)

// main runs the example and exits non-zero on error.
func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "redis-parity:", err)
		os.Exit(1)
	}
}

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context, w io.Writer) error {
	if err := offline(ctx, w); err != nil {
		return err
	}
	url := os.Getenv(envURL)
	if url == "" {
		fmt.Fprintln(w, envURL, "is not set: the live run is skipped")
		return nil
	}
	return live(ctx, w, url)
}

// offline prints what the driver declares and shows its two refusals at
// Start, both decided before Start touches Redis.
func offline(ctx context.Context, w io.Writer) error {
	rdb := goredis.NewClient(&goredis.Options{Addr: unreachable})
	defer rdb.Close()

	// snippet:declares
	c, err := redis.New(rdb)
	if err != nil {
		return err
	}
	for _, cp := range []workflow.Caps{
		workflow.CapWaitForEvent, workflow.CapConcurrencyKey, workflow.CapRateLimit,
		workflow.CapQueue, workflow.CapPauseWorkflow, workflow.CapPauseQueue,
		workflow.CapRunFinished, workflow.CapChain, workflow.CapScheduleWhen,
		workflow.CapShutdownRequeue,
		workflow.CapClosure, workflow.CapQueueLimitGlobal, // two it does not declare; CapSlotCost, CapConcurrencyLimits, CapConcurrencyScope and CapHoldWhileRunning are the others
	} {
		fmt.Fprintf(w, "%v: %v\n", cp, c.Capabilities().Has(cp))
	}
	// snippet:end

	// snippet:refuses
	// A key in an engine language: the driver evaluates keys in Go.
	exprKey, err := redis.New(rdb)
	if err != nil {
		return err
	}
	if err := exprKey.Register(workflow.Workflow{
		Name:        "per-tenant",
		Concurrency: &workflow.Concurrency{Key: workflow.KeyExpr("input.tenant"), MaxRuns: 1},
		Steps:       []workflow.Step{{Name: "only", Run: noop}},
	}); err != nil {
		return err
	}
	err = exprKey.Start(ctx)
	fmt.Fprintln(w, "KeyExpr at Start: ErrUnsupported =", errors.Is(err, workflow.ErrUnsupported))

	// A cap across every client: a run is claimed whole, so no count of a
	// queue's step attempts across clients is kept.
	limited, err := redis.New(rdb, redis.WithQueues(workflow.QueueConfig{Name: "imports", Workers: 2, Limit: 5})) // Limit is the refused field
	if err != nil {
		return err
	}
	err = limited.Start(ctx)
	fmt.Fprintln(w, "QueueConfig.Limit at Start: ErrUnsupported =", errors.Is(err, workflow.ErrUnsupported))
	// snippet:end
	return nil
}

// live runs every parity feature against a real Redis.
func live(ctx context.Context, w io.Writer, url string) (err error) {
	opts, err := goredis.ParseURL(url)
	if err != nil {
		return err
	}
	rdb := goredis.NewClient(opts)
	defer func() { err = errors.Join(err, rdb.Close()) }()
	prefix := "parity-" + strconv.FormatInt(time.Now().UnixNano(), prefixBase)
	defer func() { _ = deletePrefix(context.Background(), rdb, prefix) }()

	// snippet:options
	var ids atomic.Int64 // shared by every client of this process
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	// newWorker builds one client of the prefix with every option set.
	newWorker := func(node string, extra ...redis.Option) (*redis.Client, error) {
		opts := []redis.Option{
			redis.WithPrefix(prefix),           // clients of one prefix share runs and schedules
			redis.WithClock(clock.Real{}),      // due times, sleeps, backoff, cron fires
			redis.WithLeaseClock(clock.Real{}), // leases and polling: keep this one real
			redis.WithLease(leaseFor),          // a claim's life without renewal (default 30s)
			redis.WithMaxReclaims(maxReclaims), // expiries in a row before ErrAbandoned (default 5)
			redis.WithPollInterval(pollEvery),  // fallback when no wake-up message came (default 1s)
			redis.WithWorkers(4),               // runs this client executes at once (default 10)
			redis.WithIDFunc(func() string { // unique across the prefix, no ':' or '{'
				return fmt.Sprintf("%s-%06d", prefix, ids.Add(1))
			}),
			redis.WithLogger(logger),                              // a failed renewal, a lost hand-back
			redis.WithMiddleware(),                                // step middleware, outermost first
			redis.WithCronParser(cron.Parse),                      // the same parser on every client
			redis.WithJitterSource(func() float64 { return 0.5 }), // pin Backoff.Jitter's draw
			redis.WithNodeID(node),
		}
		return redis.New(rdb, append(opts, extra...)...)
	}
	// snippet:end

	var mu sync.Mutex
	var order []string // the chain's steps, in the order they ran
	// record appends one chain step's name as it runs.
	record := func(s string) {
		mu.Lock()
		defer mu.Unlock()
		order = append(order, s)
	}
	var running, most atomic.Int32 // per-customer runs in flight, and the peak
	approved := make(chan string, 1)
	observed := make(chan workflow.RunEvent, 1)
	var whenCalls atomic.Int32
	ticks := make(chan workflow.Tick, tickBuffer)

	// snippet:workflows
	approval := workflow.Workflow{Name: "approval", Steps: []workflow.Step{
		{Name: "await", Wait: &workflow.WaitSpec{
			Event:   "approved",
			KeyFrom: workflow.KeyFunc(func(j workflow.Job) string { return j.Labels["order"] }),
		}},
		{Name: "ship", After: []string{"await"}, Run: func(_ context.Context, in []byte) ([]byte, error) {
			approved <- string(in) // the wait step's output is the signal's payload
			return nil, nil
		}},
	}}
	perCustomer := workflow.Workflow{
		Name: "invoice",
		Concurrency: &workflow.Concurrency{ // one run per customer at a time, across clients
			Key:     workflow.KeyFunc(func(j workflow.Job) string { return j.Labels["customer"] }),
			MaxRuns: 1,
		},
		Steps: []workflow.Step{{Name: "bill", Run: func(context.Context, []byte) ([]byte, error) {
			n := running.Add(1)
			for {
				m := most.Load()
				if n <= m || most.CompareAndSwap(m, n) {
					break
				}
			}
			time.Sleep(stepWork)
			running.Add(-1)
			return nil, nil
		}}},
	}
	sms := workflow.Workflow{
		Name: "sms",
		RateLimits: []workflow.RateLimit{{ // a sliding window kept in Redis
			Key:   workflow.KeyFunc(func(workflow.Job) string { return "provider" }),
			Limit: smsPerHour,
			Per:   time.Hour,
		}},
		Steps: []workflow.Step{{Name: "send", Run: noop}},
	}
	extract := workflow.Workflow{Name: "extract", Steps: []workflow.Step{{Name: "run", Run: func(context.Context, []byte) ([]byte, error) {
		record("extract")
		return nil, nil
	}}}}
	load := workflow.Workflow{Name: "load", Steps: []workflow.Step{{Name: "run", Run: func(context.Context, []byte) ([]byte, error) {
		record("load")
		return nil, nil
	}}}}
	heartbeat := workflow.Workflow{
		Name: "heartbeat",
		// Decided per tick on the firing client: skip the first tick only.
		ScheduleWhen: func(context.Context, workflow.Tick) bool { return whenCalls.Add(1) > 1 },
		Steps:        []workflow.Step{{Name: "beat", Run: noop}},
	}
	// snippet:end
	render := workflow.Workflow{Name: "render", Queue: "reports", Steps: []workflow.Step{{Name: "draw", Run: noop}}}

	wctx, stopAll := context.WithCancel(ctx)
	defer stopAll()
	worker, err := newWorker("node-a")
	if err != nil {
		return err
	}
	for _, wf := range []workflow.Workflow{approval, perCustomer, sms, extract, load, heartbeat} {
		if err := worker.Register(wf); err != nil {
			return err
		}
	}
	// snippet:observers
	err = worker.OnRunFinished(func(_ context.Context, ev workflow.RunEvent) { // before Start
		if ev.Workflow == "approval" {
			observed <- ev
		}
	})
	if err != nil {
		return err
	}
	err = worker.OnTick(func(_ context.Context, t workflow.Tick) {
		if t.Workflow == "heartbeat" {
			select {
			case ticks <- t:
			default:
			}
		}
	})
	// snippet:end
	if err != nil {
		return err
	}
	if err := worker.Start(wctx); err != nil {
		return err
	}
	second, err := newWorker("node-b") // a second client of the prefix for the key test
	if err != nil {
		return err
	}
	if err := second.Register(perCustomer); err != nil {
		return err
	}
	if err := second.Start(wctx); err != nil {
		return err
	}
	// snippet:queues
	reports, err := newWorker("reports-node", redis.WithQueues(workflow.QueueConfig{Name: "reports", Workers: 1}))
	// snippet:end
	if err != nil {
		return err
	}
	if err := reports.Register(render); err != nil {
		return err
	}
	if err := reports.Start(wctx); err != nil {
		return err
	}

	// A durable wait: the run holds no worker until the signal arrives.
	// snippet:signal
	ref, err := worker.Enqueue(ctx, workflow.Job{Workflow: "approval", Labels: map[string]string{"order": "o-7"}})
	if err != nil {
		return err
	}
	if err := worker.Signal(ctx, workflow.Event{Name: "approved", Key: "o-7", Payload: []byte(`"by ops"`)}); err != nil {
		return err
	}
	// snippet:end
	if err := waitStatus(ctx, worker, ref, workflow.StatusSucceeded); err != nil {
		return err
	}
	fmt.Fprintln(w, "approval: the step after the wait got", <-approved)
	ev := <-observed
	fmt.Fprintln(w, "run-finished observer saw approval:", ev.Status)

	// Concurrency key: three runs of one customer across two clients.
	var invoices []workflow.RunRef
	for range invoiceRuns {
		ref, err := worker.Enqueue(ctx, workflow.Job{Workflow: "invoice", Labels: map[string]string{"customer": "acme"}})
		if err != nil {
			return err
		}
		invoices = append(invoices, ref)
	}
	for _, ref := range invoices {
		if err := waitStatus(ctx, worker, ref, workflow.StatusSucceeded); err != nil {
			return err
		}
	}
	fmt.Fprintf(w, "invoice: %d runs of customer acme on two clients, at most %d at a time\n", len(invoices), most.Load())

	// Rate limit: two per hour, the third waits in Redis.
	var texts []workflow.RunRef
	for range smsPerHour + 1 {
		ref, err := worker.Enqueue(ctx, workflow.Job{Workflow: "sms"})
		if err != nil {
			return err
		}
		texts = append(texts, ref)
	}
	for _, ref := range texts[:smsPerHour] {
		if err := waitStatus(ctx, worker, ref, workflow.StatusSucceeded); err != nil {
			return err
		}
	}
	time.Sleep(holdCheck)
	st, err := worker.Status(ctx, texts[smsPerHour])
	if err != nil {
		return err
	}
	fmt.Fprintf(w, "sms: %d succeeded, the next one is %s (%d per hour)\n", smsPerHour, st, smsPerHour)
	if err := worker.Cancel(ctx, texts[smsPerHour]); err != nil {
		return err
	}

	// A named queue served by its own worker, and a pause stored in Redis.
	// snippet:pause
	// Any client of the prefix pauses; every client obeys it.
	if err := worker.Pause(ctx, workflow.PauseWorkflow("render")); err != nil {
		return err
	}
	// A worker client enqueues only what it registered (or use WithEnqueueOnly).
	held, err := reports.Enqueue(ctx, workflow.Job{Workflow: "render"})
	if err != nil {
		return err
	}
	// snippet:end
	time.Sleep(holdCheck)
	if st, err = worker.Status(ctx, held); err != nil {
		return err
	}
	paused, err := worker.Paused(ctx)
	if err != nil {
		return err
	}
	fmt.Fprintf(w, "render while paused: %s, paused targets %d\n", st, len(paused))
	if err := worker.Resume(ctx, workflow.PauseWorkflow("render")); err != nil {
		return err
	}
	if err := waitStatus(ctx, worker, held, workflow.StatusSucceeded); err != nil {
		return err
	}
	page, err := worker.Runs(ctx, workflow.RunQuery{Ref: held})
	if err != nil {
		return err
	}
	fmt.Fprintf(w, "render after resume: %s on queue %s by %v\n", page.Runs[0].Status, page.Runs[0].Queue, page.Runs[0].AttemptedBy)

	// snippet:chain
	first, err := worker.Enqueue(ctx, workflow.Job{
		Workflow: "extract",
		Chain:    []workflow.Job{{Workflow: "load"}}, // starts once extract succeeded, stored with the run
	})
	// snippet:end
	if err != nil {
		return err
	}
	if err := waitStatus(ctx, worker, first, workflow.StatusSucceeded); err != nil {
		return err
	}
	if err := waitFor(func() bool { mu.Lock(); defer mu.Unlock(); return len(order) == chainSteps }); err != nil {
		return fmt.Errorf("chain: %w", err)
	}
	fmt.Fprintf(w, "chain: %s then %s\n", order[0], order[1])

	// A schedule condition, decided per tick: every second, first tick skipped.
	// snippet:condition
	id, err := worker.Schedule(ctx, workflow.Schedule{Name: "beat", Workflow: "heartbeat", Cron: "* * * * * *", Seconds: true, Location: time.UTC})
	if err != nil {
		return err
	}
	// snippet:end
	var skipped, fired bool
	var reason string
	deadline := time.After(tickWatch)
	for !skipped || !fired {
		select {
		case t := <-ticks:
			switch t.Outcome {
			case workflow.TickSkipped:
				skipped, reason = true, t.Reason
			case workflow.TickFired:
				fired = true
			}
		case <-deadline:
			return fmt.Errorf("schedule condition: skipped %v fired %v after %s", skipped, fired, tickWatch)
		}
	}
	if err := worker.Unschedule(ctx, id); err != nil {
		return err
	}
	fmt.Fprintf(w, "heartbeat: a tick skipped (reason %q), a later one fired\n", reason)

	if err := cleanStop(ctx, w, newWorker); err != nil {
		return err
	}
	if err := stores(ctx, w, rdb, prefix); err != nil {
		return err
	}
	stopAll()
	for _, c := range []*redis.Client{worker, second, reports} {
		c.Stop()
	}
	return nil
}

// cleanStop shows CapShutdownRequeue: a worker stopped through its Start
// context hands its run back at once, and the next worker runs the step
// with the same attempt number.
func cleanStop(ctx context.Context, w io.Writer, newWorker func(string, ...redis.Option) (*redis.Client, error)) error {
	started := make(chan struct{}, 1)
	var attempts []int
	var mu sync.Mutex
	long := workflow.Workflow{Name: "long", Steps: []workflow.Step{{Name: "export", Run: func(ctx context.Context, _ []byte) ([]byte, error) {
		info, _ := workflow.StepInfoFrom(ctx)
		mu.Lock()
		attempts = append(attempts, info.Attempt)
		n := len(attempts)
		mu.Unlock()
		if n == 1 {
			started <- struct{}{}
			<-ctx.Done() // cut short by the clean stop
			return nil, ctx.Err()
		}
		return nil, nil
	}}}}

	// snippet:clean-stop
	first, err := newWorker("deploy-old")
	if err != nil {
		return err
	}
	if err := first.Register(long); err != nil {
		return err
	}
	firstCtx, stopFirst := context.WithCancel(ctx)
	if err := first.Start(firstCtx); err != nil {
		stopFirst()
		return err
	}
	ref, err := first.Enqueue(ctx, workflow.Job{Workflow: "long"})
	if err != nil {
		stopFirst()
		return err
	}
	<-started
	stopFirst() // a clean stop: the run goes back at once, the attempt not counted
	first.Stop()

	next, err := newWorker("deploy-new") // the new version of the worker
	if err != nil {
		return err
	}
	if err := next.Register(long); err != nil {
		return err
	}
	// snippet:end
	nextCtx, stopNext := context.WithCancel(ctx)
	defer stopNext()
	if err := next.Start(nextCtx); err != nil {
		return err
	}
	defer next.Stop()
	if err := waitStatus(ctx, next, ref, workflow.StatusSucceeded); err != nil {
		return err
	}
	mu.Lock()
	defer mu.Unlock()
	fmt.Fprintf(w, "long: cut short on deploy-old, finished on deploy-new, attempts %v\n", attempts)
	return nil
}

// stores builds the shared stores on their own, without a driver client,
// over the same Redis and prefix, and uses three of them.
func stores(ctx context.Context, w io.Writer, rdb *goredis.Client, prefix string) error {
	// snippet:stores
	// keep is how long a settled promise or a step memo is kept.
	const keep = 24 * time.Hour
	data, err := redis.NewDataStore(rdb, prefix) // workflow.DataStore: SetData, GetData, UpdateData
	if err != nil {
		return err
	}
	promises, err := redis.NewPromiseStore(rdb, prefix, keep) // workflow.PromiseStore
	if err != nil {
		return err
	}
	journal, err := redis.NewJournalStore(rdb, prefix, keep) // workflow.JournalStore, step memos
	if err != nil {
		return err
	}
	inbox, err := redis.NewInbox(rdb, prefix) // workflow.Inbox: a consumer's dedup of messages
	if err != nil {
		return err
	}
	buckets, err := redis.NewBucketStore(rdb, prefix) // workflow.BucketStore: leaky buckets
	if err != nil {
		return err
	}
	aggregates, err := redis.NewAggregateStore(rdb, prefix) // middleware.Aggregate's pending items
	if err != nil {
		return err
	}
	// snippet:end
	_, _, _ = inbox, buckets, aggregates

	scope := workflow.Folder("tenants/acme")
	ver, err := data.Put(ctx, scope, "plan", []byte(`"pro"`), 0) // 0: only if no entry yet
	if err != nil {
		return err
	}
	val, _, _, err := data.Get(ctx, scope, "plan")
	if err != nil {
		return err
	}
	fmt.Fprintf(w, "data: plan=%s version %d\n", val, ver)

	_, firstSettle, err := promises.Settle(ctx, "invoice-42", []byte(`"paid"`))
	if err != nil {
		return err
	}
	stored, again, err := promises.Settle(ctx, "invoice-42", []byte(`"refunded"`))
	if err != nil {
		return err
	}
	fmt.Fprintf(w, "promise: first settle %v, second settle %v, value %s\n", firstSettle, again, stored)

	if err := journal.Save(ctx, "run-1", "fetch", []byte(`42`)); err != nil {
		return err
	}
	memo, ok, err := journal.Load(ctx, "run-1", "fetch")
	if err != nil {
		return err
	}
	fmt.Fprintf(w, "journal: fetch=%s found %v\n", memo, ok)
	return nil
}

// noop is a step that returns its input.
func noop(_ context.Context, in []byte) ([]byte, error) { return in, nil }

// waitStatus polls until the run reads want, or fails after runTimeout or
// when it ends in another state.
func waitStatus(ctx context.Context, c workflow.Client, ref workflow.RunRef, want workflow.Status) error {
	deadline := time.Now().Add(runTimeout)
	for {
		st, err := c.Status(ctx, ref)
		if err != nil {
			return err
		}
		if st == want {
			return nil
		}
		if st.IsTerminal() || time.Now().After(deadline) {
			return fmt.Errorf("run %s is %s, want %s", ref.RunID, st, want)
		}
		time.Sleep(statusPoll)
	}
}

// waitFor polls cond until it holds or runTimeout passes.
func waitFor(cond func() bool) error {
	deadline := time.Now().Add(runTimeout)
	for !cond() {
		if time.Now().After(deadline) {
			return fmt.Errorf("not done after %s", runTimeout)
		}
		time.Sleep(statusPoll)
	}
	return nil
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
