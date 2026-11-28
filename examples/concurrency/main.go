// Command concurrency caps how many runs of a workflow execute at once per
// key (one report per tenant at a time), shows what each strategy does to
// the run over the cap, rate-limits starts per key, orders waiting work by
// priority, caps one workflow on two keys at once, caps two workflows
// together through a shared scope, lets a run waiting to
// retry give its slot back (Concurrency.Hold), and shows which key form
// each driver refuses.
//
// It runs on the in-memory driver with a fake clock, so a one-minute rate
// window passes when the program moves the clock. It is the program behind
// docs/jobs/concurrency.md and the priority section of docs/jobs/enqueue.md:
// the guides copy its regions (marked "snippet:") and task docs:snippets
// fails when they drift.
package main

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/hatchet"
	"github.com/ubgo/workflow/memory"
)

// start is the fake clock's first instant; any fixed time works.
var start = time.Date(2026, 1, 5, 9, 0, 0, 0, time.UTC)

// gated is a step whose runs report when they start and then hold until
// the program releases them, so the program can look at a full bucket.
type gated struct {
	started chan string
	gates   map[string]chan struct{} // run name -> closed to let it finish
}

// handler reports the run's "name" label and waits for its gate, or for
// the run to be cancelled.
func (g gated) handler(ctx context.Context, _ []byte) ([]byte, error) {
	info, _ := workflow.StepInfoFrom(ctx)
	name := info.Labels["name"]
	g.started <- name
	select {
	case <-g.gates[name]:
		return nil, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func main() {
	if err := run(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, "concurrency:", err)
		os.Exit(1)
	}
}

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context) error {
	g := gated{started: make(chan string, 16), gates: map[string]chan struct{}{}}
	for _, n := range []string{"acme-1", "acme-2", "globex-1", "sync-1", "preview-1", "preview-2", "mail-1", "mail-2", "mail-3"} {
		g.gates[n] = make(chan struct{})
	}
	clk := clock.NewFake(start)
	c := memory.New(memory.WithClock(clk))

	// snippet:per-key
	tenant := workflow.KeyFunc(func(j workflow.Job) string { return j.Labels["tenant"] })
	report := workflow.Workflow{
		Name: "report",
		Concurrency: &workflow.Concurrency{
			Key:      tenant, // one bucket per tenant
			MaxRuns:  1,      // one report per tenant at a time
			Strategy: workflow.ConcurrencyQueueNewest,
		},
		Steps: []workflow.Step{{Name: "build", Run: g.handler}},
	}
	// snippet:end

	// snippet:strategies
	sync := workflow.Workflow{
		Name:        "sync",
		Concurrency: &workflow.Concurrency{MaxRuns: 1, Strategy: workflow.ConcurrencyDropNewest},
		Steps:       []workflow.Step{{Name: "pull", Run: g.handler}},
	}
	preview := workflow.Workflow{
		Name:        "preview",
		Concurrency: &workflow.Concurrency{MaxRuns: 1, Strategy: workflow.ConcurrencyCancelInProgress},
		Steps:       []workflow.Step{{Name: "render", Run: g.handler}},
	}
	// snippet:end

	// snippet:rate-limit
	mail := workflow.Workflow{
		Name: "mail",
		RateLimits: []workflow.RateLimit{{
			Key:   workflow.KeyFunc(func(j workflow.Job) string { return j.Labels["domain"] }),
			Limit: 2,           // at most two starts
			Per:   time.Minute, // per minute, per recipient domain
		}},
		Steps: []workflow.Step{{Name: "send", Run: g.handler}},
	}
	// snippet:end

	for _, wf := range []workflow.Workflow{report, sync, preview, mail} {
		if err := c.Register(wf); err != nil {
			return err
		}
	}
	ctx, cancel := context.WithCancel(ctx)
	defer c.Stop()
	defer cancel()
	if err := c.Start(ctx); err != nil {
		return err
	}
	enqueue := func(wf, name string, labels map[string]string) (workflow.RunRef, error) {
		l := map[string]string{"name": name}
		maps.Copy(l, labels)
		return c.Enqueue(ctx, workflow.Job{Workflow: wf, Labels: l})
	}
	// status is for printing only: an error reads as an empty status,
	// which the expected output in main_test.go catches.
	status := func(ref workflow.RunRef) workflow.Status {
		st, _ := c.Status(ctx, ref)
		return st
	}

	// One run per tenant: acme's second report waits, globex's runs.
	fmt.Println("per key:")
	// An enqueue error returns at once: the receives from g.started below
	// would otherwise wait forever for a run that was never created.
	a1, err := enqueue("report", "acme-1", map[string]string{"tenant": "acme"})
	if err != nil {
		return err
	}
	a2, err := enqueue("report", "acme-2", map[string]string{"tenant": "acme"})
	if err != nil {
		return err
	}
	g1, err := enqueue("report", "globex-1", map[string]string{"tenant": "globex"})
	if err != nil {
		return err
	}
	<-g.started
	<-g.started
	fmt.Printf("  acme-1 %s, acme-2 %s, globex-1 %s\n", status(a1), status(a2), status(g1))
	close(g.gates["acme-1"])
	fmt.Println("  acme-1 finished, started:", <-g.started)
	close(g.gates["acme-2"])
	close(g.gates["globex-1"])

	fmt.Println("drop newest:")
	if _, err := enqueue("sync", "sync-1", nil); err != nil {
		return err
	}
	<-g.started
	// snippet:dropped
	s2, err := enqueue("sync", "sync-2", nil) // the bucket is full
	fmt.Printf("  second sync: error %v, status %s\n", err, status(s2))
	// snippet:end
	close(g.gates["sync-1"])

	fmt.Println("cancel in progress:")
	p1, err := enqueue("preview", "preview-1", nil)
	if err != nil {
		return err
	}
	<-g.started
	p2, err := enqueue("preview", "preview-2", nil)
	if err != nil {
		return err
	}
	fmt.Printf("  started %s; preview-1 %s, preview-2 %s\n", <-g.started, status(p1), status(p2))
	close(g.gates["preview-2"])

	fmt.Println("rate limit:")
	d := map[string]string{"domain": "example.com"}
	for _, n := range []string{"mail-1", "mail-2"} {
		if _, err := enqueue("mail", n, d); err != nil {
			return err
		}
	}
	m3, err := enqueue("mail", "mail-3", d)
	if err != nil {
		return err
	}
	first := []string{<-g.started, <-g.started}
	slices.Sort(first) // the two start on two workers, in either order
	fmt.Printf("  started %s; mail-3 %s\n", strings.Join(first, " and "), status(m3))
	clk.Advance(time.Minute)
	fmt.Printf("  a minute later, started %s\n", <-g.started)
	for _, n := range []string{"mail-1", "mail-2", "mail-3"} {
		close(g.gates[n])
	}

	if err := priority(ctx); err != nil {
		return err
	}
	if err := severalKeys(ctx); err != nil {
		return err
	}
	if err := sharedScope(ctx); err != nil {
		return err
	}
	if err := releaseWhileWaiting(ctx); err != nil {
		return err
	}
	return refusals(ctx)
}

// severalKeys caps one workflow on two keys at once: two syncs per
// connection and one per account. A run starts only when both of its
// keys have room, and takes both at once.
func severalKeys(ctx context.Context) error {
	fmt.Println("several keys:")
	g := gated{started: make(chan string, 8), gates: map[string]chan struct{}{}}
	for _, n := range []string{"acme-1", "acme-2", "widgets-1", "widgets-2"} {
		g.gates[n] = make(chan struct{})
	}
	// snippet:several-keys
	label := func(name string) workflow.ConcurrencyKey {
		return workflow.KeyFunc(func(j workflow.Job) string { return j.Labels[name] })
	}
	syncAccount := workflow.Workflow{
		Name: "sync-account",
		ConcurrencyLimits: []workflow.ConcurrencyLimit{
			{Key: label("connection"), MaxRuns: 2}, // two syncs per connection
			{Key: label("account"), MaxRuns: 1},    // and one per account
		},
		Steps: []workflow.Step{{Name: "pull", Run: g.handler}},
	}
	// snippet:end
	c := memory.New()
	if err := c.Register(syncAccount); err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer c.Stop()
	defer cancel()
	if err := c.Start(ctx); err != nil {
		return err
	}
	refs := map[string]workflow.RunRef{}
	for _, j := range []struct{ name, account, connection string }{
		{"acme-1", "acme", "conn-1"},
		{"acme-2", "acme", "conn-2"},       // acme already has its one sync
		{"widgets-1", "widgets", "conn-1"}, // conn-1 has a second slot
		{"widgets-2", "widgets", "conn-2"}, // widgets already has its one sync
	} {
		ref, err := c.Enqueue(ctx, workflow.Job{Workflow: syncAccount.Name, Labels: map[string]string{
			"name": j.name, "account": j.account, "connection": j.connection,
		}})
		if err != nil {
			return err
		}
		refs[j.name] = ref
	}
	first := []string{<-g.started, <-g.started}
	slices.Sort(first) // the two start on two workers, in either order
	st := func(name string) workflow.Status { s, _ := c.Status(ctx, refs[name]); return s }
	fmt.Printf("  started %s; acme-2 %s, widgets-2 %s\n", strings.Join(first, " and "), st("acme-2"), st("widgets-2"))
	close(g.gates["acme-1"])
	fmt.Println("  acme-1 finished, started:", <-g.started)
	close(g.gates["widgets-1"])
	fmt.Println("  widgets-1 finished, started:", <-g.started)
	close(g.gates["acme-2"])
	close(g.gates["widgets-2"])

	// The Hatchet driver refuses the workflow at Register instead of
	// honouring one key only.
	byExpr := syncAccount
	byExpr.ConcurrencyLimits = []workflow.ConcurrencyLimit{
		{Key: workflow.KeyExpr("input.connection"), MaxRuns: 2},
		{Key: workflow.KeyExpr("input.account"), MaxRuns: 1},
	}
	err := hatchet.New(hatchet.Config{}).Register(byExpr)
	fmt.Println("  hatchet:", errors.Is(err, workflow.ErrUnsupported), "| missing:", missingOf(err))
	return nil
}

// sharedScope caps two workflows together: a sync and a check of one
// account call the same external account, so at most one run of either
// may touch an account at a time. Both limits name the scope "account";
// each workflow still reads the account its own way.
func sharedScope(ctx context.Context) error {
	fmt.Println("shared scope:")
	g := gated{started: make(chan string, 8), gates: map[string]chan struct{}{}}
	for _, n := range []string{"sync-acme", "check-acme", "check-widgets"} {
		g.gates[n] = make(chan struct{})
	}
	// snippet:shared-scope
	account := workflow.KeyFunc(func(j workflow.Job) string { return j.Labels["account"] })
	perAccount := workflow.ConcurrencyLimit{
		Key:     account,
		MaxRuns: 1,         // one run per account at a time
		Scope:   "account", // counted across every workflow naming this scope
	}
	sync := workflow.Workflow{
		Name:              "sync",
		ConcurrencyLimits: []workflow.ConcurrencyLimit{perAccount},
		Steps:             []workflow.Step{{Name: "pull", Run: g.handler}},
	}
	check := workflow.Workflow{
		Name:              "check",
		ConcurrencyLimits: []workflow.ConcurrencyLimit{perAccount},
		Steps:             []workflow.Step{{Name: "verify", Run: g.handler}},
	}
	// snippet:end
	c := memory.New()
	for _, wf := range []workflow.Workflow{sync, check} {
		if err := c.Register(wf); err != nil {
			return err
		}
	}
	ctx, cancel := context.WithCancel(ctx)
	defer c.Stop()
	defer cancel()
	if err := c.Start(ctx); err != nil {
		return err
	}
	enqueue := func(wf, name, acct string) (workflow.RunRef, error) {
		return c.Enqueue(ctx, workflow.Job{Workflow: wf, Labels: map[string]string{"name": name, "account": acct}})
	}
	if _, err := enqueue("sync", "sync-acme", "acme"); err != nil {
		return err
	}
	first := <-g.started
	checkAcme, err := enqueue("check", "check-acme", "acme")
	if err != nil {
		return err
	}
	if _, err := enqueue("check", "check-widgets", "widgets"); err != nil {
		return err
	}
	second := <-g.started
	st, _ := c.Status(ctx, checkAcme)
	fmt.Printf("  started %s; check-acme %s; started %s\n", first, st, second)
	close(g.gates["sync-acme"])
	fmt.Println("  sync-acme finished, started:", <-g.started)
	close(g.gates["check-acme"])
	close(g.gates["check-widgets"])

	// snippet:scope-mismatch
	lighter := check
	lighter.ConcurrencyLimits = []workflow.ConcurrencyLimit{{Key: account, MaxRuns: 2, Scope: "account"}}
	m := memory.New()
	if err := m.Register(sync); err != nil {
		return err
	}
	err = m.Register(lighter) // the scope's cap is the account's, not the workflow's
	// snippet:end
	fmt.Println("  invalid:", errors.Is(err, workflow.ErrInvalid))
	fmt.Println(" ", err)
	return nil
}

// missingOf is the capabilities an UnsupportedError names, or "" for any
// other error.
func missingOf(err error) string {
	var ue *workflow.UnsupportedError
	if errors.As(err, &ue) {
		return ue.Missing.String()
	}
	return ""
}

// releaseWhileWaiting runs one sync per account under each hold policy.
// acme-1's first call is rate limited and its retry is an hour away. With
// the default (HoldWholeRun) acme-1 keeps the account through the hour and
// acme-2 waits for it; with HoldWhileRunning acme-1 gives the account back
// while it waits, acme-2 runs at once, and acme-1 takes the account back
// when its hour has passed.
func releaseWhileWaiting(ctx context.Context) error {
	fmt.Println("release while waiting:")
	for _, hold := range []workflow.ConcurrencyHold{workflow.HoldWholeRun, workflow.HoldWhileRunning} {
		if err := holdPolicy(ctx, hold); err != nil {
			return err
		}
	}
	return nil
}

// holdPolicy is one half of releaseWhileWaiting.
func holdPolicy(ctx context.Context, hold workflow.ConcurrencyHold) error {
	attempts := map[string]int{}
	var mu sync.Mutex
	failed := make(chan struct{}, 1)
	pull := func(ctx context.Context, _ []byte) ([]byte, error) {
		info, _ := workflow.StepInfoFrom(ctx)
		name := info.Labels["name"]
		mu.Lock()
		attempts[name]++
		first := attempts[name] == 1
		mu.Unlock()
		if name == "acme-1" && first {
			failed <- struct{}{}
			return nil, errors.New("rate limited: come back in an hour")
		}
		return nil, nil
	}
	account := workflow.KeyFunc(func(j workflow.Job) string { return j.Labels["account"] })
	// snippet:release-while-waiting
	syncAccount := workflow.Workflow{
		Name: "sync-account",
		Concurrency: &workflow.Concurrency{
			Key:     account,
			MaxRuns: 1,                         // one sync per account at a time
			Hold:    workflow.HoldWhileRunning, // but not while one waits to retry
		},
		Steps: []workflow.Step{{Name: "pull", Run: pull, Retry: &workflow.RetryPolicy{
			Max:     3,
			Backoff: workflow.Backoff{Initial: time.Hour}, // the API asks to come back in an hour
		}}},
	}
	// snippet:end
	syncAccount.Concurrency.Hold = hold
	clk := clock.NewFake(start)
	c := memory.New(memory.WithClock(clk))
	if err := c.Register(syncAccount); err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer c.Stop()
	defer cancel()
	if err := c.Start(ctx); err != nil {
		return err
	}
	enqueue := func(name string) (workflow.RunRef, error) {
		return c.Enqueue(ctx, workflow.Job{Workflow: syncAccount.Name, Labels: map[string]string{"name": name, "account": "acme"}})
	}
	// wait polls ref until it is terminal or a few seconds pass.
	wait := func(ref workflow.RunRef) workflow.Status {
		deadline := time.Now().Add(5 * time.Second)
		for {
			st, _ := c.Status(ctx, ref)
			if st.IsTerminal() || time.Now().After(deadline) {
				return st
			}
			time.Sleep(time.Millisecond)
		}
	}
	timers := clk.Pending()
	a1, err := enqueue("acme-1")
	if err != nil {
		return err
	}
	<-failed
	bctx, bcancel := context.WithTimeout(ctx, 5*time.Second)
	defer bcancel()
	if err := clk.BlockUntil(bctx, timers+1); err != nil { // acme-1's backoff timer
		return err
	}
	a2, err := enqueue("acme-2")
	if err != nil {
		return err
	}
	if hold.WhileRunning() {
		fmt.Printf("  %s: acme-1 retrying in an hour; acme-2 %s at once\n", hold, wait(a2))
		clk.Advance(time.Hour)
		fmt.Printf("    an hour later: acme-1 %s\n", wait(a1))
		return nil
	}
	st, _ := c.Status(ctx, a2)
	fmt.Printf("  %s: acme-1 retrying in an hour; acme-2 %s\n", hold, st)
	clk.Advance(time.Hour)
	fmt.Printf("    an hour later: acme-1 %s, then acme-2 %s\n", wait(a1), wait(a2))
	return nil
}

// priority runs one worker, holds it busy, enqueues three jobs with
// different priorities and prints the order they run in.
func priority(ctx context.Context) error {
	fmt.Println("priority:")
	busy := make(chan struct{})
	holding := make(chan struct{})
	ran := make(chan string, 4)
	c := memory.New(memory.WithWorkers(1))
	err := c.Register(workflow.Workflow{
		Name: "task",
		Steps: []workflow.Step{{Name: "do", Run: func(ctx context.Context, in []byte) ([]byte, error) {
			if string(in) == "blocker" {
				close(holding)
				<-busy
				return nil, nil
			}
			ran <- string(in)
			return nil, nil
		}}},
	})
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer c.Stop()
	defer cancel()
	if err := c.Start(ctx); err != nil {
		return err
	}
	if _, err := c.Enqueue(ctx, workflow.Job{Workflow: "task", Payload: []byte("blocker")}); err != nil {
		return err
	}
	<-holding // the only worker is busy, so the next jobs wait in the queue
	// snippet:priority
	for _, j := range []workflow.Job{
		{Workflow: "task", Payload: []byte("backfill"), Priority: -10},
		{Workflow: "task", Payload: []byte("normal")}, // Priority 0 is the default
		{Workflow: "task", Payload: []byte("password-reset"), Priority: 10},
	} {
		if _, err := c.Enqueue(ctx, j); err != nil {
			return err
		}
	}
	// snippet:end
	close(busy)
	fmt.Printf("  %s, %s, %s\n", <-ran, <-ran, <-ran)
	return nil
}

// refusals shows the key form each driver cannot evaluate: the memory
// driver runs Go functions only and refuses an expression at Start; the
// Hatchet driver evaluates CEL only and refuses a Go function at Register
// (it connects only at Start, so Register runs without a server).
func refusals(ctx context.Context) error {
	fmt.Println("refusals:")
	noop := func(context.Context, []byte) ([]byte, error) { return nil, nil }
	// snippet:key-forms
	byExpr := workflow.Workflow{
		Name:        "report",
		Concurrency: &workflow.Concurrency{Key: workflow.KeyExpr("input.tenant"), MaxRuns: 1},
		Steps:       []workflow.Step{{Name: "build", Run: noop}},
	}
	byFunc := byExpr
	byFunc.Concurrency = &workflow.Concurrency{
		Key:     workflow.KeyFunc(func(j workflow.Job) string { return j.Labels["tenant"] }),
		MaxRuns: 1,
	}

	m := memory.New()
	if err := m.Register(byExpr); err != nil { // accepted here; Start validates the key
		return err
	}
	err := m.Start(ctx)
	fmt.Println("  memory, KeyExpr:", err)
	m.Stop()

	err = hatchet.New(hatchet.Config{}).Register(byFunc)
	fmt.Println("  hatchet, KeyFunc:", err, "| unsupported:", errors.Is(err, workflow.ErrUnsupported))
	// snippet:end
	return nil
}
