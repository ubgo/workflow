package recordertest

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/recorder"
	"github.com/ubgo/workflow/workflowtest"
)

// Timings of the driver cases, per clock kind, matching the workflowtest
// defaults: a fake clock moves only when told, so its waits are short.
const (
	fakeWait = 5 * time.Second
	realWait = 90 * time.Second
	fakePoll = 2 * time.Millisecond
	realPoll = 200 * time.Millisecond
	// fakeStep is how far each poll moves a fake clock, enough to pass any
	// retry backoff the cases set.
	fakeStep = time.Second
)

// driverEnv is one driver case: the wrapped client, its store and clock.
type driverEnv struct {
	t        *testing.T
	base     workflow.Client
	c        workflow.Client
	store    recorder.Store
	newStore NewStore
	rec      *recorder.Recorder
	clk      *clock.Fake
	prefix   string
	wait     time.Duration
	poll     time.Duration
	errMu    sync.Mutex
	errs     []error
}

// RunDriver runs the recorder over the driver h builds, through only the
// public seams (workflow.Use with the recorder's plugin), and checks the
// records it leaves: a run with its source, steps, outputs, events and set
// outcome; a run that retried and failed with its error history and
// default outcome; redaction of every stored body and attribute; a delayed
// run cancelled before it started; and the Reconciler, which backfills a
// run the recorder never saw, finishes a record whose end it missed and
// flags one the client does not know. Cases a driver cannot run (no
// CapDelayedStart, no CapRunQuery) are skipped with the reason.
//
// Only Harness.New, Prefix, Wait and Poll are read; the recorder needs no
// restart, crash or queue seams.
func RunDriver(t *testing.T, h workflowtest.Harness, opts ...DriverOption) {
	t.Helper()
	var cfg driverConfig
	for _, o := range opts {
		if o != nil {
			o(&cfg)
		}
	}
	if h.New == nil {
		t.Fatal("recordertest: Harness.New is nil")
	}
	nonce := make([]byte, 4)
	if _, err := rand.Read(nonce); err != nil {
		t.Fatal(err)
	}
	prefix := h.Prefix
	if prefix == "" {
		prefix = "rectest"
	}
	prefix += "-" + hex.EncodeToString(nonce)
	cases := []struct {
		name string
		run  func(e *driverEnv)
	}{
		{"records_run_steps_events_outcome", driverCaseSuccess},
		{"records_retries_and_failure", driverCaseFailure},
		{"redacts_and_bounds_bodies", driverCaseRedaction},
		{"delayed_run_cancelled", driverCaseDelayedCancel},
		{"reconciler_backfills_external_run", driverCaseBackfill},
		{"reconciler_finishes_and_flags", driverCaseReconcile},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			in := h.New(t)
			if in.Client == nil {
				t.Fatal("recordertest: Harness.New returned a nil Client")
			}
			e := &driverEnv{t: t, base: in.Client, clk: in.Clock, prefix: prefix + "-" + c.name, wait: realWait, poll: realPoll, newStore: cfg.newStore}
			if in.Clock != nil {
				e.wait, e.poll = fakeWait, fakePoll
			}
			if h.Wait > 0 {
				e.wait = h.Wait
			}
			if h.Poll > 0 {
				e.poll = h.Poll
			}
			c.run(e)
		})
	}
}

// DriverOption configures RunDriver.
type DriverOption func(*driverConfig)

// driverConfig is what the options set.
type driverConfig struct {
	newStore NewStore
}

// WithStore makes RunDriver record into stores newStore builds instead of a
// recorder.MemoryStore, so a store and a driver are proven together (the
// SQL store next to the SQL driver, say).
func WithStore(newStore NewStore) DriverOption {
	return func(c *driverConfig) { c.newStore = newStore }
}

// name is a workflow name unique to this case and suite run, so a shared
// engine never mixes runs of two suite runs.
func (e *driverEnv) name(suffix string) string {
	return strings.ReplaceAll(e.prefix+"-"+suffix, "_", "-")
}

// recClock is the clock records are stamped with: the driver's fake clock,
// else real time.
func (e *driverEnv) recClock() clock.Clock {
	if e.clk != nil {
		return e.clk
	}
	return clock.Real{}
}

// wrap builds the store and recorder and applies the plugin to the client.
func (e *driverEnv) wrap(opts ...recorder.Option) {
	e.t.Helper()
	var store recorder.Store
	if e.newStore != nil {
		store = e.newStore(e.t, StoreConfig{Clock: e.recClock()})
	} else {
		ms, err := recorder.NewMemoryStore(recorder.WithMemoryClock(e.recClock()))
		if err != nil {
			e.t.Fatal(err)
		}
		store = ms
	}
	base := []recorder.Option{
		recorder.WithClock(e.recClock()),
		recorder.WithOnError(func(_ context.Context, op recorder.WriteOp, ref workflow.RunRef, err error) {
			e.errMu.Lock()
			defer e.errMu.Unlock()
			e.errs = append(e.errs, fmt.Errorf("%s %s: %w", op, recorder.KeyOf(ref), err))
		}),
	}
	if rr, ok := e.base.(workflow.ResultReader); ok && e.base.Capabilities().Has(workflow.CapResult) {
		base = append(base, recorder.WithResultReader(rr))
	}
	rec, err := recorder.New(store, append(base, opts...)...)
	if err != nil {
		e.t.Fatal(err)
	}
	c, err := workflow.Use(e.base, rec.Plugin())
	if err != nil {
		e.t.Fatalf("workflow.Use(recorder): %v", err)
	}
	e.store, e.rec, e.c = store, rec, c
	e.t.Cleanup(func() {
		e.errMu.Lock()
		defer e.errMu.Unlock()
		for _, err := range e.errs {
			e.t.Errorf("recorder write failed: %v", err)
		}
	})
}

// start registers wfs on the wrapped client (bare holds workflows to
// register on the base client, past the recorder) and starts it.
func (e *driverEnv) start(wfs []workflow.Workflow, bare ...workflow.Workflow) {
	e.t.Helper()
	for _, wf := range wfs {
		if err := e.c.Register(wf); err != nil {
			e.t.Fatalf("Register(%s): %v", wf.Name, err)
		}
	}
	for _, wf := range bare {
		if err := e.base.Register(wf); err != nil {
			e.t.Fatalf("Register bare (%s): %v", wf.Name, err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	e.t.Cleanup(cancel)
	if err := e.c.Start(ctx); err != nil {
		e.t.Fatalf("Start: %v", err)
	}
}

// enqueue enqueues job on c or fails.
func (e *driverEnv) enqueue(c workflow.Client, job workflow.Job) workflow.RunRef {
	e.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), e.wait)
	defer cancel()
	ref, err := c.Enqueue(ctx, job)
	if err != nil {
		e.t.Fatalf("Enqueue(%s): %v", job.Workflow, err)
	}
	return ref
}

// eventually polls cond, moving a fake clock forward between polls.
func (e *driverEnv) eventually(desc string, cond func() bool) {
	e.t.Helper()
	deadline := time.Now().Add(e.wait)
	for !cond() {
		if time.Now().After(deadline) {
			e.t.Fatalf("timed out after %s waiting for %s", e.wait, desc)
		}
		if e.clk != nil {
			e.clk.Advance(fakeStep)
		}
		time.Sleep(e.poll)
	}
}

// waitRecord waits until the record of ref satisfies cond and returns it.
func (e *driverEnv) waitRecord(ref workflow.RunRef, desc string, cond func(recorder.RunRecord) bool) recorder.RunRecord {
	e.t.Helper()
	var last recorder.RunRecord
	e.eventually(desc, func() bool {
		r, err := e.store.Run(context.Background(), ref)
		if err != nil {
			return false
		}
		last = r
		return cond(r)
	})
	return last
}

// terminal is a waitRecord condition: the record is finished.
func terminal(r recorder.RunRecord) bool { return r.Status.IsTerminal() }

func driverCaseSuccess(e *driverEnv) {
	t := e.t
	e.wrap()
	wf := workflow.Workflow{Name: e.name("ok"), Steps: []workflow.Step{
		{Name: "fetch", Run: func(ctx context.Context, in []byte) ([]byte, error) {
			workflow.RecordEvent(ctx, "downloaded", slog.Int("rows", 4000))
			return []byte(`{"rows":4000}`), nil
		}},
		{Name: "store", After: []string{"fetch"}, Run: func(ctx context.Context, in []byte) ([]byte, error) {
			if err := workflow.SetOutcome(ctx, workflow.OutcomePartial, "3 of 4000 rows rejected"); err != nil {
				return nil, err
			}
			return []byte(`"done"`), nil
		}},
	}}
	e.start([]workflow.Workflow{wf})
	job := workflow.WithSource(workflow.Job{Workflow: wf.Name, Payload: []byte(`{"shop":"s1"}`), Labels: map[string]string{"team": "data"}}, "webhook", "delivery-7")
	ref := e.enqueue(e.c, job)
	r := e.waitRecord(ref, "a succeeded record", terminal)
	if r.Status != workflow.StatusSucceeded {
		t.Fatalf("status %q error %q", r.Status, r.Error)
	}
	if r.Source != "webhook" || r.SourceRef != "delivery-7" || r.Labels["team"] != "data" || r.Workflow != wf.Name {
		t.Fatalf("identity: source %q ref %q labels %v wf %q", r.Source, r.SourceRef, r.Labels, r.Workflow)
	}
	if string(r.Payload) != `{"shop":"s1"}` || r.EnqueuedAt.IsZero() || r.StartedAt.IsZero() || r.FinishedAt.IsZero() {
		t.Fatalf("payload %s enq %v start %v fin %v", r.Payload, r.EnqueuedAt, r.StartedAt, r.FinishedAt)
	}
	if r.Outcome != workflow.OutcomePartial || r.OutcomeNote != "3 of 4000 rows rejected" {
		t.Fatalf("outcome %q %q", r.Outcome, r.OutcomeNote)
	}
	if e.base.Capabilities().Has(workflow.CapResult) && string(r.Output) != `"done"` {
		t.Fatalf("result = %s, want the sink step's output", r.Output)
	}
	if len(r.Steps) != 2 || r.Steps[0].Step != "fetch" || r.Steps[1].Step != "store" {
		t.Fatalf("steps = %+v", r.Steps)
	}
	f := r.Steps[0]
	if f.Status != recorder.StepSucceeded || string(f.Output) != `{"rows":4000}` || f.Attempt != 1 {
		t.Fatalf("fetch = %+v", f)
	}
	if len(f.Events) != 1 || f.Events[0].Message != "downloaded" || len(f.Events[0].Attrs) != 1 || f.Events[0].Attrs[0] != (workflow.EventAttr{Key: "rows", Value: "4000"}) {
		t.Fatalf("events = %+v", f.Events)
	}
	if string(r.Steps[1].Input) != `{"rows":4000}` {
		t.Fatalf("store input = %s, want fetch's output", r.Steps[1].Input)
	}
	// The store answers workflow.RunQuerier with the same run, and the
	// recorder-only filters find it.
	page, err := e.store.Runs(context.Background(), workflow.RunQuery{Workflow: wf.Name})
	if err != nil || len(page.Runs) != 1 || page.Runs[0].Status != workflow.StatusSucceeded {
		t.Fatalf("Runs = %+v, %v", page, err)
	}
	rp, err := e.store.Records(context.Background(), recorder.Query{Sources: []workflow.Source{"webhook"}, Outcomes: []workflow.Outcome{workflow.OutcomePartial}, Search: "delivery-7"})
	if err != nil || len(rp.Runs) != 1 {
		t.Fatalf("Records by source/outcome/search = %+v, %v", rp, err)
	}
}

func driverCaseFailure(e *driverEnv) {
	t := e.t
	e.wrap()
	boom := errors.New("upstream said no")
	wf := workflow.Workflow{Name: e.name("fail"), Retry: workflow.RetryPolicy{Max: 1}, Steps: []workflow.Step{
		{Name: "call", Run: func(context.Context, []byte) ([]byte, error) { return nil, boom }},
	}}
	e.start([]workflow.Workflow{wf})
	ref := e.enqueue(e.c, workflow.Job{Workflow: wf.Name, Payload: []byte(`{}`)})
	r := e.waitRecord(ref, "a failed record", terminal)
	if r.Status != workflow.StatusFailed {
		t.Fatalf("status = %q, want failed", r.Status)
	}
	if r.Attempts != 2 || len(r.Steps) != 2 || r.Steps[0].Status != recorder.StepFailed || r.Steps[1].Attempt != 2 {
		t.Fatalf("attempts %d steps %+v", r.Attempts, r.Steps)
	}
	if len(r.Errors) != 2 || !strings.Contains(r.Errors[1].Error, "upstream said no") || !strings.Contains(r.Error, "upstream said no") {
		t.Fatalf("errors %+v error %q", r.Errors, r.Error)
	}
	if r.Outcome != workflow.OutcomeError || r.OutcomeSet {
		t.Fatalf("outcome %q set %v, want the default error", r.Outcome, r.OutcomeSet)
	}
	if r.Source != "" {
		t.Fatalf("source = %q, want none (no label, no default)", r.Source)
	}
}

func driverCaseRedaction(e *driverEnv) {
	t := e.t
	red, err := recorder.JSONPaths("password", "card.number", "token")
	if err != nil {
		t.Fatal(err)
	}
	e.wrap(recorder.WithRedactor(red), recorder.WithMaxPayload(256), recorder.WithDefaultSource("api"))
	big := `"` + strings.Repeat("x", 300) + `"`
	wf := workflow.Workflow{Name: e.name("redact"), Steps: []workflow.Step{
		{Name: "s", Run: func(ctx context.Context, in []byte) ([]byte, error) {
			workflow.RecordEvent(ctx, "auth", slog.String("token", "secret-token"), slog.String("user", "u1"))
			return []byte(big), nil
		}},
	}}
	e.start([]workflow.Workflow{wf})
	ref := e.enqueue(e.c, workflow.Job{Workflow: wf.Name, Payload: []byte(`{"user":"u1","password":"hunter2","card":{"number":"4111"}}`)})
	r := e.waitRecord(ref, "a finished record", terminal)
	for name, body := range map[string][]byte{"run input": r.Payload, "step input": r.Steps[0].Input} {
		s := string(body)
		if strings.Contains(s, "hunter2") || strings.Contains(s, "4111") || !strings.Contains(s, recorder.Redacted) || !strings.Contains(s, "u1") {
			t.Fatalf("%s not redacted: %s", name, s)
		}
	}
	if r.Steps[0].Output != nil || r.Steps[0].OutputSize != len(big) {
		t.Fatalf("an output over MaxPayload was stored: %d bytes, size %d", len(r.Steps[0].Output), r.Steps[0].OutputSize)
	}
	ev := r.Steps[0].Events[0]
	if ev.Attrs[0].Value != recorder.Redacted || ev.Attrs[1].Value != "u1" {
		t.Fatalf("event attrs = %+v", ev.Attrs)
	}
	if r.Source != "api" {
		t.Fatalf("default source = %q", r.Source)
	}
}

func driverCaseDelayedCancel(e *driverEnv) {
	t := e.t
	if !e.base.Capabilities().Has(workflow.CapDelayedStart | workflow.CapCancel) {
		t.Skip("needs CapDelayedStart and CapCancel")
	}
	e.wrap()
	wf := workflow.Workflow{Name: e.name("later"), Steps: []workflow.Step{
		{Name: "s", Run: func(context.Context, []byte) ([]byte, error) { return nil, nil }},
	}}
	e.start([]workflow.Workflow{wf})
	now := time.Now()
	if e.clk != nil {
		now = e.clk.Now()
	}
	ref := e.enqueue(e.c, workflow.Job{Workflow: wf.Name, RunAt: now.Add(24 * time.Hour)})
	r, err := e.store.Run(context.Background(), ref)
	if err != nil {
		t.Fatalf("the delayed run was not recorded at enqueue: %v", err)
	}
	if r.Status != workflow.StatusScheduled && r.Status != workflow.StatusQueued {
		t.Fatalf("status at enqueue = %q", r.Status)
	}
	ctx, cancel := context.WithTimeout(context.Background(), e.wait)
	defer cancel()
	if err := e.c.Cancel(ctx, ref); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	r = e.waitRecord(ref, "a cancelled record", terminal)
	if r.Status != workflow.StatusCancelled || len(r.Steps) != 0 || r.Outcome != "" {
		t.Fatalf("cancelled record = %+v", r)
	}
}

func driverCaseBackfill(e *driverEnv) {
	t := e.t
	if !e.base.Capabilities().Has(workflow.CapRunQuery) {
		t.Skip("needs CapRunQuery")
	}
	if _, ok := e.base.(workflow.RunQuerier); !ok {
		t.Skip("the client is not a workflow.RunQuerier")
	}
	e.wrap()
	seen := workflow.Workflow{Name: e.name("seen"), Steps: []workflow.Step{{Name: "s", Run: func(context.Context, []byte) ([]byte, error) { return nil, nil }}}}
	unseen := workflow.Workflow{Name: e.name("unseen"), Steps: []workflow.Step{{Name: "s", Run: func(context.Context, []byte) ([]byte, error) { return []byte("x"), nil }}}}
	e.start([]workflow.Workflow{seen}, unseen)
	// Enqueued on the bare client, past the enqueue seam; registered on
	// it, past the step seam. Only the run-finished seam, which observes
	// the whole client, may see it end.
	ref := e.enqueue(e.base, workflow.Job{Workflow: unseen.Name, Labels: map[string]string{"origin": "engine-ui"}})
	e.eventually("the unseen run to end", func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), e.wait)
		defer cancel()
		st, err := e.base.Status(ctx, ref)
		return err == nil && st.IsTerminal()
	})
	// A fake clock moves fast while polling; real time needs only the
	// last minutes, and an engine shared with other suites is scoped to
	// this case's workflow.
	window := 15 * time.Minute
	if e.clk != nil {
		window = 48 * time.Hour
	}
	rc, err := recorder.NewReconciler(e.base, e.store, recorder.WithReconcileClock(e.recClock()),
		recorder.WithBackfillWindow(window), recorder.WithBackfillQuery(workflow.RunQuery{Workflow: unseen.Name}))
	if err != nil {
		t.Fatal(err)
	}
	var rep recorder.ReconcileReport
	e.eventually("the backfill to record the run", func() bool {
		rep, err = rc.Pass(context.Background())
		if err != nil {
			t.Fatalf("Pass: %v", err)
		}
		r, err := e.store.Run(context.Background(), ref)
		return err == nil && r.Status.IsTerminal() && r.Workflow == unseen.Name
	})
	if rep.BackfillSkipped {
		t.Fatal("backfill skipped on a RunQuerier")
	}
	r := get(t, e.store, ref)
	if r.Status != workflow.StatusSucceeded || r.Labels["origin"] != "engine-ui" {
		t.Fatalf("backfilled record = %+v", r)
	}
	// The run-finished seam may have recorded the end before the backfill;
	// either way a run started past the recorder carries no source but
	// SourceExternal or none.
	if r.Source != workflow.SourceExternal && r.Source != "" {
		t.Fatalf("source = %q", r.Source)
	}
	// A second pass changes nothing.
	before := get(t, e.store, ref)
	if _, err := rc.Pass(context.Background()); err != nil {
		t.Fatal(err)
	}
	after := get(t, e.store, ref)
	if before.Status != after.Status || before.Source != after.Source || !before.UpdatedAt.Equal(after.UpdatedAt) {
		t.Fatalf("second pass changed the record: %+v -> %+v", before, after)
	}
}

func driverCaseReconcile(e *driverEnv) {
	t := e.t
	e.wrap()
	quiet := workflow.Workflow{Name: e.name("quiet"), Steps: []workflow.Step{{Name: "s", Run: func(context.Context, []byte) ([]byte, error) { return nil, nil }}}}
	e.start(nil, quiet)
	ctx := context.Background()
	// A record the recorder holds as queued, whose run ends unseen (bare
	// workflow; a driver without guaranteed run-finished delivery may
	// also lose the end).
	ref := e.enqueue(e.base, workflow.Job{Workflow: quiet.Name})
	now := e.recClock().Now()
	must(t, e.store.UpsertRun(ctx, recorder.RunWrite{Ref: ref, Workflow: quiet.Name, Status: workflow.StatusQueued, EnqueuedAt: now, At: now}))
	// A record of a run the client has never heard of.
	ghost := workflow.RunRef{RunID: workflow.RunID(e.name("ghost-run"))}
	must(t, e.store.UpsertRun(ctx, recorder.RunWrite{Ref: ghost, Workflow: quiet.Name, Status: workflow.StatusQueued, EnqueuedAt: now, At: now}))

	rc, err := recorder.NewReconciler(e.base, e.store, recorder.WithReconcileClock(e.recClock()), recorder.WithBackfill(false))
	if err != nil {
		t.Fatal(err)
	}
	e.eventually("the reconciler to finish the record", func() bool {
		if _, err := rc.Pass(ctx); err != nil {
			t.Fatalf("Pass: %v", err)
		}
		r, err := e.store.Run(ctx, ref)
		return err == nil && r.Status.IsTerminal()
	})
	if r := get(t, e.store, ref); r.Status != workflow.StatusSucceeded {
		t.Fatalf("reconciled status = %q", r.Status)
	}
	g := get(t, e.store, ghost)
	if !g.Stale || g.Status != workflow.StatusQueued || g.CheckedAt.IsZero() {
		t.Fatalf("ghost record = stale %v status %q checked %v", g.Stale, g.Status, g.CheckedAt)
	}
}
