// Command recorder-custom-store keeps the run history in a recorder.Store
// of the app's own (docStore, in store.go, built on the recorder's shared
// merge rules and held to the store contract by main_test.go with
// recordertest), and shows the recorder options around it: a Redactor of
// its own that reads PayloadInfo, chained after JSONPaths for one workflow
// with PerWorkflow; body bounds; the error hook that hears of every failed
// history write (ErrorFunc, WriteOp) while the store is down for
// maintenance; and a Reconciler, tuned with its options, that records the
// run the failed writes missed once the store is back.
//
// It is the program behind the redaction, error hook, Reconciler and
// bring-your-own-store sections of docs/observability/run-recorder.md: the
// guide copies its regions (marked "snippet:") and task docs:snippets
// fails when they drift.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/memory"
	"github.com/ubgo/workflow/recorder"
)

// Recorder settings.
const (
	// maxBody is the largest body kept; a larger one is recorded by size.
	maxBody = 4 << 10
	// errorHistory is how many attempt errors a run's record keeps.
	errorHistory = 5
)

// Reconciler settings, each a value an app tunes to its volume.
const (
	// reconcileBatch is how many unfinished records one pass checks, and
	// the page size of the backfill listing.
	reconcileBatch = 50
	// backfillPages bounds the pages one backfill pass reads.
	backfillPages = 4
	// backfillWindow is how far back each pass looks for runs the
	// recorder missed; longer than the longest store outage you expect.
	backfillWindow = 6 * time.Hour
	// staleAfter flags an unfinished run with no recorded activity.
	staleAfter = 15 * time.Minute
	// reconcileEvery is the pause between passes of rc.Run.
	reconcileEvery = time.Minute
)

// Timings of the demo itself.
const (
	// waitFor bounds every wait for a history write, so a write that never
	// lands fails the program instead of hanging it.
	waitFor = 5 * time.Second
	// waitPoll is how often a wait checks again: the writes land in
	// microseconds on the memory driver.
	waitPoll = 5 * time.Millisecond
)

// start is the fake clock's first instant; any fixed time works.
var start = time.Date(2026, 3, 2, 9, 0, 0, 0, time.UTC)

// main runs the example and exits non-zero on error.
func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "recorder-custom-store:", err)
		os.Exit(1)
	}
}

// snippet:redactor
// receipts is a Redactor of the app's own. It reads PayloadInfo to treat
// one step differently: the charge step's output is the payment provider's
// raw response, so none of it is stored (nil keeps the size only), and an
// email in any event attribute is masked down to its domain. It must not
// change the slice it is given, and it runs on the step's goroutine, so it
// stays quick.
type receipts struct{}

// RedactPayload implements recorder.Redactor: it drops the charge step's
// output and keeps every other body as it is.
func (receipts) RedactPayload(info recorder.PayloadInfo, body []byte) []byte {
	if info.Kind == recorder.PayloadStepOutput && info.Step == "charge" {
		return nil
	}
	return body
}

// RedactAttr implements recorder.Redactor: it masks an email attribute
// down to its first letter and domain.
func (receipts) RedactAttr(_ recorder.PayloadInfo, a workflow.EventAttr) workflow.EventAttr {
	if at := strings.IndexByte(a.Value, '@'); a.Key == "email" && at > 0 {
		a.Value = a.Value[:1] + "***" + a.Value[at:]
	}
	return a
}

// snippet:end

// maintenance stands in for the history database during an outage: while
// down, every write fails. Reads pass through.
type maintenance struct {
	recorder.Store
	mu   sync.Mutex
	down bool
}

// errDown is every write's error during the outage.
var errDown = errors.New("history store in maintenance")

// fail is the outage: errDown while down.
func (m *maintenance) fail() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.down {
		return errDown
	}
	return nil
}

// set starts or ends the outage.
func (m *maintenance) set(down bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.down = down
}

// UpsertRun implements recorder.Store: it fails while down, else goes to
// the store.
func (m *maintenance) UpsertRun(ctx context.Context, w recorder.RunWrite) error {
	if err := m.fail(); err != nil {
		return err
	}
	return m.Store.UpsertRun(ctx, w)
}

// StartStep implements recorder.Store: it fails while down, else goes to
// the store.
func (m *maintenance) StartStep(ctx context.Context, s recorder.StepStart) error {
	if err := m.fail(); err != nil {
		return err
	}
	return m.Store.StartStep(ctx, s)
}

// EndStep implements recorder.Store: it fails while down, else goes to
// the store.
func (m *maintenance) EndStep(ctx context.Context, e recorder.StepEnd) error {
	if err := m.fail(); err != nil {
		return err
	}
	return m.Store.EndStep(ctx, e)
}

// AddEvent implements recorder.Store: it fails while down, else goes to
// the store.
func (m *maintenance) AddEvent(ctx context.Context, e recorder.EventWrite) error {
	if err := m.fail(); err != nil {
		return err
	}
	return m.Store.AddEvent(ctx, e)
}

// SetOutcome implements recorder.Store: it fails while down, else goes to
// the store.
func (m *maintenance) SetOutcome(ctx context.Context, o recorder.OutcomeWrite) error {
	if err := m.fail(); err != nil {
		return err
	}
	return m.Store.SetOutcome(ctx, o)
}

// FinishRun implements recorder.Store: it fails while down, else goes to
// the store.
func (m *maintenance) FinishRun(ctx context.Context, f recorder.Finish) error {
	if err := m.fail(); err != nil {
		return err
	}
	return m.Store.FinishRun(ctx, f)
}

// failedOps collects the writes the error hook heard of.
type failedOps struct {
	mu  sync.Mutex
	ops map[recorder.WriteOp]bool
}

// add records that a write of op failed; the error hook calls it from
// driver goroutines, hence the lock.
func (f *failedOps) add(op recorder.WriteOp) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ops[op] = true
}

// has reports whether a write of op has failed, for the program to wait on.
func (f *failedOps) has(op recorder.WriteOp) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.ops[op]
}

// String implements fmt.Stringer: the failed ops, sorted, so the printed
// line is the same on every run.
func (f *failedOps) String() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var names []string
	for op := range f.ops {
		names = append(names, string(op))
	}
	slices.Sort(names)
	return strings.Join(names, " ")
}

// checkout is the workflow: charge returns the provider's raw response,
// notify records an event with the customer's email.
func checkout() workflow.Workflow {
	// charge stands in for the payment call: it returns the provider's raw response.
	charge := func(context.Context, []byte) ([]byte, error) {
		return []byte(`{"provider":"psp","raw":"auth=8812;cvc_check=pass"}`), nil
	}
	// notify stands in for the receipt mail: it records an event naming the customer's email.
	notify := func(ctx context.Context, _ []byte) ([]byte, error) {
		workflow.RecordEvent(ctx, "receipt sent", slog.String("email", "ann@example.com"))
		return []byte(`{"sent":true}`), nil
	}
	return workflow.Workflow{Name: "checkout", Steps: []workflow.Step{
		{Name: "charge", Run: charge},
		{Name: "notify", After: []string{"charge"}, Run: notify},
	}}
}

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context, w io.Writer) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	clk := clock.NewFake(start)
	store := newDocStore()
	db := &maintenance{Store: store}
	failed := &failedOps{ops: map[recorder.WriteOp]bool{}}

	// snippet:recorder
	cards, err := recorder.JSONPaths("card.number")
	if err != nil {
		return err
	}
	// checkout gets both redactors, in order; every other workflow the
	// card paths only.
	redact := recorder.PerWorkflow(map[string]recorder.Redactor{
		"checkout": recorder.Chain(cards, receipts{}),
	}, cards)
	// onError is the error hook, a recorder.ErrorFunc: told about each
	// failed history write while the run goes on, on the goroutine that
	// made the write, so it only counts.
	var onError recorder.ErrorFunc = func(_ context.Context, op recorder.WriteOp, ref workflow.RunRef, err error) {
		failed.add(op) // in an app: a counter by op, and a log line with ref and err
	}
	rec, err := recorder.New(db,
		recorder.WithRedactor(redact),
		recorder.WithMaxPayload(maxBody),        // larger bodies are kept by size only
		recorder.WithStepBodies(true),           // the default; false keeps only the run's input and result
		recorder.WithErrorHistory(errorHistory), // attempt errors kept per run
		recorder.WithClock(clk),                 // the driver's clock, so records and driver agree
		recorder.WithOnError(onError),           // every failed write, by its WriteOp
	)
	if err != nil {
		return err
	}
	// snippet:end
	mem := memory.New(memory.WithClock(clk))
	c, err := workflow.Use(mem, rec.Plugin())
	if err != nil {
		return err
	}
	if err := c.Register(checkout()); err != nil {
		return err
	}
	if err := c.Start(ctx); err != nil {
		return err
	}
	// enqueue starts a checkout for order, with a card number for the
	// redactor to hide and the order as the run's source.
	enqueue := func(order string) (workflow.RunRef, error) {
		job := workflow.Job{Workflow: "checkout", Payload: fmt.Appendf(nil, `{"order":%q,"card":{"number":"4111111111111111"}}`, order)}
		return c.Enqueue(ctx, workflow.WithSource(job, "api", order))
	}

	first, err := enqueue("order-1")
	if err != nil {
		return err
	}
	if err := waitUntil(func() bool { r, err := store.Run(ctx, first); return err == nil && r.Status.IsTerminal() }); err != nil {
		return err
	}
	r, err := store.Run(ctx, first)
	if err != nil {
		return err
	}
	fmt.Fprintf(w, "input as stored: %s\n", r.Payload)
	for _, s := range r.Steps {
		out := string(s.Output)
		if s.Output == nil {
			out = "none"
		}
		fmt.Fprintf(w, "  step %s output=%s (%d bytes before redaction)\n", s.Step, out, s.OutputSize)
		for _, e := range s.Events {
			fmt.Fprintf(w, "    event %q %v\n", e.Message, e.Attrs)
		}
	}

	fmt.Fprintln(w, "-- the history store goes into maintenance")
	db.set(true)
	second, err := enqueue("order-2")
	if err != nil {
		return err
	}
	if err := waitUntil(func() bool { return failed.has(recorder.OpFinishRun) }); err != nil {
		return err
	}
	st, err := c.Status(ctx, second)
	if err != nil {
		return err
	}
	_, err = store.Run(ctx, second)
	fmt.Fprintf(w, "history writes that failed: %s\n", failed)
	fmt.Fprintf(w, "run status from the driver: %s; in the history: %v\n", st, errors.Is(err, workflow.ErrNotFound))
	db.set(false)

	fmt.Fprintln(w, "-- maintenance over: one Reconciler pass")
	// snippet:reconciler
	rc, err := recorder.NewReconciler(c, store,
		recorder.WithReconcileClock(clk),            // the recorder's clock
		recorder.WithBatch(reconcileBatch),          // records checked per pass, and the backfill page size
		recorder.WithBackfill(true),                 // the default: list the client's recent runs too
		recorder.WithBackfillWindow(backfillWindow), // how far back the listing reaches
		recorder.WithBackfillPages(backfillPages),   // and how many pages one pass reads at most
		recorder.WithStaleAfter(staleAfter),
		recorder.WithInterval(reconcileEvery), // between passes of rc.Run
		recorder.WithReconcileOnError(func(_ context.Context, err error) {
			fmt.Fprintln(w, "reconcile error:", err) // one run's failure; the pass goes on
		}),
	)
	if err != nil {
		return err
	}
	rep, err := rc.Pass(ctx) // go rc.Run(ctx) next to the client in an app
	if err != nil {
		return err
	}
	fmt.Fprintf(w, "reconcile: checked=%d finished=%d backfilled=%d errors=%d\n", rep.Checked, rep.Finished, rep.Backfilled, rep.Errors)
	// snippet:end
	r, err = store.Run(ctx, second)
	if err != nil {
		return err
	}
	fmt.Fprintf(w, "second run in the history: %s source=%s/%s steps=%d\n", r.Status, r.Source, r.SourceRef, len(r.Steps))
	return nil
}

// waitUntil polls cond until it holds, for writes that land on driver
// goroutines.
func waitUntil(cond func() bool) error {
	deadline := time.Now().Add(waitFor)
	for time.Now().Before(deadline) {
		if cond() {
			return nil
		}
		time.Sleep(waitPoll)
	}
	return fmt.Errorf("condition not met within %s", waitFor)
}
