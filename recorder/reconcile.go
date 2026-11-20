package recorder

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
)

// Defaults of a Reconciler.
const (
	// DefaultReconcileInterval is the pause between two passes of Run.
	//
	//ds:def id=rec-defaultreconcileinterval-6wpsuxsy owner=@khanakia stability=stable
	DefaultReconcileInterval = 30 * time.Second

	// DefaultStaleAfter is how long an unfinished run may go without any
	// recorded activity before it is flagged stale.
	//
	//ds:def id=rec-defaultstaleafter-ecr2t7eq owner=@khanakia stability=stable
	DefaultStaleAfter = 10 * time.Minute

	// DefaultBackfillWindow is how far back each pass lists the client's
	// runs looking for ones the recorder never saw.
	//
	//ds:def id=rec-defaultbackfillwindow-7nsg79u2 owner=@khanakia stability=stable
	DefaultBackfillWindow = time.Hour

	// DefaultReconcileBatch is how many unfinished records one pass checks,
	// and the page size of the backfill listing.
	//
	//ds:def id=rec-defaultreconcilebatch-6zk3gwwh owner=@khanakia stability=stable
	DefaultReconcileBatch = 100

	// DefaultBackfillPages bounds the pages one backfill pass reads, so a
	// huge window cannot make one pass unbounded.
	//
	//ds:def id=rec-defaultbackfillpages-mejfjkfb owner=@khanakia stability=stable
	DefaultBackfillPages = 10
)

// Reconciler closes the gaps the recorder's seams leave:
//
//   - Check: every unfinished record (oldest check first, a batch per pass)
//     is compared with the client's Status. A run the client reports
//     terminal is finished on the record, with its error and end time when
//     the client is a workflow.RunQuerier. A run the client still reports
//     unfinished takes the client's status, and is flagged Stale when
//     nothing was recorded for it for StaleAfter. A run the client does not
//     know (pruned, or a lost delayed start) is flagged Stale. Nothing is
//     ever cancelled: Stale is for an operator to look at.
//   - Backfill: when the client is a workflow.RunQuerier, the runs it
//     lists enqueued within the backfill window are upserted, so a run
//     started past the recorder (an engine UI or CLI, a crash between
//     accept and record, a workflow registered on the bare driver) gets a
//     record, with SourceExternal when its labels name no source. Upserts
//     only fill, so a run the recorder already holds is left as recorded
//     (its ScheduledID and RunID are joined when the listing shows both).
//
// A Reconciler works on any client; backfill and error details need the
// client to be a workflow.RunQuerier and are skipped otherwise
// (ReconcileReport.BackfillSkipped). Run several Reconcilers over one store
// safely: every write is an upsert.
//
//ds:def id=rec-reconciler-55m8wpc9 owner=@khanakia stability=stable
type Reconciler struct {
	client     workflow.Client
	store      Store
	clk        clock.Clock
	interval   time.Duration
	staleAfter time.Duration
	window     time.Duration
	batch      int
	pages      int
	backfill   bool
	scope      workflow.RunQuery
	onError    func(ctx context.Context, err error)
}

// ReconcileOption configures a Reconciler.
type ReconcileOption func(*Reconciler) error

// WithInterval sets the pause between passes of Run.
//
//ds:def id=rec-withinterval-xygvejf4 owner=@khanakia stability=stable
func WithInterval(d time.Duration) ReconcileOption {
	return func(r *Reconciler) error { return positive("WithInterval", d, &r.interval) }
}

// WithStaleAfter sets how long an unfinished run may be silent before it is
// flagged stale.
//
//ds:def id=rec-withstaleafter-94d6u87k owner=@khanakia stability=stable
func WithStaleAfter(d time.Duration) ReconcileOption {
	return func(r *Reconciler) error { return positive("WithStaleAfter", d, &r.staleAfter) }
}

// WithBackfillWindow sets how far back the backfill lists runs.
//
//ds:def id=rec-withbackfillwindow-tb9q9u8e owner=@khanakia stability=stable
func WithBackfillWindow(d time.Duration) ReconcileOption {
	return func(r *Reconciler) error { return positive("WithBackfillWindow", d, &r.window) }
}

// WithBackfill turns the backfill on or off (on by default).
//
//ds:def id=rec-withbackfill-euvuqmgs owner=@khanakia stability=stable
func WithBackfill(on bool) ReconcileOption {
	return func(r *Reconciler) error { r.backfill = on; return nil }
}

// WithBackfillQuery scopes the backfill listing to the runs q matches
// (a workflow, a tenant, labels), for a client whose engine other apps
// share. Its From, Limit and Cursor are the Reconciler's own and are
// ignored; the rest must pass workflow.ValidateRunQuery.
//
//ds:def id=rec-withbackfillquery-qc3kqct7 owner=@khanakia stability=stable
func WithBackfillQuery(q workflow.RunQuery) ReconcileOption {
	return func(r *Reconciler) error {
		q.From, q.Limit, q.Cursor = time.Time{}, 0, ""
		if err := workflow.ValidateRunQuery(q); err != nil {
			return fmt.Errorf("recorder.WithBackfillQuery: %w", err)
		}
		r.scope = q
		return nil
	}
}

// WithBatch sets how many records one pass checks and the backfill page
// size, from 1 to workflow.MaxRunsLimit.
//
//ds:def id=rec-withbatch-7wqg86rr owner=@khanakia stability=stable
func WithBatch(n int) ReconcileOption {
	return func(r *Reconciler) error {
		if n < 1 || n > workflow.MaxRunsLimit {
			return fmt.Errorf("%w: recorder.WithBatch(%d) is outside [1, %d]", workflow.ErrInvalid, n, workflow.MaxRunsLimit)
		}
		r.batch = n
		return nil
	}
}

// WithBackfillPages bounds the pages one backfill pass reads.
//
//ds:def id=rec-withbackfillpages-eyrfe4qc owner=@khanakia stability=stable
func WithBackfillPages(n int) ReconcileOption {
	return func(r *Reconciler) error {
		if n < 1 {
			return fmt.Errorf("%w: recorder.WithBackfillPages(%d)", workflow.ErrInvalid, n)
		}
		r.pages = n
		return nil
	}
}

// WithReconcileClock sets the clock the Reconciler measures with
// (clock.Real by default); use the recorder's.
//
//ds:def id=rec-withreconcileclock-9n7x2xtd owner=@khanakia stability=stable
func WithReconcileClock(c clock.Clock) ReconcileOption {
	return func(r *Reconciler) error {
		if c == nil {
			return fmt.Errorf("%w: recorder.WithReconcileClock(nil)", workflow.ErrInvalid)
		}
		r.clk = c
		return nil
	}
}

// WithReconcileOnError is told about every error of a pass that did not
// stop it (one run's check or write); Run also hands it a pass's error. By
// default errors are dropped by Pass and logged by Run with slog.Default.
//
//ds:def id=rec-withreconcileonerror-muvwebjc owner=@khanakia stability=stable
func WithReconcileOnError(fn func(ctx context.Context, err error)) ReconcileOption {
	return func(r *Reconciler) error { r.onError = fn; return nil }
}

// positive stores d in dst, refusing zero or less.
func positive(name string, d time.Duration, dst *time.Duration) error {
	if d <= 0 {
		return fmt.Errorf("%w: recorder.%s(%s) is not positive", workflow.ErrInvalid, name, d)
	}
	*dst = d
	return nil
}

// NewReconciler returns a Reconciler of s against c. It fails on a nil
// client or store, or an invalid option, with workflow.ErrInvalid.
//
//ds:def id=rec-newreconciler-su363hcv owner=@khanakia stability=stable
func NewReconciler(c workflow.Client, s Store, opts ...ReconcileOption) (*Reconciler, error) {
	if c == nil || s == nil {
		return nil, fmt.Errorf("%w: recorder.NewReconciler needs a client and a store", workflow.ErrInvalid)
	}
	r := &Reconciler{
		client: c, store: s, clk: clock.Real{}, interval: DefaultReconcileInterval, staleAfter: DefaultStaleAfter,
		window: DefaultBackfillWindow, batch: DefaultReconcileBatch, pages: DefaultBackfillPages, backfill: true,
	}
	for _, o := range opts {
		if o == nil {
			continue
		}
		if err := o(r); err != nil {
			return nil, err
		}
	}
	return r, nil
}

// ReconcileReport is what one Pass did.
//
//ds:def id=rec-reconcilereport-bnz3pwpj owner=@khanakia stability=stable
type ReconcileReport struct {
	// Checked counts unfinished records compared with the client.
	Checked int
	// Finished counts records the pass finished because the client
	// reported them terminal.
	Finished int
	// Stale counts records flagged stale by this pass.
	Stale int
	// Missing counts records the client did not know (also flagged stale).
	Missing int
	// Backfilled counts runs the backfill upserted (new or filled).
	Backfilled int
	// BackfillSkipped is true when the client is not a RunQuerier, so no
	// backfill ran.
	BackfillSkipped bool
	// Errors counts per-run errors that did not stop the pass.
	Errors int
}

// Pass runs one check and one backfill. It returns an error only when it
// could not read the store's pending records or the client's run listing;
// a single run's failure is counted, handed to the error hook, and the
// pass goes on.
//
//ds:def id=rec-reconciler-pass-526p96px owner=@khanakia stability=stable
func (r *Reconciler) Pass(ctx context.Context) (ReconcileReport, error) {
	var rep ReconcileReport
	if err := r.check(ctx, &rep); err != nil {
		return rep, err
	}
	if err := r.backfillPass(ctx, &rep); err != nil {
		return rep, err
	}
	return rep, nil
}

// Run calls Pass every interval until ctx is cancelled and then returns
// nil. A pass's error goes to the error hook (logged by default) and the
// loop goes on.
//
//ds:def id=rec-reconciler-run-hkpv3q3x owner=@khanakia stability=stable
func (r *Reconciler) Run(ctx context.Context) error {
	var wait time.Duration // the first pass runs at once
	for {
		t := r.clk.NewTimer(wait)
		select {
		case <-ctx.Done():
			t.Stop()
			return nil
		case <-t.C():
		}
		if _, err := r.Pass(ctx); err != nil && ctx.Err() == nil {
			r.report(ctx, err, true)
		}
		wait = r.interval
	}
}

// report hands err to the hook; a pass-level error with no hook is logged.
func (r *Reconciler) report(ctx context.Context, err error, passLevel bool) {
	switch {
	case r.onError != nil:
		r.onError(ctx, err)
	case passLevel:
		logError(ctx, "reconcile", workflow.RunRef{}, err)
	}
}

// check compares a batch of unfinished records with the client.
func (r *Reconciler) check(ctx context.Context, rep *ReconcileReport) error {
	pending, err := r.store.Pending(ctx, r.batch)
	if err != nil {
		return fmt.Errorf("recorder reconcile: pending: %w", err)
	}
	querier, _ := r.client.(workflow.RunQuerier)
	for _, rec := range pending {
		if ctx.Err() != nil {
			return nil
		}
		rep.Checked++
		if err := r.checkOne(ctx, querier, rec, rep); err != nil {
			rep.Errors++
			r.report(ctx, fmt.Errorf("recorder reconcile %s: %w", rec.Key(), err), false)
		}
	}
	return nil
}

// checkOne compares one record with the client and writes the verdict.
func (r *Reconciler) checkOne(ctx context.Context, q workflow.RunQuerier, rec RunRecord, rep *ReconcileReport) error {
	now := r.clk.Now()
	info, found, err := r.lookup(ctx, q, rec.Ref)
	if err != nil {
		return err
	}
	if !found {
		rep.Missing++
		rep.Stale++
		return r.store.MarkChecked(ctx, Check{Ref: rec.Ref, Stale: true, At: now})
	}
	if info.Status.IsTerminal() {
		f := Finish{
			Ref: info.Ref, Workflow: info.Workflow, Tenant: info.Tenant, Labels: info.Labels,
			Status: info.Status, Error: info.Error, FailedStep: info.FailedStep, FinishedAt: info.FinishedAt, At: now,
		}
		if f.Ref.IsZero() {
			f.Ref = rec.Ref
		} else {
			f.Ref = joinRefs(rec.Ref, f.Ref)
		}
		if f.FinishedAt.IsZero() {
			f.FinishedAt = now
		}
		rep.Finished++
		return r.store.FinishRun(ctx, f)
	}
	stale := now.Sub(rec.UpdatedAt) >= r.staleAfter
	if stale && !rec.Stale {
		rep.Stale++
	}
	ref := rec.Ref
	if !info.Ref.IsZero() {
		ref = joinRefs(rec.Ref, info.Ref)
		if ref != rec.Ref {
			// The listing revealed the RunID of a delayed run: join the
			// record kept under its ScheduledID with the run's own.
			if err := r.store.UpsertRun(ctx, RunWrite{Ref: ref, At: rec.UpdatedAt}); err != nil {
				return err
			}
		}
	}
	return r.store.MarkChecked(ctx, Check{Ref: ref, Status: info.Status, Stale: stale, At: now})
}

// lookup asks the client about ref: the full RunInfo through the
// RunQuerier when there is one (and it answers), else only the Status.
// found is false when the client does not know the run.
func (r *Reconciler) lookup(ctx context.Context, q workflow.RunQuerier, ref workflow.RunRef) (workflow.RunInfo, bool, error) {
	if q != nil {
		page, err := q.Runs(ctx, workflow.RunQuery{Ref: ref, Limit: 1})
		if err == nil && len(page.Runs) == 1 {
			return page.Runs[0], true, nil
		}
		// An empty page or a refusal falls through to Status, which every
		// client answers.
	}
	st, err := r.client.Status(ctx, ref)
	switch {
	case errors.Is(err, workflow.ErrNotFound):
		return workflow.RunInfo{}, false, nil
	case err != nil:
		return workflow.RunInfo{}, false, err
	}
	return workflow.RunInfo{Ref: ref, Status: st}, true, nil
}

// joinRefs returns a with the IDs b adds.
func joinRefs(a, b workflow.RunRef) workflow.RunRef {
	if a.RunID.IsZero() {
		a.RunID = b.RunID
	}
	if a.ScheduledID.IsZero() {
		a.ScheduledID = b.ScheduledID
	}
	return a
}

// backfillPass upserts the client's runs of the backfill window.
func (r *Reconciler) backfillPass(ctx context.Context, rep *ReconcileReport) error {
	if !r.backfill {
		return nil
	}
	q, ok := r.client.(workflow.RunQuerier)
	if !ok || !r.client.Capabilities().Has(workflow.CapRunQuery) {
		rep.BackfillSkipped = true
		return nil
	}
	now := r.clk.Now()
	query := r.scope
	query.From, query.Limit = now.Add(-r.window), r.batch
	if !query.To.IsZero() && !query.From.Before(query.To) {
		return nil // the scope ends before the window starts
	}
	for page := 0; page < r.pages; page++ {
		res, err := q.Runs(ctx, query)
		if err != nil {
			return fmt.Errorf("recorder reconcile: backfill listing: %w", err)
		}
		for _, info := range res.Runs {
			if err := r.backfillOne(ctx, info, now); err != nil {
				rep.Errors++
				r.report(ctx, fmt.Errorf("recorder backfill %s: %w", KeyOf(info.Ref), err), false)
				continue
			}
			rep.Backfilled++
		}
		if res.Next == "" || ctx.Err() != nil {
			return nil
		}
		query.Cursor = res.Next
	}
	return nil
}

// backfillOne upserts one listed run and, when it is terminal, its end.
// Fill-only upserts leave a run the recorder already holds as recorded.
func (r *Reconciler) backfillOne(ctx context.Context, info workflow.RunInfo, now time.Time) error {
	src, srcRef := workflow.SourceOf(info.Labels)
	if src == "" {
		src = workflow.SourceExternal
	}
	w := RunWrite{
		Ref: info.Ref, Workflow: info.Workflow, Queue: info.Queue, Tenant: info.Tenant, Labels: info.Labels,
		Source: src, SourceRef: srcRef, Status: info.Status, Priority: info.Priority, RunAt: info.RunAt,
		EnqueuedAt: info.EnqueuedAt, At: now,
	}
	// A record that exists is not "touched" by the backfill: its
	// UpdatedAt measures the run's own activity, so keep it.
	existing, err := r.store.Run(ctx, info.Ref)
	found := err == nil
	switch {
	case found:
		w.At = existing.UpdatedAt
	case !errors.Is(err, workflow.ErrNotFound):
		return err
	}
	if err := r.store.UpsertRun(ctx, w); err != nil {
		return err
	}
	// A record the recorder already finished keeps its own end; the check
	// finishes the unfinished ones it holds.
	if !info.Status.IsTerminal() || (found && existing.Status.IsTerminal()) {
		return nil
	}
	finished := info.FinishedAt
	if finished.IsZero() {
		finished = now
	}
	return r.store.FinishRun(ctx, Finish{
		Ref: info.Ref, Workflow: info.Workflow, Tenant: info.Tenant, Status: info.Status,
		Error: info.Error, FailedStep: info.FailedStep, FinishedAt: finished, At: w.At,
	})
}
