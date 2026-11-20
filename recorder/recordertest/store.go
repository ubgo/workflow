// Package recordertest holds the two conformance suites of the recorder:
// RunStore holds any recorder.Store to the Store contract (the in-memory
// store, sqlrecorder on every database, an app's own store), and RunDriver
// runs a Recorder over any workflow driver through a workflowtest.Harness,
// so the recorder is proven on memory, SQL, Redis and Hatchet alike.
package recordertest

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/recorder"
)

// StoreConfig is what a store under test is built with. A store that has
// no retention ignores Retention; the prune cases then skip (they run only
// when the store implements recorder.Pruner).
//
//ds:def id=rectest-storeconfig-ehg5t6ys owner=@khanakia stability=stable
type StoreConfig struct {
	// Retention is the store's retention.
	Retention recorder.Retention

	// Clock is the clock the store measures retention ages with.
	Clock clock.Clock
}

// NewStore builds a fresh, empty store for one subtest; cleanup goes on t.
//
//ds:def id=rectest-newstore-ppek35b3 owner=@khanakia stability=stable
type NewStore func(t *testing.T, cfg StoreConfig) recorder.Store

// t0 is the first instant of every store case.
var t0 = time.Date(2026, 3, 2, 9, 0, 0, 0, time.UTC)

// at is t0 plus n seconds, so cases read as a timeline.
func at(n int) time.Time { return t0.Add(time.Duration(n) * time.Second) }

// storeCase is one assertion of the store contract.
type storeCase struct {
	name string
	run  func(t *testing.T, newStore NewStore)
}

// RunStore runs the Store contract against stores made by newStore: upserts
// that fill but never overwrite, step attempts, events, outcome, finish and
// re-open, the join of a delayed run's two IDs, listings with every filter
// and paging, Pending and MarkChecked, copies on read, concurrent writes,
// the refusal of malformed writes, and, when the store is a Pruner, the two
// retention stages with archive.
//
//ds:def id=rectest-runstore-w3fd43ef owner=@khanakia stability=stable
func RunStore(t *testing.T, newStore NewStore) {
	t.Helper()
	if newStore == nil {
		t.Fatal("recordertest: newStore is nil")
	}
	for _, c := range storeCases {
		t.Run(c.name, func(t *testing.T) { c.run(t, newStore) })
	}
}

var storeCases = []storeCase{
	{"upsert_fills_never_overwrites", caseUpsertFills},
	{"refuses_malformed_writes", caseRefusals},
	{"step_lifecycle_and_events", caseStepLifecycle},
	{"retries_and_error_history", caseRetries},
	{"repeated_start_counts_once", caseRepeatedStart},
	{"finish_outcome_and_reopen", caseFinish},
	{"delayed_run_joins_ids", caseJoinIDs},
	{"listing_order_and_paging", caseListing},
	{"listing_filters", caseFilters},
	{"pending_and_checks", casePending},
	{"reads_are_copies", caseCopies},
	{"concurrent_writes", caseConcurrent},
	{"prune_two_stages", casePrune},
	{"prune_archive_failure", casePruneArchiveFailure},
}

// fresh builds a store with no retention.
func fresh(t *testing.T, newStore NewStore) recorder.Store {
	t.Helper()
	s := newStore(t, StoreConfig{Clock: clock.Real{}})
	if s == nil {
		t.Fatal("recordertest: newStore returned nil")
	}
	return s
}

// must fails the test on err.
func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

// get reads one record or fails.
func get(t *testing.T, s recorder.Store, ref workflow.RunRef) recorder.RunRecord {
	t.Helper()
	r, err := s.Run(context.Background(), ref)
	if err != nil {
		t.Fatalf("Run(%s): %v", recorder.KeyOf(ref), err)
	}
	return r
}

// rid is a ref holding only a RunID.
func rid(id string) workflow.RunRef { return workflow.RunRef{RunID: workflow.RunID(id)} }

func caseUpsertFills(t *testing.T, newStore NewStore) {
	ctx := context.Background()
	s := fresh(t, newStore)
	ref := rid("r1")
	w := recorder.RunWrite{
		Ref: ref, Workflow: "export", Queue: "default", Tenant: "acme", Labels: map[string]string{"k": "v"},
		Source: "webhook", SourceRef: "d1", Status: workflow.StatusQueued, Payload: []byte(`{"a":1}`), PayloadSize: 7,
		Priority: 3, EnqueuedAt: at(10), At: at(10),
	}
	must(t, s.UpsertRun(ctx, w))
	// A duplicate with other values changes nothing it already holds, but
	// an earlier enqueue time wins.
	dup := w
	dup.Workflow, dup.Tenant, dup.Source, dup.Payload, dup.Status = "other", "zzz", "api", []byte(`{}`), workflow.StatusRunning
	dup.EnqueuedAt, dup.At = at(5), at(11)
	must(t, s.UpsertRun(ctx, dup))
	r := get(t, s, ref)
	if r.Workflow != "export" || r.Queue != "default" || r.Tenant != "acme" || r.Labels["k"] != "v" {
		t.Fatalf("identity overwritten: %+v", r)
	}
	if r.Source != "webhook" || r.SourceRef != "d1" || string(r.Payload) != `{"a":1}` || r.PayloadSize != 7 || r.Priority != 3 {
		t.Fatalf("fields overwritten: %+v", r)
	}
	if r.Status != workflow.StatusQueued {
		t.Fatalf("status = %q, want queued", r.Status)
	}
	if !r.EnqueuedAt.Equal(at(5)) || !r.UpdatedAt.Equal(at(11)) {
		t.Fatalf("EnqueuedAt %v UpdatedAt %v", r.EnqueuedAt, r.UpdatedAt)
	}
	if len(r.Steps) != 0 {
		t.Fatalf("steps = %v", r.Steps)
	}
	if _, err := s.Run(ctx, rid("nope")); !errors.Is(err, workflow.ErrNotFound) {
		t.Fatalf("unknown run: %v, want ErrNotFound", err)
	}
}

func caseRefusals(t *testing.T, newStore NewStore) {
	ctx := context.Background()
	s := fresh(t, newStore)
	sched := workflow.RunRef{ScheduledID: "s1"}
	checks := []struct {
		name string
		err  error
	}{
		{"UpsertRun empty ref", s.UpsertRun(ctx, recorder.RunWrite{})},
		{"StartStep without RunID", s.StartStep(ctx, recorder.StepStart{Ref: sched, Step: "a", Attempt: 1})},
		{"StartStep empty step", s.StartStep(ctx, recorder.StepStart{Ref: rid("r"), Attempt: 1})},
		{"StartStep attempt 0", s.StartStep(ctx, recorder.StepStart{Ref: rid("r"), Step: "a"})},
		{"EndStep running", s.EndStep(ctx, recorder.StepEnd{Ref: rid("r"), Step: "a", Attempt: 1, Status: recorder.StepRunning})},
		{"EndStep unknown status", s.EndStep(ctx, recorder.StepEnd{Ref: rid("r"), Step: "a", Attempt: 1, Status: "weird"})},
		{"AddEvent without RunID", s.AddEvent(ctx, recorder.EventWrite{Ref: sched, Step: "a", Attempt: 1})},
		{"SetOutcome invalid", s.SetOutcome(ctx, recorder.OutcomeWrite{Ref: rid("r"), Outcome: "meh"})},
		{"SetOutcome empty ref", s.SetOutcome(ctx, recorder.OutcomeWrite{Outcome: workflow.OutcomeSuccess})},
		{"FinishRun not terminal", s.FinishRun(ctx, recorder.Finish{Ref: rid("r"), Status: workflow.StatusRunning})},
		{"FinishRun empty ref", s.FinishRun(ctx, recorder.Finish{Status: workflow.StatusSucceeded})},
		{"MarkChecked empty ref", s.MarkChecked(ctx, recorder.Check{})},
	}
	for _, c := range checks {
		if !errors.Is(c.err, workflow.ErrInvalid) {
			t.Errorf("%s: %v, want ErrInvalid", c.name, c.err)
		}
	}
	if _, err := s.Run(ctx, workflow.RunRef{}); !errors.Is(err, workflow.ErrInvalid) {
		t.Errorf("Run empty ref: %v, want ErrInvalid", err)
	}
	if _, err := s.Pending(ctx, 0); !errors.Is(err, workflow.ErrInvalid) {
		t.Errorf("Pending 0: %v, want ErrInvalid", err)
	}
	// Nothing was recorded by the refused writes.
	p, err := s.Records(ctx, recorder.Query{})
	must(t, err)
	if len(p.Runs) != 0 {
		t.Fatalf("refused writes left records: %+v", p.Runs)
	}
}

func caseStepLifecycle(t *testing.T, newStore NewStore) {
	ctx := context.Background()
	s := fresh(t, newStore)
	ref := rid("r1")
	// No UpsertRun: a run fired by a schedule is first seen at a step.
	must(t, s.StartStep(ctx, recorder.StepStart{
		Ref: ref, Workflow: "sync", Queue: "q", Tenant: "t1", Labels: map[string]string{workflow.LabelSchedule: "sch"},
		Source: workflow.SourceSchedule, SourceRef: "sch", Step: "fetch", Attempt: 1, Node: "n1",
		Input: []byte("in"), InputSize: 2, At: at(1),
	}))
	r := get(t, s, ref)
	if r.Status != workflow.StatusRunning || r.Attempts != 1 || !r.StartedAt.Equal(at(1)) || !r.EnqueuedAt.Equal(at(1)) {
		t.Fatalf("after start: %+v", r)
	}
	if r.Workflow != "sync" || r.Source != workflow.SourceSchedule || r.SourceRef != "sch" || !slices.Equal(r.AttemptedBy, []string{"n1"}) {
		t.Fatalf("identity after start: %+v", r)
	}
	must(t, s.AddEvent(ctx, recorder.EventWrite{Ref: ref, Step: "fetch", Attempt: 1, Message: "downloaded", Level: slog.LevelInfo, Attrs: []workflow.EventAttr{{Key: "rows", Value: "4000"}}, At: at(2)}))
	must(t, s.AddEvent(ctx, recorder.EventWrite{Ref: ref, Step: "fetch", Attempt: 1, Message: "slow", Level: slog.LevelWarn, At: at(3)}))
	must(t, s.EndStep(ctx, recorder.StepEnd{Ref: ref, Step: "fetch", Attempt: 1, Status: recorder.StepSucceeded, Output: []byte("out"), OutputSize: 3, Node: "n1", ErrorHistory: 5, At: at(4)}))
	must(t, s.StartStep(ctx, recorder.StepStart{Ref: ref, Workflow: "sync", Step: "store", Attempt: 1, Node: "n2", At: at(5)}))
	must(t, s.EndStep(ctx, recorder.StepEnd{Ref: ref, Step: "store", Attempt: 1, Status: recorder.StepSucceeded, At: at(6)}))

	r = get(t, s, ref)
	if len(r.Steps) != 2 || r.Steps[0].Step != "fetch" || r.Steps[1].Step != "store" {
		t.Fatalf("steps = %+v", r.Steps)
	}
	f := r.Steps[0]
	if f.Status != recorder.StepSucceeded || string(f.Input) != "in" || string(f.Output) != "out" || f.InputSize != 2 || f.OutputSize != 3 {
		t.Fatalf("fetch = %+v", f)
	}
	if !f.StartedAt.Equal(at(1)) || !f.FinishedAt.Equal(at(4)) || f.Duration() != 3*time.Second || f.Node != "n1" {
		t.Fatalf("fetch timings = %+v", f)
	}
	if len(f.Events) != 2 || f.Events[0].Seq != 1 || f.Events[1].Seq != 2 || f.Events[0].Message != "downloaded" ||
		f.Events[1].Level != slog.LevelWarn || len(f.Events[0].Attrs) != 1 || f.Events[0].Attrs[0].Value != "4000" || !f.Events[0].At.Equal(at(2)) {
		t.Fatalf("events = %+v", f.Events)
	}
	if r.Attempts != 2 || !slices.Equal(r.AttemptedBy, []string{"n1", "n2"}) || !r.UpdatedAt.Equal(at(6)) {
		t.Fatalf("run after steps: attempts %d by %v updated %v", r.Attempts, r.AttemptedBy, r.UpdatedAt)
	}
	// An end for an attempt never seen starting creates it.
	must(t, s.EndStep(ctx, recorder.StepEnd{Ref: ref, Step: "late", Attempt: 1, Status: recorder.StepFailed, Error: "x", ErrorHistory: 5, At: at(7)}))
	r = get(t, s, ref)
	if len(r.Steps) != 3 || r.Steps[2].Step != "late" || r.Steps[2].Status != recorder.StepFailed || !r.Steps[2].StartedAt.Equal(at(7)) {
		t.Fatalf("late end: %+v", r.Steps)
	}
}

func caseRetries(t *testing.T, newStore NewStore) {
	ctx := context.Background()
	s := fresh(t, newStore)
	ref := rid("r1")
	for a := 1; a <= 3; a++ {
		must(t, s.StartStep(ctx, recorder.StepStart{Ref: ref, Workflow: "w", Step: "s", Attempt: a, At: at(a * 10)}))
		must(t, s.EndStep(ctx, recorder.StepEnd{Ref: ref, Step: "s", Attempt: a, Status: recorder.StepFailed, Error: fmt.Sprintf("boom %d", a), Stack: "stack", ErrorHistory: 2, At: at(a*10 + 1)}))
	}
	// A repeated end of attempt 3 does not add a history entry.
	must(t, s.EndStep(ctx, recorder.StepEnd{Ref: ref, Step: "s", Attempt: 3, Status: recorder.StepFailed, Error: "boom 3", ErrorHistory: 2, At: at(32)}))
	r := get(t, s, ref)
	if r.Attempts != 3 || len(r.Steps) != 3 {
		t.Fatalf("attempts %d steps %d", r.Attempts, len(r.Steps))
	}
	if len(r.Errors) != 2 || r.Errors[0].Attempt != 2 || r.Errors[1].Attempt != 3 || r.Errors[1].Error != "boom 3" || r.Errors[1].Stack != "stack" {
		t.Fatalf("errors = %+v", r.Errors)
	}
	if r.Error != "boom 3" || r.Status != workflow.StatusRunning {
		t.Fatalf("current error %q status %q", r.Error, r.Status)
	}
	// ErrorHistory zero keeps none.
	ref2 := rid("r2")
	must(t, s.EndStep(ctx, recorder.StepEnd{Ref: ref2, Step: "s", Attempt: 1, Status: recorder.StepFailed, Error: "e", ErrorHistory: 0, At: at(1)}))
	if r2 := get(t, s, ref2); len(r2.Errors) != 0 {
		t.Fatalf("history kept with ErrorHistory 0: %+v", r2.Errors)
	}
	// A snooze is not a failure.
	ref3 := rid("r3")
	must(t, s.EndStep(ctx, recorder.StepEnd{Ref: ref3, Step: "s", Attempt: 1, Status: recorder.StepSnoozed, Error: "snoozed", ErrorHistory: 5, At: at(1)}))
	if r3 := get(t, s, ref3); len(r3.Errors) != 0 || r3.Error != "" || r3.Steps[0].Status != recorder.StepSnoozed {
		t.Fatalf("snooze: %+v", r3)
	}
}

func caseRepeatedStart(t *testing.T, newStore NewStore) {
	ctx := context.Background()
	s := fresh(t, newStore)
	ref := rid("r1")
	// An event that raced ahead of its attempt's start is kept.
	must(t, s.AddEvent(ctx, recorder.EventWrite{Ref: ref, Step: "s", Attempt: 1, Message: "early", At: at(1)}))
	must(t, s.StartStep(ctx, recorder.StepStart{Ref: ref, Workflow: "w", Step: "s", Attempt: 1, At: at(2)}))
	must(t, s.EndStep(ctx, recorder.StepEnd{Ref: ref, Step: "s", Attempt: 1, Status: recorder.StepSnoozed, Error: "later", At: at(3)}))
	// The snoozed attempt runs again with the same number.
	must(t, s.StartStep(ctx, recorder.StepStart{Ref: ref, Workflow: "w", Step: "s", Attempt: 1, At: at(4)}))
	r := get(t, s, ref)
	if r.Attempts != 1 || len(r.Steps) != 1 {
		t.Fatalf("attempts %d steps %+v", r.Attempts, r.Steps)
	}
	st := r.Steps[0]
	if st.Status != recorder.StepRunning || st.Error != "" || !st.FinishedAt.IsZero() || len(st.Events) != 1 || st.Events[0].Message != "early" {
		t.Fatalf("restarted attempt = %+v", st)
	}
}

func caseFinish(t *testing.T, newStore NewStore) {
	ctx := context.Background()
	s := fresh(t, newStore)
	ref := rid("r1")
	must(t, s.StartStep(ctx, recorder.StepStart{Ref: ref, Workflow: "w", Step: "s", Attempt: 1, At: at(1)}))
	must(t, s.EndStep(ctx, recorder.StepEnd{Ref: ref, Step: "s", Attempt: 1, Status: recorder.StepFailed, Error: "bad", ErrorHistory: 5, At: at(2)}))
	fin := recorder.Finish{Ref: ref, Workflow: "w", Status: workflow.StatusFailed, Error: "bad", FailedStep: "s", FinishedAt: at(3), At: at(3)}
	must(t, s.FinishRun(ctx, fin))
	must(t, s.FinishRun(ctx, fin)) // at least once
	r := get(t, s, ref)
	if r.Status != workflow.StatusFailed || r.Error != "bad" || r.FailedStep != "s" || !r.FinishedAt.Equal(at(3)) {
		t.Fatalf("finished: %+v", r)
	}
	if r.Outcome != workflow.OutcomeError || r.OutcomeSet {
		t.Fatalf("default outcome = %q set %v", r.Outcome, r.OutcomeSet)
	}
	// A retry re-opens the run: a new attempt after the end.
	must(t, s.StartStep(ctx, recorder.StepStart{Ref: ref, Workflow: "w", Step: "s", Attempt: 2, At: at(10)}))
	r = get(t, s, ref)
	if r.Status != workflow.StatusRunning || !r.FinishedAt.IsZero() || r.Error != "" || r.FailedStep != "" {
		t.Fatalf("re-opened: %+v", r)
	}
	// The old finish delivered late is ignored.
	must(t, s.FinishRun(ctx, fin))
	if r = get(t, s, ref); r.Status != workflow.StatusRunning {
		t.Fatalf("late duplicate finish applied: %q", r.Status)
	}
	// A late start of an attempt older than a newer end keeps the end.
	must(t, s.SetOutcome(ctx, recorder.OutcomeWrite{Ref: ref, Outcome: workflow.OutcomePartial, Note: "3 of 100", At: at(11)}))
	must(t, s.EndStep(ctx, recorder.StepEnd{Ref: ref, Step: "s", Attempt: 2, Status: recorder.StepSucceeded, Output: []byte("o"), OutputSize: 1, At: at(12)}))
	must(t, s.FinishRun(ctx, recorder.Finish{Ref: ref, Status: workflow.StatusSucceeded, Output: []byte("res"), OutputSize: 3, FinishedAt: at(13), At: at(13)}))
	must(t, s.StartStep(ctx, recorder.StepStart{Ref: ref, Workflow: "w", Step: "s", Attempt: 2, At: at(10)}))
	r = get(t, s, ref)
	if r.Status != workflow.StatusSucceeded || string(r.Output) != "res" || r.OutputSize != 3 {
		t.Fatalf("succeeded: %+v", r)
	}
	if r.Outcome != workflow.OutcomePartial || r.OutcomeNote != "3 of 100" || !r.OutcomeSet {
		t.Fatalf("set outcome lost: %q %q %v", r.Outcome, r.OutcomeNote, r.OutcomeSet)
	}
	// A finish for a run never seen creates it.
	ghost := rid("ghost")
	must(t, s.FinishRun(ctx, recorder.Finish{Ref: ghost, Workflow: "w", Status: workflow.StatusCancelled, Error: "cancelled", FinishedAt: at(5), At: at(5)}))
	if g := get(t, s, ghost); g.Status != workflow.StatusCancelled || g.Workflow != "w" || g.Outcome != "" {
		t.Fatalf("ghost: %+v", g)
	}
}

func caseJoinIDs(t *testing.T, newStore NewStore) {
	ctx := context.Background()
	s := fresh(t, newStore)
	sched := workflow.RunRef{ScheduledID: "s1"}
	both := workflow.RunRef{ScheduledID: "s1", RunID: "r1"}
	must(t, s.UpsertRun(ctx, recorder.RunWrite{Ref: sched, Workflow: "w", Source: "api", Status: workflow.StatusScheduled, Payload: []byte("p"), PayloadSize: 1, RunAt: at(100), EnqueuedAt: at(1), At: at(1)}))
	if r := get(t, s, sched); r.Status != workflow.StatusScheduled || !r.RunAt.Equal(at(100)) {
		t.Fatalf("scheduled: %+v", r)
	}
	// The run starts: its steps know only the RunID.
	must(t, s.StartStep(ctx, recorder.StepStart{Ref: rid("r1"), Workflow: "w", Step: "a", Attempt: 1, At: at(100)}))
	must(t, s.EndStep(ctx, recorder.StepEnd{Ref: rid("r1"), Step: "a", Attempt: 1, Status: recorder.StepSucceeded, At: at(101)}))
	// The end carries both IDs and joins the two records.
	must(t, s.FinishRun(ctx, recorder.Finish{Ref: both, Status: workflow.StatusSucceeded, FinishedAt: at(102), At: at(102)}))
	for _, ref := range []workflow.RunRef{sched, rid("r1"), both} {
		r := get(t, s, ref)
		if r.Ref != both || r.Status != workflow.StatusSucceeded || string(r.Payload) != "p" || r.Source != "api" || !r.EnqueuedAt.Equal(at(1)) || len(r.Steps) != 1 {
			t.Fatalf("Run(%s) = %+v", recorder.KeyOf(ref), r)
		}
	}
	p, err := s.Records(ctx, recorder.Query{})
	must(t, err)
	if len(p.Runs) != 1 {
		t.Fatalf("listing after join has %d records", len(p.Runs))
	}
	// An upsert with both IDs joins too (the Reconciler's path).
	must(t, s.UpsertRun(ctx, recorder.RunWrite{Ref: workflow.RunRef{ScheduledID: "s2"}, Workflow: "w", EnqueuedAt: at(1), At: at(1)}))
	must(t, s.StartStep(ctx, recorder.StepStart{Ref: rid("r2"), Workflow: "w", Step: "a", Attempt: 1, At: at(5)}))
	must(t, s.UpsertRun(ctx, recorder.RunWrite{Ref: workflow.RunRef{ScheduledID: "s2", RunID: "r2"}, At: at(5)}))
	if p, _ = s.Records(ctx, recorder.Query{}); len(p.Runs) != 2 {
		t.Fatalf("listing after second join has %d records", len(p.Runs))
	}
	if r := get(t, s, workflow.RunRef{ScheduledID: "s2"}); r.Ref.RunID != "r2" || r.Status != workflow.StatusRunning {
		t.Fatalf("second join: %+v", r)
	}
}

// seed writes n queued runs r01..rNN, enqueued one second apart, so the
// newest is the highest number.
func seed(t *testing.T, s recorder.Store, n int) {
	t.Helper()
	for i := 1; i <= n; i++ {
		must(t, s.UpsertRun(context.Background(), recorder.RunWrite{
			Ref: rid(fmt.Sprintf("r%02d", i)), Workflow: "w", Status: workflow.StatusQueued, EnqueuedAt: at(i), At: at(i),
		}))
	}
}

func caseListing(t *testing.T, newStore NewStore) {
	ctx := context.Background()
	s := fresh(t, newStore)
	seed(t, s, 7)
	// Two runs with one enqueue time order by key, descending.
	must(t, s.UpsertRun(ctx, recorder.RunWrite{Ref: rid("tie-a"), Workflow: "w", EnqueuedAt: at(3), At: at(3)}))
	must(t, s.UpsertRun(ctx, recorder.RunWrite{Ref: rid("tie-b"), Workflow: "w", EnqueuedAt: at(3), At: at(3)}))
	var got []string
	q := recorder.Query{RunQuery: workflow.RunQuery{Limit: 3}}
	for pages := 0; ; pages++ {
		if pages > 10 {
			t.Fatal("paging does not end")
		}
		p, err := s.Records(ctx, q)
		must(t, err)
		if len(p.Runs) > 3 {
			t.Fatalf("page of %d with Limit 3", len(p.Runs))
		}
		for _, r := range p.Runs {
			got = append(got, string(r.Ref.RunID))
			if r.Steps != nil {
				t.Fatalf("listing carries steps: %+v", r.Steps)
			}
		}
		if p.Next == "" {
			break
		}
		q.Cursor = p.Next
	}
	want := []string{"r07", "r06", "r05", "r04", "tie-b", "tie-a", "r03", "r02", "r01"}
	if !slices.Equal(got, want) {
		t.Fatalf("order = %v, want %v", got, want)
	}
	// Runs answers the same through workflow.RunQuerier.
	page, err := s.Runs(ctx, workflow.RunQuery{Limit: 2})
	must(t, err)
	if len(page.Runs) != 2 || page.Runs[0].Ref.RunID != "r07" || page.Next == "" {
		t.Fatalf("Runs page = %+v", page)
	}
	if _, err := s.Records(ctx, recorder.Query{RunQuery: workflow.RunQuery{Cursor: "not-a-cursor!"}}); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("bad cursor: %v, want ErrInvalid", err)
	}
	if _, err := s.Runs(ctx, workflow.RunQuery{Limit: workflow.MaxRunsLimit + 1}); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("over-limit: %v, want ErrInvalid", err)
	}
	// Ref picks one run, by either ID.
	page, err = s.Runs(ctx, workflow.RunQuery{Ref: rid("r04")})
	must(t, err)
	if len(page.Runs) != 1 || page.Runs[0].Ref.RunID != "r04" {
		t.Fatalf("Ref filter = %+v", page.Runs)
	}
}

func caseFilters(t *testing.T, newStore NewStore) {
	ctx := context.Background()
	s := fresh(t, newStore)
	write := func(id, wf, queue, tenant string, src workflow.Source, srcRef string, labels map[string]string, enq int, runAt time.Time) {
		must(t, s.UpsertRun(ctx, recorder.RunWrite{
			Ref: rid(id), Workflow: wf, Queue: queue, Tenant: tenant, Source: src, SourceRef: srcRef, Labels: labels,
			Status: workflow.StatusQueued, RunAt: runAt, EnqueuedAt: at(enq), At: at(enq),
		}))
	}
	write("a1", "export", "default", "acme", "webhook", "Delivery-XYZ", map[string]string{"team": "data", "path": "/a/b"}, 1, time.Time{})
	write("a2", "export", "bulk", "beta", "api", "", map[string]string{"team": "ops", "path": "/c"}, 2, at(500))
	write("a3", "import", "default", "acme", workflow.SourceSchedule, "s1", nil, 3, time.Time{})
	must(t, s.FinishRun(ctx, recorder.Finish{Ref: rid("a1"), Status: workflow.StatusSucceeded, FinishedAt: at(10), At: at(10)}))
	must(t, s.SetOutcome(ctx, recorder.OutcomeWrite{Ref: rid("a2"), Outcome: workflow.OutcomeSkipped, At: at(11)}))
	must(t, s.MarkChecked(ctx, recorder.Check{Ref: rid("a3"), Stale: true, At: at(12)}))

	cases := []struct {
		name string
		q    recorder.Query
		want []string
	}{
		{"all", recorder.Query{}, []string{"a3", "a2", "a1"}},
		{"workflow", recorder.Query{RunQuery: workflow.RunQuery{Workflow: "export"}}, []string{"a2", "a1"}},
		{"queue", recorder.Query{RunQuery: workflow.RunQuery{Queue: "bulk"}}, []string{"a2"}},
		{"statuses", recorder.Query{RunQuery: workflow.RunQuery{Statuses: []workflow.Status{workflow.StatusSucceeded}}}, []string{"a1"}},
		{"labels", recorder.Query{RunQuery: workflow.RunQuery{Labels: map[string]string{"team": "ops"}}}, []string{"a2"}},
		{"label prefix", recorder.Query{RunQuery: workflow.RunQuery{LabelPrefixes: map[string]string{"path": "/a"}}}, []string{"a1"}},
		{"tenant", recorder.Query{RunQuery: workflow.RunQuery{Tenant: "acme"}}, []string{"a3", "a1"}},
		{"from to", recorder.Query{RunQuery: workflow.RunQuery{From: at(2), To: at(3)}}, []string{"a2"}},
		{"due", recorder.Query{RunQuery: workflow.RunQuery{DueFrom: at(400), DueTo: at(600)}}, []string{"a2"}},
		{"sources", recorder.Query{Sources: []workflow.Source{"webhook", workflow.SourceSchedule}}, []string{"a3", "a1"}},
		{"outcomes", recorder.Query{Outcomes: []workflow.Outcome{workflow.OutcomeSuccess, workflow.OutcomeSkipped}}, []string{"a2", "a1"}},
		{"search run id", recorder.Query{Search: "A2"}, []string{"a2"}},
		{"search source ref", recorder.Query{Search: "delivery-xyz"}, []string{"a1"}},
		{"search workflow", recorder.Query{Search: "port"}, []string{"a3", "a2", "a1"}},
		{"search literal percent", recorder.Query{Search: "%"}, nil},
		{"search literal underscore", recorder.Query{Search: "_"}, nil},
		{"stale", recorder.Query{OnlyStale: true}, []string{"a3"}},
		{"combined", recorder.Query{RunQuery: workflow.RunQuery{Tenant: "acme"}, Sources: []workflow.Source{"webhook"}, Search: "a"}, []string{"a1"}},
	}
	for _, c := range cases {
		p, err := s.Records(ctx, c.q)
		if err != nil {
			t.Errorf("%s: %v", c.name, err)
			continue
		}
		var got []string
		for _, r := range p.Runs {
			got = append(got, string(r.Ref.RunID))
		}
		if !slices.Equal(got, c.want) {
			t.Errorf("%s: got %v, want %v", c.name, got, c.want)
		}
	}
	bad := []recorder.Query{
		{Sources: []workflow.Source{"Bad Source"}},
		{Outcomes: []workflow.Outcome{"meh"}},
		{Search: string(make([]byte, recorder.MaxSearchLen+1))},
		{RunQuery: workflow.RunQuery{Statuses: []workflow.Status{"nope"}}},
	}
	for i, q := range bad {
		if _, err := s.Records(ctx, q); !errors.Is(err, workflow.ErrInvalid) {
			t.Errorf("bad query %d: %v, want ErrInvalid", i, err)
		}
	}
}

func casePending(t *testing.T, newStore NewStore) {
	ctx := context.Background()
	s := fresh(t, newStore)
	seed(t, s, 4)
	must(t, s.FinishRun(ctx, recorder.Finish{Ref: rid("r04"), Status: workflow.StatusSucceeded, FinishedAt: at(20), At: at(20)}))
	must(t, s.MarkChecked(ctx, recorder.Check{Ref: rid("r01"), Status: workflow.StatusRunning, Stale: true, At: at(30)}))
	pend, err := s.Pending(ctx, 10)
	must(t, err)
	var keys []string
	for _, r := range pend {
		keys = append(keys, string(r.Ref.RunID))
		if r.Steps != nil {
			t.Fatalf("pending carries steps")
		}
	}
	// Never checked first (by key), then checked; the finished run is out.
	if want := []string{"r02", "r03", "r01"}; !slices.Equal(keys, want) {
		t.Fatalf("pending = %v, want %v", keys, want)
	}
	if pend, _ = s.Pending(ctx, 1); len(pend) != 1 || pend[0].Ref.RunID != "r02" {
		t.Fatalf("pending limit 1 = %+v", pend)
	}
	r := get(t, s, rid("r01"))
	if !r.Stale || r.Status != workflow.StatusRunning || !r.CheckedAt.Equal(at(30)) || !r.UpdatedAt.Equal(at(1)) {
		t.Fatalf("checked: stale %v status %q checked %v updated %v", r.Stale, r.Status, r.CheckedAt, r.UpdatedAt)
	}
	// A later write about the run clears Stale; a write no later does not.
	must(t, s.UpsertRun(ctx, recorder.RunWrite{Ref: rid("r01"), At: at(1)}))
	if r = get(t, s, rid("r01")); !r.Stale {
		t.Fatal("a fill-only write cleared Stale")
	}
	must(t, s.AddEvent(ctx, recorder.EventWrite{Ref: rid("r01"), Step: "s", Attempt: 1, Message: "alive", At: at(40)}))
	if r = get(t, s, rid("r01")); r.Stale {
		t.Fatal("activity did not clear Stale")
	}
	// A check never moves a terminal record, and never sets a terminal
	// status.
	must(t, s.MarkChecked(ctx, recorder.Check{Ref: rid("r04"), Status: workflow.StatusRunning, Stale: true, At: at(50)}))
	if r = get(t, s, rid("r04")); r.Status != workflow.StatusSucceeded || r.Stale {
		t.Fatalf("check moved a finished run: %+v", r)
	}
	must(t, s.MarkChecked(ctx, recorder.Check{Ref: rid("r02"), Status: workflow.StatusSucceeded, At: at(50)}))
	if r = get(t, s, rid("r02")); r.Status != workflow.StatusQueued {
		t.Fatalf("check set a terminal status: %q", r.Status)
	}
	// A check of an unknown run is not an error.
	must(t, s.MarkChecked(ctx, recorder.Check{Ref: rid("unknown"), Stale: true, At: at(50)}))
}

func caseCopies(t *testing.T, newStore NewStore) {
	ctx := context.Background()
	s := fresh(t, newStore)
	ref := rid("r1")
	labels := map[string]string{"k": "v"}
	payload := []byte("payload")
	must(t, s.UpsertRun(ctx, recorder.RunWrite{Ref: ref, Workflow: "w", Labels: labels, Payload: payload, PayloadSize: 7, EnqueuedAt: at(1), At: at(1)}))
	// The caller's values are copied on write.
	labels["k"], payload[0] = "changed", 'X'
	must(t, s.StartStep(ctx, recorder.StepStart{Ref: ref, Step: "s", Attempt: 1, Input: []byte("in"), At: at(2)}))
	must(t, s.AddEvent(ctx, recorder.EventWrite{Ref: ref, Step: "s", Attempt: 1, Message: "m", Attrs: []workflow.EventAttr{{Key: "a", Value: "1"}}, At: at(3)}))
	r := get(t, s, ref)
	if r.Labels["k"] != "v" || string(r.Payload) != "payload" {
		t.Fatalf("write was not copied: %v %s", r.Labels, r.Payload)
	}
	// Mutating a read leaves the store alone.
	r.Labels["k"], r.Payload[0], r.Steps[0].Input[0] = "x", 'X', 'X'
	r.Steps[0].Events[0].Attrs[0].Value = "x"
	r2 := get(t, s, ref)
	if r2.Labels["k"] != "v" || string(r2.Payload) != "payload" || string(r2.Steps[0].Input) != "in" || r2.Steps[0].Events[0].Attrs[0].Value != "1" {
		t.Fatalf("read shares state: %+v", r2)
	}
	p, err := s.Records(ctx, recorder.Query{})
	must(t, err)
	p.Runs[0].Labels["k"] = "x"
	if get(t, s, ref).Labels["k"] != "v" {
		t.Fatal("listing shares state")
	}
}

func caseConcurrent(t *testing.T, newStore NewStore) {
	ctx := context.Background()
	s := fresh(t, newStore)
	ref := rid("r1")
	must(t, s.UpsertRun(ctx, recorder.RunWrite{Ref: ref, Workflow: "w", EnqueuedAt: at(0), At: at(0)}))
	const steps, events = 8, 5
	var wg sync.WaitGroup
	errs := make(chan error, steps*(events+2))
	for i := range steps {
		wg.Add(1)
		go func() {
			defer wg.Done()
			name := fmt.Sprintf("step-%d", i)
			errs <- s.StartStep(ctx, recorder.StepStart{Ref: ref, Workflow: "w", Step: name, Attempt: 1, At: at(1)})
			for e := range events {
				errs <- s.AddEvent(ctx, recorder.EventWrite{Ref: ref, Step: name, Attempt: 1, Message: fmt.Sprint(e), At: at(2)})
			}
			errs <- s.EndStep(ctx, recorder.StepEnd{Ref: ref, Step: name, Attempt: 1, Status: recorder.StepSucceeded, At: at(3)})
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		must(t, err)
	}
	r := get(t, s, ref)
	if r.Attempts != steps || len(r.Steps) != steps {
		t.Fatalf("attempts %d steps %d, want %d", r.Attempts, len(r.Steps), steps)
	}
	for _, st := range r.Steps {
		if st.Status != recorder.StepSucceeded || len(st.Events) != events {
			t.Fatalf("step %s: status %q events %d", st.Step, st.Status, len(st.Events))
		}
		for i, e := range st.Events {
			if e.Seq != i+1 {
				t.Fatalf("step %s: event seqs %+v", st.Step, st.Events)
			}
		}
	}
}

// prunable builds a store with retention on a fake clock, or skips when the
// store is no Pruner.
func prunable(t *testing.T, newStore NewStore, ret recorder.Retention) (recorder.Store, recorder.Pruner, *clock.Fake) {
	t.Helper()
	clk := clock.NewFake(at(0))
	s := newStore(t, StoreConfig{Retention: ret, Clock: clk})
	p, ok := s.(recorder.Pruner)
	if !ok {
		t.Skip("the store does not implement recorder.Pruner")
	}
	return s, p, clk
}

// finished writes a run of wf with one step, finished at fin.
func finished(t *testing.T, s recorder.Store, id, wf string, fin time.Time) {
	t.Helper()
	ctx := context.Background()
	ref := rid(id)
	must(t, s.StartStep(ctx, recorder.StepStart{Ref: ref, Workflow: wf, Step: "s", Attempt: 1, At: fin.Add(-time.Second)}))
	must(t, s.AddEvent(ctx, recorder.EventWrite{Ref: ref, Step: "s", Attempt: 1, Message: "e", At: fin.Add(-time.Second)}))
	must(t, s.EndStep(ctx, recorder.StepEnd{Ref: ref, Step: "s", Attempt: 1, Status: recorder.StepSucceeded, At: fin}))
	must(t, s.FinishRun(ctx, recorder.Finish{Ref: ref, Workflow: wf, Status: workflow.StatusSucceeded, FinishedAt: fin, At: fin}))
}

func casePrune(t *testing.T, newStore NewStore) {
	ctx := context.Background()
	var archived []string
	var mu sync.Mutex
	ret := recorder.Retention{
		RetentionRule: recorder.RetentionRule{Steps: time.Hour, Runs: 24 * time.Hour},
		PerWorkflow:   map[string]recorder.RetentionRule{"keep": {}},
		Archive: func(_ context.Context, runs []recorder.RunRecord) error {
			mu.Lock()
			defer mu.Unlock()
			for _, r := range runs {
				archived = append(archived, string(r.Ref.RunID))
			}
			return nil
		},
		Batch: 1,
	}
	s, p, clk := prunable(t, newStore, ret)
	finished(t, s, "old1", "w", at(10))
	finished(t, s, "old2", "w", at(20))
	finished(t, s, "kept", "keep", at(10))
	must(t, s.StartStep(ctx, recorder.StepStart{Ref: rid("live"), Workflow: "w", Step: "s", Attempt: 1, At: at(5)}))

	// Before any age passes, nothing is pruned.
	res, err := p.Prune(ctx)
	must(t, err)
	if res != (recorder.PruneResult{}) {
		t.Fatalf("early prune: %+v", res)
	}
	// Stage one: steps go, the record stays.
	clk.Set(at(20).Add(time.Hour))
	res, err = p.Prune(ctx)
	must(t, err)
	if res.StepsCleared != 2 || res.RunsDeleted != 0 {
		t.Fatalf("stage one: %+v", res)
	}
	if r := get(t, s, rid("old1")); len(r.Steps) != 0 || r.Status != workflow.StatusSucceeded {
		t.Fatalf("old1 after stage one: %+v", r)
	}
	if r := get(t, s, rid("kept")); len(r.Steps) != 1 {
		t.Fatal("a per-workflow rule of zero did not keep the steps")
	}
	if r := get(t, s, rid("live")); len(r.Steps) != 1 {
		t.Fatal("an unfinished run lost its steps")
	}
	// Stage two: the records go, through the archive, one batch at a time.
	clk.Set(at(20).Add(24 * time.Hour))
	res, err = p.Prune(ctx)
	must(t, err)
	if res.RunsDeleted != 2 || res.Archived != 2 {
		t.Fatalf("stage two: %+v", res)
	}
	mu.Lock()
	slices.Sort(archived)
	got := slices.Clone(archived)
	mu.Unlock()
	if !slices.Equal(got, []string{"old1", "old2"}) {
		t.Fatalf("archived %v", got)
	}
	for _, id := range []string{"old1", "old2"} {
		if _, err := s.Run(ctx, rid(id)); !errors.Is(err, workflow.ErrNotFound) {
			t.Fatalf("%s survived stage two: %v", id, err)
		}
	}
	for _, id := range []string{"kept", "live"} {
		get(t, s, rid(id))
	}
}

func casePruneArchiveFailure(t *testing.T, newStore NewStore) {
	ctx := context.Background()
	down := errors.New("archive down")
	ret := recorder.Retention{
		RetentionRule: recorder.RetentionRule{Runs: time.Hour},
		Archive:       func(context.Context, []recorder.RunRecord) error { return down },
	}
	s, p, clk := prunable(t, newStore, ret)
	finished(t, s, "r1", "w", at(10))
	clk.Set(at(10).Add(2 * time.Hour))
	if _, err := p.Prune(ctx); !errors.Is(err, down) {
		t.Fatalf("prune with a failing archive: %v, want the archive error", err)
	}
	if r := get(t, s, rid("r1")); len(r.Steps) != 1 {
		t.Fatalf("a failed archive lost data: %+v", r)
	}
}
