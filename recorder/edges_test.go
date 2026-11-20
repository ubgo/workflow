package recorder_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/memory"
	"github.com/ubgo/workflow/recorder"
)

// spyStore records what the recorder hands the store: the ErrorHistory of
// each EndStep and the time left on each UpsertRun's context.
type spyStore struct {
	*recorder.MemoryStore
	mu        sync.Mutex
	history   []int
	remaining []time.Duration
}

func (s *spyStore) EndStep(ctx context.Context, e recorder.StepEnd) error {
	s.mu.Lock()
	s.history = append(s.history, e.ErrorHistory)
	s.mu.Unlock()
	return s.MemoryStore.EndStep(ctx, e)
}

func (s *spyStore) UpsertRun(ctx context.Context, w recorder.RunWrite) error {
	left := time.Duration(-1)
	if dl, ok := ctx.Deadline(); ok {
		left = time.Until(dl)
	}
	s.mu.Lock()
	s.remaining = append(s.remaining, left)
	s.mu.Unlock()
	return s.MemoryStore.UpsertRun(ctx, w)
}

func (s *spyStore) seen() ([]int, []time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.history), slices.Clone(s.remaining)
}

// TestErrorHistoryAndWriteTimeoutReachTheStore: WithErrorHistory is the
// bound each step end carries to the store (zero included), and every
// write runs under a deadline of WithWriteTimeout.
func TestErrorHistoryAndWriteTimeoutReachTheStore(t *testing.T) {
	for _, c := range []struct {
		history int
		timeout time.Duration
	}{{0, 3 * time.Second}, {2, time.Hour}} {
		spy := &spyStore{MemoryStore: mustStore(t)}
		rec, err := recorder.New(spy, recorder.WithErrorHistory(c.history), recorder.WithWriteTimeout(c.timeout))
		if err != nil {
			t.Fatal(err)
		}
		cl := startClient(t, rec, workflow.Workflow{Name: "w", Steps: []workflow.Step{{Name: "s", Run: func(_ context.Context, in []byte) ([]byte, error) { return in, nil }}}})
		ref, err := cl.Enqueue(context.Background(), workflow.Job{Workflow: "w"})
		if err != nil {
			t.Fatal(err)
		}
		if st, err := workflow.Await(context.Background(), cl, ref, workflow.AwaitOptions{Poll: time.Millisecond}); err != nil || st != workflow.StatusSucceeded {
			t.Fatalf("run = %q %v", st, err)
		}
		waitFor(t, "the step end", func() bool { h, _ := spy.seen(); return len(h) == 1 })
		hist, left := spy.seen()
		if hist[0] != c.history {
			t.Fatalf("EndStep.ErrorHistory = %d, want %d", hist[0], c.history)
		}
		if len(left) == 0 {
			t.Fatal("no UpsertRun reached the store")
		}
		for _, d := range left {
			if d <= 0 || d > c.timeout {
				t.Fatalf("write deadline %s away, want within (0, %s]", d, c.timeout)
			}
		}
	}
}

// syncBuffer is a bytes.Buffer safe for the concurrent writes of a logger.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) lines() []map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []map[string]any
	for _, l := range strings.Split(strings.TrimSpace(s.b.String()), "\n") {
		var m map[string]any
		if json.Unmarshal([]byte(l), &m) == nil {
			out = append(out, m)
		}
	}
	return out
}

// captureDefaultLog points slog.Default at a JSON buffer for the test.
func captureDefaultLog(t *testing.T) *syncBuffer {
	t.Helper()
	buf := &syncBuffer{}
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(buf, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return buf
}

// findLog returns the first line with the given op attribute.
func findLog(buf *syncBuffer, op string) map[string]any {
	for _, l := range buf.lines() {
		if l["op"] == op {
			return l
		}
	}
	return nil
}

// TestDefaultErrorHookLogs: without WithOnError a failed write is logged
// on slog.Default at warn level, with the op, the run and the error.
func TestDefaultErrorHookLogs(t *testing.T) {
	buf := captureDefaultLog(t)
	rec, err := recorder.New(failingStore{MemoryStore: mustStore(t)})
	if err != nil {
		t.Fatal(err)
	}
	cl := startClient(t, rec, workflow.Workflow{Name: "w", Steps: []workflow.Step{{Name: "s", Run: func(_ context.Context, in []byte) ([]byte, error) { return in, nil }}}})
	ref, err := cl.Enqueue(context.Background(), workflow.Job{Workflow: "w"})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the failed upsert to be logged", func() bool { return findLog(buf, string(recorder.OpUpsertRun)) != nil })
	l := findLog(buf, string(recorder.OpUpsertRun))
	if l["level"] != "WARN" || l["msg"] != "recorder: write failed" || l["run"] != recorder.KeyOf(ref) || !strings.Contains(l["error"].(string), errStoreDown.Error()) {
		t.Fatalf("log line = %v", l)
	}
}

// TestReconcilerRunLogsPassErrors: a Reconciler with no error hook logs a
// failed pass on slog.Default and keeps looping.
func TestReconcilerRunLogsPassErrors(t *testing.T) {
	buf := captureDefaultLog(t)
	rc, err := recorder.NewReconciler(memory.New(), brokenStore{mustStore(t)}, recorder.WithInterval(time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- rc.Run(ctx) }()
	waitFor(t, "a logged pass failure", func() bool { return findLog(buf, "reconcile") != nil })
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run = %v", err)
	}
	if l := findLog(buf, "reconcile"); l["level"] != "WARN" || !strings.Contains(l["error"].(string), "recorder reconcile: pending: db down") {
		t.Fatalf("log line = %v", l)
	}
}

// listing is a RunQuerier client: Runs answers a lookup by Ref from byRef
// and a backfill listing from pages by cursor (or fails with listErr); it
// records the cursors it was asked for. Everything else is the memory
// client's.
type listing struct {
	workflow.Client
	mu      sync.Mutex
	byRef   map[workflow.RunRef]workflow.RunInfo
	pages   map[string]workflow.RunPage
	listErr error
	cursors []string
}

func (l *listing) Runs(_ context.Context, q workflow.RunQuery) (workflow.RunPage, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !q.Ref.IsZero() {
		if info, ok := l.byRef[q.Ref]; ok {
			return workflow.RunPage{Runs: []workflow.RunInfo{info}}, nil
		}
		return workflow.RunPage{}, nil
	}
	l.cursors = append(l.cursors, q.Cursor)
	if l.listErr != nil {
		return workflow.RunPage{}, l.listErr
	}
	return l.pages[q.Cursor], nil
}

// faultyStore fails Run for one ref and UpsertRun for another; the rest
// is a MemoryStore.
type faultyStore struct {
	*recorder.MemoryStore
	runFails, upsertFails workflow.RunRef
}

var errFaulty = errors.New("store fault")

func (s faultyStore) Run(ctx context.Context, ref workflow.RunRef) (recorder.RunRecord, error) {
	if ref == s.runFails {
		return recorder.RunRecord{}, errFaulty
	}
	return s.MemoryStore.Run(ctx, ref)
}

func (s faultyStore) UpsertRun(ctx context.Context, w recorder.RunWrite) error {
	if w.Ref == s.upsertFails {
		return errFaulty
	}
	return s.MemoryStore.UpsertRun(ctx, w)
}

// TestReconcilerBackfill: the backfill lists the window page by page up to
// the page bound, records each listed run (finishing a terminal one at the
// clock when the client has no end time), counts and reports a run whose
// store write fails and goes on, fails the pass when the listing fails,
// and skips a scope that ends before the window.
func TestReconcilerBackfill(t *testing.T) {
	ctx := context.Background()
	clk := clock.NewFake(start)
	info := func(id string, st workflow.Status) workflow.RunInfo {
		return workflow.RunInfo{Ref: workflow.RunRef{RunID: workflow.RunID(id)}, Workflow: "w", Status: st, EnqueuedAt: start.Add(-time.Hour)}
	}
	cl := &listing{Client: memory.New(), pages: map[string]workflow.RunPage{
		"":   {Runs: []workflow.RunInfo{info("a", workflow.StatusRunning), info("bad-read", workflow.StatusQueued)}, Next: "p2"},
		"p2": {Runs: []workflow.RunInfo{info("b", workflow.StatusFailed), info("bad-write", workflow.StatusQueued)}, Next: "p3"},
		"p3": {Runs: []workflow.RunInfo{info("never", workflow.StatusQueued)}},
	}}
	store := faultyStore{MemoryStore: mustStore(t), runFails: workflow.RunRef{RunID: "bad-read"}, upsertFails: workflow.RunRef{RunID: "bad-write"}}
	var reported []error
	rc, err := recorder.NewReconciler(cl, store, recorder.WithReconcileClock(clk), recorder.WithBackfillPages(2),
		recorder.WithReconcileOnError(func(_ context.Context, err error) { reported = append(reported, err) }))
	if err != nil {
		t.Fatal(err)
	}
	rep, err := rc.Pass(ctx)
	if err != nil || rep.Backfilled != 2 || rep.Errors != 2 || len(reported) != 2 {
		t.Fatalf("report %+v, err %v, reported %v", rep, err, reported)
	}
	if !slices.Equal(cl.cursors, []string{"", "p2"}) {
		t.Fatalf("cursors asked = %q, want two pages", cl.cursors)
	}
	for _, e := range reported {
		if !errors.Is(e, errFaulty) || !strings.HasPrefix(e.Error(), "recorder backfill r:bad-") {
			t.Fatalf("reported %v", e)
		}
	}
	b, err := store.Run(ctx, workflow.RunRef{RunID: "b"})
	if err != nil || b.Status != workflow.StatusFailed || !b.FinishedAt.Equal(start) || b.Source != workflow.SourceExternal {
		t.Fatalf("backfilled terminal run = %+v, %v", b, err)
	}
	if _, err := store.Run(ctx, workflow.RunRef{RunID: "never"}); !errors.Is(err, workflow.ErrNotFound) {
		t.Fatalf("a page past the bound was read: %v", err)
	}

	// A failing listing fails the pass.
	cl.listErr = errors.New("engine down")
	if _, err := rc.Pass(ctx); err == nil || !strings.Contains(err.Error(), "backfill listing: engine down") {
		t.Fatalf("listing failure: %v", err)
	}

	// A scope that ends before the window starts lists nothing.
	cl.listErr, cl.cursors = nil, nil
	rc2, err := recorder.NewReconciler(cl, mustStore(t), recorder.WithReconcileClock(clk), recorder.WithBackfillWindow(time.Hour),
		recorder.WithBackfillQuery(workflow.RunQuery{To: start.Add(-2 * time.Hour)}))
	if err != nil {
		t.Fatal(err)
	}
	if rep, err := rc2.Pass(ctx); err != nil || rep.Backfilled != 0 || len(cl.cursors) != 0 {
		t.Fatalf("empty scope: %+v %v, listed %q", rep, err, cl.cursors)
	}
}

// TestReconcilerCheckEdges: a terminal answer with no ref finishes the
// record under its own ref; a failed join of a delayed run's IDs is
// counted and reported; a cancelled context stops the check.
func TestReconcilerCheckEdges(t *testing.T) {
	ctx := context.Background()
	clk := clock.NewFake(start)
	store := faultyStore{MemoryStore: mustStore(t), upsertFails: workflow.RunRef{RunID: "r2", ScheduledID: "s2"}}
	r1 := workflow.RunRef{RunID: "r1"}
	s2 := workflow.RunRef{ScheduledID: "s2"}
	for _, ref := range []workflow.RunRef{r1, s2} {
		if err := store.UpsertRun(ctx, recorder.RunWrite{Ref: ref, Workflow: "w", Status: workflow.StatusQueued, EnqueuedAt: start, At: start}); err != nil {
			t.Fatal(err)
		}
	}
	cl := &listing{Client: memory.New(), byRef: map[workflow.RunRef]workflow.RunInfo{
		r1: {Status: workflow.StatusSucceeded},
		s2: {Ref: workflow.RunRef{RunID: "r2", ScheduledID: "s2"}, Status: workflow.StatusRunning},
	}}
	var reported []error
	rc, err := recorder.NewReconciler(cl, store, recorder.WithReconcileClock(clk), recorder.WithBackfill(false),
		recorder.WithReconcileOnError(func(_ context.Context, err error) { reported = append(reported, err) }))
	if err != nil {
		t.Fatal(err)
	}

	cctx, cancel := context.WithCancel(ctx)
	cancel()
	if rep, err := rc.Pass(cctx); err != nil || rep.Checked != 0 {
		t.Fatalf("cancelled pass: %+v %v", rep, err)
	}

	clk.Advance(time.Minute)
	rep, err := rc.Pass(ctx)
	if err != nil || rep.Checked != 2 || rep.Finished != 1 || rep.Errors != 1 {
		t.Fatalf("report %+v, err %v", rep, err)
	}
	if len(reported) != 1 || !errors.Is(reported[0], errFaulty) || !strings.HasPrefix(reported[0].Error(), "recorder reconcile s:s2") {
		t.Fatalf("reported %v", reported)
	}
	r, err := store.Run(ctx, r1)
	if err != nil || r.Status != workflow.StatusSucceeded || !r.FinishedAt.Equal(clk.Now()) {
		t.Fatalf("r1 = %+v, %v", r, err)
	}
	if r, _ := store.Run(ctx, s2); !r.Ref.RunID.IsZero() {
		t.Fatalf("a failed join still joined: %+v", r.Ref)
	}
}

// TestMergeRecords: the RunID record's fields win; the ScheduledID record
// fills what it lacks, its outcome when the run has none, the later
// update time, the earlier enqueue time, and its steps join in order.
func TestMergeRecords(t *testing.T) {
	run := recorder.NewRecord(workflow.RunRef{RunID: "r1"})
	run.Workflow, run.EnqueuedAt, run.UpdatedAt = "wf", start.Add(time.Minute), start.Add(time.Minute)
	run.Steps = []recorder.StepRecord{{Step: "b", Attempt: 1, StartedAt: start.Add(2 * time.Minute)}}
	sched := recorder.NewRecord(workflow.RunRef{ScheduledID: "s1"})
	sched.Workflow, sched.Queue, sched.Payload, sched.PayloadSize, sched.Priority = "other", "q", []byte("p"), 1, 5
	sched.RunAt, sched.EnqueuedAt, sched.UpdatedAt = start.Add(time.Hour), start, start.Add(3*time.Minute)
	sched.Outcome, sched.OutcomeNote, sched.OutcomeSet = workflow.OutcomeSkipped, "why", true
	sched.Steps = []recorder.StepRecord{{Step: "a", Attempt: 1, StartedAt: start}}

	m := recorder.MergeRecords(run, sched)
	if m.Ref != (workflow.RunRef{RunID: "r1", ScheduledID: "s1"}) || m.Workflow != "wf" || m.Queue != "q" || string(m.Payload) != "p" ||
		m.PayloadSize != 1 || m.Priority != 5 || !m.RunAt.Equal(sched.RunAt) || !m.EnqueuedAt.Equal(start) || !m.UpdatedAt.Equal(sched.UpdatedAt) ||
		m.Outcome != workflow.OutcomeSkipped || m.OutcomeNote != "why" || !m.OutcomeSet {
		t.Fatalf("merged = %+v", m)
	}
	if len(m.Steps) != 2 || m.Steps[0].Step != "a" || m.Steps[1].Step != "b" {
		t.Fatalf("merged steps = %+v", m.Steps)
	}
	// The merge copies: changing it leaves the inputs alone.
	m.Payload[0], m.Steps[0].Step = 'x', "changed"
	if string(sched.Payload) != "p" || sched.Steps[0].Step != "a" {
		t.Fatal("MergeRecords shares memory with its input")
	}
	// The run's own outcome and later update win.
	run.Outcome, run.OutcomeSet, run.UpdatedAt = workflow.OutcomeSuccess, true, start.Add(time.Hour)
	m = recorder.MergeRecords(run, sched)
	if m.Outcome != workflow.OutcomeSuccess || !m.UpdatedAt.Equal(run.UpdatedAt) {
		t.Fatalf("run's own fields lost: %+v", m)
	}
}

// TestSortPendingAndDuration: never-checked records first, then the least
// recently checked, ties by key (equal records keep a stable relative
// position as equals); a step's Duration is zero until it finishes.
func TestSortPendingAndDuration(t *testing.T) {
	rec := func(id string, checked time.Time) recorder.RunRecord {
		r := recorder.NewRecord(workflow.RunRef{RunID: workflow.RunID(id)})
		r.CheckedAt = checked
		return r
	}
	rs := []recorder.RunRecord{rec("c", start), rec("b", time.Time{}), rec("a", start), rec("b", time.Time{}), rec("a", time.Time{})}
	recorder.SortPending(rs)
	var got []string
	for _, r := range rs {
		got = append(got, string(r.Ref.RunID))
	}
	if strings.Join(got, ",") != "a,b,b,a,c" {
		t.Fatalf("order = %v", got)
	}
	s := recorder.StepRecord{StartedAt: start}
	if s.Duration() != 0 {
		t.Fatalf("running step duration = %s", s.Duration())
	}
	s.FinishedAt = start.Add(1500 * time.Millisecond)
	if s.Duration() != 1500*time.Millisecond {
		t.Fatalf("duration = %s", s.Duration())
	}
}

// TestMemoryStorePrunesScheduledRun: a finished run known only by its
// ScheduledID is pruned with its index entry, so a later lookup by that ID
// finds nothing and a new write starts a fresh record.
func TestMemoryStorePrunesScheduledRun(t *testing.T) {
	ctx := context.Background()
	clk := clock.NewFake(start)
	s, err := recorder.NewMemoryStore(recorder.WithMemoryClock(clk), recorder.WithMemoryRetention(recorder.Retention{RetentionRule: recorder.RetentionRule{Runs: time.Hour}}))
	if err != nil {
		t.Fatal(err)
	}
	ref := workflow.RunRef{ScheduledID: "s1"}
	if err := s.UpsertRun(ctx, recorder.RunWrite{Ref: ref, Workflow: "w", Labels: map[string]string{"k": "v"}, At: start}); err != nil {
		t.Fatal(err)
	}
	if err := s.FinishRun(ctx, recorder.Finish{Ref: ref, Status: workflow.StatusCancelled, FinishedAt: start, At: start}); err != nil {
		t.Fatal(err)
	}
	clk.Advance(2 * time.Hour)
	res, err := s.Prune(ctx)
	if err != nil || res.RunsDeleted != 1 {
		t.Fatalf("prune = %+v %v", res, err)
	}
	if _, err := s.Run(ctx, ref); !errors.Is(err, workflow.ErrNotFound) {
		t.Fatalf("pruned run still found: %v", err)
	}
	if err := s.UpsertRun(ctx, recorder.RunWrite{Ref: ref, Workflow: "w2", At: clk.Now()}); err != nil {
		t.Fatal(err)
	}
	if r, err := s.Run(ctx, ref); err != nil || r.Workflow != "w2" || r.Labels != nil || r.Status.IsTerminal() {
		t.Fatalf("new record after prune = %+v %v", r, err)
	}
	if err := s.EndStep(ctx, recorder.StepEnd{Ref: ref, Attempt: 1, Status: recorder.StepSucceeded}); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("EndStep without a step: %v", err)
	}
}

// TestJSONPathsArrayWithoutMatch: a wildcard path into an array whose
// elements hold nothing at the rest of the path leaves the body as it was;
// one element that does is redacted alone.
func TestJSONPathsArrayWithoutMatch(t *testing.T) {
	r, err := recorder.JSONPaths("items.*.secret", "list.1")
	if err != nil {
		t.Fatal(err)
	}
	for in, want := range map[string]string{
		`{"items": [1, "x", {"other": 2}]}`:       `{"items": [1, "x", {"other": 2}]}`,
		`{"list": [1]}`:                           `{"list": [1]}`,
		`{"items":[{"secret":"s"},3],"list":[1]}`: `{"items":[{"secret":"` + recorder.Redacted + `"},3],"list":[1]}`,
	} {
		if got := string(r.RedactPayload(recorder.PayloadInfo{}, []byte(in))); got != want {
			t.Errorf("%s: got %s, want %s", in, got, want)
		}
	}
}

// expander is a Redactor whose output is longer than its input, as a
// redactor that replaces a short value with a long marker is.
type expander struct{}

func (expander) RedactPayload(_ recorder.PayloadInfo, b []byte) []byte {
	return append(slices.Clone(b), strings.Repeat("#", 32)...)
}

func (expander) RedactAttr(_ recorder.PayloadInfo, a workflow.EventAttr) workflow.EventAttr { return a }

// TestRedactedBodyOverTheBoundIsDropped: a body within WithMaxPayload that
// redaction grows past it is stored by its size only, never cut.
func TestRedactedBodyOverTheBoundIsDropped(t *testing.T) {
	store := mustStore(t)
	rec, err := recorder.New(store, recorder.WithMaxPayload(16), recorder.WithRedactor(expander{}))
	if err != nil {
		t.Fatal(err)
	}
	cl := startClient(t, rec, workflow.Workflow{Name: "w", Steps: []workflow.Step{{Name: "s", Run: func(_ context.Context, in []byte) ([]byte, error) { return in, nil }}}})
	ref, err := cl.Enqueue(context.Background(), workflow.Job{Workflow: "w", Payload: []byte("12345")})
	if err != nil {
		t.Fatal(err)
	}
	if st, err := workflow.Await(context.Background(), cl, ref, workflow.AwaitOptions{Poll: time.Millisecond}); err != nil || st != workflow.StatusSucceeded {
		t.Fatalf("run = %q %v", st, err)
	}
	waitFor(t, "the finished record", func() bool {
		r, err := store.Run(context.Background(), ref)
		return err == nil && r.Status == workflow.StatusSucceeded && len(r.Steps) == 1 && !r.Steps[0].FinishedAt.IsZero()
	})
	r, _ := store.Run(context.Background(), ref)
	if r.Payload != nil || r.PayloadSize != 5 || r.Steps[0].Input != nil || r.Steps[0].InputSize != 5 {
		t.Fatalf("payload %q (%d), step input %q (%d)", r.Payload, r.PayloadSize, r.Steps[0].Input, r.Steps[0].InputSize)
	}
}
