package recorder_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/memory"
	"github.com/ubgo/workflow/recorder"
)

func TestJSONPaths(t *testing.T) {
	red, err := recorder.JSONPaths("password", "card.number", "users.*.token", "list.1", "deep.*.*")
	if err != nil {
		t.Fatal(err)
	}
	info := recorder.PayloadInfo{Kind: recorder.PayloadInput}
	in := `{"user":"u","password":"p","card":{"number":"4111","exp":"12/30"},"users":[{"token":"a","id":1},{"token":"b"}],"list":["x","y","z"],"deep":{"a":{"b":1}}}`
	out := red.RedactPayload(info, []byte(in))
	var got struct {
		User     string `json:"user"`
		Password string `json:"password"`
		Card     struct {
			Number string `json:"number"`
			Exp    string `json:"exp"`
		} `json:"card"`
		Users []struct {
			Token string `json:"token"`
			ID    int    `json:"id"`
		} `json:"users"`
		List []string                  `json:"list"`
		Deep map[string]map[string]any `json:"deep"`
	}
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatalf("redacted body is not JSON: %v: %s", err, out)
	}
	r := recorder.Redacted
	if got.User != "u" || got.Password != r || got.Card.Number != r || got.Card.Exp != "12/30" ||
		got.Users[0].Token != r || got.Users[1].Token != r || got.Users[0].ID != 1 ||
		got.List[0] != "x" || got.List[1] != r || got.List[2] != "z" || got.Deep["a"]["b"] != r {
		t.Fatalf("redacted = %s", out)
	}
	if !strings.Contains(string([]byte(in)), `"password":"p"`) {
		t.Fatal("the input slice was modified")
	}
	// Nothing to redact: byte for byte the same.
	same := []byte(`{ "user" : "u" }`)
	if got := red.RedactPayload(info, same); string(got) != string(same) {
		t.Fatalf("unchanged body re-encoded: %s", got)
	}
	// Not JSON: unchanged. Out-of-range index and scalar paths: unchanged.
	for _, b := range []string{"plain text", `{"list":["x"]}`, `{"card":"flat"}`, `"str"`, `[1,2]`} {
		if got := red.RedactPayload(info, []byte(b)); string(got) != b {
			t.Errorf("%s -> %s", b, got)
		}
	}
	// Attributes match by dot-split key.
	if a := red.RedactAttr(info, workflow.EventAttr{Key: "card.number", Value: "4111"}); a.Value != r {
		t.Errorf("attr card.number = %q", a.Value)
	}
	if a := red.RedactAttr(info, workflow.EventAttr{Key: "users.7.token", Value: "t"}); a.Value != r {
		t.Errorf("attr users.7.token = %q", a.Value)
	}
	if a := red.RedactAttr(info, workflow.EventAttr{Key: "card", Value: "v"}); a.Value != "v" {
		t.Errorf("attr card = %q", a.Value)
	}
	if _, err := recorder.JSONPaths("a..b"); !errors.Is(err, workflow.ErrInvalid) {
		t.Errorf("empty segment: %v", err)
	}
	if _, err := recorder.JSONPaths(""); !errors.Is(err, workflow.ErrInvalid) {
		t.Errorf("empty path: %v", err)
	}
	none, _ := recorder.JSONPaths()
	if got := none.RedactPayload(info, []byte(`{"password":"p"}`)); string(got) != `{"password":"p"}` {
		t.Errorf("no paths changed the body: %s", got)
	}
}

func TestPerWorkflowAndChain(t *testing.T) {
	a, _ := recorder.JSONPaths("a")
	b, _ := recorder.JSONPaths("b")
	body := []byte(`{"a":1,"b":2}`)
	pw := recorder.PerWorkflow(map[string]recorder.Redactor{"wa": a, "nil": nil}, b)
	if got := string(pw.RedactPayload(recorder.PayloadInfo{Workflow: "wa"}, body)); !strings.Contains(got, `"a":"[REDACTED]"`) || !strings.Contains(got, `"b":2`) {
		t.Errorf("wa: %s", got)
	}
	if got := string(pw.RedactPayload(recorder.PayloadInfo{Workflow: "other"}, body)); !strings.Contains(got, `"b":"[REDACTED]"`) {
		t.Errorf("fallback: %s", got)
	}
	// A nil map entry falls back.
	if got := string(pw.RedactPayload(recorder.PayloadInfo{Workflow: "nil"}, body)); !strings.Contains(got, `"b":"[REDACTED]"`) {
		t.Errorf("nil entry: %s", got)
	}
	noFallback := recorder.PerWorkflow(map[string]recorder.Redactor{"wa": a}, nil)
	if got := string(noFallback.RedactPayload(recorder.PayloadInfo{Workflow: "other"}, body)); got != string(body) {
		t.Errorf("no fallback: %s", got)
	}
	if got := noFallback.RedactAttr(recorder.PayloadInfo{Workflow: "other"}, workflow.EventAttr{Key: "a", Value: "1"}); got.Value != "1" {
		t.Errorf("no fallback attr: %+v", got)
	}
	if got := pw.RedactAttr(recorder.PayloadInfo{Workflow: "wa"}, workflow.EventAttr{Key: "a", Value: "1"}); got.Value != recorder.Redacted {
		t.Errorf("wa attr: %+v", got)
	}
	ch := recorder.Chain(a, nil, b)
	if got := string(ch.RedactPayload(recorder.PayloadInfo{}, body)); strings.Contains(got, "1") || strings.Contains(got, "2") {
		t.Errorf("chain: %s", got)
	}
	if got := ch.RedactAttr(recorder.PayloadInfo{}, workflow.EventAttr{Key: "b", Value: "2"}); got.Value != recorder.Redacted {
		t.Errorf("chain attr: %+v", got)
	}
}

func TestCursorRoundTripAndRefusals(t *testing.T) {
	c := recorder.Cursor{EnqueuedAt: time.Date(2026, 1, 2, 3, 4, 5, 6, time.UTC), Key: "r:abc|def"}
	got, err := recorder.DecodeCursor(recorder.EncodeCursor(c))
	if err != nil || !got.EnqueuedAt.Equal(c.EnqueuedAt) || got.Key != c.Key {
		t.Fatalf("round trip = %+v, %v", got, err)
	}
	for _, bad := range []string{"!!!", "bm9zZXA", "eHx5", "fHI6YQ"} { // garbage, "nosep", "x|y", "|r:a"
		if _, err := recorder.DecodeCursor(bad); !errors.Is(err, workflow.ErrInvalid) {
			t.Errorf("DecodeCursor(%q) = %v", bad, err)
		}
	}
}

func TestValidateQueryAndKeyOf(t *testing.T) {
	if err := recorder.ValidateQuery(recorder.Query{RunQuery: workflow.RunQuery{From: start, To: start}}); !errors.Is(err, workflow.ErrInvalid) {
		t.Errorf("From == To: %v", err)
	}
	if recorder.KeyOf(workflow.RunRef{}) != "" || recorder.KeyOf(workflow.RunRef{RunID: "x", ScheduledID: "y"}) != "r:x" || recorder.KeyOf(workflow.RunRef{ScheduledID: "y"}) != "s:y" {
		t.Error("KeyOf")
	}
	if !recorder.StepSucceeded.Valid() || recorder.StepStatus("x").Valid() || len(recorder.StepStatuses()) != 4 {
		t.Error("StepStatus")
	}
}

func TestRetention(t *testing.T) {
	r := recorder.Retention{PerWorkflow: map[string]recorder.RetentionRule{"w": {Steps: time.Hour}}}
	if r.IsZero() || r.Rule("w").Steps != time.Hour || r.Rule("x").Steps != 0 || r.BatchSize() != recorder.DefaultPruneBatch {
		t.Fatalf("retention helpers: %+v", r)
	}
	if !(recorder.Retention{}).IsZero() {
		t.Fatal("zero retention not zero")
	}
	bad := []recorder.Retention{
		{RetentionRule: recorder.RetentionRule{Steps: -1}},
		{PerWorkflow: map[string]recorder.RetentionRule{"w": {Runs: -1}}},
		{Batch: -1},
		{Batch: recorder.MaxPruneBatch + 1},
	}
	for i, b := range bad {
		if err := recorder.ValidateRetention(b); !errors.Is(err, workflow.ErrInvalid) {
			t.Errorf("bad %d: %v", i, err)
		}
		if _, err := recorder.NewMemoryStore(recorder.WithMemoryRetention(b)); !errors.Is(err, workflow.ErrInvalid) {
			t.Errorf("NewMemoryStore bad %d: %v", i, err)
		}
	}
	// No retention: Prune does nothing.
	s := mustStore(t)
	if res, err := s.Prune(context.Background()); err != nil || res != (recorder.PruneResult{}) {
		t.Fatalf("prune without retention: %+v %v", res, err)
	}
}

// countingPruner counts Prune calls and fails every other one.
type countingPruner struct{ n atomic.Int32 }

func (c *countingPruner) Prune(context.Context) (recorder.PruneResult, error) {
	if c.n.Add(1)%2 == 0 {
		return recorder.PruneResult{}, errors.New("prune failed")
	}
	return recorder.PruneResult{}, nil
}

func TestPruneEvery(t *testing.T) {
	if err := recorder.PruneEvery(context.Background(), nil, time.Second, nil); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("nil pruner: %v", err)
	}
	if err := recorder.PruneEvery(context.Background(), &countingPruner{}, 0, nil); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("zero interval: %v", err)
	}
	p := &countingPruner{}
	var errs atomic.Int32
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- recorder.PruneEvery(ctx, p, time.Millisecond, func(error) { errs.Add(1) }) }()
	waitFor(t, "a few prunes", func() bool { return p.n.Load() >= 4 })
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("PruneEvery returned %v on cancel", err)
	}
	if errs.Load() == 0 {
		t.Fatal("prune errors were not reported")
	}
}

func TestReconcilerRefusals(t *testing.T) {
	store := mustStore(t)
	c := memory.New()
	if _, err := recorder.NewReconciler(nil, store); !errors.Is(err, workflow.ErrInvalid) {
		t.Errorf("nil client: %v", err)
	}
	if _, err := recorder.NewReconciler(c, nil); !errors.Is(err, workflow.ErrInvalid) {
		t.Errorf("nil store: %v", err)
	}
	bad := map[string]recorder.ReconcileOption{
		"interval":     recorder.WithInterval(0),
		"stale":        recorder.WithStaleAfter(-time.Second),
		"window":       recorder.WithBackfillWindow(0),
		"batch 0":      recorder.WithBatch(0),
		"batch big":    recorder.WithBatch(workflow.MaxRunsLimit + 1),
		"pages":        recorder.WithBackfillPages(0),
		"clock":        recorder.WithReconcileClock(nil),
		"scope status": recorder.WithBackfillQuery(workflow.RunQuery{Statuses: []workflow.Status{"x"}}),
	}
	for name, opt := range bad {
		if _, err := recorder.NewReconciler(c, store, opt); !errors.Is(err, workflow.ErrInvalid) {
			t.Errorf("%s: %v", name, err)
		}
	}
	if _, err := recorder.NewReconciler(c, store, nil, recorder.WithBatch(5), recorder.WithBackfillPages(2), recorder.WithInterval(time.Second), recorder.WithStaleAfter(time.Minute)); err != nil {
		t.Errorf("valid options: %v", err)
	}
}

// statusOnly is a client that answers Status only (no RunQuerier), to cover
// the backfill skip and the Status fallback.
type statusOnly struct {
	workflow.Client
	status workflow.Status
	err    error
}

func (s statusOnly) Status(context.Context, workflow.RunRef) (workflow.Status, error) {
	return s.status, s.err
}

func TestReconcilerWithoutRunQuerier(t *testing.T) {
	ctx := context.Background()
	clk := clock.NewFake(start)
	store := mustStore(t)
	ref := workflow.RunRef{RunID: "r1"}
	if err := store.UpsertRun(ctx, recorder.RunWrite{Ref: ref, Workflow: "w", Status: workflow.StatusQueued, EnqueuedAt: start, At: start}); err != nil {
		t.Fatal(err)
	}
	// Running and silent past StaleAfter: flagged, status taken.
	rc, _ := recorder.NewReconciler(statusOnly{Client: memory.New(), status: workflow.StatusRunning}, store, recorder.WithReconcileClock(clk), recorder.WithStaleAfter(time.Minute))
	clk.Advance(2 * time.Minute)
	rep, err := rc.Pass(ctx)
	if err != nil || !rep.BackfillSkipped || rep.Checked != 1 || rep.Stale != 1 {
		t.Fatalf("report %+v err %v", rep, err)
	}
	r, _ := store.Run(ctx, ref)
	if !r.Stale || r.Status != workflow.StatusRunning {
		t.Fatalf("record %+v", r)
	}
	// Terminal through Status alone: finished at the reconciler's clock.
	rc, _ = recorder.NewReconciler(statusOnly{Client: memory.New(), status: workflow.StatusCancelled}, store, recorder.WithReconcileClock(clk))
	if rep, err = rc.Pass(ctx); err != nil || rep.Finished != 1 {
		t.Fatalf("report %+v err %v", rep, err)
	}
	r, _ = store.Run(ctx, ref)
	if r.Status != workflow.StatusCancelled || !r.FinishedAt.Equal(clk.Now()) || r.Stale {
		t.Fatalf("record %+v", r)
	}
	// A Status error is counted and reported; the pass goes on.
	ref2 := workflow.RunRef{RunID: "r2"}
	_ = store.UpsertRun(ctx, recorder.RunWrite{Ref: ref2, Workflow: "w", Status: workflow.StatusQueued, EnqueuedAt: start, At: start})
	var reported int
	rc, _ = recorder.NewReconciler(statusOnly{Client: memory.New(), err: errors.New("engine down")}, store, recorder.WithReconcileClock(clk),
		recorder.WithReconcileOnError(func(context.Context, error) { reported++ }))
	if rep, err = rc.Pass(ctx); err != nil || rep.Errors != 1 || reported != 1 {
		t.Fatalf("report %+v err %v reported %d", rep, err, reported)
	}
}

// brokenStore fails Pending, so a pass fails.
type brokenStore struct{ *recorder.MemoryStore }

func (brokenStore) Pending(context.Context, int) ([]recorder.RunRecord, error) {
	return nil, errors.New("db down")
}

func TestReconcilerRunLoop(t *testing.T) {
	var calls atomic.Int32
	rc, err := recorder.NewReconciler(memory.New(), brokenStore{mustStore(t)}, recorder.WithInterval(time.Millisecond),
		recorder.WithReconcileOnError(func(context.Context, error) { calls.Add(1) }))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rc.Pass(context.Background()); err == nil {
		t.Fatal("Pass with a failing store returned nil")
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- rc.Run(ctx) }()
	waitFor(t, "repeated passes", func() bool { return calls.Load() >= 3 })
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run returned %v", err)
	}
}

// TestReconcilerJoinsDelayedRun: a delayed run recorded under its
// ScheduledID gets its RunID from the client's listing.
func TestReconcilerJoinsDelayedRun(t *testing.T) {
	ctx := context.Background()
	clk := clock.NewFake(start)
	c := memory.New(memory.WithClock(clk))
	if err := c.Register(workflow.Workflow{Name: "w", Steps: []workflow.Step{{Name: "s", Run: func(ctx context.Context, _ []byte) ([]byte, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}}}}); err != nil {
		t.Fatal(err)
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	if err := c.Start(runCtx); err != nil {
		t.Fatal(err)
	}
	ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "w", RunAt: start.Add(time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	store := mustStore(t)
	_ = store.UpsertRun(ctx, recorder.RunWrite{Ref: ref, Workflow: "w", Status: workflow.StatusScheduled, EnqueuedAt: start, At: start})
	clk.Advance(2 * time.Minute)
	waitFor(t, "the run to start", func() bool {
		st, err := c.Status(ctx, ref)
		return err == nil && st == workflow.StatusRunning
	})
	rc, _ := recorder.NewReconciler(c, store, recorder.WithReconcileClock(clk), recorder.WithBackfill(false))
	if _, err := rc.Pass(ctx); err != nil {
		t.Fatal(err)
	}
	r, err := store.Run(ctx, workflow.RunRef{ScheduledID: ref.ScheduledID})
	if err != nil || r.Ref.RunID.IsZero() || r.Status != workflow.StatusRunning {
		t.Fatalf("record %+v err %v", r, err)
	}
}
