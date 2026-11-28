package workflow_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ubgo/workflow"
)

// TestExpandScheduleKey pins every placeholder, a fixed key, and the
// refusals.
func TestExpandScheduleKey(t *testing.T) {
	berlin, err := time.LoadLocation("Europe/Berlin")
	if err != nil {
		t.Skip("no tz database")
	}
	at := time.Date(2026, 10, 31, 23, 30, 0, 0, time.UTC) // already November in Berlin
	for tmpl, want := range map[string]string{
		"sync:acme":         "sync:acme",
		"t:{fire}":          "t:2026-10-31T23:30:00Z",
		"d:{day}":           "d:2026-11-01",
		"w:{week}":          "w:2026-W44",
		"m:{month}/{year}":  "m:2026-11/2026",
		"q:{quarter}-{day}": "q:2026-Q4-2026-11-01",
	} {
		got, err := workflow.ExpandScheduleKey(tmpl, at, berlin)
		if err != nil || got != want {
			t.Errorf("ExpandScheduleKey(%q) = %q, %v; want %q", tmpl, got, err, want)
		}
	}
	for _, tmpl := range []string{"", "x:{hour}", "x:{day"} {
		if _, err := workflow.ExpandScheduleKey(tmpl, at, berlin); !errors.Is(err, workflow.ErrInvalid) {
			t.Errorf("ExpandScheduleKey(%q): %v, want ErrInvalid", tmpl, err)
		}
	}
	if _, err := workflow.ExpandScheduleKey("x", at, time.Local); !errors.Is(err, workflow.ErrInvalid) {
		t.Errorf("time.Local: %v, want ErrInvalid", err)
	}
}

// TestScheduleKeyValidationAndCaps: a bad template or a lone OnHeld is
// ErrInvalid, and a key needs CapScheduleKey, refused naming the field.
func TestScheduleKeyValidationAndCaps(t *testing.T) {
	base := workflow.Schedule{Workflow: "w", Cron: "* * * * *", Location: time.UTC}
	bad := []workflow.Schedule{base, base, base}
	bad[0].IdempotencyKey = "k:{nope}"
	bad[1].OnHeld = workflow.HeldRefuse
	bad[2].IdempotencyKey, bad[2].OnHeld = "k", "sometimes"
	for i, s := range bad {
		if err := workflow.ValidateSchedule(s); !errors.Is(err, workflow.ErrInvalid) {
			t.Errorf("schedule %d: %v, want ErrInvalid", i, err)
		}
	}
	ok := base
	ok.IdempotencyKey, ok.OnHeld = "k:{day}", workflow.HeldRefuse
	if err := workflow.ValidateSchedule(ok); err != nil {
		t.Fatalf("a valid keyed schedule: %v", err)
	}
	err := workflow.CheckScheduleCaps(workflow.CapsOf(workflow.CapCron, workflow.CapIdempotencyKey), ok)
	var u *workflow.UnsupportedError
	if !errors.As(err, &u) || u.Field != "Schedule.IdempotencyKey" || !u.Missing.Has(workflow.CapScheduleKey) {
		t.Fatalf("CheckScheduleCaps without CapScheduleKey: %v", err)
	}
}

// TestCoalesceValidationAndCaps: Coalesce needs CapCoalesce, refuses an
// unknown payload choice and IdempotencyOff.
func TestCoalesceValidationAndCaps(t *testing.T) {
	noop := func(context.Context, []byte) ([]byte, error) { return nil, nil }
	wf := workflow.Workflow{Name: "w", Steps: []workflow.Step{{Name: "s", Run: noop}}, Coalesce: &workflow.Coalesce{}}
	if err := workflow.Validate(wf); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if !workflow.Requires(wf).Has(workflow.CapsOf(workflow.CapCoalesce, workflow.CapIdempotencyKey)) {
		t.Errorf("Requires = %v, want CapCoalesce and CapIdempotencyKey", workflow.Requires(wf))
	}
	wf.Coalesce.Payload = "keep-middle"
	if err := workflow.Validate(wf); !errors.Is(err, workflow.ErrInvalid) {
		t.Errorf("an unknown Coalesce.Payload: %v, want ErrInvalid", err)
	}
	wf.Coalesce.Payload, wf.Idempotency = workflow.CoalesceKeepLast, workflow.IdempotencyOff
	if err := workflow.Validate(wf); !errors.Is(err, workflow.ErrInvalid) {
		t.Errorf("Coalesce with IdempotencyOff: %v, want ErrInvalid", err)
	}
}

// fakeCheckpointer keeps one checkpoint, to pin the core helpers.
type fakeCheckpointer struct {
	data []byte
	ttl  time.Duration
}

func (f *fakeCheckpointer) SaveCheckpoint(_ context.Context, b []byte, ttl time.Duration) error {
	f.data, f.ttl = b, ttl
	return nil
}
func (f *fakeCheckpointer) LoadCheckpoint(context.Context) ([]byte, bool, error) {
	return f.data, f.data != nil, nil
}
func (f *fakeCheckpointer) ClearCheckpoint(context.Context) error { f.data = nil; return nil }

// TestCheckpointHelpers pins the typed accessor, the TTL option, the size
// check and the refusal outside a step.
func TestCheckpointHelpers(t *testing.T) {
	if err := workflow.SaveCheckpoint(context.Background(), []byte("x")); !errors.Is(err, workflow.ErrUnsupported) {
		t.Fatalf("outside a step: %v, want ErrUnsupported", err)
	}
	fc := &fakeCheckpointer{}
	ctx := workflow.WithCheckpointer(context.Background(), fc)
	type cursor struct{ Page int }
	cp := workflow.DefineCheckpoint[cursor]()
	if _, ok, err := cp.Load(ctx); ok || err != nil {
		t.Fatalf("Load before a save = %v, %v", ok, err)
	}
	if err := cp.Save(ctx, cursor{Page: 7}, workflow.CheckpointTTL(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if v, ok, err := cp.Load(ctx); !ok || err != nil || v.Page != 7 || fc.ttl != time.Minute {
		t.Fatalf("Load = %+v, %v, %v (ttl %v)", v, ok, err, fc.ttl)
	}
	if err := workflow.SaveCheckpoint(ctx, nil, workflow.CheckpointTTL(-time.Second)); !errors.Is(err, workflow.ErrInvalid) {
		t.Errorf("a negative TTL: %v, want ErrInvalid", err)
	}
	fc.data = []byte("not json")
	if _, _, err := cp.Load(ctx); !workflow.IsPermanent(err) {
		t.Errorf("undecodable bytes: %v, want a Permanent error", err)
	}
	if err := cp.Clear(ctx); err != nil || fc.data != nil {
		t.Errorf("Clear: %v, data %q", err, fc.data)
	}
	err := workflow.CheckCheckpointSize("s", make([]byte, 11), 10, "test")
	var tl *workflow.CheckpointTooLargeError
	if !errors.As(err, &tl) || !errors.Is(err, workflow.ErrCheckpointTooLarge) || !errors.Is(err, workflow.ErrInvalid) || tl.Size != 11 {
		t.Errorf("CheckCheckpointSize over the limit: %v", err)
	}
	if err := workflow.CheckCheckpointSize("s", make([]byte, workflow.DefaultMaxCheckpoint), 0, "test"); err != nil {
		t.Errorf("the default limit itself: %v", err)
	}
}

// TestCompletionTx pins the handle accessor and Forward's refusal.
func TestCompletionTx(t *testing.T) {
	if _, ok := workflow.CompletionTx[*int](context.Background()); ok {
		t.Error("CompletionTx outside fn reported a handle")
	}
	n := 1
	ctx := workflow.WithCompletionTx(context.Background(), &n)
	if got, ok := workflow.CompletionTx[*int](ctx); !ok || got != &n {
		t.Error("CompletionTx lost the handle")
	}
	if _, ok := workflow.CompletionTx[string](ctx); ok {
		t.Error("CompletionTx returned a handle of the wrong type")
	}
	f := workflow.Forward{Inner: nil}
	if err := f.CompleteInTx(context.Background(), nil, func(context.Context) error { return nil }); !errors.Is(err, workflow.ErrUnsupported) {
		t.Errorf("Forward over a client without TxCompleter: %v, want ErrUnsupported", err)
	}
}
