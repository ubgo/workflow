package failover_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/failover"
	"github.com/ubgo/workflow/memory"
)

// TestClassifiers_Unavailable pins both classifiers over the error shapes
// a driver returns: marked unavailable, unknown, a caller fault, a
// driver timeout marked unavailable, and Permanent.
func TestClassifiers_Unavailable(t *testing.T) {
	marked := workflow.Unavailable(errors.New("dial tcp: connection refused"))
	cases := []struct {
		name        string
		err         error
		def, strict bool
	}{
		{"marked", marked, true, true},
		{"marked and wrapped", fmt.Errorf("hatchet: %w", marked), true, true},
		{"driver timeout marked", workflow.Unavailable(context.DeadlineExceeded), true, true},
		{"unknown", errors.New("something odd"), true, false},
		{"caller fault", workflow.ErrInvalid, false, false},
		{"unmarked timeout", context.DeadlineExceeded, false, false},
		{"permanent and marked", workflow.Permanent(marked), false, false},
		{"nil", nil, false, false},
	}
	for _, tc := range cases {
		if got := failover.DefaultClassifier(tc.err); got != tc.def {
			t.Errorf("DefaultClassifier(%s) = %v, want %v", tc.name, got, tc.def)
		}
		if got := failover.StrictClassifier(tc.err); got != tc.strict {
			t.Errorf("StrictClassifier(%s) = %v, want %v", tc.name, got, tc.strict)
		}
	}
}

// TestStrictClassifier_EndToEnd: under StrictClassifier a marked outage
// fails over (allow) and an unknown error does not (deny).
func TestStrictClassifier_EndToEnd(t *testing.T) {
	fx := newFixture(t, failover.WithClassifier(failover.StrictClassifier))
	fx.up(waitWF)
	fx.p.down.Store(true)
	if _, err := fx.c.Enqueue(fx.ctx, workflow.Job{Workflow: "wait", IdempotencyKey: "a"}); !errors.Is(err, errUnavailable) {
		t.Fatalf("unknown error: %v, want the primary's error", err)
	}
	if fx.s.calls.Load() != 0 {
		t.Fatal("strict classifier failed over on an unknown error")
	}
	fx.p.downErr = workflow.Unavailable(errUnavailable)
	if ref := fx.enqueue("b"); !strings.HasPrefix(string(ref.RunID), "secondary:") {
		t.Fatalf("marked outage: %q, want the secondary", ref.RunID)
	}
}

type recorded struct {
	mu  sync.Mutex
	evs []failover.Event
}

func (r *recorded) observe(_ context.Context, ev failover.Event) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.evs = append(r.evs, ev)
}

func (r *recorded) kinds() []failover.EventKind {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]failover.EventKind, len(r.evs))
	for i, ev := range r.evs {
		out[i] = ev.Kind
	}
	return out
}

func TestObserver(t *testing.T) {
	clk := clock.NewFake(start)
	p := &switchable{Client: memory.New(memory.WithClock(clk))}
	s := &switchable{Client: memory.New(memory.WithClock(clk))}
	rec := &recorded{}
	c := failover.New(p, s, failover.WithCoolOff(2, time.Minute), failover.WithClock(clk), failover.WithObserver(rec.observe))
	if err := c.Register(waitWF); err != nil {
		t.Fatal(err)
	}
	if err := c.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	enq := func(key string) {
		t.Helper()
		if _, err := c.Enqueue(t.Context(), workflow.Job{Workflow: "wait", IdempotencyKey: key}); err != nil {
			t.Fatal(err)
		}
	}
	enq("ok") // primary up: nothing to report
	p.down.Store(true)
	enq("a")
	enq("b")
	enq("c")
	p.down.Store(false)
	clk.Advance(time.Minute)
	enq("d")
	want := []failover.EventKind{
		failover.EventFailedOver,
		failover.EventFailedOver, failover.EventCoolOffStarted,
		failover.EventPrimarySkipped,
		failover.EventCoolOffEnded,
	}
	got := rec.kinds()
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("events = %v, want %v", got, want)
	}
	rec.mu.Lock()
	defer rec.mu.Unlock()
	if ev := rec.evs[0]; !errors.Is(ev.Err, errUnavailable) || ev.Workflow != "wait" || !ev.At.Equal(start) {
		t.Fatalf("failover event = %+v", ev)
	}
	if ev := rec.evs[2]; !ev.Until.Equal(start.Add(time.Minute)) {
		t.Fatalf("cool-off event = %+v", ev)
	}
}
