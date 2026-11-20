package hatchet

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/hatchet-dev/hatchet/pkg/client/rest"
)

// lagLookup is a scheduleLookup that replays the engine's read lag: the
// scheduled record is already gone (the engine deletes it when it fires,
// and Cancel deletes it), and the fired run, if any, is listed only from
// the listedAfter-th search on.
type lagLookup struct {
	mu          sync.Mutex
	runID       string // "" when nothing fired
	listedAfter int    // searches before the run is listed
	searches    int
}

func (l *lagLookup) schedule(context.Context, string) (*rest.ScheduledWorkflows, error) {
	return nil, nil
}

func (l *lagLookup) firedRun(context.Context, scheduledRef) (string, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.searches++
	if l.runID == "" || l.searches <= l.listedAfter {
		return "", nil
	}
	return l.runID, nil
}

func (l *lagLookup) count() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.searches
}

func never(string) bool { return false }

// A delayed start enqueued for tomorrow and moved to now by Reschedule
// keeps its ScheduledID, which still names tomorrow. When it fires, the
// engine deletes the scheduled record before its read side lists the run;
// in that window the run must not read cancelled. Resolving by the
// ScheduledID's RunAt ("nothing can fire before it") did read it
// cancelled, which the live conformance case "Reschedule moves a delayed
// run earlier" caught under load.
func TestResolveScheduledMovedEarlierWaitsForTheRun(t *testing.T) {
	look := &lagLookup{runID: "0198d6a0-0000-7000-8000-000000000001", listedAfter: 3}
	sid := scheduledRef{token: newScheduleToken(), runAt: time.Now().Add(24 * time.Hour)}
	runID, state, err := resolveScheduled(context.Background(), look, sid, 5*time.Second, never)
	if err != nil {
		t.Fatalf("resolveScheduled: %v", err)
	}
	if state != stateRun || runID != look.runID {
		t.Fatalf("resolveScheduled = (%q, %v), want the fired run %q (stateRun)", runID, state, look.runID)
	}
	if n := look.count(); n != 4 {
		t.Errorf("searched %d times, want 4 (three misses while the read side lags, then the run)", n)
	}
}

// A delayed start deleted by someone else (another process's Cancel, the
// dashboard) has no run: it reads gone once the read side had lag to list
// a run it might have fired, and not before.
func TestResolveScheduledGoneAfterLag(t *testing.T) {
	look := &lagLookup{}
	sid := scheduledRef{token: newScheduleToken(), runAt: time.Now().Add(24 * time.Hour)}
	lag := 400 * time.Millisecond
	start := time.Now()
	runID, state, err := resolveScheduled(context.Background(), look, sid, lag, never)
	if err != nil || state != stateGone || runID != "" {
		t.Fatalf("resolveScheduled = (%q, %v, %v), want stateGone", runID, state, err)
	}
	if took := time.Since(start); took < lag {
		t.Errorf("called gone after %s, before the %s read lag passed", took, lag)
	}
}

// A delayed start this client deleted well before it was due reads gone
// at once: nothing can have fired it, so there is no lag to wait out.
func TestResolveScheduledDeletedHereIsGoneAtOnce(t *testing.T) {
	look := &lagLookup{}
	sid := scheduledRef{token: newScheduleToken(), runAt: time.Now().Add(24 * time.Hour)}
	var d deletedStarts
	d.add(sid.token)
	start := time.Now()
	_, state, err := resolveScheduled(context.Background(), look, sid, time.Hour, d.has)
	if err != nil || state != stateGone {
		t.Fatalf("resolveScheduled = (%v, %v), want stateGone", state, err)
	}
	if took := time.Since(start); took > time.Second {
		t.Errorf("took %s, want an answer without waiting the read lag", took)
	}
	if n := look.count(); n != 1 {
		t.Errorf("searched %d times, want 1", n)
	}
}

// The ctx ends the wait for a lagging read.
func TestResolveScheduledHonoursCtx(t *testing.T) {
	look := &lagLookup{}
	sid := scheduledRef{token: newScheduleToken(), runAt: time.Now()}
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	if _, _, err := resolveScheduled(ctx, look, sid, time.Hour, never); err != context.DeadlineExceeded {
		t.Fatalf("resolveScheduled with an ended ctx = %v, want context.DeadlineExceeded", err)
	}
}

// deletedStarts keeps the most recent deletedStartsMax tokens.
func TestDeletedStartsBounded(t *testing.T) {
	var d deletedStarts
	if d.has("x") {
		t.Fatal("an empty set has a token")
	}
	first := "t-first"
	d.add(first)
	d.add(first) // a repeat takes no second slot
	for i := 0; i < deletedStartsMax-1; i++ {
		d.add(newScheduleToken())
	}
	if !d.has(first) {
		t.Fatal("the oldest token was dropped before the set was over its bound")
	}
	d.add(newScheduleToken())
	if d.has(first) {
		t.Error("the oldest token is kept past the bound")
	}
	if n := len(d.set); n != deletedStartsMax {
		t.Errorf("the set holds %d tokens, want %d", n, deletedStartsMax)
	}
}
