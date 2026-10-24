package hatchet

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ubgo/workflow"
)

// permText is the Permanent step's error text, checked verbatim on the
// way back to the user.
const permText = "card declined: insufficient funds"

// TestRetrySettleLive proves the driver never reports a run failed in the
// moment the engine shows FAILED between a failed attempt and its retry
// (failed.go), and that a Permanent failure with retries left is still
// reported failed at once, through the marker, not after the settle
// window, with the step's own text and no marker in what users read. Status is polled every few milliseconds, which caught the
// window in about one run in four before the fix.
func TestRetrySettleLive(t *testing.T) {
	if os.Getenv("HATCHET_CLIENT_TOKEN") == "" {
		t.Skip("HATCHET_CLIENT_TOKEN is not set: needs a live engine (task test:live)")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	c := New(Config{WorkerName: "wftest-settle"})
	prefix := fmt.Sprintf("wftest_settle_%d", time.Now().UnixNano())
	const runs = 8
	var mu sync.Mutex
	attempts := map[string]int{}
	flaky := workflow.Workflow{Name: prefix + "_flaky", Retry: workflow.RetryPolicy{Max: 2}, Steps: []workflow.Step{{Name: "only", Run: func(_ context.Context, in []byte) ([]byte, error) {
		mu.Lock()
		attempts[string(in)]++
		n := attempts[string(in)]
		mu.Unlock()
		if n < 3 {
			return nil, errors.New("boom")
		}
		return in, nil
	}}}}
	var failedAt atomic.Int64
	perm := workflow.Workflow{Name: prefix + "_perm", Retry: workflow.RetryPolicy{Max: 5}, Steps: []workflow.Step{{Name: "only", Run: func(context.Context, []byte) ([]byte, error) {
		failedAt.Store(time.Now().UnixNano())
		return nil, workflow.Permanent(errors.New(permText))
	}}}}
	for _, wf := range []workflow.Workflow{flaky, perm} {
		if err := c.Register(wf); err != nil {
			t.Fatal(err)
		}
	}
	permEnded := make(chan workflow.RunEvent, 4)
	if err := c.OnRunFinished(func(_ context.Context, ev workflow.RunEvent) {
		if ev.Workflow == perm.Name {
			permEnded <- ev
		}
	}); err != nil {
		t.Fatal(err)
	}
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}

	poll := func(ref workflow.RunRef) workflow.Status {
		for {
			st, err := c.Status(ctx, ref)
			if err != nil {
				t.Fatal(err)
			}
			if st.IsTerminal() {
				return st
			}
			select {
			case <-ctx.Done():
				t.Fatal("run did not end")
			case <-time.After(5 * time.Millisecond):
			}
		}
	}
	var wg sync.WaitGroup
	for i := range runs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ref, err := c.Enqueue(ctx, workflow.Job{Workflow: flaky.Name, Payload: []byte(fmt.Sprint(i))})
			if err != nil {
				t.Error(err)
				return
			}
			if st := poll(ref); st != workflow.StatusSucceeded {
				mu.Lock()
				n := attempts[fmt.Sprint(i)]
				mu.Unlock()
				t.Errorf("run %d read %q after %d attempts, want succeeded after 3", i, st, n)
			}
		}()
	}
	wg.Wait()

	ref, err := c.Enqueue(ctx, workflow.Job{Workflow: perm.Name})
	if err != nil {
		t.Fatal(err)
	}
	if st := poll(ref); st != workflow.StatusFailed {
		t.Fatalf("permanent run = %q, want failed", st)
	}
	if lag := time.Since(time.Unix(0, failedAt.Load())); lag >= DefaultFailedSettle {
		t.Errorf("permanent failure reported %s after the step failed: it waited out the settle window instead of reading the marker", lag)
	}
	page, err := c.Runs(ctx, workflow.RunQuery{Ref: ref})
	if err != nil || len(page.Runs) != 1 {
		t.Fatalf("Runs = %+v, %v", page.Runs, err)
	}
	// The step's text reaches the user unchanged: no driver marker in
	// RunInfo.Error (what admin shows) or the run-finished event.
	if e := page.Runs[0].Error; !strings.Contains(e, permText) || strings.Contains(e, finalMarker) {
		t.Errorf("RunInfo.Error = %q: want the step's message %q without the driver's marker", e, permText)
	}
	select {
	case ev := <-permEnded:
		if ev.Status != workflow.StatusFailed || ev.Err == nil || !strings.Contains(ev.Err.Error(), permText) || strings.Contains(ev.Err.Error(), finalMarker) {
			t.Errorf("RunEvent = %+v: want failed with the step's message and no marker", ev)
		}
	case <-ctx.Done():
		t.Fatal("no run-finished event for the permanent run")
	}
}
