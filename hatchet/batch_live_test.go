package hatchet_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/batch"
	"github.com/ubgo/workflow/hatchet"
)

// batchQuiet is how long the test keeps watching after the callbacks ran,
// so a duplicate or a stray Then would show up.
const batchQuiet = 5 * time.Second

// TestBatchLive runs a 3-job batch with one failing job on the real
// engine, on the best-effort run-finished hook plus the status poller:
// Catch and Finally each run exactly once with the final Progress, and
// Then never runs.
func TestBatchLive(t *testing.T) {
	if os.Getenv(envToken) == "" {
		t.Skipf("%s is not set: the live batch test needs a Hatchet engine (task test:live)", envToken)
	}
	prefix := "wfbatch_" + uuid.NewString()[:8]
	var mu sync.Mutex
	calls := map[string][]batch.Progress{}
	callback := func(kind string) workflow.Handler {
		return func(_ context.Context, in []byte) ([]byte, error) {
			var p batch.Progress
			if err := json.Unmarshal(in, &p); err != nil {
				return nil, workflow.Permanent(err)
			}
			mu.Lock()
			calls[kind] = append(calls[kind], p)
			mu.Unlock()
			return nil, nil
		}
	}
	one := func(name string, h workflow.Handler) workflow.Workflow {
		return workflow.Workflow{Name: prefix + "_" + name, Steps: []workflow.Step{{Name: "a", Run: h}}}
	}
	wfs := []workflow.Workflow{
		one("ok", func(context.Context, []byte) ([]byte, error) { return []byte(`"ok"`), nil }),
		one("bad", func(context.Context, []byte) ([]byte, error) {
			return nil, workflow.Permanent(errors.New("job failed"))
		}),
		one("then", callback("then")),
		one("catch", callback("catch")),
		one("finally", callback("finally")),
	}

	c := hatchet.New(hatchet.Config{WorkerName: "wfbatch"})
	m := batch.New(c, batch.NewMemoryStore(), batch.WithStatusPoll(500*time.Millisecond))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := m.Attach(ctx); err != nil {
		t.Fatal(err)
	}
	for _, wf := range wfs {
		if err := c.Register(wf); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	id, err := m.Dispatch(ctx, batch.Batch{
		Name:    "live",
		Jobs:    []workflow.Job{{Workflow: prefix + "_ok"}, {Workflow: prefix + "_bad"}, {Workflow: prefix + "_ok"}},
		Then:    prefix + "_then",
		Catch:   prefix + "_catch",
		Finally: prefix + "_finally",
	})
	if err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(liveWait)
	for {
		mu.Lock()
		done := len(calls["catch"]) > 0 && len(calls["finally"]) > 0
		mu.Unlock()
		if done {
			break
		}
		if time.Now().After(deadline) {
			p, _ := m.Progress(ctx, id)
			t.Fatalf("Catch and Finally did not both run within %s; progress %+v", liveWait, p)
		}
		time.Sleep(200 * time.Millisecond)
	}
	time.Sleep(batchQuiet)

	mu.Lock()
	defer mu.Unlock()
	if n := len(calls["then"]); n != 0 {
		t.Fatalf("Then ran %d times for a batch with a failed job", n)
	}
	// Without AllowFailures the failed job cancels the batch: Catch runs at
	// once on that failure, and Finally once every job has reported back.
	if n := len(calls["catch"]); n != 1 {
		t.Fatalf("catch ran %d times, want once", n)
	}
	if p := calls["catch"][0]; p.ID != id || p.Total != 3 || p.Failed != 1 || !p.Cancelled {
		t.Fatalf("catch got progress %+v, want the failure recorded and the batch cancelled", p)
	}
	if n := len(calls["finally"]); n != 1 {
		t.Fatalf("finally ran %d times, want once", n)
	}
	if p := calls["finally"][0]; p.ID != id || p.Total != 3 || p.Failed != 1 || !p.Finished || p.Pending != 0 || p.Succeeded+p.CancelledJobs != 2 {
		t.Fatalf("finally got progress %+v, want every job finished with one failure", p)
	}
}
