package middleware_test

import (
	"bytes"
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/memory"
	"github.com/ubgo/workflow/middleware"
)

// chainSeen records what each workflow's step saw (after the step
// middleware) and what the driver carried (before it).
type chainSeen struct {
	mu      sync.Mutex
	plain   map[string][]byte
	carried map[string][]byte
}

func newChainSeen() *chainSeen {
	return &chainSeen{plain: map[string][]byte{}, carried: map[string][]byte{}}
}

func (s *chainSeen) outer(next workflow.Handler) workflow.Handler {
	return func(ctx context.Context, in []byte) ([]byte, error) {
		info, _ := workflow.StepInfoFrom(ctx)
		s.mu.Lock()
		s.carried[info.Workflow] = bytes.Clone(in)
		s.mu.Unlock()
		return next(ctx, in)
	}
}

func (s *chainSeen) record(name string, edit func(ctx context.Context) error) workflow.Workflow {
	return workflow.Workflow{Name: name, Steps: []workflow.Step{{Name: "s", Run: func(ctx context.Context, in []byte) ([]byte, error) {
		s.mu.Lock()
		s.plain[name] = bytes.Clone(in)
		s.mu.Unlock()
		if edit != nil {
			return nil, edit(ctx)
		}
		return nil, nil
	}}}}
}

func (s *chainSeen) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.plain)
}

func startChainClient(t *testing.T, c workflow.Client, mem *memory.Client, wfs ...workflow.Workflow) context.Context {
	t.Helper()
	for _, wf := range wfs {
		if err := c.Register(wf); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() { cancel(); mem.Stop() })
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	return ctx
}

// TestCodecSealsChainedJobs proves a chain's jobs, those in Job.Chain and
// those a step adds, reach their steps opened and the driver sealed: the
// driver enqueues them itself, past the enqueue middleware.
func TestCodecSealsChainedJobs(t *testing.T) {
	codec, err := middleware.Encrypt(bytes.Repeat([]byte{7}, 32))
	if err != nil {
		t.Fatal(err)
	}
	seen := newChainSeen()
	mem := memory.New(memory.WithMiddleware(seen.outer, middleware.DecodeStep(codec)))
	c := middleware.WithCodec(mem, codec)
	a := seen.record("a", func(ctx context.Context) error {
		return workflow.PrependToChain(ctx, workflow.Job{Workflow: "added", Payload: []byte("secret-added")})
	})
	ctx := startChainClient(t, c, mem, a, seen.record("b", nil), seen.record("added", nil))
	if _, err := c.Enqueue(ctx, workflow.Job{Workflow: "a", Payload: []byte("secret-a"),
		Chain: []workflow.Job{{Workflow: "b", Payload: []byte("secret-b")}}}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the chain", func() bool { return seen.count() == 3 })
	seen.mu.Lock()
	defer seen.mu.Unlock()
	for wf, want := range map[string]string{"a": "secret-a", "b": "secret-b", "added": "secret-added"} {
		if string(seen.plain[wf]) != want {
			t.Errorf("%s saw %q, want %q", wf, seen.plain[wf], want)
		}
		if strings.Contains(string(seen.carried[wf]), "secret") {
			t.Errorf("the driver carried %s's payload in the clear: %q", wf, seen.carried[wf])
		}
	}
}

// TestClaimCheckCoversChainedJobs proves a chained job's big payload, in
// Job.Chain or added by a step, is claimed and resolved for its own run.
func TestClaimCheckCoversChainedJobs(t *testing.T) {
	store := middleware.NewMemoryBlobStore()
	cc := newClaimCheck(t, store)
	seen := newChainSeen()
	mem := memory.New(memory.WithMiddleware(seen.outer, cc.Step()))
	c := middleware.WrapClient(mem, cc.Enqueue())
	bigB := []byte(strings.Repeat("b", 300))
	bigAdded := []byte(strings.Repeat("x", 300))
	a := seen.record("a", func(ctx context.Context) error {
		return workflow.AppendToChain(ctx, workflow.Job{Workflow: "added", Payload: bigAdded})
	})
	ctx := startChainClient(t, c, mem, a, seen.record("b", nil), seen.record("added", nil))
	if _, err := c.Enqueue(ctx, workflow.Job{Workflow: "a", Chain: []workflow.Job{{Workflow: "b", Payload: bigB}}}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the chain", func() bool { return seen.count() == 3 })
	seen.mu.Lock()
	defer seen.mu.Unlock()
	for wf, want := range map[string][]byte{"b": bigB, "added": bigAdded} {
		if !bytes.Equal(seen.plain[wf], want) {
			t.Errorf("%s saw %d bytes, want its %d-byte payload", wf, len(seen.plain[wf]), len(want))
		}
		if len(seen.carried[wf]) > claimThreshold {
			t.Errorf("the driver carried %s's %d-byte payload instead of a reference", wf, len(seen.carried[wf]))
		}
	}
}
