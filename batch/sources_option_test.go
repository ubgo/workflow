package batch_test

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/batch"
)

// hookCounter is a client with a guaranteed run-finished hook that counts
// how often a hook is registered, so a test sees whether Attach used the
// default HookSource.
type hookCounter struct {
	*stub
	hooks atomic.Int32
}

func (h *hookCounter) OnRunFinished(workflow.RunFinishedFunc) error { h.hooks.Add(1); return nil }

// countSource counts its Attach calls.
type countSource struct{ n atomic.Int32 }

func (s *countSource) Attach(context.Context, *batch.Manager) error { s.n.Add(1); return nil }

// TestWithCompletionSourceReplaces: WithCompletionSource replaces the
// Sources, as its doc says: the last option wins (two do not stack), one
// option with several sources attaches them all, and an empty option
// means no Source at all (the app reports run ends through RunFinished),
// not the defaults.
func TestWithCompletionSourceReplaces(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	attach := func(opts ...batch.Option) *hookCounter {
		t.Helper()
		c := &hookCounter{stub: newStub(workflow.CapsOf(workflow.CapRunFinished))}
		if err := batch.New(c, batch.NewMemoryStore(), opts...).Attach(ctx); err != nil {
			t.Fatal(err)
		}
		return c
	}
	if c := attach(); c.hooks.Load() != 1 {
		t.Fatalf("no option: %d hooks, want the default HookSource", c.hooks.Load())
	}
	if c := attach(batch.WithCompletionSource()); c.hooks.Load() != 0 {
		t.Errorf("an empty WithCompletionSource attached the default hook (%d)", c.hooks.Load())
	}
	first, second := &countSource{}, &countSource{}
	if c := attach(batch.WithCompletionSource(first), batch.WithCompletionSource(second)); c.hooks.Load() != 0 || first.n.Load() != 0 || second.n.Load() != 1 {
		t.Errorf("two options: hooks %d, first %d, second %d; want only the second attached", c.hooks.Load(), first.n.Load(), second.n.Load())
	}
	a, b := &countSource{}, &countSource{}
	if attach(batch.WithCompletionSource(a, b)); a.n.Load() != 1 || b.n.Load() != 1 {
		t.Errorf("one option with two sources: %d and %d attaches, want 1 each", a.n.Load(), b.n.Load())
	}
}
