package workflow

import (
	"context"
	"fmt"
	"sync"
)

// StepSlots returns how many worker slots one attempt of s holds:
// Step.Slots, with zero counted as one.
func StepSlots(s Step) int { return max(s.Slots, 1) }

// CheckSlots refuses a workflow with a step that needs more slots than a
// pool of size slots holds, since such a step could never start. Drivers
// with CapSlotCost call it at Register. Errors wrap ErrInvalid.
func CheckSlots(wf Workflow, slots int) error {
	for _, s := range wf.Steps {
		if n := StepSlots(s); n > slots {
			return invalidf("workflow %q step %q needs %d worker slots and this client has %d", wf.Name, s.Name, n, slots)
		}
	}
	return nil
}

// SlotPool shares a client's worker slots among step attempts by their
// Step.Slots: Acquire waits until the slots an attempt needs are free and
// Release gives them back. Waiters are served first come, first served,
// so an attempt that needs many slots is never starved by a stream of
// small ones: while it waits at the head, later attempts wait behind it.
// Drivers with CapSlotCost use it between claiming an attempt and running
// it, so every driver applies the same rule. The zero value is not
// ready; use NewSlotPool.
type SlotPool struct {
	mu      sync.Mutex
	size    int
	used    int
	waiting []*slotWaiter
}

type slotWaiter struct {
	n     int
	ready chan struct{}
}

// NewSlotPool returns a pool of size slots (at least one).
func NewSlotPool(size int) *SlotPool { return &SlotPool{size: max(size, 1)} }

// Size is the pool's number of slots.
func (p *SlotPool) Size() int { return p.size }

// InUse is the number of slots held now.
func (p *SlotPool) InUse() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.used
}

// Acquire takes n slots (at least one, at most Size), waiting behind every
// earlier waiter until they are free, or returns ctx's error when ctx ends
// first. n above Size is an error wrapping ErrInvalid: it could never be
// served.
func (p *SlotPool) Acquire(ctx context.Context, n int) error {
	n = max(n, 1)
	if n > p.size {
		return invalidf("slot pool: %d slots asked of %d", n, p.size)
	}
	p.mu.Lock()
	if len(p.waiting) == 0 && p.used+n <= p.size {
		p.used += n
		p.mu.Unlock()
		return nil
	}
	w := &slotWaiter{n: n, ready: make(chan struct{})}
	p.waiting = append(p.waiting, w)
	p.mu.Unlock()
	select {
	case <-w.ready:
		return nil
	case <-ctx.Done():
		p.mu.Lock()
		defer p.mu.Unlock()
		select {
		case <-w.ready: // granted while ctx ended: give the slots back
			p.used -= w.n
			p.grantLocked()
		default:
			for i, x := range p.waiting {
				if x == w {
					p.waiting = append(p.waiting[:i], p.waiting[i+1:]...)
					break
				}
			}
			p.grantLocked() // the head may have changed
		}
		return fmt.Errorf("slot pool: %w", context.Cause(ctx))
	}
}

// Release gives back n slots taken by Acquire and serves the waiters that
// now fit, in order.
func (p *SlotPool) Release(n int) {
	n = max(n, 1)
	p.mu.Lock()
	defer p.mu.Unlock()
	p.used -= n
	if p.used < 0 {
		p.used = 0
	}
	p.grantLocked()
}

// grantLocked serves waiters from the head while they fit.
func (p *SlotPool) grantLocked() {
	for len(p.waiting) > 0 && p.used+p.waiting[0].n <= p.size {
		w := p.waiting[0]
		p.waiting = p.waiting[1:]
		p.used += w.n
		close(w.ready)
	}
}
