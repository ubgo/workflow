package failover

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ubgo/workflow"
)

var (
	_ workflow.RunRescheduler    = (*Client)(nil)
	_ workflow.StepCanceller     = (*Client)(nil)
	_ workflow.ResultReader      = (*Client)(nil)
	_ workflow.NodeLister        = (*Client)(nil)
	_ workflow.HealthChecker     = (*Client)(nil)
	_ workflow.LifecycleNotifier = (*Client)(nil)
	_ workflow.PayloadLimiter    = (*Client)(nil)
	_ workflow.OpProber          = (*Client)(nil)
)

// Supports reports an operation as done only when both backends do it,
// since a run, or a node, may sit on either.
func (f *Client) Supports(op workflow.Op) bool {
	return workflow.Supports(f.primary, op) && workflow.Supports(f.secondary, op)
}

// Reschedule moves a delayed run on the backend its ref names, and
// prefixes the ref it returns.
func (f *Client) Reschedule(ctx context.Context, ref workflow.RunRef, at time.Time) (workflow.RunRef, error) {
	var out workflow.RunRef
	err := f.onSide(ref, func(sd side, c workflow.Client, inner workflow.RunRef) error {
		r, ok := c.(workflow.RunRescheduler)
		if !ok {
			return workflow.Unsupported("Reschedule", "the "+sd.String()+" backend does not implement it")
		}
		got, err := r.Reschedule(ctx, inner, at)
		out = encodeRef(sd, got)
		return err
	})
	if err != nil {
		return workflow.RunRef{}, err
	}
	return out, nil
}

// CancelStep withdraws a step of the run on the backend its ref names.
func (f *Client) CancelStep(ctx context.Context, ref workflow.RunRef, step string) error {
	return f.onSide(ref, func(sd side, c workflow.Client, inner workflow.RunRef) error {
		s, ok := c.(workflow.StepCanceller)
		if !ok {
			return workflow.Unsupported("CancelStep", "the "+sd.String()+" backend does not implement it")
		}
		return s.CancelStep(ctx, inner, step)
	})
}

// Result reads the result from the backend the ref names. Needs CapResult
// on both.
func (f *Client) Result(ctx context.Context, ref workflow.RunRef) ([]byte, error) {
	if err := f.need("Result", workflow.CapResult); err != nil {
		return nil, err
	}
	var out []byte
	err := f.onSide(ref, func(sd side, c workflow.Client, inner workflow.RunRef) error {
		r, ok := c.(workflow.ResultReader)
		if !ok {
			return &workflow.UnsupportedError{Field: "Result", Missing: workflow.CapResult, Reason: "the " + sd.String() + " backend does not implement it"}
		}
		var err error
		out, err = r.Result(ctx, inner)
		return err
	})
	return out, err
}

// Nodes lists the nodes of both backends, primary first. A node ID is the
// backend's own; the same process may appear twice when it runs workers
// for both. Both must implement workflow.NodeLister.
func (f *Client) Nodes(ctx context.Context) ([]workflow.NodeInfo, error) {
	pn, pok := f.primary.(workflow.NodeLister)
	sn, sok := f.secondary.(workflow.NodeLister)
	if !pok || !sok {
		return nil, workflow.Unsupported("Nodes", "both failover backends must implement workflow.NodeLister")
	}
	p, perr := pn.Nodes(ctx)
	s, serr := sn.Nodes(ctx)
	return append(p, s...), joinSides("nodes", perr, serr)
}

// Health is live while both backends are live and ready while either is
// ready, since a failover client can take work as long as one backend
// can. Each check is named after its backend.
func (f *Client) Health(ctx context.Context) workflow.HealthReport {
	p, s := workflow.CheckHealth(ctx, f.primary), workflow.CheckHealth(ctx, f.secondary)
	out := workflow.HealthReport{Live: p.Live && s.Live, Ready: p.Ready || s.Ready}
	for _, c := range p.Checks {
		c.Name = "primary " + c.Name
		out.Checks = append(out.Checks, c)
	}
	for _, c := range s.Checks {
		c.Name = "secondary " + c.Name
		out.Checks = append(out.Checks, c)
	}
	return out
}

// OnLifecycle registers fn on both backends, with each event's Ref and
// schedule label prefixed like the refs this client hands out. Both must
// implement workflow.LifecycleNotifier.
func (f *Client) OnLifecycle(fn workflow.LifecycleFunc) error {
	pl, pok := f.primary.(workflow.LifecycleNotifier)
	sl, sok := f.secondary.(workflow.LifecycleNotifier)
	if !pok || !sok {
		return workflow.Unsupported("OnLifecycle", "both failover backends must implement workflow.LifecycleNotifier")
	}
	f.mu.Lock()
	started := f.started
	f.mu.Unlock()
	if started {
		return workflow.ErrAlreadyStarted
	}
	side := func(s side) workflow.LifecycleFunc {
		return func(ctx context.Context, ev workflow.LifecycleEvent) {
			ev.Ref = encodeRef(s, ev.Ref)
			ev.Labels = sideLabels(ev.Labels, s)
			fn(ctx, ev)
		}
	}
	if err := pl.OnLifecycle(side(primarySide)); err != nil {
		return fmt.Errorf("failover: primary: %w", err)
	}
	if err := sl.OnLifecycle(side(secondarySide)); err != nil {
		if errors.Is(err, workflow.ErrUnsupported) {
			return err
		}
		return fmt.Errorf("failover: secondary: %w", err)
	}
	return nil
}

// PayloadLimit is the smaller of the backends' hard caps (zero for one
// without a cap), since a job may land on either.
func (f *Client) PayloadLimit() int {
	limit := func(c workflow.Client) int {
		if l, ok := c.(workflow.PayloadLimiter); ok {
			return l.PayloadLimit()
		}
		return 0
	}
	p, s := limit(f.primary), limit(f.secondary)
	switch {
	case p == 0:
		return s
	case s == 0:
		return p
	}
	return min(p, s)
}
