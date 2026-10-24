package workflowtest

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
)

// env is one subtest's client and the helpers that drive it.
type env struct {
	t       *testing.T
	h       *Harness
	nonce   string
	c       workflow.Client
	clk     *clock.Fake
	restart func(t *testing.T) workflow.Client
	crash   func(t *testing.T) workflow.Client
	eonly   func(t *testing.T) workflow.Client
	peer    func(t *testing.T) workflow.Client
	tm      timings
	stop    context.CancelFunc
	wfs     []workflow.Workflow
}

// newEnv builds a fresh instance for t. The client is not started.
func newEnv(t *testing.T, h *Harness, nonce string) *env {
	t.Helper()
	in := h.New(t)
	if in.Client == nil {
		t.Fatal("workflowtest: Harness.New returned a nil Client")
	}
	return &env{t: t, h: h, nonce: nonce, c: in.Client, clk: in.Clock, restart: in.Restart, crash: in.Crash, eonly: in.EnqueueOnly, peer: in.Peer, tm: h.timings(in.Clock != nil)}
}

// withQueues swaps the env onto a client built with qs (Harness.NewWithQueues),
// or skips the subtest when the harness has none. The client New made stays
// unstarted and unused.
func (e *env) withQueues(qs ...workflow.QueueConfig) {
	e.t.Helper()
	if e.h.NewWithQueues == nil {
		e.t.Skip("Harness.NewWithQueues is nil")
	}
	in := e.h.NewWithQueues(e.t, qs)
	if in.Client == nil {
		e.t.Fatal("workflowtest: Harness.NewWithQueues returned a nil Client")
	}
	e.c, e.clk, e.restart, e.crash, e.eonly, e.peer = in.Client, in.Clock, in.Restart, in.Crash, in.EnqueueOnly, in.Peer
	e.tm = e.h.timings(in.Clock != nil)
}

// need skips the subtest unless the client has every capability in want,
// naming what is missing.
func (e *env) need(want workflow.Caps) {
	e.t.Helper()
	if missing := e.c.Capabilities().Missing(want); missing != 0 {
		e.t.Skipf("driver lacks %s", missing)
	}
}

// name returns a name unique to this subtest and this Run, made of
// lowercase letters, digits and dashes so every engine accepts it.
func (e *env) name(suffix string) string {
	prefix := e.h.Prefix
	if prefix == "" {
		prefix = defaultPrefix
	}
	sub := e.t.Name()
	if i := strings.LastIndexByte(sub, '/'); i >= 0 {
		sub = sub[i+1:]
	}
	var b strings.Builder
	for _, r := range strings.ToLower(sub + "-" + suffix) {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
		} else if s := b.String(); s != "" && s[len(s)-1] != '-' {
			b.WriteByte('-')
		}
	}
	return prefix + "-" + e.nonce + "-" + strings.Trim(b.String(), "-")
}

// newNonce returns a short random hex string. It panics only if the
// operating system's random source fails, which crypto/rand documents as
// never returning an error on supported platforms.
func newNonce() string {
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b[:])
}

// register registers every wf or fails the subtest, and remembers them so
// a restart can register them again.
func (e *env) register(wfs ...workflow.Workflow) {
	e.t.Helper()
	for _, wf := range wfs {
		if err := e.c.Register(wf); err != nil {
			e.t.Fatalf("Register(%s): %v", wf.Name, err)
		}
	}
	e.wfs = append(e.wfs, wfs...)
}

// start starts the client with a context cancelled when the subtest ends.
func (e *env) start() {
	e.t.Helper()
	if err := e.tryStart(); err != nil {
		e.t.Fatalf("Start: %v", err)
	}
}

func (e *env) tryStart() error {
	ctx, cancel := context.WithCancel(context.Background())
	e.stop = cancel
	e.t.Cleanup(cancel)
	return e.c.Start(ctx)
}

// up registers wfs and starts the client.
func (e *env) up(wfs ...workflow.Workflow) {
	e.t.Helper()
	e.register(wfs...)
	e.start()
}

// doCrash swaps in Crash's client without cancelling the old one's Start
// context (a killed process shuts nothing down), then registers the same
// workflows and starts it. The old context is still cancelled when the
// subtest ends.
func (e *env) doCrash() {
	e.t.Helper()
	e.c = e.crash(e.t)
	wfs := e.wfs
	e.wfs = nil
	e.up(wfs...)
}

// doRestart stops the client, swaps in Restart's client, registers the
// same workflows and starts it.
func (e *env) doRestart() {
	e.t.Helper()
	e.stop()
	e.c = e.restart(e.t)
	wfs := e.wfs
	e.wfs = nil
	e.up(wfs...)
}

// callCtx bounds one client call by Wait.
func (e *env) callCtx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), e.tm.wait)
}

// now is the client's current time.
func (e *env) now() time.Time {
	if e.clk != nil {
		return e.clk.Now()
	}
	return time.Now()
}

// enqueue enqueues job or fails the subtest.
func (e *env) enqueue(job workflow.Job) workflow.RunRef {
	e.t.Helper()
	return e.enqueueWithin(job, 0)
}

// enqueueWithin is enqueue with the call bounded by the longer of Wait and
// min, for cases whose Enqueue legitimately queues behind a held lock.
func (e *env) enqueueWithin(job workflow.Job, min time.Duration) workflow.RunRef {
	e.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), max(e.tm.wait, min))
	defer cancel()
	ref, err := e.c.Enqueue(ctx, job)
	if err != nil {
		e.t.Fatalf("Enqueue(%s): %v", job.Workflow, err)
	}
	if ref.IsZero() {
		e.t.Fatalf("Enqueue(%s) returned an empty RunRef", job.Workflow)
	}
	return ref
}

// status returns ref's status or fails the subtest.
func (e *env) status(ref workflow.RunRef) workflow.Status {
	e.t.Helper()
	ctx, cancel := e.callCtx()
	defer cancel()
	st, err := e.c.Status(ctx, ref)
	if err != nil {
		e.t.Fatalf("Status(%+v): %v", ref, err)
	}
	if !st.Valid() {
		e.t.Fatalf("Status(%+v) = %q, not one of the workflow.Status constants", ref, st)
	}
	if (st == workflow.StatusRetrying || st == workflow.StatusDiscarded) && !e.c.Capabilities().Has(workflow.CapRunStates) {
		e.t.Fatalf("Status(%+v) = %q from a driver that does not declare CapRunStates", ref, st)
	}
	return st
}

// eventually polls cond until it holds or Wait passes. On a fake clock it
// moves time forward by advance before each poll (zero leaves it alone),
// so a driver that arms its timer after the test's last Advance still
// sees time pass. On real time advance is ignored: time passes anyway.
func (e *env) eventually(desc string, advance time.Duration, cond func() bool) {
	e.t.Helper()
	e.eventuallyWithin(e.tm.wait, desc, advance, cond)
}

// eventuallyWithin is eventually with an explicit wall-clock limit.
func (e *env) eventuallyWithin(limit time.Duration, desc string, advance time.Duration, cond func() bool) {
	e.t.Helper()
	deadline := time.Now().Add(limit)
	for {
		if cond() {
			return
		}
		if time.Now().After(deadline) {
			e.t.Fatalf("timed out after %s waiting for %s", limit, desc)
		}
		if e.clk != nil && advance > 0 {
			e.clk.Advance(advance)
		}
		time.Sleep(e.tm.poll)
	}
}

// waitStatus waits until ref reaches want. A terminal status other than
// want fails at once rather than after the full wait.
func (e *env) waitStatus(ref workflow.RunRef, want workflow.Status, advance time.Duration) {
	e.t.Helper()
	e.waitStatusWithin(e.tm.wait, ref, want, advance)
}

// waitStatusWithin is waitStatus with an explicit wall-clock limit, for
// cases whose own windows (a rate-limit Per) outlast Wait on real time.
func (e *env) waitStatusWithin(limit time.Duration, ref workflow.RunRef, want workflow.Status, advance time.Duration) {
	e.t.Helper()
	var last workflow.Status
	e.eventuallyWithin(limit, "status "+string(want), advance, func() bool {
		last = e.status(ref)
		if last != want && last.IsTerminal() {
			e.t.Fatalf("run %+v ended %q, want %q", ref, last, want)
		}
		return last == want
	})
}

// waitTerminal waits until ref is terminal and returns the status.
func (e *env) waitTerminal(ref workflow.RunRef, advance time.Duration) workflow.Status {
	e.t.Helper()
	var last workflow.Status
	e.eventually("a terminal status", advance, func() bool {
		last = e.status(ref)
		return last.IsTerminal()
	})
	return last
}

// quiet waits Quiet of wall time without moving the fake clock, giving a
// driver the chance to do something it must not do.
func (e *env) quiet() { time.Sleep(e.tm.quiet) }

// blockUntilTimers waits until the fake clock has at least n timers, so
// an Advance lands after the driver armed what it should fire.
func (e *env) blockUntilTimers(n int) {
	e.t.Helper()
	ctx, cancel := e.callCtx()
	defer cancel()
	if err := e.clk.BlockUntil(ctx, n); err != nil {
		e.t.Fatalf("waiting for %d fake-clock timers: %v", n, err)
	}
}

// gate blocks handlers until opened. Opening twice is safe, and the
// subtest's cleanup opens it so no handler outlives a failed subtest.
type gate struct {
	ch   chan struct{}
	once sync.Once
}

func newGate(t *testing.T) *gate {
	g := &gate{ch: make(chan struct{})}
	t.Cleanup(g.open)
	return g
}

func (g *gate) open() { g.once.Do(func() { close(g.ch) }) }

// wait blocks until the gate opens (returning nil) or ctx ends.
func (g *gate) wait(ctx context.Context) error {
	select {
	case <-g.ch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// counter counts handler calls and keeps the last input.
type counter struct {
	n    atomic.Int64
	mu   sync.Mutex
	last []byte
}

func (c *counter) hit(in []byte) {
	c.mu.Lock()
	c.last = append([]byte(nil), in...)
	c.mu.Unlock()
	c.n.Add(1)
}

func (c *counter) count() int { return int(c.n.Load()) }

func (c *counter) input() []byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.last
}

// echo returns a handler that counts and echoes its input.
func (c *counter) echo() workflow.Handler {
	return func(_ context.Context, in []byte) ([]byte, error) {
		c.hit(in)
		return in, nil
	}
}

// fail returns a handler that counts and returns err.
func (c *counter) fail(err error) workflow.Handler {
	return func(_ context.Context, in []byte) ([]byte, error) {
		c.hit(in)
		return nil, err
	}
}

// oneStep is a workflow named name with a single step running run.
func oneStep(name string, run workflow.Handler) workflow.Workflow {
	return workflow.Workflow{Name: name, Steps: []workflow.Step{{Name: "only", Run: run}}}
}

// errBoom is the error failing handlers return.
var errBoom = errors.New("workflowtest: boom")

// assertUnsupported fails unless err wraps ErrUnsupported through an
// UnsupportedError whose Field mentions field (when field is set) and
// whose Missing holds missing (when it is not zero).
func assertUnsupported(t *testing.T, err error, field string, missing workflow.Caps) {
	t.Helper()
	if !errors.Is(err, workflow.ErrUnsupported) {
		t.Fatalf("err = %v, want ErrUnsupported", err)
	}
	var ue *workflow.UnsupportedError
	if !errors.As(err, &ue) {
		t.Fatalf("err = %v, want a *workflow.UnsupportedError naming what is missing", err)
	}
	if field != "" && !strings.Contains(ue.Field, field) {
		t.Errorf("UnsupportedError.Field = %q, want it to name %q", ue.Field, field)
	}
	if missing != 0 && !ue.Missing.Has(missing) {
		t.Errorf("UnsupportedError.Missing = %s, want it to hold %s", ue.Missing, missing)
	}
}
