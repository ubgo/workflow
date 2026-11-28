package workflowtest

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/ubgo/workflow"
)

// Concurrency limits shared by several workflows (ConcurrencyLimit.Scope,
// CapConcurrencyScope). The cases use two workflows, "sync" and "check",
// whose limits key on the job label "a" (and, where a case says so, "b")
// and name a scope unique to the case, so the backend's other cases never
// share it. A run's payload is its name; run names are unique across the
// two workflows, so one tracker counts both.

// scopeName is a scope unique to the case.
func (e *env) scopeName(suffix string) string { return e.name("scope-" + suffix) }

// scopeLimit is a limit on label, in scope, of max runs per value.
func (e *env) scopeLimit(label, scope string, max int, hold workflow.ConcurrencyHold) workflow.ConcurrencyLimit {
	return workflow.ConcurrencyLimit{Key: e.h.keyFromLabel(label), MaxRuns: max, Scope: scope, Hold: hold}
}

// scopeFlow is a one-step workflow named after suffix, with run as its
// step (no retries) and limits.
func scopeFlow(e *env, suffix string, run workflow.Handler, limits ...workflow.ConcurrencyLimit) workflow.Workflow {
	wf := oneStep(e.name(suffix), run)
	wf.Steps[0].Retry = workflow.RetriesNone()
	wf.ConcurrencyLimits = limits
	return wf
}

// enqueueOn enqueues job on client c (a peer or an enqueue-only client).
func (e *env) enqueueOn(c workflow.Client, job workflow.Job) workflow.RunRef {
	e.t.Helper()
	ctx, cancel := e.callCtx()
	defer cancel()
	ref, err := c.Enqueue(ctx, job)
	if err != nil {
		e.t.Fatalf("Enqueue(%s): %v", job.Workflow, err)
	}
	return ref
}

// scopeLoad is one shape of the load case: where the two workflows are
// registered.
type scopeLoad struct {
	name string
	// split registers sync only on the env's client and check only on a
	// peer, so neither worker knows the other workflow; it needs a peer.
	split bool
}

// scopeUnderLoad runs many sync and check runs over two key values on
// several workers (two nodes when the driver has a peer) and checks that
// no key value ever had more than max runs of the two workflows executing
// at once, and that every run succeeded.
func scopeUnderLoad(e *env, shape scopeLoad) {
	e.need(workflow.CapConcurrencyScope)
	const perFlow, max = 12, 1
	var all []limitRun
	for i := range perFlow {
		key := []string{"x", "y"}[i%2]
		all = append(all, limitRun{fmt.Sprintf("sync-%d", i), key, ""}, limitRun{fmt.Sprintf("check-%d", i), key, ""})
	}
	tr := newHoldTracker(e, all...)
	tr.linger = 10 * time.Millisecond // attempts overlap when nothing holds them apart
	scope := e.scopeName("account")
	sync := scopeFlow(e, "sync", tr.handler("a"), e.scopeLimit(limitLabelA, scope, max, ""))
	check := scopeFlow(e, "check", tr.handler("a"), e.scopeLimit(limitLabelA, scope, max, ""))
	if e.h.NewWithQueues != nil {
		e.withQueues(workflow.QueueConfig{Name: workflow.DefaultQueue, Workers: 4})
	}
	if shape.split && e.peer == nil {
		e.t.Skip("Instance.Peer is nil: this driver has no second node over one store")
	}
	checkOn := e.c
	switch {
	case shape.split:
		e.up(sync)
		peer := e.peer(e.t)
		e.startPeer(peer, check)
		checkOn = peer
	default:
		e.up(sync, check)
		if e.peer != nil {
			e.startPeer(e.peer(e.t), sync, check) // a second node over the same store
		}
	}
	var refs []workflow.RunRef
	for _, r := range all {
		if strings.HasPrefix(r.name, "sync-") {
			refs = append(refs, e.enqueue(holdJob(sync.Name, r)))
		} else {
			refs = append(refs, e.enqueueOn(checkOn, holdJob(check.Name, r)))
		}
	}
	for _, ref := range refs {
		e.waitStatus(ref, workflow.StatusSucceeded, e.tm.unit/4)
	}
	tr.assertPeak("a=x", max)
	tr.assertPeak("a=y", max)
}

// assertInvalidNaming fails unless err wraps ErrInvalid and names every
// one of words.
func assertInvalidNaming(e *env, err error, words ...string) {
	e.t.Helper()
	if !errors.Is(err, workflow.ErrInvalid) {
		e.t.Fatalf("err = %v, want ErrInvalid", err)
	}
	for _, w := range words {
		if !strings.Contains(err.Error(), w) {
			e.t.Errorf("err = %v, want it to name %q", err, w)
		}
	}
}

// registerOrStart registers wfs on a fresh client and starts it, returning
// the first error.
func registerOrStart(e *env, wfs ...workflow.Workflow) error {
	for _, wf := range wfs {
		if err := e.c.Register(wf); err != nil {
			return err
		}
	}
	return e.tryStart()
}

// concurrencyScopeCases is the conformance group for shared scopes.
//
//ds:def id=workflowtest-concurrencyscopecases-4zgt6z8m owner=@khanakia stability=stable
var concurrencyScopeCases = []testCase{
	{"default: two workflows with the same limit and no scope count apart", func(e *env) {
		e.need(workflow.CapConcurrencyLimits)
		// The behaviour before Scope existed, and still the default: a
		// limit's buckets belong to its workflow.
		s1, c1 := limitRun{"sync-1", "x", ""}, limitRun{"check-1", "x", ""}
		tr := newLimitTracker(e, s1, c1)
		sync := scopeFlow(e, "sync", tr.handler(), e.scopeLimit(limitLabelA, "", 1, ""))
		check := scopeFlow(e, "check", tr.handler(), e.scopeLimit(limitLabelA, "", 1, ""))
		e.up(sync, check)
		a := e.enqueue(limitJob(sync.Name, s1))
		e.eventually("sync-1 to start", 0, func() bool { return tr.started(s1.name) })
		b := e.enqueue(limitJob(check.Name, c1))
		e.eventually("check-1 to start beside sync-1 (no scope: buckets per workflow)", 0, func() bool { return tr.started(c1.name) })
		if p := tr.peakOf("a=x"); p != 2 {
			e.t.Fatalf("a=x had %d runs at once, want 2 (one per workflow)", p)
		}
		tr.open(s1.name)
		tr.open(c1.name)
		e.waitStatus(a, workflow.StatusSucceeded, 0)
		e.waitStatus(b, workflow.StatusSucceeded, 0)
	}},
	{"a shared scope: a run of one workflow waits while a run of the other holds the key", func(e *env) {
		e.need(workflow.CapConcurrencyScope)
		s1, c1, c2 := limitRun{"sync-1", "x", ""}, limitRun{"check-1", "x", ""}, limitRun{"check-2", "y", ""}
		tr := newLimitTracker(e, s1, c1, c2)
		scope := e.scopeName("account")
		sync := scopeFlow(e, "sync", tr.handler(), e.scopeLimit(limitLabelA, scope, 1, ""))
		check := scopeFlow(e, "check", tr.handler(), e.scopeLimit(limitLabelA, scope, 1, ""))
		e.up(sync, check)
		a := e.enqueue(limitJob(sync.Name, s1))
		e.eventually("sync-1 to start", 0, func() bool { return tr.started(s1.name) })
		b := e.enqueue(limitJob(check.Name, c1))
		c := e.enqueue(limitJob(check.Name, c2))
		e.eventually("check-2 to start (its key, y, is free)", 0, func() bool { return tr.started(c2.name) })
		e.quiet()
		assertNotStarted(e, tr, "while sync-1 held the shared key x", c1.name)
		tr.open(s1.name)
		e.waitStatus(a, workflow.StatusSucceeded, 0)
		e.eventually("check-1 to start once sync-1 gave the shared key back", 0, func() bool { return tr.started(c1.name) })
		tr.open(c1.name)
		tr.open(c2.name)
		e.waitStatus(b, workflow.StatusSucceeded, 0)
		e.waitStatus(c, workflow.StatusSucceeded, 0)
		if p := tr.peakOf("a=x"); p != 1 {
			e.t.Fatalf("a=x had %d runs of the two workflows at once, want 1", p)
		}
	}},
	{"a shared scope under load on several workers never exceeds MaxRuns across the workflows", func(e *env) {
		scopeUnderLoad(e, scopeLoad{name: "both registered"})
	}},
	{"a shared scope holds when each workflow is served by its own node", func(e *env) {
		scopeUnderLoad(e, scopeLoad{name: "split", split: true})
	}},
	{"a waiting run is not overtaken on a shared key by a run of the other workflow behind it", func(e *env) {
		e.need(workflow.CapConcurrencyScope)
		// sync needs scope A (label a) and scope B (label b); check needs
		// only scope B. big (sync) waits for a=x, held by first. The check
		// runs behind it need only b=p, which is free: if they could take
		// it while big waits, a stream of checks would keep b=p busy
		// whenever a=x frees, and big would never start.
		first, big := limitRun{"first", "x", "f"}, limitRun{"big", "x", "p"}
		s1, s2 := limitRun{"check-1", "u1", "p"}, limitRun{"check-2", "u2", "p"}
		tr := newLimitTracker(e, first, big, s1, s2)
		scopeA, scopeB := e.scopeName("conn"), e.scopeName("account")
		sync := scopeFlow(e, "sync", tr.handler(), e.scopeLimit(limitLabelA, scopeA, 1, ""), e.scopeLimit(limitLabelB, scopeB, 1, ""))
		check := scopeFlow(e, "check", tr.handler(), e.scopeLimit(limitLabelB, scopeB, 1, ""))
		e.up(sync, check)
		f := e.enqueue(limitJob(sync.Name, first))
		e.eventually("the first run to start", 0, func() bool { return tr.started(first.name) })
		refs := []workflow.RunRef{f, e.enqueue(limitJob(sync.Name, big))}
		e.quiet() // enqueue order is admission order
		refs = append(refs, e.enqueue(limitJob(check.Name, s1)))
		e.quiet()
		refs = append(refs, e.enqueue(limitJob(check.Name, s2)))
		e.quiet()
		assertNotStarted(e, tr, "while first held a=x", big.name)
		assertNotStarted(e, tr, "ahead of big, an earlier run of the other workflow waiting for b=p", s1.name, s2.name)
		tr.open(first.name)
		e.eventually("big to start once a=x had room", 0, func() bool { return tr.started(big.name) })
		e.quiet()
		assertNotStarted(e, tr, "while big held b=p", s1.name, s2.name)
		tr.open(big.name)
		e.eventually("check-1 to start after big", 0, func() bool { return tr.started(s1.name) })
		tr.open(s1.name)
		e.eventually("check-2 to start after check-1", 0, func() bool { return tr.started(s2.name) })
		tr.open(s2.name)
		for _, ref := range refs {
			e.waitStatus(ref, workflow.StatusSucceeded, 0)
		}
		if got, want := strings.Join(tr.startOrder(), " "), "first big check-1 check-2"; got != want {
			e.t.Fatalf("order of starts:\n got %s\nwant %s", got, want)
		}
	}},
	{"a higher-priority run of the other workflow takes the shared key first", func(e *env) {
		e.need(workflow.CapConcurrencyScope)
		holder, low, high := limitRun{"sync-holder", "x", ""}, limitRun{"check-low", "x", ""}, limitRun{"sync-high", "x", ""}
		tr := newLimitTracker(e, holder, low, high)
		scope := e.scopeName("account")
		sync := scopeFlow(e, "sync", tr.handler(), e.scopeLimit(limitLabelA, scope, 1, ""))
		check := scopeFlow(e, "check", tr.handler(), e.scopeLimit(limitLabelA, scope, 1, ""))
		e.up(sync, check)
		h := e.enqueue(limitJob(sync.Name, holder))
		e.eventually("the holder to start", 0, func() bool { return tr.started(holder.name) })
		l := e.enqueue(limitJob(check.Name, low))
		e.quiet()
		hj := limitJob(sync.Name, high)
		hj.Priority = 10
		hi := e.enqueue(hj)
		e.quiet()
		tr.open(holder.name)
		e.eventually("sync-high to start first", 0, func() bool { return tr.started(high.name) })
		e.quiet()
		assertNotStarted(e, tr, "while the higher-priority sync-high held the key", low.name)
		tr.open(high.name)
		e.eventually("check-low to start after sync-high", 0, func() bool { return tr.started(low.name) })
		tr.open(low.name)
		for _, ref := range []workflow.RunRef{h, l, hi} {
			e.waitStatus(ref, workflow.StatusSucceeded, 0)
		}
		if got, want := strings.Join(tr.startOrder(), " "), "sync-holder sync-high check-low"; got != want {
			e.t.Fatalf("order of starts:\n got %s\nwant %s", got, want)
		}
	}},
	{"runs an enqueue-only client wrote share the scope", func(e *env) {
		e.need(workflow.CapsOf(workflow.CapConcurrencyScope, workflow.CapEnqueueOnly))
		if e.eonly == nil {
			e.t.Skip("Instance.EnqueueOnly is nil")
		}
		s1, c1 := limitRun{"sync-1", "x", ""}, limitRun{"check-1", "x", ""}
		tr := newLimitTracker(e, s1, c1)
		scope := e.scopeName("account")
		sync := scopeFlow(e, "sync", tr.handler(), e.scopeLimit(limitLabelA, scope, 1, ""))
		check := scopeFlow(e, "check", tr.handler(), e.scopeLimit(limitLabelA, scope, 1, ""))
		e.up(sync, check)
		// The enqueue-only client registers nothing: it knows neither
		// workflow's keys, so the workers evaluate them.
		eo := e.eonly(e.t)
		a := e.enqueueOn(eo, limitJob(sync.Name, s1))
		e.eventually("sync-1 to start", e.tm.unit/4, func() bool { return tr.started(s1.name) })
		b := e.enqueueOn(eo, limitJob(check.Name, c1))
		e.quiet()
		e.quiet()
		assertNotStarted(e, tr, "while sync-1 held the shared key x", c1.name)
		tr.open(s1.name)
		e.waitStatus(a, workflow.StatusSucceeded, e.tm.unit/4)
		e.eventually("check-1 to start", e.tm.unit/4, func() bool { return tr.started(c1.name) })
		tr.open(c1.name)
		e.waitStatus(b, workflow.StatusSucceeded, e.tm.unit/4)
	}},
	{"while_running: a parked run gives the shared slot to the other workflow and takes it back", func(e *env) {
		e.need(workflow.CapsOf(workflow.CapConcurrencyScope, workflow.CapHoldWhileRunning))
		// holder (sync) parks in a short backoff; next (check) takes the
		// shared slot; later (check) is enqueued after holder. When next
		// ends, holder, ready again and earlier in line, takes the slot
		// back before later, though later is of the workflow whose run
		// just gave the slot back.
		holder, next, later := limitRun{"sync-holder", "x", ""}, limitRun{"check-next", "x", ""}, limitRun{"check-later", "x", ""}
		tr := newHoldTracker(e, holder, next, later)
		tr.failFirst(holder.name, "a", 1) // then a short backoff: the holder parks
		tr.gated(next.name)
		scope := e.scopeName("account")
		limit := e.scopeLimit(limitLabelA, scope, 1, workflow.HoldWhileRunning)
		sync := workflow.Workflow{Name: e.name("sync"), Steps: []workflow.Step{holdStep(tr, e.tm.unit)}, ConcurrencyLimits: []workflow.ConcurrencyLimit{limit}}
		check := workflow.Workflow{Name: e.name("check"), Steps: []workflow.Step{holdStep(tr, e.tm.unit)}, ConcurrencyLimits: []workflow.ConcurrencyLimit{limit}}
		e.up(sync, check)
		h := e.enqueue(holdJob(sync.Name, holder))
		e.eventually("the holder's first attempt", 0, func() bool { return tr.attempts(holder.name) >= 1 })
		n := e.enqueue(holdJob(check.Name, next))
		e.eventually("check-next to start on the slot the parked sync run gave back", 0, func() bool { return tr.attempts(next.name) > 0 })
		l := e.enqueue(holdJob(check.Name, later))
		e.pass(3 * e.tm.unit) // the holder's backoff ends; the key is check-next's
		e.quiet()
		tr.assertAttempts(holder.name, 1, "while check-next held the shared key")
		tr.assertAttempts(later.name, 0, "while check-next held the shared key")
		tr.open(next.name)
		e.waitStatus(n, workflow.StatusSucceeded, e.tm.unit)
		e.waitStatus(h, workflow.StatusSucceeded, e.tm.unit)
		e.waitStatus(l, workflow.StatusSucceeded, e.tm.unit)
		if got, want := tr.startOrder(), "sync-holder check-next sync-holder check-later"; got != want {
			e.t.Fatalf("order of attempts:\n got %s\nwant %s", got, want)
		}
		tr.assertPeak("a=x", 1)
	}},
	{"workflows naming one scope with different MaxRuns are refused", func(e *env) {
		e.need(workflow.CapConcurrencyScope)
		var c counter
		scope := e.scopeName("account")
		sync := scopeFlow(e, "sync", c.echo(), e.scopeLimit(limitLabelA, scope, 1, ""))
		check := scopeFlow(e, "check", c.echo(), e.scopeLimit(limitLabelA, scope, 2, ""))
		assertInvalidNaming(e, registerOrStart(e, sync, check), sync.Name, check.Name, scope, "MaxRuns")
	}},
	{"workflows naming one scope with different Hold are refused", func(e *env) {
		e.need(workflow.CapsOf(workflow.CapConcurrencyScope, workflow.CapHoldWhileRunning))
		var c counter
		scope := e.scopeName("account")
		sync := scopeFlow(e, "sync", c.echo(), e.scopeLimit(limitLabelA, scope, 1, ""))
		check := scopeFlow(e, "check", c.echo(), e.scopeLimit(limitLabelA, scope, 1, workflow.HoldWhileRunning))
		assertInvalidNaming(e, registerOrStart(e, sync, check), sync.Name, check.Name, scope, "Hold")
	}},
	{"one workflow naming a scope twice is refused", func(e *env) {
		e.need(workflow.CapConcurrencyScope)
		var c counter
		scope := e.scopeName("account")
		wf := scopeFlow(e, "sync", c.echo(), e.scopeLimit(limitLabelA, scope, 1, ""), e.scopeLimit(limitLabelB, scope, 1, ""))
		assertInvalidNaming(e, registerOrStart(e, wf), wf.Name, scope)
	}},
}
