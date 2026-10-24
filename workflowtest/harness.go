// Package workflowtest holds the conformance suite every workflow driver
// runs, and two doubles for app tests: Fake, a recording client that runs
// nothing and refuses what a real driver refuses, with expressive
// assertions (AssertEnqueued and its siblings); and Recorder, which wraps a
// real client and records its enqueues. Recorder's standalone mode,
// NewRecorder(nil), is deprecated: use NewFake for a client that runs
// nothing.
//
// A driver's own test file builds a Harness and calls Run, so the memory
// driver, which apps test against, and an engine driver such as Hatchet,
// which production runs, are held to the same assertions and cannot drift
// apart: a fake that honours IdempotencyKey, RunAt and retries while an
// engine driver silently drops them keeps every test green.
//
// Every subtest checks Client.Capabilities first and calls t.Skip naming
// the missing capability, so one suite serves every driver honestly: a
// skipped assertion is reported as skipped, never silently passed.
package workflowtest

import (
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
)

// Harness adapts one driver to the suite. Only New is required; every
// other field has a default suited to the kind of clock the instance uses.
type Harness struct {
	// New returns a fresh, unstarted client for one subtest. The suite
	// registers workflows, calls Start with a context it cancels when the
	// subtest ends, and never shares a client between subtests.
	New func(t *testing.T) Instance

	// Unit is the base length of every delay, sleep, timeout and window
	// the suite asks for. Zero means one minute on a fake clock (where it
	// costs nothing) and two seconds on real time.
	Unit time.Duration

	// Wait is the wall-clock time the suite allows an expected outcome to
	// appear before failing. Zero means 5s on a fake clock and 60s on real
	// time.
	Wait time.Duration

	// Quiet is the wall-clock time the suite watches for something that
	// must not happen (a second run, a start before RunAt). Zero means
	// 50ms on a fake clock and one second on real time.
	Quiet time.Duration

	// Poll is the wall-clock interval between Status polls. Zero means 2ms
	// on a fake clock and 200ms on real time.
	Poll time.Duration

	// KeyFromLabel returns the ConcurrencyKey that buckets runs by the
	// value of Job.Labels[label]. Concurrency and rate-limit subtests use
	// it. Nil means workflow.KeyFunc reading the label, which an
	// in-process driver evaluates; an engine that only takes expressions
	// supplies a workflow.KeyExpr here.
	KeyFromLabel func(label string) workflow.ConcurrencyKey

	// WaitKeyFromLabel returns the WaitSpec.KeyFrom that keys a run's wait
	// by the value of Job.Labels[label]. It is separate from KeyFromLabel
	// because an engine may need expressions for concurrency yet evaluate
	// wait keys in Go (Hatchet has no API that turns CEL into a string).
	// Nil means workflow.KeyFunc reading the label, whatever KeyFromLabel
	// is.
	WaitKeyFromLabel func(label string) workflow.ConcurrencyKey

	// RateLimitPer is the RateLimit.Per of the rate-limit cases. Zero
	// means 2*Unit. An engine that only takes fixed windows (Hatchet:
	// second, minute, hour...) sets one it accepts, such as time.Minute;
	// the cases scale their waits from it, so a long window only makes
	// them slower on real time.
	RateLimitPer time.Duration

	// RealCron runs the cron-firing subtests on a real-time instance,
	// where each waits up to two minutes for a minute boundary. Without
	// it they are skipped on real time; on a fake clock they always run.
	RealCron bool

	// NewWithQueues is New for a client that serves exactly qs
	// (QueueConfig: Workers, Limit, Priority...), for the queue-limit
	// cases. The Instance it returns carries Restart, Crash and Peer that
	// build their clients with the same qs, so a replacement or a second
	// node enforces the same Limit. Nil skips those cases: the queue
	// configuration is a driver option the suite cannot name.
	NewWithQueues func(t *testing.T, qs []workflow.QueueConfig) Instance

	// Prefix starts every workflow and event name the suite makes, so a
	// shared engine shows which suite left a run behind. Empty means
	// "wftest". Names also carry a per-Run nonce, because real engines
	// keep state across runs of the suite.
	Prefix string
}

// Instance is one client under test plus what the suite needs to drive it.
type Instance struct {
	// Client is fresh and unstarted.
	Client workflow.Client

	// Clock is the fake clock the client reads, or nil when the client
	// runs on real time (an engine). With a fake clock the suite moves
	// time itself; without one it uses short real durations and polls
	// Status until Harness.Wait.
	Clock *clock.Fake

	// Restart, when set and the client claims CapDurable, stands for a
	// process restart: the suite has already cancelled the old client's
	// Start context, and Restart returns a new, unstarted client over the
	// same storage and the same Clock. The suite registers the same
	// workflows on it and starts it. Nil skips the durability subtests.
	//
	// Restart is also the kill-a-worker seam: the suite may cancel the old
	// Start context while a step is running, and a durable driver must
	// then resume the run on the new client (re-running the step, at once
	// or after a lease expires on Clock) rather than mark it cancelled.
	// Several workers over one store are modelled the same way: the old
	// client is the lost worker, the new one the survivor.
	Restart func(t *testing.T) workflow.Client

	// Crash, when set and the client claims CapDurable, stands for the
	// process dying mid-step: the suite does NOT cancel the old client's
	// Start context first, and Crash must abandon the old client the way a
	// killed process would (no lease release, no status writes, no further
	// claims), then return a new, unstarted client over the same storage
	// and Clock. The suite registers and starts it and expects the step
	// that was running to run again within Wait, once the old worker's
	// lease expires on whatever clock the driver measures leases with.
	// The abandoned step's handler stays blocked until the subtest ends.
	// Nil skips the crash subtest.
	Crash func(t *testing.T) workflow.Client

	// EnqueueOnly, when set and the client claims CapEnqueueOnly, returns
	// a client built in the driver's enqueue-only mode over the same
	// storage and Clock as Client. The suite never calls Start on it:
	// it enqueues through it and expects Client (registered and started)
	// to run the work. Nil skips the enqueue-only subtests.
	EnqueueOnly func(t *testing.T) workflow.Client

	// Peer, when set, returns a second, unstarted, fully working client
	// over the same storage and Clock as Client (another node of the same
	// deployment; Client stays alive). Only an Instance from
	// Harness.NewWithQueues uses it, and the peer is built with the same
	// queues. Nil skips the two-client cases: a driver whose state is one
	// process has no second node.
	Peer func(t *testing.T) workflow.Client
}

// Default timings, per clock kind. A fake clock moves only when told, so
// its Unit can be long and its waits short; real time is the reverse.
const (
	fakeUnit  = time.Minute
	fakeWait  = 5 * time.Second
	fakeQuiet = 50 * time.Millisecond
	fakePoll  = 2 * time.Millisecond

	realUnit  = 2 * time.Second
	realWait  = 60 * time.Second
	realQuiet = time.Second
	realPoll  = 200 * time.Millisecond

	// realCronWait covers the next minute boundary plus engine latency.
	realCronWait = 2 * time.Minute

	defaultPrefix = "wftest"
)

// timings is a Harness's knobs resolved for one instance's clock kind.
type timings struct {
	unit, wait, quiet, poll time.Duration
}

func (h *Harness) timings(fake bool) timings {
	tm := timings{unit: realUnit, wait: realWait, quiet: realQuiet, poll: realPoll}
	if fake {
		tm = timings{unit: fakeUnit, wait: fakeWait, quiet: fakeQuiet, poll: fakePoll}
	}
	if h.Unit > 0 {
		tm.unit = h.Unit
	}
	if h.Wait > 0 {
		tm.wait = h.Wait
	}
	if h.Quiet > 0 {
		tm.quiet = h.Quiet
	}
	if h.Poll > 0 {
		tm.poll = h.Poll
	}
	return tm
}

func (h *Harness) keyFromLabel(label string) workflow.ConcurrencyKey {
	if h.KeyFromLabel != nil {
		return h.KeyFromLabel(label)
	}
	return workflow.KeyFunc(func(j workflow.Job) string { return j.Labels[label] })
}

func (h *Harness) waitKeyFromLabel(label string) workflow.ConcurrencyKey {
	if h.WaitKeyFromLabel != nil {
		return h.WaitKeyFromLabel(label)
	}
	return workflow.KeyFunc(func(j workflow.Job) string { return j.Labels[label] })
}

func (h *Harness) rateLimitPer(unit time.Duration) time.Duration {
	if h.RateLimitPer > 0 {
		return h.RateLimitPer
	}
	return 2 * unit
}
