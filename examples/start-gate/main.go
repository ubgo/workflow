// Command start-gate holds a client's Start until a readiness check passes:
// an edge device boots with its clock at 1970, and nothing may be scheduled
// or run until the clock has synchronised. A fake clock stands in for the
// device's real-time clock, so the boot and the sync happen on cue.
//
// It is the program behind docs/operations/start-gate.md: the guide copies
// its regions (marked "snippet:") and task docs:snippets fails when they
// drift.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/memory"
)

const (
	// poll is how often the gate asks its ReadyFunc in this program; the
	// library default is workflow.DefaultStartWhenPoll (one second).
	poll = 5 * time.Second
	// gateTimeout bounds how long the program waits for the gate to open,
	// generous for a slow CI host.
	gateTimeout = 10 * time.Second
	// failTimeout is the deadline of the gate whose check never passes.
	failTimeout = 50 * time.Millisecond
)

var (
	// bootTime is what the device's clock reads right after a power cycle:
	// its real-time clock has no battery and starts at the Unix epoch.
	bootTime = time.Unix(0, 0).UTC()
	// buildTime stands in for the binary's build time, set at link time in
	// a real app (go build -ldflags "-X main.buildTime=..."). A clock that
	// reads before it cannot have synchronised.
	buildTime = time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	// syncedTime is what the network sync sets the clock to.
	syncedTime = time.Date(2026, 11, 21, 9, 30, 0, 0, time.UTC)

	// errNoDatabase is what the database check in readyAll returns.
	errNoDatabase = errors.New("database unreachable")
)

// main runs the example and exits non-zero on error.
func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "start-gate:", err)
		os.Exit(1)
	}
}

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context, w io.Writer) error {
	fmt.Fprintf(w, "default poll: %s\n", workflow.DefaultStartWhenPoll)
	if err := clockGate(ctx, w); err != nil {
		return err
	}
	if err := failingCheck(ctx, w); err != nil {
		return err
	}
	return neverReady(ctx, w)
}

// clockGate boots a device with its clock at 1970, shows that nothing can
// be enqueued before the sync, then syncs the clock and runs a job.
func clockGate(ctx context.Context, w io.Writer) error {
	device := clock.NewFake(bootTime) // the device's real-time clock
	c := memory.New(memory.WithClock(device))
	ran := make(chan string, 1)
	err := c.Register(workflow.Workflow{Name: "report", Steps: []workflow.Step{{
		Name: "send",
		Run: func(context.Context, []byte) ([]byte, error) {
			ran <- device.Now().Format(time.DateTime)
			return nil, nil
		},
	}}})
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	defer c.Stop()

	// snippet:gate
	ready := workflow.ClockAfter(device, buildTime) // ready once the clock reads the build time or later
	started := make(chan error, 1)
	go func() {
		// StartWhen blocks until ready reports true, then calls c.Start(ctx).
		started <- workflow.StartWhen(ctx, c, ready, workflow.StartWhenOptions{
			Poll:  poll,   // ask every 5s; zero means DefaultStartWhenPoll
			Clock: device, // times the pauses; nil means clock.Real
		})
	}()
	// snippet:end

	// Wait until the gate has asked once and armed its pause timer.
	if err := device.BlockUntil(ctx, 1); err != nil {
		return err
	}

	// snippet:before
	_, err = c.Enqueue(ctx, workflow.Job{Workflow: "report"})
	fmt.Fprintf(w, "clock %s, enqueue: %v\n", device.Now().Format(time.DateTime), err)
	fmt.Fprintf(w, "not started yet: %t\n", errors.Is(err, workflow.ErrNotStarted))
	// snippet:end

	// The network sync sets the clock; the pending pause fires, the gate
	// asks again, the check passes and Start runs.
	device.Set(syncedTime)
	select {
	case err := <-started:
		if err != nil {
			return err
		}
	case <-time.After(gateTimeout):
		return errors.New("the gate did not open after the clock synced")
	}
	if _, err := c.Enqueue(ctx, workflow.Job{Workflow: "report"}); err != nil {
		return err
	}
	select {
	case at := <-ran:
		fmt.Fprintf(w, "clock synced, started; the job ran at %s\n", at)
	case <-time.After(gateTimeout):
		return errors.New("the job did not run")
	}
	return nil
}

// failingCheck combines two checks into one ReadyFunc; the second fails,
// which stops the wait with that error instead of waiting for ever.
func failingCheck(ctx context.Context, w io.Writer) error {
	device := clock.NewFake(syncedTime)
	c := memory.New(memory.WithClock(device))
	defer c.Stop()

	// snippet:ready-func
	clockOK := workflow.ClockAfter(device, buildTime)
	pingDB := func(ctx context.Context) error { return errNoDatabase } // stands in for db.PingContext
	// readyAll passes once the clock has synced and the database answers.
	var readyAll workflow.ReadyFunc = func(ctx context.Context) (bool, error) {
		if ok, err := clockOK(ctx); !ok || err != nil {
			return ok, err // not yet: ask again after Poll
		}
		if err := pingDB(ctx); err != nil {
			return false, err // an error ends the wait, and StartWhen returns it
		}
		return true, nil
	}
	err := workflow.StartWhen(ctx, c, readyAll, workflow.StartWhenOptions{Clock: device})
	fmt.Fprintf(w, "check failed: %v\n", err)
	// snippet:end

	if !errors.Is(err, errNoDatabase) {
		return fmt.Errorf("StartWhen returned %v, want an error wrapping %v", err, errNoDatabase)
	}
	return nil
}

// neverReady gives the gate a deadline, so a check that never passes is an
// error instead of a client that stays stopped for ever.
func neverReady(ctx context.Context, w io.Writer) error {
	c := memory.New()
	defer c.Stop()

	// snippet:deadline
	ctx, cancel := context.WithTimeout(ctx, failTimeout) // in production: minutes, not milliseconds
	defer cancel()
	never := func(context.Context) (bool, error) { return false, nil } // a check that never passes
	err := workflow.StartWhen(ctx, c, never, workflow.StartWhenOptions{Poll: time.Millisecond})
	fmt.Fprintf(w, "gave up: %v\n", err)
	// snippet:end

	if !errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("StartWhen returned %v, want %v", err, context.DeadlineExceeded)
	}
	return nil
}
