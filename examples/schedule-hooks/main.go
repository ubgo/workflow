// Command schedule-hooks controls when a schedule's ticks fire and reacts
// to each one, on the in-memory driver with a fake clock: time-of-day
// windows, a ScheduleWhen condition, a tick observer that sees fired and
// skipped ticks, per-schedule hooks with heartbeat pings when a run starts,
// succeeds or fails, the schedule list with next fire times, and pausing
// one schedule.
//
// It is the program behind docs/schedules/conditions.md,
// docs/schedules/hooks.md and docs/schedules/manage.md: the guides copy
// its regions (marked "snippet:") and task docs:snippets fails when they
// drift.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/memory"
	"github.com/ubgo/workflow/middleware"
)

// main runs the example and exits non-zero on error.
func main() {
	if err := run(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, "schedule-hooks:", err)
		os.Exit(1)
	}
}

// pingTimeout bounds one heartbeat ping. The local stand-in answers at
// once; against a real service it keeps a slow one from delaying the
// hooks that run after the ping on the same goroutine.
const pingTimeout = 2 * time.Second

const (
	// eventTimeout bounds every real-time wait: for a tick, a run's end
	// or the pings, which the local stand-in delivers at once.
	eventTimeout = 5 * time.Second
	// pingPoll is how often heartbeat.wait counts the pings again.
	pingPoll = time.Millisecond
	// eventBuffer holds ticks and run ends until the program reads them,
	// so the observers never block the scheduler.
	eventBuffer = 16
	// expectedPings is every ping the program causes: four fired ticks
	// and four run ends.
	expectedPings = 8
	// failHour is the hour whose run fails, to show the failure hook.
	failHour = 11
)

// start is Monday 5 January 2026, 07:30 UTC: the first hourly tick is
// 08:00.
var start = time.Date(2026, 1, 5, 7, 30, 0, 0, time.UTC)

// heartbeat is a stand-in for a heartbeat service (healthchecks.io,
// Cronitor, Better Stack): it counts the paths it is pinged on.
type heartbeat struct {
	mu    sync.Mutex
	paths map[string]int
}

// ServeHTTP implements http.Handler: it counts the ping's path and answers
// 200.
func (h *heartbeat) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mu.Lock()
	h.paths[r.URL.Path]++
	h.mu.Unlock()
	w.WriteHeader(http.StatusOK)
}

// String implements fmt.Stringer: each path pinged and how often, sorted.
func (h *heartbeat) String() string {
	h.mu.Lock()
	defer h.mu.Unlock()
	var parts []string
	for _, p := range slices.Sorted(maps.Keys(h.paths)) {
		parts = append(parts, fmt.Sprintf("%s x%d", p, h.paths[p]))
	}
	return strings.Join(parts, ", ")
}

// wait polls until n pings have arrived.
func (h *heartbeat) wait(n int) error {
	deadline := time.Now().Add(eventTimeout)
	for {
		h.mu.Lock()
		total := 0
		for _, c := range h.paths {
			total += c
		}
		h.mu.Unlock()
		if total >= n {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("%d of %d pings arrived in %s", total, n, eventTimeout)
		}
		time.Sleep(pingPoll)
	}
}

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context) error {
	clk := clock.NewFake(start)
	mem := memory.New(memory.WithClock(clk))
	var c workflow.Client = mem // the optional interfaces are found by type assertion, as through any wrapper
	need := workflow.CapsOf(workflow.CapScheduleWhen, workflow.CapScheduleWindow, workflow.CapTickEvents, workflow.CapScheduleList, workflow.CapSchedulePause)
	if !c.Capabilities().Has(need) {
		return fmt.Errorf("the driver lacks %s", need.Without(c.Capabilities()))
	}

	// snippet:when
	var deploying atomic.Bool // set by the app while a deploy runs
	err := c.Register(workflow.Workflow{
		Name: "sync-orders",
		// Asked before every tick of every schedule of this workflow.
		ScheduleWhen: func(_ context.Context, t workflow.Tick) bool {
			return !deploying.Load()
		},
		Steps: []workflow.Step{{Name: "sync", Run: syncOrders(clk)}},
	})
	// snippet:end
	if err != nil {
		return err
	}

	srv := &heartbeat{paths: map[string]int{}}
	hb := httptest.NewServer(srv)
	defer hb.Close()
	ticks := make(chan workflow.Tick, eventBuffer)
	ends := make(chan workflow.RunEvent, eventBuffer)

	// snippet:observer
	err = c.(workflow.TickNotifier).OnTick(func(_ context.Context, t workflow.Tick) {
		ticks <- t // every fired, skipped and misfired tick, from every schedule
	})
	// snippet:end
	if err != nil {
		return err
	}

	// snippet:pinger
	ping := middleware.NewPinger(
		middleware.HTTPPing(hb.Client()),                                  // a GET; a status outside 2xx is a failure
		middleware.WithPingTimeout(pingTimeout),                           // a dead service cannot hold up the hooks
		middleware.WithPingLogger(slog.Default().With("check", "orders")), // failures at Warn, never with the URL
	)
	// snippet:end

	// snippet:hooks
	err = workflow.AttachScheduleHooks(c, workflow.ScheduleHooks{
		Workflow:  "sync-orders",
		Name:      "office-hours",               // one schedule; empty means all of the workflow's
		Fired:     ping.Tick(hb.URL + "/start"), // a run started
		OnSuccess: ping.Run(hb.URL + "/ok"),     // it succeeded
		OnFailure: ping.Run(hb.URL + "/fail"),   // it failed
		After: func(_ context.Context, ev workflow.RunEvent) { // after
			ends <- ev
		},
	})
	// snippet:end
	if err != nil {
		return err
	}

	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if err := c.Start(cctx); err != nil {
		return err
	}
	defer mem.Stop()

	// snippet:windows
	id, err := c.Schedule(ctx, workflow.Schedule{
		Name:     "office-hours",
		Workflow: "sync-orders",
		Cron:     "0 * * * *", // hourly
		Location: time.UTC,
		Misfire:  workflow.Misfire{Policy: workflow.MisfireSkip}, // after an outage, fire no missed tick
		Windows: workflow.Windows{
			Between: []workflow.Window{{From: "09:00", To: "17:00"}}, // office hours only
			Unless:  []workflow.Window{{From: "12:00", To: "13:00"}}, // not over lunch
		},
	})
	// snippet:end
	if err != nil {
		return err
	}

	fmt.Println("-- schedules")
	if err := list(ctx, c); err != nil {
		return err
	}

	fmt.Println("-- ticks")
	// hour moves to the next hourly tick and prints what happened.
	hour := func() error { return advanceHour(ctx, clk, ticks, ends) }
	for _, step := range []func(){
		nil,                               // 08:00 outside the window
		nil,                               // 09:00 fires
		func() { deploying.Store(true) },  // 10:00 ScheduleWhen says no
		func() { deploying.Store(false) }, // 11:00 fires and fails
		nil,                               // 12:00 lunch
		nil,                               // 13:00 fires
	} {
		if step != nil {
			step()
		}
		if err := hour(); err != nil {
			return err
		}
	}

	fmt.Println("-- pause")
	// snippet:pause
	pauser := c.(workflow.SchedulePauser)
	if err := pauser.PauseSchedule(ctx, workflow.ScheduleTarget{ID: id}); err != nil {
		return err
	}
	// snippet:end
	if err := list(ctx, c); err != nil {
		return err
	}
	if err := hour(); err != nil { // 14:00 paused
		return err
	}
	if err := clk.BlockUntil(ctx, 1); err != nil { // the 15:00 timer is armed
		return err
	}
	clk.Advance(30 * time.Minute) // resume at 14:30
	// snippet:resume
	if err := pauser.ResumeSchedule(ctx, workflow.ScheduleTarget{ID: id}); err != nil {
		return err
	}
	// snippet:end
	if err := hour(); err != nil { // 15:00 fires again
		return err
	}

	fmt.Println("-- heartbeat pings")
	// Tick hooks and run hooks run on separate observer goroutines, so wait
	// for all eight pings (four fired ticks, four run ends) to land.
	if err := srv.wait(expectedPings); err != nil {
		return err
	}
	fmt.Println(" ", srv)
	return nil
}

// syncOrders is the scheduled step: it fails at 11:00, to show the
// failure hook.
func syncOrders(clk clock.Clock) workflow.Handler {
	return func(context.Context, []byte) ([]byte, error) {
		if clk.Now().Hour() == failHour {
			return nil, errors.New("orders API timed out")
		}
		return nil, nil
	}
}

// list prints every live schedule with its next fire time.
func list(ctx context.Context, c workflow.Client) error {
	// snippet:list
	infos, err := c.(workflow.ScheduleLister).Schedules(ctx)
	if err != nil {
		return err
	}
	for _, s := range infos {
		next := "never (paused)"
		if !s.Next.IsZero() {
			next = s.Next.In(s.Location).Format("Mon 15:04 MST")
		}
		fmt.Printf("  %-12s %-11s %-9q next %s\n", s.Workflow, s.Name, s.Cron, next)
	}
	// snippet:end
	return err
}

// advanceHour moves the clock to the next hourly tick, then prints the
// tick's outcome and, for a fired tick, how its run ended.
func advanceHour(ctx context.Context, clk *clock.Fake, ticks <-chan workflow.Tick, ends <-chan workflow.RunEvent) error {
	if err := clk.BlockUntil(ctx, 1); err != nil {
		return err
	}
	now := clk.Now()
	clk.Advance(now.Truncate(time.Hour).Add(time.Hour).Sub(now))
	var t workflow.Tick
	select {
	case t = <-ticks:
	case <-time.After(eventTimeout):
		return fmt.Errorf("no tick in %s", eventTimeout)
	}
	at := t.At.In(time.UTC).Format("15:04")
	if t.Outcome != workflow.TickFired || t.Err != nil {
		fmt.Printf("  %s %s\n", at, describe(t))
		return nil
	}
	select {
	case ev := <-ends:
		fmt.Printf("  %s %s, run %s\n", at, describe(t), ev.Status)
	case <-time.After(eventTimeout):
		return fmt.Errorf("the fired run did not end in %s", eventTimeout)
	}
	return nil
}

// describe says what happened to a tick, from its Outcome and, for a
// skipped tick, its Reason.
// snippet:outcome
func describe(t workflow.Tick) string {
	switch t.Outcome {
	case workflow.TickFired: // a run was enqueued (t.Run), or the enqueue failed (t.Err)
		if t.Err != nil {
			return "fired, enqueue failed: " + t.Err.Error()
		}
		if t.Manual {
			return "fired by hand"
		}
		return "fired"
	case workflow.TickSkipped: // due and on time, but not fired
		switch t.Reason {
		case workflow.TickReasonWindow:
			return "skipped (window)" // outside the schedule's Windows
		case workflow.TickReasonWhen:
			return "skipped (when)" // ScheduleWhen returned false
		case workflow.TickReasonPaused:
			return "skipped (paused)" // SchedulePauser holds the schedule
		}
		return "skipped (" + t.Reason + ")"
	case workflow.TickMisfired: // the misfire policy dropped late ticks
		return fmt.Sprintf("misfired, %d dropped, newest %s", t.Missed, t.At.Format("15:04"))
	}
	return string(t.Outcome)
}

// snippet:end
