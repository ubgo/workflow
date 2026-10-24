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
	"io"
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

func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "schedule-hooks:", err)
		os.Exit(1)
	}
}

// start is Monday 5 January 2026, 07:30 UTC: the first hourly tick is
// 08:00.
var start = time.Date(2026, 1, 5, 7, 30, 0, 0, time.UTC)

// heartbeat is a stand-in for a heartbeat service (healthchecks.io,
// Cronitor, Better Stack): it counts the paths it is pinged on.
type heartbeat struct {
	mu    sync.Mutex
	paths map[string]int
}

func (h *heartbeat) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mu.Lock()
	h.paths[r.URL.Path]++
	h.mu.Unlock()
	w.WriteHeader(http.StatusOK)
}

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
	deadline := time.Now().Add(5 * time.Second)
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
			return fmt.Errorf("%d of %d pings arrived in 5s", total, n)
		}
		time.Sleep(time.Millisecond)
	}
}

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context, out io.Writer) error {
	clk := clock.NewFake(start)
	mem := memory.New(memory.WithClock(clk))
	var c workflow.Client = mem // the optional interfaces are found by type assertion, as through any wrapper
	need := workflow.CapScheduleWhen | workflow.CapScheduleWindow | workflow.CapTickEvents | workflow.CapScheduleList | workflow.CapSchedulePause
	if !c.Capabilities().Has(need) {
		return fmt.Errorf("the driver lacks %s", need&^c.Capabilities())
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
	ticks := make(chan workflow.Tick, 16)
	ends := make(chan workflow.RunEvent, 16)

	// snippet:observer
	err = c.(workflow.TickNotifier).OnTick(func(_ context.Context, t workflow.Tick) {
		ticks <- t // every fired, skipped and misfired tick, from every schedule
	})
	// snippet:end
	if err != nil {
		return err
	}

	// snippet:hooks
	ping := middleware.NewPinger(middleware.HTTPPing(hb.Client()))
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

	fmt.Fprintln(out, "-- schedules")
	if err := list(ctx, c, out); err != nil {
		return err
	}

	fmt.Fprintln(out, "-- ticks")
	hour := func() error { return advanceHour(ctx, clk, ticks, ends, out) }
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

	fmt.Fprintln(out, "-- pause")
	// snippet:pause
	pauser := c.(workflow.SchedulePauser)
	if err := pauser.PauseSchedule(ctx, workflow.ScheduleTarget{ID: id}); err != nil {
		return err
	}
	// snippet:end
	if err := list(ctx, c, out); err != nil {
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

	fmt.Fprintln(out, "-- heartbeat pings")
	// Tick hooks and run hooks run on separate observer goroutines, so wait
	// for all eight pings (four fired ticks, four run ends) to land.
	if err := srv.wait(8); err != nil {
		return err
	}
	fmt.Fprintln(out, " ", srv)
	return nil
}

// syncOrders is the scheduled step: it fails at 11:00, to show the
// failure hook.
func syncOrders(clk clock.Clock) workflow.Handler {
	return func(context.Context, []byte) ([]byte, error) {
		if clk.Now().Hour() == 11 {
			return nil, errors.New("orders API timed out")
		}
		return nil, nil
	}
}

// list prints every live schedule with its next fire time.
func list(ctx context.Context, c workflow.Client, out io.Writer) error {
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
		fmt.Fprintf(out, "  %-12s %-11s %-9q next %s\n", s.Workflow, s.Name, s.Cron, next)
	}
	// snippet:end
	return err
}

// advanceHour moves the clock to the next hourly tick, then prints the
// tick's outcome and, for a fired tick, how its run ended.
func advanceHour(ctx context.Context, clk *clock.Fake, ticks <-chan workflow.Tick, ends <-chan workflow.RunEvent, out io.Writer) error {
	if err := clk.BlockUntil(ctx, 1); err != nil {
		return err
	}
	now := clk.Now()
	clk.Advance(now.Truncate(time.Hour).Add(time.Hour).Sub(now))
	var t workflow.Tick
	select {
	case t = <-ticks:
	case <-time.After(5 * time.Second):
		return errors.New("no tick in 5s")
	}
	at := t.At.In(time.UTC).Format("15:04")
	if t.Outcome != workflow.TickFired {
		fmt.Fprintf(out, "  %s %s (%s)\n", at, t.Outcome, t.Reason)
		return nil
	}
	select {
	case ev := <-ends:
		fmt.Fprintf(out, "  %s fired, run %s\n", at, ev.Status)
	case <-time.After(5 * time.Second):
		return errors.New("the fired run did not end in 5s")
	}
	return nil
}
