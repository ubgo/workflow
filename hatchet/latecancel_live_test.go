package hatchet_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/hatchet"
)

// TestLateCancelLive proves Cancel of a delayed start close to its due
// time on the engine. The engine loads a scheduled run into its ticker up
// to five seconds before it is due and fires the loaded timer whatever a
// later delete says, so a delete inside that window may not stop it. Each
// sample is cancelled at its due time minus an offset (10s, 5s, 3s, 1s,
// 0.5s); the due times are spread over the engine's five-second poll so
// every phase of it is met. After due + 15s the test counts the step's
// executions and reads Status. The promise checked for every sample: a
// nil Cancel means the step never ran and Status reads cancelled; a
// refused one matches hatchet.ErrCancelTooLate and workflow.ErrWrongState.
// WFTEST_LATECANCEL_SAMPLES sets the samples per offset (default 5).
func TestLateCancelLive(t *testing.T) {
	skipWithoutEngine(t)
	samples := 5
	if v, err := strconv.Atoi(os.Getenv("WFTEST_LATECANCEL_SAMPLES")); err == nil && v > 0 {
		samples = v
	}
	offsets := []time.Duration{10 * time.Second, 5 * time.Second, 3 * time.Second, time.Second, 500 * time.Millisecond}
	const (
		poll    = 5 * time.Second  // the engine's schedule poll interval
		observe = 15 * time.Second // how long after due the step may still show up
	)

	prefix := "wflc_" + uuid.NewString()[:8]
	var mu sync.Mutex
	ran := map[string]int{}
	wf := workflow.Workflow{Name: prefix, Steps: []workflow.Step{{Name: "a", Run: func(_ context.Context, in []byte) ([]byte, error) {
		mu.Lock()
		ran[string(in)]++
		mu.Unlock()
		return nil, nil
	}}}}
	c, _ := startLive(t, hatchet.Config{WorkerName: prefix}, wf)
	ctx := context.Background()

	type sample struct {
		off       time.Duration
		id        string
		due       time.Time
		ref       workflow.RunRef
		cancelErr error
		took      time.Duration
		late      bool // the cancel could not be sent at due - off
	}
	n := len(offsets) * samples
	all := make([]*sample, 0, n)
	base := time.Now().Add(20*time.Second + time.Duration(n)*200*time.Millisecond)
	for oi, off := range offsets {
		for i := range samples {
			// Spread the due times over one poll interval, and shift each
			// offset a little, so every phase of the engine's poll is met.
			due := base.Add(time.Duration(i)*poll/time.Duration(samples) + time.Duration(oi)*137*time.Millisecond)
			s := &sample{off: off, id: fmt.Sprintf(`"%s-%d"`, off, i), due: due}
			ref, err := c.Enqueue(ctx, workflow.Job{Workflow: wf.Name, RunAt: due, Payload: []byte(s.id)})
			if err != nil {
				t.Fatal(err)
			}
			s.ref = ref
			all = append(all, s)
		}
	}

	var wg sync.WaitGroup
	for _, s := range all {
		wg.Add(1)
		go func() {
			defer wg.Done()
			at := s.due.Add(-s.off)
			if d := time.Until(at); d > 0 {
				time.Sleep(d)
			} else if d < -100*time.Millisecond {
				s.late = true
			}
			start := time.Now()
			s.cancelErr = c.Cancel(ctx, s.ref)
			s.took = time.Since(start)
		}()
	}
	wg.Wait()
	last := all[0].due
	for _, s := range all {
		if s.due.After(last) {
			last = s.due
		}
	}
	time.Sleep(time.Until(last.Add(observe)))

	type row struct {
		samples, nilCancel, refused, started int
		statuses                             map[workflow.Status]int
		maxTook                              time.Duration
	}
	rows := map[time.Duration]*row{}
	for _, s := range all {
		r := rows[s.off]
		if r == nil {
			r = &row{statuses: map[workflow.Status]int{}}
			rows[s.off] = r
		}
		mu.Lock()
		runs := ran[s.id]
		mu.Unlock()
		st, serr := c.Status(ctx, s.ref)
		if serr != nil {
			st = workflow.Status("error: " + serr.Error())
		}
		r.samples++
		r.statuses[st]++
		r.maxTook = max(r.maxTook, s.took)
		if runs > 0 {
			r.started++
		}
		switch {
		case s.late:
			t.Errorf("sample %s: the cancel was sent late (machine too loaded); rerun", s.id)
		case s.cancelErr == nil:
			r.nilCancel++
			if runs > 0 || st != workflow.StatusCancelled {
				t.Errorf("sample %s (cancel at due-%s): Cancel = nil, yet the step ran %d times and Status = %s", s.id, s.off, runs, st)
			}
		default:
			r.refused++
			if !errors.Is(s.cancelErr, hatchet.ErrCancelTooLate) || !errors.Is(s.cancelErr, workflow.ErrWrongState) {
				t.Errorf("sample %s (cancel at due-%s): Cancel = %v, want nil or ErrCancelTooLate", s.id, s.off, s.cancelErr)
			}
		}
	}
	var b strings.Builder
	b.WriteString("\n| cancel at | samples | Cancel nil | Cancel refused | step started | final Status | slowest Cancel |\n|---|---|---|---|---|---|---|\n")
	for _, off := range offsets {
		r := rows[off]
		var sts []string
		for st, k := range r.statuses {
			sts = append(sts, fmt.Sprintf("%s x%d", st, k))
		}
		slices.Sort(sts)
		fmt.Fprintf(&b, "| due - %s | %d | %d | %d | %d | %s | %s |\n", off, r.samples, r.nilCancel, r.refused, r.started, strings.Join(sts, ", "), r.maxTook.Round(10*time.Millisecond))
	}
	t.Log(b.String())
}
