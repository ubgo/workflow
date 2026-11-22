package memory

import (
	"container/heap"
	"time"

	"github.com/ubgo/workflow/clock"
)

// WithScheduleHeap runs every schedule of the client, declared crons and
// Client.Schedule calls alike, from one scheduler goroutine and one clock
// timer: the schedules sit in a heap ordered by their next tick, the timer
// is armed for the earliest, and each wake handles every schedule due by
// then (EM-04). Without it, the default, each schedule runs its own
// goroutine with its own timer.
//
// What fires is the same either way: the same ticks, misfire plans,
// pauses, windows, ScheduleWhen, overlap and tick events. Two things
// differ, which is why it is opt-in:
//
//   - On a fake clock the client holds one pending timer for all its
//     schedules instead of one each, so a test waits for the schedules to
//     be armed with BlockUntil(ctx, 1) plus any other timers it expects,
//     not one per schedule.
//   - Schedules due at the same moment are handled one after another on
//     the scheduler goroutine, in the order of their tick and then of
//     their arming, so a slow ScheduleWhen delays the others due with it.
//
// Use it for many schedules in one process (thousands of per-tenant
// crons), where a goroutine and a timer each is the cost that matters.
//
//ds:def id=memory-withscheduleheap-4r7u9kqg owner=@khanakia stability=stable
func WithScheduleHeap() Option { return func(m *Client) { m.schedHeap = true } }

// schedEntry is one schedule in the heap, due at at. seq breaks ties in
// arming order, so schedules due together are handled deterministically.
type schedEntry struct {
	at  time.Time
	seq uint64
	s   *schedule
}

// schedQueue is a min-heap of schedEntry by (at, seq). Guarded by
// Client.mu.
type schedQueue []schedEntry

func (q schedQueue) Len() int { return len(q) }
func (q schedQueue) Less(i, j int) bool {
	if c := q[i].at.Compare(q[j].at); c != 0 {
		return c < 0
	}
	return q[i].seq < q[j].seq
}
func (q schedQueue) Swap(i, j int) { q[i], q[j] = q[j], q[i] }
func (q *schedQueue) Push(x any)   { *q = append(*q, x.(schedEntry)) }
func (q *schedQueue) Pop() any {
	old := *q
	e := old[len(old)-1]
	old[len(old)-1] = schedEntry{} // drop the *schedule for the GC
	*q = old[:len(old)-1]
	return e
}

// pushScheduleLocked puts s in the heap at its next tick after the moment
// it last caught up and, with poke, wakes the scheduler so it can re-arm
// for an earlier head (the scheduler itself re-arms after its own pushes).
// A schedule that never fires again is logged and left out. Callers hold
// Client.mu.
func (c *Client) pushScheduleLocked(s *schedule, poke bool) {
	next := s.s.Next(s.caught, s.loc)
	if next.IsZero() {
		c.log.Warn("memory: cron schedule never fires again", "workflow", s.workflow, "cron", s.s.String())
		return
	}
	c.seq++
	heap.Push(&c.schedQ, schedEntry{at: next, seq: c.seq, s: s})
	if poke {
		c.pokeSchedulerLocked()
	}
}

// pokeSchedulerLocked wakes the scheduler goroutine to re-read the heap's
// head: a new schedule may be due earlier, or the head was halted. It
// never blocks. Callers hold Client.mu.
func (c *Client) pokeSchedulerLocked() {
	if c.schedPoke == nil {
		return
	}
	select {
	case c.schedPoke <- struct{}{}:
	default: // a wake is already pending; it reads the heap afresh
	}
}

// scheduler is the one goroutine WithScheduleHeap runs: it sleeps on one
// timer until the earliest schedule is due (or a poke says the head may
// have changed), then takes every schedule due by the clock's reading and
// handles each as the per-schedule loop would (wakeScheduleLocked, fire),
// and puts it back at its next tick. Halted schedules are dropped as they
// reach the head.
//
// The armed timer is replaced only when the head moves earlier than its
// deadline. A poke that leaves the head where it was (an upsert, an
// Unschedule, a later schedule added) keeps it, so a test that saw the
// timer pending and advances a fake clock never races a stop and re-arm,
// which could read the clock before the advance and arm the tick a period
// late.
//
//ds:def id=memory-client-scheduler-xqnr8ude owner=@khanakia stability=stable
func (c *Client) scheduler() {
	defer c.wg.Done()
	var t clock.Timer
	var armed time.Time // t's deadline; zero while no timer is armed
	defer func() {
		if t != nil {
			t.Stop()
		}
	}()
	for {
		c.mu.Lock()
		for len(c.schedQ) > 0 && c.schedQ[0].s.halted {
			heap.Pop(&c.schedQ)
		}
		var head time.Time
		if len(c.schedQ) > 0 {
			head = c.schedQ[0].at
		}
		c.mu.Unlock()

		if !head.IsZero() && (t == nil || head.Before(armed)) {
			if t != nil {
				t.Stop()
			}
			// Read the clock as late as possible, as the per-schedule
			// loop does: a fake clock advanced in between would make an
			// earlier reading arm the tick late.
			t, armed = c.clk.NewTimer(head.Sub(c.clk.Now())), head
		}
		var due <-chan time.Time
		if t != nil {
			due = t.C()
		}
		select {
		case <-due:
			t, armed = nil, time.Time{} // fired; a stale one (its head halted) just leads to an empty pass
		case <-c.schedPoke:
			continue
		case <-c.ctx.Done():
			return
		}

		c.mu.Lock()
		now := c.clk.Now()
		var ready []*schedule
		for len(c.schedQ) > 0 && !c.schedQ[0].at.After(now) {
			if e := heap.Pop(&c.schedQ).(schedEntry); !e.s.halted {
				ready = append(ready, e.s)
			}
		}
		c.mu.Unlock()
		for _, s := range ready {
			c.mu.Lock()
			w, ok := c.wakeScheduleLocked(s)
			c.mu.Unlock()
			if !ok {
				continue // halted while the others were handled
			}
			if w.fire {
				c.fire(s, w.reg, w.ticks, w.windows, w.labels, w.input)
			}
			c.mu.Lock()
			if !s.halted && !c.stopped {
				c.pushScheduleLocked(s, false)
			}
			c.mu.Unlock()
		}
	}
}
