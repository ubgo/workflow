package workflowtest

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/ubgo/workflow"
)

// Fairness under ConcurrencyRoundRobin (FC-05) and Concurrency.Weight
// (FC-06). Each case runs the workflow on a queue with one worker, holds
// that worker with a first run while the backlog is enqueued, then opens
// the gate and reads the order in which the worker started the rest: with
// one worker the order of starts is the order of turns, so the share each
// key gets is measured exactly, not on average.

// fairRun is one run of a fairness case: its key and its name.
type fairRun struct{ key, name string }

// fairOrder runs wf's backlog behind a held first run and returns the
// order of starts after it, as "key:name" strings. weight is the
// workflow's Concurrency.Weight (nil: none).
func fairOrder(e *env, weight func(string) int, backlog []fairRun, prio map[string]int) []string {
	e.t.Helper()
	e.withQueues(workflow.QueueConfig{Name: queueA, Workers: 1})
	g := newGate(e.t)
	var mu sync.Mutex
	var order []string
	wf := oneStep(e.name("fair"), func(ctx context.Context, in []byte) ([]byte, error) {
		if string(in) == "hold:h" {
			return nil, g.wait(ctx)
		}
		mu.Lock()
		order = append(order, string(in))
		mu.Unlock()
		return nil, nil
	})
	wf.Queue = queueA
	wf.Concurrency = &workflow.Concurrency{Key: e.h.keyFromLabel(concurrencyLabel), MaxRuns: len(backlog) + 1,
		Strategy: workflow.ConcurrencyRoundRobin, Weight: weight}
	e.up(wf)
	hold := e.enqueue(job(wf.Name, "hold", "h"))
	e.waitStatus(hold, workflow.StatusRunning, 0)
	refs := make([]workflow.RunRef, 0, len(backlog))
	for _, r := range backlog {
		j := job(wf.Name, r.key, r.name)
		j.Priority = prio[r.key+":"+r.name]
		refs = append(refs, e.enqueue(j))
	}
	// Every run is admitted (and given its turn) before the worker is
	// free: drivers admit on a dispatcher pass, not inside Enqueue.
	e.quiet()
	e.quiet()
	g.open()
	for _, ref := range refs {
		e.waitStatus(ref, workflow.StatusSucceeded, 0)
	}
	mu.Lock()
	defer mu.Unlock()
	return append([]string(nil), order...)
}

// fairBacklog lists n runs of key, named key1..keyn.
func fairBacklog(key string, n int) []fairRun {
	out := make([]fairRun, n)
	for i := range out {
		out[i] = fairRun{key: key, name: fmt.Sprintf("%s%d", key, i+1)}
	}
	return out
}

// share counts the starts of each key among the first n of order.
func share(order []string, n int) map[string]int {
	got := map[string]int{}
	for _, s := range order[:min(n, len(order))] {
		got[strings.SplitN(s, ":", 2)[0]]++
	}
	return got
}

var fairCases = []testCase{
	{"keys take turns: a backlog enqueued first does not hold back later keys", func(e *env) {
		e.need(workflow.CapsOf(workflow.CapRoundRobin, workflow.CapQueue))
		backlog := append(append(fairBacklog("a", 6), fairBacklog("b", 3)...), fairBacklog("c", 3)...)
		order := fairOrder(e, nil, backlog, nil)
		want := "a:a1 b:b1 c:c1 a:a2 b:b2 c:c2 a:a3 b:b3 c:c3 a:a4 a:a5 a:a6"
		if got := strings.Join(order, " "); got != want {
			e.t.Fatalf("order of starts:\n got %s\nwant %s", got, want)
		}
	}},
	{"a higher Job.Priority still goes first", func(e *env) {
		e.need(workflow.CapsOf(workflow.CapRoundRobin, workflow.CapQueue))
		backlog := append(fairBacklog("a", 3), fairRun{"b", "b1"}, fairRun{"a", "urgent"})
		order := fairOrder(e, nil, backlog, map[string]int{"a:urgent": 5})
		want := "a:urgent a:a1 b:b1 a:a2 a:a3"
		if got := strings.Join(order, " "); got != want {
			e.t.Fatalf("order of starts:\n got %s\nwant %s", got, want)
		}
	}},
	{"weights: a key of weight 3 gets three turns for each turn of weight 1", func(e *env) {
		e.need(workflow.CapsOf(workflow.CapKeyWeights, workflow.CapQueue))
		backlog := append(fairBacklog("a", 12), fairBacklog("b", 12)...)
		order := fairOrder(e, workflow.KeyWeights(map[string]int{"a": 3}, 1), backlog, nil)
		// While both keys have work waiting, every window of 4 turns is
		// 3 of a and 1 of b; then b's remaining runs.
		for _, n := range []int{4, 8, 12, 16} {
			if got := share(order, n); got["a"] != 3*n/4 || got["b"] != n/4 {
				e.t.Fatalf("first %d starts: a %d, b %d; want a %d, b %d (order %v)", n, got["a"], got["b"], 3*n/4, n/4, order)
			}
		}
	}},
	{"weights: three keys share 1:2:3", func(e *env) {
		e.need(workflow.CapsOf(workflow.CapKeyWeights, workflow.CapQueue))
		backlog := append(append(fairBacklog("x", 10), fairBacklog("y", 10)...), fairBacklog("z", 10)...)
		order := fairOrder(e, workflow.KeyWeights(map[string]int{"x": 1, "y": 2, "z": 3}, 1), backlog, nil)
		// Shares hold within one turn of each key's ideal at every prefix
		// while all three have work waiting (z, the fastest, runs out
		// after its 10th turn, about 20 turns in).
		for n := 6; n <= 18; n += 6 {
			got := share(order, n)
			for k, w := range map[string]int{"x": 1, "y": 2, "z": 3} {
				ideal := n * w / 6
				if d := got[k] - ideal; d < -1 || d > 1 {
					e.t.Fatalf("first %d starts: %s got %d, ideal %d (order %v)", n, k, got[k], ideal, order)
				}
			}
		}
	}},
	{"a key that was idle rejoins at the current turn, without credit", func(e *env) {
		e.need(workflow.CapsOf(workflow.CapRoundRobin, workflow.CapQueue))
		e.withQueues(workflow.QueueConfig{Name: queueA, Workers: 1})
		g1, g2 := newGate(e.t), newGate(e.t)
		var mu sync.Mutex
		var order []string
		wf := oneStep(e.name("fair"), func(ctx context.Context, in []byte) ([]byte, error) {
			switch string(in) {
			case "hold:h1":
				return nil, g1.wait(ctx)
			case "hold:h2":
				return nil, g2.wait(ctx)
			}
			mu.Lock()
			order = append(order, string(in))
			mu.Unlock()
			return nil, nil
		})
		wf.Queue = queueA
		wf.Concurrency = &workflow.Concurrency{Key: e.h.keyFromLabel(concurrencyLabel), MaxRuns: 20, Strategy: workflow.ConcurrencyRoundRobin}
		e.up(wf)
		// Round one: a alone runs 5 runs, so its turns advance.
		h1 := e.enqueue(job(wf.Name, "hold", "h1"))
		e.waitStatus(h1, workflow.StatusRunning, 0)
		var refs []workflow.RunRef
		for _, r := range fairBacklog("a", 5) {
			refs = append(refs, e.enqueue(job(wf.Name, r.key, r.name)))
		}
		e.quiet()
		g1.open()
		for _, ref := range refs {
			e.waitStatus(ref, workflow.StatusSucceeded, 0)
		}
		// Round two: b, idle so far, and a enqueue together. b gets no
		// credit for round one (it would otherwise run 5 times first):
		// they alternate.
		h2 := e.enqueue(job(wf.Name, "hold", "h2"))
		e.waitStatus(h2, workflow.StatusRunning, 0)
		refs = refs[:0]
		for _, r := range []fairRun{{"a", "a6"}, {"a", "a7"}, {"b", "b1"}, {"b", "b2"}} {
			refs = append(refs, e.enqueue(job(wf.Name, r.key, r.name)))
		}
		e.quiet()
		e.quiet()
		g2.open()
		for _, ref := range refs {
			e.waitStatus(ref, workflow.StatusSucceeded, 0)
		}
		mu.Lock()
		got := strings.Join(order[5:], " ")
		mu.Unlock()
		if want := "a:a6 b:b1 a:a7 b:b2"; got != want {
			e.t.Fatalf("round two:\n got %s\nwant %s", got, want)
		}
	}},
}
