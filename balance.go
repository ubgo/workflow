package workflow

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"slices"
	"time"

	"github.com/ubgo/workflow/clock"
)

// QueueLoad is one queue's load on one client, as a WorkerScaler reports
// it and a Balancer reads it.
type QueueLoad struct {
	// Queue is the queue name.
	Queue string

	// Depth is how many step attempts are ready and waiting for a worker
	// of this client (attempts in a retry backoff or a delayed start are
	// not counted: no worker could take them yet).
	Depth int

	// OldestWait is how long the oldest of those attempts has waited,
	// on the driver's clock; zero when Depth is zero.
	OldestWait time.Duration

	// Workers is the queue's current worker count on this client.
	Workers int

	// Running is how many of its attempts execute now.
	Running int
}

// Balancer decides how many workers each queue should have, from the
// queues' loads. It returns the desired count per queue name; a queue it
// leaves out keeps its count. A count below one is refused by the
// scaler, so a Balancer never stops a queue. It must be a pure function
// of loads (time arrives through QueueLoad.OldestWait), so a test can call
// it directly. ByDepth and ByWait are the built-in strategies; any other
// policy (a schedule, a cost model, an external autoscaler's answer)
// implements this interface or BalancerFunc.
type Balancer interface {
	Balance(loads []QueueLoad) map[string]int
}

// BalancerFunc adapts a function to Balancer.
type BalancerFunc func(loads []QueueLoad) map[string]int

// Balance calls f.
func (f BalancerFunc) Balance(loads []QueueLoad) map[string]int { return f(loads) }

// WorkerScaler is implemented by a client whose per-queue worker counts
// can change while it runs. A client implements it when it declares
// CapWorkerScale. It covers the queues the client was configured with
// (its QueueConfig list); the counts are this client's own, so on a
// shared store each process balances its own workers.
type WorkerScaler interface {
	// QueueLoads returns the load of every configured queue, sorted by
	// name. Before Start it returns ErrNotStarted.
	QueueLoads(ctx context.Context) ([]QueueLoad, error)

	// SetWorkers sets queue's worker count to n, effective for the next
	// attempt claimed: lowering it never interrupts running attempts.
	// n below one wraps ErrInvalid; a queue the client was not configured
	// with wraps ErrNotFound. Before Start it returns ErrNotStarted.
	SetWorkers(ctx context.Context, queue string, n int) error
}

// QueueBounds limits one queue's worker count under a Balancer.
type QueueBounds struct {
	// Min is the fewest workers the queue keeps, however idle it is.
	// Zero means one.
	Min int
	// Max is the most workers it gets, however busy. Zero means the
	// whole budget (BalanceConfig.Total).
	Max int
}

// BalanceConfig is the shared configuration of the built-in strategies.
type BalanceConfig struct {
	// Total is the worker budget shared by the queues. Zero means the
	// sum of their current Workers, so balancing moves workers between
	// queues without adding any. It is raised to the sum of the minimums
	// when lower.
	Total int

	// Bounds holds per-queue bounds by queue name.
	Bounds map[string]QueueBounds

	// Default bounds a queue absent from Bounds.
	Default QueueBounds

	// MaxShift caps how far one Balance call moves a queue's count, up or
	// down, so a burst does not swing every worker at once. Zero means no
	// cap. With a cap the counts can briefly sum to more or less than
	// Total while they converge.
	MaxShift int
}

// ByDepth balances by backlog: after each queue's minimum, the budget is
// shared in proportion to each queue's Depth plus Running (the work it
// has in hand), within each queue's maximum.
func ByDepth(cfg BalanceConfig) Balancer {
	return Proportional(cfg, func(l QueueLoad) float64 { return float64(l.Depth + l.Running) })
}

// ByWait balances by latency: after each queue's minimum, the budget is
// shared in proportion to how long each queue's oldest attempt has
// waited, within each queue's maximum, so the queue falling furthest
// behind gets the most help.
func ByWait(cfg BalanceConfig) Balancer {
	return Proportional(cfg, func(l QueueLoad) float64 { return l.OldestWait.Seconds() })
}

// Proportional is the strategy behind ByDepth and ByWait with any weight:
// every queue gets its minimum, then the rest of the budget is shared in
// proportion to weight (negative counts as zero), never past a queue's
// maximum; what a capped queue cannot take goes to the others. When no
// queue has weight the rest is shared evenly. Ties go to the queue whose
// name sorts first, so the result is deterministic.
func Proportional(cfg BalanceConfig, weight func(QueueLoad) float64) Balancer {
	return BalancerFunc(func(loads []QueueLoad) map[string]int {
		return proportional(cfg, weight, loads)
	})
}

func proportional(cfg BalanceConfig, weight func(QueueLoad) float64, loads []QueueLoad) map[string]int {
	if len(loads) == 0 {
		return map[string]int{}
	}
	ls := slices.Clone(loads)
	slices.SortFunc(ls, func(a, b QueueLoad) int { return cmp.Compare(a.Queue, b.Queue) })
	total := cfg.Total
	if total <= 0 {
		total = 0
		for _, l := range ls {
			total += l.Workers
		}
	}
	n := len(ls)
	lo, hi, w, got := make([]int, n), make([]int, n), make([]float64, n), make([]int, n)
	sumMin := 0
	for i, l := range ls {
		b, ok := cfg.Bounds[l.Queue]
		if !ok {
			b = cfg.Default
		}
		lo[i] = max(b.Min, 1)
		sumMin += lo[i]
		w[i] = max(weight(l), 0)
		if math.IsNaN(w[i]) || math.IsInf(w[i], 0) {
			w[i] = 0
		}
	}
	total = max(total, sumMin)
	for i, l := range ls {
		b, ok := cfg.Bounds[l.Queue]
		if !ok {
			b = cfg.Default
		}
		hi[i] = b.Max
		if hi[i] <= 0 {
			hi[i] = total
		}
		hi[i] = max(hi[i], lo[i])
		got[i] = lo[i]
	}
	rest := total - sumMin
	for rest > 0 {
		var active []int
		sum := 0.0
		for i := range ls {
			if got[i] < hi[i] {
				active = append(active, i)
				sum += w[i]
			}
		}
		if len(active) == 0 {
			break
		}
		share := func(i int) float64 {
			if sum == 0 {
				return float64(rest) / float64(len(active))
			}
			return float64(rest) * w[i] / sum
		}
		given := 0
		for _, i := range active {
			add := min(int(share(i)), hi[i]-got[i])
			got[i] += add
			given += add
		}
		if given == 0 { // every share is below one: the largest takes one
			best := active[0]
			for _, i := range active[1:] {
				if share(i) > share(best) {
					best = i
				}
			}
			got[best]++
			given = 1
		}
		rest -= given
	}
	out := make(map[string]int, n)
	for i, l := range ls {
		d := got[i]
		if cfg.MaxShift > 0 && l.Workers > 0 {
			d = min(max(d, l.Workers-cfg.MaxShift), l.Workers+cfg.MaxShift)
			d = min(max(d, lo[i]), hi[i])
		}
		out[l.Queue] = d
	}
	return out
}

// DefaultBalanceEvery is how often a BalanceLoop with no Every runs.
const DefaultBalanceEvery = 5 * time.Second

// BalanceLoop applies a Balancer to a WorkerScaler on an interval.
// Balancing is off unless an app runs one: no driver balances on its
// own. Run it on the client's Start ctx so it stops with the client.
type BalanceLoop struct {
	// Scaler is the client to balance (a client declaring
	// CapWorkerScale).
	Scaler WorkerScaler

	// Balancer decides the counts.
	Balancer Balancer

	// Every is the interval; zero means DefaultBalanceEvery.
	Every time.Duration

	// Clock drives the interval; nil means clock.Real.
	Clock clock.Clock

	// OnError receives an error of one round; the loop goes on. Nil logs
	// it with Logger (nil: slog.Default()).
	OnError func(error)
	Logger  *slog.Logger
}

// Once runs one round: it reads the loads, asks the Balancer and sets
// every count that changed. It returns every failure joined; a count it
// could not set does not stop the others.
func (l BalanceLoop) Once(ctx context.Context) error {
	if l.Scaler == nil || l.Balancer == nil {
		return invalidf("balance loop: Scaler and Balancer are required")
	}
	loads, err := l.Scaler.QueueLoads(ctx)
	if err != nil {
		return fmt.Errorf("balance loop: queue loads: %w", err)
	}
	current := make(map[string]int, len(loads))
	for _, ld := range loads {
		current[ld.Queue] = ld.Workers
	}
	want := l.Balancer.Balance(loads)
	names := make([]string, 0, len(want))
	for q := range want {
		names = append(names, q)
	}
	slices.Sort(names)
	var errs []error
	for _, q := range names {
		if cur, ok := current[q]; ok && cur == want[q] {
			continue
		}
		if err := l.Scaler.SetWorkers(ctx, q, want[q]); err != nil {
			errs = append(errs, fmt.Errorf("balance loop: queue %q to %d workers: %w", q, want[q], err))
		}
	}
	return errors.Join(errs...)
}

// Run calls Once every Every until ctx ends, and returns nil then. It
// returns at once with an error when Scaler or Balancer is missing.
func (l BalanceLoop) Run(ctx context.Context) error {
	if l.Scaler == nil || l.Balancer == nil {
		return invalidf("balance loop: Scaler and Balancer are required")
	}
	every := l.Every
	if every <= 0 {
		every = DefaultBalanceEvery
	}
	clk := l.Clock
	if clk == nil {
		clk = clock.Real{}
	}
	onError := l.OnError
	if onError == nil {
		logger := l.Logger
		if logger == nil {
			logger = slog.Default()
		}
		onError = func(err error) { logger.ErrorContext(ctx, "workflow: balance round failed", "error", err) }
	}
	for {
		t := clk.NewTimer(every)
		select {
		case <-ctx.Done():
			t.Stop()
			return nil
		case <-t.C():
		}
		if err := l.Once(ctx); err != nil && ctx.Err() == nil {
			onError(err)
		}
	}
}
