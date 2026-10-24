package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Benchmarks measure wall-clock time, so this file reads time.Now
// directly: there is no scheduling decision here for an injected clock to
// make testable, only durations to report.

// kind tells the handler which phase a job belongs to.
type kind string

const (
	kindWarm kind = "warm" // warm-up job: counted, not timed
	kindTput kind = "tput" // throughput job: counted towards the phase's end
	kindLat  kind = "lat"  // latency probe: its pickup time is one sample
)

// message is every job's payload: the same JSON for every system, padded
// to the configured size so each one moves the same bytes.
type message struct {
	T   int64  `json:"t"`   // enqueue time, Unix nanoseconds, taken just before the enqueue call
	K   kind   `json:"k"`   // phase
	N   int    `json:"n"`   // sequence number within the phase
	Pad string `json:"pad"` // filler up to the payload size
}

// encode builds a payload of exactly size bytes when size is larger than
// the unpadded message, and the unpadded message otherwise.
func encode(m message, size int) ([]byte, error) {
	b, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}
	if extra := size - len(b); extra > 0 {
		m.Pad = strings.Repeat("x", extra)
		return json.Marshal(m)
	}
	return b, nil
}

// config is the workload; every system runs the same one.
type config struct {
	Jobs        int           // throughput-phase jobs
	Concurrency int           // worker slots, and enqueuing goroutines
	PayloadSize int           // bytes per payload
	Warmup      int           // jobs run (untimed) before the throughput phase
	LatSamples  int           // sequential latency probes
	LatGapMin   time.Duration // shortest pause before a latency probe
	LatGapSpan  time.Duration // probes spread their pause over [min, min+span)
	Timeout     time.Duration // longest wait for one phase to finish
	Separate    bool          // enqueue through a second, worker-less client
}

// handler is what every system calls once per job delivery, with the
// payload it was given.
type handler func(payload []byte) error

// enqueueFunc puts one payload on the system's queue.
type enqueueFunc func(ctx context.Context, payload []byte) error

// system is one queue under test. start brings it up with h as its job
// handler and conc worker slots and returns how to enqueue; close stops
// it and removes every database, table and key it created.
type system interface {
	start(ctx context.Context, conc int, h handler) (enqueueFunc, error)
	close(ctx context.Context) error
}

// separateProducer is a system that can enqueue through a second client
// that runs no workers, the way a web node enqueues for a worker node: no
// in-process wake-up reaches the workers, so pickup latency is what
// another node would see. Called after start.
type separateProducer interface {
	producer(ctx context.Context) (enqueueFunc, error)
}

// errNoProducer marks a system that has no worker-less client.
var errNoProducer = errors.New("no worker-less producer client")

// recorder is the handler's side of a run: it counts deliveries per phase
// and hands latency probes back to the probing loop.
type recorder struct {
	mu       sync.Mutex
	tputSeen []bool // throughput job n delivered
	tputDone int    // distinct throughput jobs delivered
	tputDups int    // repeated deliveries of a throughput job
	tputLast time.Time
	tputAll  chan struct{} // closed when every throughput job arrived

	warm    atomic.Int64
	lat     chan time.Duration
	badJobs atomic.Int64 // payloads that did not decode
}

func newRecorder(jobs int) *recorder {
	return &recorder{
		tputSeen: make([]bool, jobs),
		tputAll:  make(chan struct{}),
		lat:      make(chan time.Duration, 1),
	}
}

// handle is the job body every system runs: decode, record, return.
func (r *recorder) handle(payload []byte) error {
	now := time.Now()
	var m message
	if err := json.Unmarshal(payload, &m); err != nil {
		r.badJobs.Add(1)
		return fmt.Errorf("bench: decode payload: %w", err)
	}
	switch m.K {
	case kindWarm:
		r.warm.Add(1)
	case kindLat:
		select {
		case r.lat <- now.Sub(time.Unix(0, m.T)):
		default: // a duplicate delivery of a probe already answered
		}
	case kindTput:
		r.mu.Lock()
		defer r.mu.Unlock()
		if m.N < 0 || m.N >= len(r.tputSeen) {
			r.badJobs.Add(1)
			return nil
		}
		if r.tputSeen[m.N] {
			r.tputDups++
			return nil
		}
		r.tputSeen[m.N] = true
		r.tputDone++
		r.tputLast = now
		if r.tputDone == len(r.tputSeen) {
			close(r.tputAll)
		}
	default:
		r.badJobs.Add(1)
	}
	return nil
}

// result is one system's numbers.
type result struct {
	System      string
	EnqueuePerS float64       // throughput jobs / time to enqueue them all
	E2EPerS     float64       // throughput jobs / (first enqueue → last handler start)
	P50         time.Duration // pickup latency percentiles of the sequential probes
	P95         time.Duration
	P99         time.Duration
	Max         time.Duration
	Dups        int   // throughput jobs delivered more than once
	Err         error // the measurement failed; the numbers are not valid
	CleanupErr  error // the numbers stand, but something may be left behind
}

// run drives one system through warm-up, the throughput phase and the
// latency phase, and always closes it.
func run(ctx context.Context, name string, sys system, cfg config) (res result) {
	res.System = name
	rec := newRecorder(cfg.Jobs)
	enqueue, err := sys.start(ctx, cfg.Concurrency, rec.handle)
	defer func() {
		if cerr := sys.close(context.WithoutCancel(ctx)); cerr != nil {
			res.CleanupErr = cerr
		}
	}()
	if err != nil {
		res.Err = fmt.Errorf("start: %w", err)
		return res
	}
	if cfg.Separate {
		sp, ok := sys.(separateProducer)
		if !ok {
			res.Err = errNoProducer
			return res
		}
		if enqueue, err = sp.producer(ctx); errors.Is(err, errNoProducer) {
			res.Err = err
			return res
		} else if err != nil {
			res.Err = fmt.Errorf("producer: %w", err)
			return res
		}
	}
	if err := warmup(ctx, enqueue, rec, cfg); err != nil {
		res.Err = fmt.Errorf("warm-up: %w", err)
		return res
	}
	if err := throughput(ctx, enqueue, rec, cfg, &res); err != nil {
		res.Err = fmt.Errorf("throughput: %w", err)
		return res
	}
	if err := latency(ctx, enqueue, rec, cfg, &res); err != nil {
		res.Err = fmt.Errorf("latency: %w", err)
		return res
	}
	if n := rec.badJobs.Load(); n > 0 {
		res.Err = fmt.Errorf("%d payloads did not decode", n)
	}
	return res
}

// warmup runs cfg.Warmup jobs and waits for them, so connection pools,
// prepared statements and worker goroutines exist before timing starts.
func warmup(ctx context.Context, enqueue enqueueFunc, rec *recorder, cfg config) error {
	for i := range cfg.Warmup {
		p, err := encode(message{T: time.Now().UnixNano(), K: kindWarm, N: i}, cfg.PayloadSize)
		if err != nil {
			return err
		}
		if err := enqueue(ctx, p); err != nil {
			return err
		}
	}
	deadline := time.Now().Add(cfg.Timeout)
	for rec.warm.Load() < int64(cfg.Warmup) {
		if time.Now().After(deadline) {
			return fmt.Errorf("%d of %d warm-up jobs ran within %s", rec.warm.Load(), cfg.Warmup, cfg.Timeout)
		}
		time.Sleep(10 * time.Millisecond)
	}
	return nil
}

// throughput enqueues cfg.Jobs payloads from cfg.Concurrency goroutines
// while the workers run, then waits for every one to reach its handler.
func throughput(ctx context.Context, enqueue enqueueFunc, rec *recorder, cfg config, res *result) error {
	payloads := make([][]byte, cfg.Jobs)
	for i := range payloads {
		// Built before the clock starts so encoding stays out of the timed
		// loop; T is not read for throughput jobs.
		p, err := encode(message{T: 0, K: kindTput, N: i}, cfg.PayloadSize)
		if err != nil {
			return err
		}
		payloads[i] = p
	}
	var (
		next    atomic.Int64
		wg      sync.WaitGroup
		errOnce sync.Once
		firstEr error
	)
	start := time.Now()
	for range cfg.Concurrency {
		wg.Go(func() {
			for {
				i := int(next.Add(1) - 1)
				if i >= cfg.Jobs {
					return
				}
				if err := enqueue(ctx, payloads[i]); err != nil {
					errOnce.Do(func() { firstEr = fmt.Errorf("job %d: %w", i, err) })
					return
				}
			}
		})
	}
	wg.Wait()
	enqDur := time.Since(start)
	if firstEr != nil {
		return firstEr
	}
	res.EnqueuePerS = float64(cfg.Jobs) / enqDur.Seconds()

	select {
	case <-rec.tputAll:
	case <-time.After(cfg.Timeout):
		rec.mu.Lock()
		defer rec.mu.Unlock()
		return fmt.Errorf("%d of %d jobs ran within %s", rec.tputDone, cfg.Jobs, cfg.Timeout)
	case <-ctx.Done():
		return ctx.Err()
	}
	rec.mu.Lock()
	res.E2EPerS = float64(cfg.Jobs) / rec.tputLast.Sub(start).Seconds()
	res.Dups = rec.tputDups
	rec.mu.Unlock()
	return nil
}

// latency enqueues one probe at a time on an otherwise idle system and
// measures enqueue → handler start. Each probe waits a different pause
// first (spread evenly over LatGapSpan), so probes land at every phase of
// a polling driver's interval instead of right after its last poll.
func latency(ctx context.Context, enqueue enqueueFunc, rec *recorder, cfg config, res *result) error {
	samples := make([]time.Duration, 0, cfg.LatSamples)
	for i := range cfg.LatSamples {
		gap := cfg.LatGapMin
		if cfg.LatGapSpan > 0 && cfg.LatSamples > 1 {
			gap += cfg.LatGapSpan * time.Duration(i*37%cfg.LatSamples) / time.Duration(cfg.LatSamples)
		}
		time.Sleep(gap)
		p, err := encode(message{T: time.Now().UnixNano(), K: kindLat, N: i}, cfg.PayloadSize)
		if err != nil {
			return err
		}
		if err := enqueue(ctx, p); err != nil {
			return fmt.Errorf("probe %d: %w", i, err)
		}
		select {
		case d := <-rec.lat:
			samples = append(samples, d)
		case <-time.After(cfg.Timeout):
			return fmt.Errorf("probe %d did not run within %s", i, cfg.Timeout)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	slices.Sort(samples)
	res.P50 = percentile(samples, 50)
	res.P95 = percentile(samples, 95)
	res.P99 = percentile(samples, 99)
	res.Max = samples[len(samples)-1]
	return nil
}

// percentile is the nearest-rank percentile of sorted samples.
func percentile(sorted []time.Duration, p int) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	rank := (p*len(sorted) + 99) / 100 // ceil(p/100 * n)
	if rank < 1 {
		rank = 1
	}
	return sorted[rank-1]
}
