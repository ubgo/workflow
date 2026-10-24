package workflow

import (
	"context"
	"errors"
	"fmt"
	"runtime/metrics"
	"sync"
	"time"
)

// ErrRecycle is matched by the error a client reports when it stopped
// itself because a Recycle limit was reached. A supervisor (systemd, a
// container restart policy, a process manager) then starts a fresh
// process, which frees whatever the old one leaked.
var ErrRecycle = errors.New("workflow: worker recycled")

// RecycleReason names the limit that stopped a client.
type RecycleReason string

// The recycle reasons.
const (
	RecycleMaxJobs   RecycleReason = "max_jobs"
	RecycleMaxTime   RecycleReason = "max_time"
	RecycleMaxMemory RecycleReason = "max_memory"
)

// Recycle sets when a client stops itself so its process can be replaced:
// after MaxJobs step attempts, after MaxTime since Start, or once the
// memory reading passes MaxMemory. A zero field is no limit; the zero
// Recycle never recycles. When a limit is reached the client drains as
// Drain does (claims nothing more, lets running attempts finish), then
// stops; its StopCause is a *RecycleError. A driver takes it through its
// own option (memory.WithRecycle, sqlstore.WithRecycle).
type Recycle struct {
	// MaxJobs is how many step attempts this client may finish, whatever
	// their outcome. Attempts already running when it is reached still
	// finish, so a client with several workers may finish a few more.
	MaxJobs int

	// MaxTime is how long the client may run, from Start, on the
	// driver's clock.
	MaxTime time.Duration

	// MaxMemory is the memory reading, in bytes, above which the client
	// recycles. Checked every CheckEvery and after every attempt.
	MaxMemory uint64

	// Memory reads the current memory use in bytes. Nil means
	// HeapObjectBytes. A test passes a function returning what it needs;
	// an app may read RSS or a cgroup counter instead.
	Memory func() uint64

	// CheckEvery is how often the driver checks MaxMemory between
	// attempts. Zero means DefaultRecycleCheck.
	CheckEvery time.Duration
}

// DefaultRecycleCheck is Recycle.CheckEvery when it is zero.
const DefaultRecycleCheck = time.Second

// Enabled reports whether any limit is set.
func (r Recycle) Enabled() bool { return r.MaxJobs > 0 || r.MaxTime > 0 || r.MaxMemory > 0 }

// ValidateRecycle refuses negative limits and a negative CheckEvery.
// Errors wrap ErrInvalid.
func ValidateRecycle(r Recycle) error {
	switch {
	case r.MaxJobs < 0:
		return invalidf("Recycle.MaxJobs is negative")
	case r.MaxTime < 0:
		return invalidf("Recycle.MaxTime is negative")
	case r.CheckEvery < 0:
		return invalidf("Recycle.CheckEvery is negative")
	}
	return nil
}

// heapObjectsMetric is the runtime/metrics name HeapObjectBytes reads.
const heapObjectsMetric = "/memory/classes/heap/objects:bytes"

// HeapObjectBytes returns the bytes held by live and not yet swept heap
// objects (runtime/metrics), the default Recycle.Memory. It reads no
// process-wide state other than the runtime's own counters.
func HeapObjectBytes() uint64 {
	s := []metrics.Sample{{Name: heapObjectsMetric}}
	metrics.Read(s)
	if s[0].Value.Kind() != metrics.KindUint64 {
		return 0
	}
	return s[0].Value.Uint64()
}

// RecycleError says which limit stopped a client. It matches ErrRecycle.
type RecycleError struct {
	Reason RecycleReason
	// Jobs is how many step attempts the client had finished.
	Jobs int
	// Elapsed is the time from Start to the decision.
	Elapsed time.Duration
	// Memory is the last memory reading (zero when MaxMemory is unset).
	Memory uint64
}

func (e *RecycleError) Error() string {
	return fmt.Sprintf("workflow: worker recycled (%s) after %d jobs, %s, %d bytes", e.Reason, e.Jobs, e.Elapsed, e.Memory)
}

// Is makes errors.Is(err, ErrRecycle) true.
func (e *RecycleError) Is(target error) bool { return target == ErrRecycle }

// RecycleWatch is the decision every driver shares: it counts finished
// attempts and checks the limits. It latches: once a limit is reached
// every later call returns the same error. Safe for concurrent use.
type RecycleWatch struct {
	r       Recycle
	read    func() uint64
	started time.Time

	mu   sync.Mutex
	jobs int
	hit  *RecycleError
}

// NewRecycleWatch starts watching r from started (the driver's clock at
// Start).
func NewRecycleWatch(r Recycle, started time.Time) *RecycleWatch {
	read := r.Memory
	if read == nil {
		read = HeapObjectBytes
	}
	return &RecycleWatch{r: r, read: read, started: started}
}

// CheckEvery is the memory check interval: zero when MaxMemory is unset
// (no periodic check needed), else CheckEvery or DefaultRecycleCheck.
func (w *RecycleWatch) CheckEvery() time.Duration {
	if w.r.MaxMemory == 0 {
		return 0
	}
	if w.r.CheckEvery > 0 {
		return w.r.CheckEvery
	}
	return DefaultRecycleCheck
}

// Deadline is when MaxTime ends, and false when it is unset.
func (w *RecycleWatch) Deadline() (time.Time, bool) {
	if w.r.MaxTime <= 0 {
		return time.Time{}, false
	}
	return w.started.Add(w.r.MaxTime), true
}

// JobDone counts one finished attempt at now and checks every limit.
func (w *RecycleWatch) JobDone(now time.Time) *RecycleError {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.jobs++
	return w.checkLocked(now)
}

// Check checks every limit at now without counting an attempt.
func (w *RecycleWatch) Check(now time.Time) *RecycleError {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.checkLocked(now)
}

func (w *RecycleWatch) checkLocked(now time.Time) *RecycleError {
	if w.hit != nil {
		return w.hit
	}
	elapsed := now.Sub(w.started)
	var mem uint64
	if w.r.MaxMemory > 0 {
		mem = w.read()
	}
	var reason RecycleReason
	switch {
	case w.r.MaxJobs > 0 && w.jobs >= w.r.MaxJobs:
		reason = RecycleMaxJobs
	case w.r.MaxTime > 0 && elapsed >= w.r.MaxTime:
		reason = RecycleMaxTime
	case w.r.MaxMemory > 0 && mem > w.r.MaxMemory:
		reason = RecycleMaxMemory
	default:
		return nil
	}
	w.hit = &RecycleError{Reason: reason, Jobs: w.jobs, Elapsed: elapsed, Memory: mem}
	return w.hit
}

// StopCauser reports why a stopped client stopped. A driver with
// recycling implements it; Forward, failover and the middleware client
// wrappers pass it through.
//
// StopCause returns nil while the client runs and after a stop that was
// asked for (the Start ctx ended, Drain, a stop-when-idle option), and a
// *RecycleError when a Recycle limit stopped it. It is final once the
// Drainer's Done channel is closed.
type StopCauser interface {
	StopCause() error
}

// Serve starts client and blocks until it stops, so a worker's main is
// one call whose error says whether to exit for a restart:
//
//	if err := workflow.Serve(ctx, client); errors.Is(err, workflow.ErrRecycle) {
//		os.Exit(0) // the supervisor starts a fresh process
//	}
//
// It returns Start's error, the client's StopCause (a *RecycleError) when
// a recycle limit stopped it, and nil for every requested stop. It waits
// on the Drainer's Done channel; a client without CapDrain is waited on
// until ctx ends.
func Serve(ctx context.Context, client Client) error {
	if err := client.Start(ctx); err != nil {
		return err
	}
	d, ok := client.(Drainer)
	if !ok || !client.Capabilities().Has(CapDrain) {
		<-ctx.Done()
		return nil
	}
	<-d.Done()
	if sc, ok := client.(StopCauser); ok {
		return sc.StopCause()
	}
	return nil
}
