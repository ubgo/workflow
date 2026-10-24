package admin

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
)

// MaxMetricsPoints bounds how many points one Metrics call returns, so a
// chart request over a year at one-minute steps is refused rather than
// answered with half a million points.
const MaxMetricsPoints = 1440

// MetricsQuery asks for run ends over [From, To) in steps of Step.
type MetricsQuery struct {
	From, To time.Time
	// Step is the width of one point. A source with a coarser resolution
	// rounds it up to a multiple of its own (MetricsSeries.Step says what
	// was used).
	Step time.Duration
	// Workflow narrows the counts to one workflow; empty counts all.
	Workflow string
}

// MetricsPoint is the run ends in [At, At+Step), by terminal status.
type MetricsPoint struct {
	At        time.Time
	Succeeded int64
	Failed    int64
	Cancelled int64
	// Discarded is counted only from a driver with CapRunStates; on
	// others a discarded run arrives as failed or cancelled.
	Discarded int64
}

// Total is every run end in the point.
func (p MetricsPoint) Total() int64 { return p.Succeeded + p.Failed + p.Cancelled + p.Discarded }

// MetricsSeries is the answer to a MetricsQuery: one point per step from
// From to To, zero-filled where nothing ended, oldest first.
type MetricsSeries struct {
	Step   time.Duration
	Points []MetricsPoint
	// Since is the earliest time the source has data for; points before
	// it read zero because nothing was recorded, not because nothing ran.
	// Zero when the source cannot say.
	Since time.Time
}

// MetricsSource answers historical run-end counts for the charts screen.
// The library keeps no history of its own: an app with a metrics store
// (Prometheus, a warehouse table, an OpenTelemetry backend) adapts it
// with MetricsSourceFunc, and MemoryMetrics is the in-process default for
// an app that has none. Implementations must return one point per step
// over the query, zero-filled.
type MetricsSource interface {
	Series(ctx context.Context, q MetricsQuery) (MetricsSeries, error)
}

// MetricsSourceFunc adapts a function to MetricsSource.
type MetricsSourceFunc func(ctx context.Context, q MetricsQuery) (MetricsSeries, error)

// Series calls f.
func (f MetricsSourceFunc) Series(ctx context.Context, q MetricsQuery) (MetricsSeries, error) {
	return f(ctx, q)
}

// WithMetrics lets Admin.Metrics, the metrics route and the UI's charts
// read run-end history from src. Without it they report that no metrics
// source is wired.
func WithMetrics(src MetricsSource) Option { return func(a *Admin) { a.metrics = src } }

// HasMetrics reports whether a MetricsSource is wired (WithMetrics).
func (a *Admin) HasMetrics() bool { return a.metrics != nil }

// errNoMetrics is the refusal of Metrics without WithMetrics.
var errNoMetrics = &workflow.UnsupportedError{Field: "admin.metrics", Reason: "no metrics source is wired (admin.WithMetrics)"}

// ValidateMetricsQuery checks q's shape: From before To, a positive Step,
// and at most MaxMetricsPoints points. Errors wrap workflow.ErrInvalid.
func ValidateMetricsQuery(q MetricsQuery) error {
	if q.From.IsZero() || q.To.IsZero() || !q.From.Before(q.To) {
		return fmt.Errorf("%w: admin: metrics: From must be set and before To", workflow.ErrInvalid)
	}
	if q.Step <= 0 {
		return fmt.Errorf("%w: admin: metrics: Step %s is not positive", workflow.ErrInvalid, q.Step)
	}
	if n := (q.To.Sub(q.From) + q.Step - 1) / q.Step; n > MaxMetricsPoints {
		return fmt.Errorf("%w: admin: metrics: %d points asked, at most %d; widen Step", workflow.ErrInvalid, n, MaxMetricsPoints)
	}
	return nil
}

// Metrics returns run ends over q from the wired MetricsSource. Without
// WithMetrics it returns an error matching workflow.ErrUnsupported.
func (a *Admin) Metrics(ctx context.Context, q MetricsQuery) (MetricsSeries, error) {
	if a.metrics == nil {
		return MetricsSeries{}, errNoMetrics
	}
	if err := ValidateMetricsQuery(q); err != nil {
		return MetricsSeries{}, err
	}
	return a.metrics.Series(ctx, q)
}

// The MemoryMetrics defaults.
const (
	DefaultMetricsBucket = time.Minute
	DefaultMetricsKeep   = 24 * time.Hour
	// DefaultMetricsDedupe is how many recent run ends MemoryMetrics
	// remembers to drop a repeated delivery (delivery is at least once).
	DefaultMetricsDedupe = 4096
)

// MemoryMetricsConfig configures NewMemoryMetrics. Zero or less in a
// duration or count keeps its default.
type MemoryMetricsConfig struct {
	// Bucket is the resolution counts are kept at (DefaultMetricsBucket).
	Bucket time.Duration
	// Keep is how long counts are kept (DefaultMetricsKeep).
	Keep time.Duration
	// Dedupe is how many recent (run, status) pairs are remembered so a
	// redelivered end is counted once (DefaultMetricsDedupe). A repeat
	// arriving after that many other ends is counted again.
	Dedupe int
	// Clock prunes old buckets and stamps an event without a FinishedAt;
	// nil means clock.Real.
	Clock clock.Clock
}

// counts is one bucket's run ends for one workflow.
type counts struct{ succeeded, failed, cancelled, discarded int64 }

// MemoryMetrics is an in-process, bounded MetricsSource: it counts run
// ends by status, per workflow, in fixed buckets, fed by the run-finished
// hook (register Observe with workflow.RunNotifier.OnRunFinished before
// Start). Each process counts the runs it was told about, so on a durable
// driver where every process receives every end it shows the fleet, and
// where ends are delivered to one process it shows that process; it
// starts empty on every restart (Series.Since says when). An app that
// needs history across restarts wires its own MetricsSource.
type MemoryMetrics struct {
	cfg     MemoryMetricsConfig
	mu      sync.Mutex
	buckets map[int64]map[string]*counts // bucket start (unix ns) -> workflow -> counts
	since   time.Time
	seen    map[dedupeKey]struct{}
	ring    []dedupeKey // insertion order of seen, oldest at head
	head    int
}

type dedupeKey struct {
	run    workflow.RunID
	sched  workflow.ScheduledID
	status workflow.Status
	at     int64
}

// NewMemoryMetrics returns an empty MemoryMetrics.
func NewMemoryMetrics(cfg MemoryMetricsConfig) *MemoryMetrics {
	if cfg.Bucket <= 0 {
		cfg.Bucket = DefaultMetricsBucket
	}
	if cfg.Keep <= 0 {
		cfg.Keep = DefaultMetricsKeep
	}
	if cfg.Dedupe <= 0 {
		cfg.Dedupe = DefaultMetricsDedupe
	}
	if cfg.Clock == nil {
		cfg.Clock = clock.Real{}
	}
	return &MemoryMetrics{cfg: cfg, buckets: map[int64]map[string]*counts{}, since: cfg.Clock.Now(), seen: map[dedupeKey]struct{}{}}
}

// Observe counts one run end. Its signature is workflow.RunFinishedFunc,
// so it registers with OnRunFinished directly. A non-terminal status is
// ignored; a repeat of a recent (run, status, finish time) is counted
// once.
func (m *MemoryMetrics) Observe(_ context.Context, ev workflow.RunEvent) {
	if !ev.Status.IsTerminal() {
		return
	}
	at := ev.FinishedAt
	if at.IsZero() {
		at = m.cfg.Clock.Now()
	}
	key := dedupeKey{run: ev.Ref.RunID, sched: ev.Ref.ScheduledID, status: ev.Status, at: at.UnixNano()}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, dup := m.seen[key]; dup {
		return
	}
	m.remember(key)
	m.prune()
	if at.Before(m.cfg.Clock.Now().Add(-m.cfg.Keep)) {
		return
	}
	if at.Before(m.since) {
		// A late or redelivered end (a durable driver after a restart)
		// predates this source: Since covers it.
		m.since = at
	}
	b := m.bucketOf(at)
	byWf := m.buckets[b]
	if byWf == nil {
		byWf = map[string]*counts{}
		m.buckets[b] = byWf
	}
	c := byWf[ev.Workflow]
	if c == nil {
		c = &counts{}
		byWf[ev.Workflow] = c
	}
	switch ev.Status {
	case workflow.StatusSucceeded:
		c.succeeded++
	case workflow.StatusFailed:
		c.failed++
	case workflow.StatusCancelled:
		c.cancelled++
	case workflow.StatusDiscarded:
		c.discarded++
	}
}

// remember adds key to the dedupe set, forgetting the oldest past the
// bound. The caller holds mu.
func (m *MemoryMetrics) remember(key dedupeKey) {
	if len(m.ring) < m.cfg.Dedupe {
		m.ring = append(m.ring, key)
	} else {
		delete(m.seen, m.ring[m.head])
		m.ring[m.head] = key
		m.head = (m.head + 1) % m.cfg.Dedupe
	}
	m.seen[key] = struct{}{}
}

// bucketOf is the start of t's bucket, in unix nanoseconds.
func (m *MemoryMetrics) bucketOf(t time.Time) int64 {
	n := t.UnixNano()
	w := int64(m.cfg.Bucket)
	r := n % w
	if r < 0 {
		r += w
	}
	return n - r
}

// prune drops buckets older than Keep. The caller holds mu.
func (m *MemoryMetrics) prune() {
	cut := m.bucketOf(m.cfg.Clock.Now().Add(-m.cfg.Keep))
	for b := range m.buckets {
		if b < cut {
			delete(m.buckets, b)
		}
	}
}

// Series sums the buckets into q's steps. q.Step is rounded up to a
// multiple of the bucket, and From down to a bucket boundary, so every
// bucket falls in exactly one point.
func (m *MemoryMetrics) Series(_ context.Context, q MetricsQuery) (MetricsSeries, error) {
	if err := ValidateMetricsQuery(q); err != nil {
		return MetricsSeries{}, err
	}
	bw := m.cfg.Bucket
	step := max(bw, (q.Step+bw-1)/bw*bw)
	m.mu.Lock()
	defer m.mu.Unlock()
	m.prune()
	from := m.bucketOf(q.From)
	to := q.To.UnixNano()
	n := int((to - from + int64(step) - 1) / int64(step))
	pts := make([]MetricsPoint, n)
	for i := range pts {
		pts[i].At = time.Unix(0, from+int64(i)*int64(step)).In(q.From.Location())
	}
	for b, byWf := range m.buckets {
		if b < from || b >= to {
			continue
		}
		i := int((b - from) / int64(step))
		for wf, c := range byWf {
			if q.Workflow != "" && wf != q.Workflow {
				continue
			}
			pts[i].Succeeded += c.succeeded
			pts[i].Failed += c.failed
			pts[i].Cancelled += c.cancelled
			pts[i].Discarded += c.discarded
		}
	}
	since := m.since
	if keep := m.cfg.Clock.Now().Add(-m.cfg.Keep); keep.After(since) {
		since = keep
	}
	return MetricsSeries{Step: step, Points: pts, Since: since}, nil
}
