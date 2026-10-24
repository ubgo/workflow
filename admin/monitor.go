package admin

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ubgo/workflow"
)

// AlertKind is which threshold an alert is about.
type AlertKind string

// The alert kinds.
const (
	// AlertQueueDepth: more runs wait in a queue than Threshold.MaxDepth.
	AlertQueueDepth AlertKind = "queue_depth"
	// AlertQueueWait: the oldest queued run has waited longer than
	// Threshold.MaxWait.
	AlertQueueWait AlertKind = "queue_wait"
)

// Threshold is when a queue is in trouble: too deep, or its oldest run
// waiting too long. A zero field is not checked.
type Threshold struct {
	// Queue is the queue it applies to. Empty is the default for every
	// queue that has no Threshold of its own.
	Queue string
	// MaxDepth alerts when Depth is above it.
	MaxDepth int
	// MaxWait alerts when OldestWait is above it.
	MaxWait time.Duration
}

// Alert is one threshold crossing, or the end of one.
type Alert struct {
	Kind  AlertKind
	Queue string
	// Depth and Wait are the queue's values at the poll that raised the
	// alert (Wait is its OldestWait).
	Depth int
	Wait  time.Duration
	// Threshold is the one crossed.
	Threshold Threshold
	// Resolved is true on the alert that reports the queue back under the
	// threshold.
	Resolved bool
	// Truncated is copied from the QueueReport: Depth is a lower bound.
	Truncated bool
	// At is the poll time on the Admin's clock.
	At time.Time
}

// Alerter receives alerts. Send them to a chat channel, a pager or a log.
// Alert runs on the Monitor's goroutine, one call at a time; a slow
// Alerter delays the next poll, and a panic is recovered and reported to
// the error handler.
type Alerter interface {
	Alert(ctx context.Context, a Alert)
}

// AlerterFunc adapts a function to Alerter.
type AlerterFunc func(ctx context.Context, a Alert)

// Alert calls f.
func (f AlerterFunc) Alert(ctx context.Context, a Alert) { f(ctx, a) }

// DefaultMonitorInterval is how often a Monitor polls when WithInterval is
// not given.
const DefaultMonitorInterval = 30 * time.Second

// MonitorOption configures NewMonitor.
type MonitorOption func(*Monitor)

// WithInterval sets how often Run polls. Zero or less keeps
// DefaultMonitorInterval.
func WithInterval(d time.Duration) MonitorOption { return func(m *Monitor) { m.interval = d } }

// WithRepeat re-sends an alert every d while its queue stays over the
// threshold. Zero, the default, alerts once on crossing and once on
// resolving.
func WithRepeat(d time.Duration) MonitorOption { return func(m *Monitor) { m.repeat = d } }

// WithErrorHandler receives poll errors and recovered Alerter panics. Nil,
// the default, drops them; Run keeps polling either way.
func WithErrorHandler(fn func(error)) MonitorOption { return func(m *Monitor) { m.onErr = fn } }

// Monitor polls Admin.QueueStats on the Admin's clock and calls an Alerter
// when a queue crosses a Threshold, and again when it is back under. It
// keeps which alerts are open in memory only, so two Monitors on two
// nodes each alert; run one per deployment.
type Monitor struct {
	a          *Admin
	alerter    Alerter
	byQueue    map[string]Threshold
	def        Threshold
	hasDefault bool
	interval   time.Duration
	repeat     time.Duration
	onErr      func(error)

	mu   sync.Mutex
	open map[alertKey]time.Time // open alert -> when it was last sent
}

type alertKey struct {
	kind  AlertKind
	queue string
}

// NewMonitor returns a Monitor over a. It refuses, with workflow.ErrInvalid,
// a nil Alerter, no thresholds, a negative field, a threshold that checks
// nothing, an invalid queue name or two thresholds for one queue; and,
// with an UnsupportedError, a client QueueStats cannot work on.
func NewMonitor(a *Admin, alerter Alerter, thresholds []Threshold, opts ...MonitorOption) (*Monitor, error) {
	if alerter == nil {
		return nil, fmt.Errorf("%w: admin: Monitor needs an Alerter", workflow.ErrInvalid)
	}
	if len(thresholds) == 0 {
		return nil, fmt.Errorf("%w: admin: Monitor needs at least one Threshold", workflow.ErrInvalid)
	}
	if err := a.check(OpQueues); err != nil {
		return nil, err
	}
	m := &Monitor{a: a, alerter: alerter, byQueue: map[string]Threshold{}, open: map[alertKey]time.Time{}}
	for i, t := range thresholds {
		if t.MaxDepth < 0 || t.MaxWait < 0 {
			return nil, fmt.Errorf("%w: admin: thresholds[%d] is negative", workflow.ErrInvalid, i)
		}
		if t.MaxDepth == 0 && t.MaxWait == 0 {
			return nil, fmt.Errorf("%w: admin: thresholds[%d] sets neither MaxDepth nor MaxWait", workflow.ErrInvalid, i)
		}
		if t.Queue == "" {
			if m.hasDefault {
				return nil, fmt.Errorf("%w: admin: two default thresholds", workflow.ErrInvalid)
			}
			m.def, m.hasDefault = t, true
			continue
		}
		if err := workflow.ValidateQueueName(t.Queue); err != nil {
			return nil, fmt.Errorf("admin: thresholds[%d]: %w", i, err)
		}
		if _, dup := m.byQueue[t.Queue]; dup {
			return nil, fmt.Errorf("%w: admin: two thresholds for queue %q", workflow.ErrInvalid, t.Queue)
		}
		m.byQueue[t.Queue] = t
	}
	for _, o := range opts {
		if o != nil {
			o(m)
		}
	}
	if m.interval <= 0 {
		m.interval = DefaultMonitorInterval
	}
	return m, nil
}

// threshold returns the threshold for queue and whether one applies.
func (m *Monitor) threshold(queue string) (Threshold, bool) {
	if t, ok := m.byQueue[queue]; ok {
		return t, true
	}
	return m.def, m.hasDefault
}

// Check polls once and sends the alerts the poll calls for. Run calls it
// on every tick; tests and a cron job can call it directly.
func (m *Monitor) Check(ctx context.Context) error {
	rep, err := m.a.QueueStats(ctx)
	if err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	seen := map[alertKey]bool{}
	stats := map[string]QueueStat{}
	for _, s := range rep.Queues {
		stats[s.Queue] = s
		t, ok := m.threshold(s.Queue)
		if !ok {
			continue
		}
		if t.MaxDepth > 0 && s.Depth > t.MaxDepth {
			seen[alertKey{AlertQueueDepth, s.Queue}] = true
			m.raise(ctx, alertKey{AlertQueueDepth, s.Queue}, s, t, rep)
		}
		if t.MaxWait > 0 && s.OldestWait > t.MaxWait {
			seen[alertKey{AlertQueueWait, s.Queue}] = true
			m.raise(ctx, alertKey{AlertQueueWait, s.Queue}, s, t, rep)
		}
	}
	// An open alert not seen this poll is resolved, including for a queue
	// that emptied and so is absent from the report.
	for k := range m.open {
		if seen[k] {
			continue
		}
		delete(m.open, k)
		s := stats[k.queue]
		s.Queue = k.queue
		t, _ := m.threshold(k.queue)
		m.send(ctx, Alert{Kind: k.kind, Queue: k.queue, Depth: s.Depth, Wait: s.OldestWait, Threshold: t, Resolved: true, Truncated: rep.Truncated, At: rep.At})
	}
	return nil
}

// raise sends k's alert when it is new, or when WithRepeat says it is due
// again.
func (m *Monitor) raise(ctx context.Context, k alertKey, s QueueStat, t Threshold, rep QueueReport) {
	last, open := m.open[k]
	if open && (m.repeat <= 0 || rep.At.Sub(last) < m.repeat) {
		return
	}
	m.open[k] = rep.At
	m.send(ctx, Alert{Kind: k.kind, Queue: k.queue, Depth: s.Depth, Wait: s.OldestWait, Threshold: t, Truncated: rep.Truncated, At: rep.At})
}

// send calls the Alerter, recovering a panic so one bad alert never stops
// the Monitor.
func (m *Monitor) send(ctx context.Context, a Alert) {
	defer func() {
		if r := recover(); r != nil {
			m.report(fmt.Errorf("admin: Alerter panicked: %v", r))
		}
	}()
	m.alerter.Alert(ctx, a)
}

func (m *Monitor) report(err error) {
	if m.onErr != nil && err != nil {
		m.onErr(err)
	}
}

// Run polls every interval until ctx ends, then returns ctx's error. A
// poll error goes to the error handler and the next poll still runs, so a
// database blip does not stop alerting. Nothing runs before Run is
// called.
func (m *Monitor) Run(ctx context.Context) error {
	for {
		t := m.a.clk.NewTimer(m.interval)
		select {
		case <-ctx.Done():
			t.Stop()
			return ctx.Err()
		case <-t.C():
		}
		if err := m.Check(ctx); err != nil && ctx.Err() == nil {
			m.report(err)
		}
	}
}
