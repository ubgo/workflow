package middleware

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
)

// DefaultAlertDedup is how long Alerts stays quiet about a key it has
// just alerted on, when AlertConfig.Limiter is nil: one page per broken
// workflow every ten minutes, not one per failed run.
const DefaultAlertDedup = 10 * time.Minute

// Alert is one alert: the failure, the dedup key it was counted under
// and how many alerts for that key the limiter held back since the last
// one sent.
type Alert struct {
	workflow.Failure

	// Key is the dedup key (AlertConfig.Key).
	Key string

	// Suppressed counts the alerts for Key the limiter held back since
	// the previous one sent, so a sink can say "and 12 more".
	Suppressed int
}

// Alerter sends an alert where a human sees it: Slack, email, a pager.
// The sink is the app's; this package only decides when to call it.
// It is called on a driver goroutine and should not block for long.
type Alerter interface {
	Alert(ctx context.Context, a Alert) error
}

// AlerterFunc adapts a function to Alerter.
type AlerterFunc func(ctx context.Context, a Alert) error

// Alert calls fn.
func (fn AlerterFunc) Alert(ctx context.Context, a Alert) error { return fn(ctx, a) }

// AlertLimiter decides whether an alert for key goes out at at. It
// reports how many alerts for key it held back since the last one it let
// through (reset when it lets one through). A shared implementation (a
// database row per key) makes the limit fleet-wide; MemoryAlertLimiter
// holds only inside one process.
type AlertLimiter interface {
	Allow(ctx context.Context, key string, at time.Time) (ok bool, suppressed int, err error)
}

// AlertConfig configures Alerts. The zero value alerts on every final
// failure, deduplicated per workflow and step for DefaultAlertDedup.
type AlertConfig struct {
	// Discarded also alerts on runs a step discarded.
	Discarded bool

	// When, if set, filters failures: only those it returns true for
	// alert.
	When func(workflow.Failure) bool

	// Key is the dedup key of a failure. Nil means workflow + "/" + step.
	Key func(workflow.Failure) string

	// Limiter decides dedup and rate limits. Nil means a
	// MemoryAlertLimiter with Dedup DefaultAlertDedup on Clock; pass one
	// built with AlertLimits{} to send every alert.
	Limiter AlertLimiter

	// Clock stamps Failure.At and drives the default limiter. Nil means
	// clock.Real.
	Clock clock.Clock

	// Logger receives errors from the Alerter and the Limiter. Nil
	// discards them.
	Logger *slog.Logger
}

// Alerts returns a run-finished observer that calls a on every run that
// ends failed (and discarded, when cfg.Discarded) and that the limiter
// lets through. A limiter error sends the alert anyway: a broken limit
// store must not hide a failure. Register it with OnRunFinished, or use
// AlertPlugin.
func Alerts(a Alerter, cfg AlertConfig) workflow.RunFinishedFunc {
	clk := cfg.Clock
	if clk == nil {
		clk = clock.Real{}
	}
	lim := cfg.Limiter
	if lim == nil {
		lim = NewMemoryAlertLimiter(clk, AlertLimits{Dedup: DefaultAlertDedup})
	}
	key := cfg.Key
	if key == nil {
		key = defaultAlertKey
	}
	log := cfg.Logger
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	return func(ctx context.Context, ev workflow.RunEvent) {
		f, ok := workflow.FailureOf(ev, cfg.Discarded, clk.Now())
		if !ok || (cfg.When != nil && !cfg.When(f)) {
			return
		}
		k := key(f)
		allow, suppressed, err := lim.Allow(ctx, k, f.At)
		if err != nil {
			log.Error("workflow/middleware: alert limiter failed; alerting anyway", "key", k, "error", err)
			allow = true
		}
		if !allow {
			return
		}
		if err := a.Alert(ctx, Alert{Failure: f, Key: k, Suppressed: suppressed}); err != nil {
			log.Error("workflow/middleware: alert not sent", "key", k, "workflow", f.Workflow, "error", err)
		}
	}
}

// defaultAlertKey is workflow + "/" + step.
func defaultAlertKey(f workflow.Failure) string { return f.Workflow + "/" + f.Step }

// AlertPlugin bundles Alerts as a plugin named "alerts" for workflow.Use.
func AlertPlugin(a Alerter, cfg AlertConfig) workflow.Plugin {
	return observerPlugin{name: "alerts", fn: Alerts(a, cfg)}
}

// observerPlugin is a plugin contributing one run-finished observer.
type observerPlugin struct {
	name string
	fn   workflow.RunFinishedFunc
	mw   []workflow.Middleware
}

func (p observerPlugin) Name() string { return p.name }

func (p observerPlugin) RunFinishedObservers() []workflow.RunFinishedFunc {
	return []workflow.RunFinishedFunc{p.fn}
}

// AlertLimits are MemoryAlertLimiter's rules. The zero value lets every
// alert through.
type AlertLimits struct {
	// Dedup holds back an alert for a key that alerted less than Dedup
	// ago. Zero turns dedup off.
	Dedup time.Duration

	// Max and Per cap alerts across all keys at Max per sliding Per
	// window. Either zero turns the cap off.
	Max int
	Per time.Duration
}

// MemoryAlertLimiter is the in-process AlertLimiter. Its state lives in
// one process: each replica of a fleet dedups on its own.
type MemoryAlertLimiter struct {
	clk  clock.Clock
	lim  AlertLimits
	mu   sync.Mutex
	keys map[string]*alertKeyState
	sent []time.Time // alerts let through within Per, oldest first
}

type alertKeyState struct {
	last       time.Time
	suppressed int
}

// NewMemoryAlertLimiter returns a limiter with rules l. clk prunes
// state; nil means clock.Real.
func NewMemoryAlertLimiter(clk clock.Clock, l AlertLimits) *MemoryAlertLimiter {
	if clk == nil {
		clk = clock.Real{}
	}
	return &MemoryAlertLimiter{clk: clk, lim: l, keys: map[string]*alertKeyState{}}
}

// Allow implements AlertLimiter. It never fails.
func (m *MemoryAlertLimiter) Allow(_ context.Context, key string, at time.Time) (bool, int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pruneLocked(at)
	st := m.keys[key]
	if st == nil {
		st = &alertKeyState{}
		m.keys[key] = st
	}
	if m.lim.Dedup > 0 && !st.last.IsZero() && at.Sub(st.last) < m.lim.Dedup {
		st.suppressed++
		return false, 0, nil
	}
	if m.lim.Max > 0 && m.lim.Per > 0 && len(m.sent) >= m.lim.Max {
		st.suppressed++
		return false, 0, nil
	}
	suppressed := st.suppressed
	st.last, st.suppressed = at, 0
	if m.lim.Max > 0 && m.lim.Per > 0 {
		m.sent = append(m.sent, at)
	}
	return true, suppressed, nil
}

// pruneLocked forgets sends older than Per and keys quiet for longer
// than Dedup with nothing held back, so memory stays bounded by the keys
// active within the windows.
func (m *MemoryAlertLimiter) pruneLocked(at time.Time) {
	if m.lim.Per > 0 {
		i := 0
		for i < len(m.sent) && at.Sub(m.sent[i]) >= m.lim.Per {
			i++
		}
		m.sent = m.sent[i:]
	}
	for k, st := range m.keys {
		if st.suppressed == 0 && at.Sub(st.last) >= m.lim.Dedup {
			delete(m.keys, k)
		}
	}
}
