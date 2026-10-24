package middleware

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"time"

	"github.com/ubgo/workflow"
)

// DefaultPingTimeout bounds one ping when no WithPingTimeout is given: long
// enough for a heartbeat service across the internet, short enough that a
// dead one does not hold up the driver goroutine that delivers hooks.
const DefaultPingTimeout = 10 * time.Second

// maxPingBody is how much of a ping's response body is read before the
// connection is closed, so the connection can be reused without trusting
// the service to send a small body.
const maxPingBody = 64 << 10

// PingFunc sends one ping to target, a URL. It is the seam for any heartbeat
// service: HTTPPing is the stock one, and a PingFunc can POST a body, add
// an auth header or call a client library instead.
type PingFunc func(ctx context.Context, target string) error

// HTTPPing returns a PingFunc that sends GET target with client (nil means
// http.DefaultClient; the Pinger's timeout applies either way) and fails
// on a status outside 2xx. Its errors never contain the URL, because a
// heartbeat URL usually carries the check's secret token and errors end
// up in logs.
func HTTPPing(client *http.Client) PingFunc {
	if client == nil {
		client = http.DefaultClient
	}
	return func(ctx context.Context, target string) error {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
		if err != nil {
			return errors.New("workflow/middleware: ping: invalid URL")
		}
		resp, err := client.Do(req)
		if err != nil {
			var ue *url.Error
			if errors.As(err, &ue) {
				err = ue.Err // drop the URL the transport error repeats
			}
			return fmt.Errorf("workflow/middleware: ping: %w", err)
		}
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxPingBody))
		_ = resp.Body.Close()
		if resp.StatusCode < 200 || resp.StatusCode > 299 {
			return fmt.Errorf("workflow/middleware: ping: status %d", resp.StatusCode)
		}
		return nil
	}
}

// Pinger turns heartbeat URLs into schedule hooks: a ping when a tick
// fires, after a run, on success or on failure. Its methods return funcs
// with the shapes of workflow.ScheduleHooks' fields, so a hook is one
// assignment:
//
//	p := middleware.NewPinger(nil)
//	workflow.ScheduleHooks{
//		Workflow:  "backup",
//		Fired:     p.Tick("https://hc.example/abc/start"), // a run started
//		OnSuccess: p.Run("https://hc.example/abc"),        // it succeeded
//		OnFailure: p.Run("https://hc.example/abc/fail"),   // it failed
//	}
//
// After takes p.Run(url) for a ping whatever the outcome. A ping is best effort: a failure
// is logged at Warn (without the URL) and never fails the run or the
// tick. A Pinger holds no state beyond its options and is safe for
// concurrent use.
type Pinger struct {
	send    PingFunc
	timeout time.Duration
	logger  *slog.Logger
}

// PingerOption configures a Pinger.
type PingerOption func(*Pinger)

// WithPingTimeout bounds each ping (DefaultPingTimeout when not given). A
// d of zero or less keeps the default, so a ping can never block the
// hook goroutine forever.
func WithPingTimeout(d time.Duration) PingerOption {
	return func(p *Pinger) {
		if d > 0 {
			p.timeout = d
		}
	}
}

// WithPingLogger sets where failed pings are logged; nil drops them.
func WithPingLogger(l *slog.Logger) PingerOption {
	return func(p *Pinger) { p.logger = l }
}

// NewPinger returns a Pinger that sends through send (nil means
// HTTPPing(nil)) and logs failures to slog.Default() unless
// WithPingLogger says otherwise.
func NewPinger(send PingFunc, opts ...PingerOption) *Pinger {
	if send == nil {
		send = HTTPPing(nil)
	}
	p := &Pinger{send: send, timeout: DefaultPingTimeout, logger: slog.Default()}
	for _, o := range opts {
		o(p)
	}
	return p
}

// Tick returns a tick hook that pings target when a tick fired a run: for
// ScheduleHooks.Fired, or as a workflow.TickObserver of its
// own. A skipped or misfired tick, or a fired tick whose enqueue failed,
// sends nothing: no run started, so there is nothing to report as begun.
func (p *Pinger) Tick(target string) func(ctx context.Context, t workflow.Tick) {
	return func(ctx context.Context, t workflow.Tick) {
		if t.Outcome != workflow.TickFired || t.Err != nil {
			return
		}
		p.ping(ctx, target, slog.String("workflow", t.Workflow), slog.String("schedule", t.Name))
	}
}

// Run returns a run hook that pings target for every run event it is given:
// for ScheduleHooks.After, OnSuccess or OnFailure, which already choose
// the outcome.
func (p *Pinger) Run(target string) func(ctx context.Context, ev workflow.RunEvent) {
	return func(ctx context.Context, ev workflow.RunEvent) {
		p.ping(ctx, target, slog.String("workflow", ev.Workflow), slog.String("status", string(ev.Status)))
	}
}

// ping sends one ping under the Pinger's timeout. The deadline is real
// time on purpose: it bounds network I/O, not a scheduling decision, so
// it does not come from the driver's clock.
func (p *Pinger) ping(ctx context.Context, target string, attrs ...slog.Attr) {
	ctx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	if err := p.send(ctx, target); err != nil && p.logger != nil {
		attrs = append(attrs, slog.String("error", err.Error()))
		p.logger.LogAttrs(ctx, slog.LevelWarn, "workflow schedule ping failed", attrs...)
	}
}
