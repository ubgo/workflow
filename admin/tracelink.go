package admin

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/ubgo/workflow"
)

// WithTraceLink adds a link to each run's trace in a tracing UI (Jaeger,
// Tempo, Honeycomb, a vendor console): GET /runs/{id} carries it as
// "trace_url" and the web UI's run page shows it as a link. fn builds the
// URL from the run; the otel module's TraceIDOf reads the trace ID the
// otel middleware carried in the run's labels:
//
//	admin.WithTraceLink(func(r workflow.RunInfo) string {
//		id, ok := otel.TraceIDOf(r.Labels)
//		if !ok {
//			return ""
//		}
//		return "https://tracing.example.com/trace/" + id
//	})
//
// An empty string means no link. Only an absolute http or https URL with
// a host is shown; anything else (a javascript: or data: URL, a relative
// path) is dropped and logged, so a label an attacker controls cannot
// turn the link into script. A panic in fn is recovered and means no
// link. The UI keeps its Content-Security-Policy: the link is a plain
// navigation that opens in a new tab without a referrer. Nil, the
// default, shows no link.
//
//ds:def id=admin-withtracelink-rwcnu3e6 owner=@khanakia stability=stable
func WithTraceLink(fn func(workflow.RunInfo) string) HandlerOption {
	return func(c *handlerConfig) { c.traceLink = fn }
}

// traceURL returns the run's checked trace link, or "".
//
//ds:def id=admin-handler-traceurl-x89t3656 owner=@khanakia stability=stable
func (h *handler[P]) traceURL(info workflow.RunInfo) string {
	if h.cfg.traceLink == nil {
		return ""
	}
	raw := func() (s string) {
		defer func() {
			if p := recover(); p != nil {
				h.cfg.logger.Error("admin: trace link panicked", "run", refString(info.Ref), "panic", fmt.Sprint(p))
				s = ""
			}
		}()
		return h.cfg.traceLink(info)
	}()
	if raw == "" {
		return ""
	}
	if !safeLink(raw) {
		h.cfg.logger.Warn("admin: trace link dropped: not an absolute http or https URL", "run", refString(info.Ref))
		return ""
	}
	return raw
}

// safeLink reports whether s is an absolute http or https URL with a host
// and no control characters.
//
//ds:def id=admin-safelink-g9znk3p8 owner=@khanakia stability=stable
func safeLink(s string) bool {
	if strings.ContainsFunc(s, func(r rune) bool { return r < 0x20 || r == 0x7f }) {
		return false
	}
	u, err := url.Parse(s)
	if err != nil || u.Host == "" || u.Opaque != "" {
		return false
	}
	scheme := strings.ToLower(u.Scheme)
	return scheme == "http" || scheme == "https"
}
