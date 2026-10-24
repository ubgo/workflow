package middleware

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
)

// Output is what CaptureOutput delivers for one step attempt: the log
// records the step wrote through OutputLogger, how it ended, and its
// result bytes when CaptureResult was given. For a scheduled task or a
// Func (one step) one Output is one run attempt.
type Output struct {
	// Info is the attempt's StepInfo; HasInfo is false when the ctx
	// carried none (a handler called outside a driver).
	Info    workflow.StepInfo
	HasInfo bool

	// Started and Finished bound the attempt, on the middleware's clock.
	Started, Finished time.Time

	// Records are the log records the step wrote through OutputLogger, in
	// order, at or above the capture level.
	Records []OutputRecord

	// Result is the step's output bytes. It is nil unless CaptureResult
	// was given, because a result may hold payload data an app wants kept
	// out of logs, files and mail.
	Result []byte

	// Err is the attempt's error; nil when it succeeded.
	Err error
}

// OutputRecord is one captured log record.
type OutputRecord struct {
	Time    time.Time
	Level   slog.Level
	Message string
	Attrs   []slog.Attr
}

// OutputSink receives every captured Output. Deliver runs on the step's
// goroutine after the attempt returned and before the driver records the
// result, so a slow sink holds the worker; a sink that sends mail or calls
// a network service should hand off to its own goroutine or queue. Its
// error never changes the step's result; it goes to CaptureOnSinkError.
type OutputSink interface {
	Deliver(ctx context.Context, out Output) error
}

// OutputSinkFunc adapts a function to OutputSink: the way to mail output,
// post it to a chat or store it anywhere the library does not know.
type OutputSinkFunc func(ctx context.Context, out Output) error

// Deliver calls f.
func (f OutputSinkFunc) Deliver(ctx context.Context, out Output) error { return f(ctx, out) }

// FormatOutput writes out as text: one header line naming the run, step,
// attempt and outcome, one line per record, then the result when it was
// captured. It is the format WriterSink and FileSink use.
func FormatOutput(w io.Writer, out Output) error {
	var b strings.Builder
	status := "ok"
	if out.Err != nil {
		status = "error: " + out.Err.Error()
	}
	if out.HasInfo {
		fmt.Fprintf(&b, "== %s %s/%s attempt %d [%s] %s\n", out.Finished.UTC().Format(time.RFC3339),
			out.Info.Workflow, out.Info.Step, out.Info.Attempt, out.Info.RunID, status)
	} else {
		fmt.Fprintf(&b, "== %s %s\n", out.Finished.UTC().Format(time.RFC3339), status)
	}
	for _, r := range out.Records {
		fmt.Fprintf(&b, "%s %s %s", r.Time.UTC().Format(time.RFC3339), r.Level, r.Message)
		for _, a := range r.Attrs {
			fmt.Fprintf(&b, " %s=%v", a.Key, a.Value)
		}
		b.WriteByte('\n')
	}
	if out.Result != nil {
		fmt.Fprintf(&b, "result: %s\n", out.Result)
	}
	_, err := io.WriteString(w, b.String())
	return err
}

// WriterSink writes each Output to w with FormatOutput, one delivery at a
// time.
func WriterSink(w io.Writer) OutputSink {
	var mu sync.Mutex
	return OutputSinkFunc(func(_ context.Context, out Output) error {
		mu.Lock()
		defer mu.Unlock()
		return FormatOutput(w, out)
	})
}

// FileMode says whether FileSink replaces or extends its file.
type FileMode int

// The file modes.
const (
	// FileReplace truncates the file on every delivery, so it holds the
	// latest attempt's output only.
	FileReplace FileMode = iota
	// FileAppend appends every delivery to the file.
	FileAppend
)

// fileSinkPerm is the permission a FileSink creates its file with: owner
// read and write only, since output can hold whatever a step logged.
const fileSinkPerm = 0o600

// FileSink writes each Output to the file at path with FormatOutput,
// creating it when missing (mode 0600). Deliveries from one sink are
// serialised; two sinks on one path are not.
func FileSink(path string, mode FileMode) OutputSink {
	var mu sync.Mutex
	return OutputSinkFunc(func(_ context.Context, out Output) error {
		mu.Lock()
		defer mu.Unlock()
		flag := os.O_CREATE | os.O_WRONLY
		if mode == FileAppend {
			flag |= os.O_APPEND
		} else {
			flag |= os.O_TRUNC
		}
		f, err := os.OpenFile(path, flag, fileSinkPerm)
		if err != nil {
			return err
		}
		if err := FormatOutput(f, out); err != nil {
			_ = f.Close()
			return err
		}
		return f.Close()
	})
}

// CaptureOption configures CaptureOutput.
type CaptureOption func(*captureConfig)

type captureConfig struct {
	clk       clock.Clock
	result    bool
	failOnly  bool
	level     slog.Leveler
	tee       slog.Handler
	only      func(workflow.StepInfo) bool
	onSinkErr func(ctx context.Context, out Output, err error)
}

// CaptureResult includes the step's output bytes in Output.Result. Off by
// default: a result can hold payload data.
func CaptureResult() CaptureOption { return func(c *captureConfig) { c.result = true } }

// CaptureOnlyFailures delivers only attempts that returned an error.
func CaptureOnlyFailures() CaptureOption { return func(c *captureConfig) { c.failOnly = true } }

// CaptureLevel keeps records at or above l; the default is slog.LevelInfo.
// Nil is ignored.
func CaptureLevel(l slog.Leveler) CaptureOption {
	return func(c *captureConfig) {
		if l != nil {
			c.level = l
		}
	}
}

// CaptureTee also passes every record OutputLogger receives to h (the
// app's normal log handler), whatever the capture level. Nil is ignored.
func CaptureTee(h slog.Handler) CaptureOption {
	return func(c *captureConfig) {
		if h != nil {
			c.tee = h
		}
	}
}

// CaptureOnly captures only the attempts keep returns true for, such as
// one workflow's steps; others run untouched. An attempt without a
// StepInfo is passed the zero StepInfo. Nil is ignored.
func CaptureOnly(keep func(workflow.StepInfo) bool) CaptureOption {
	return func(c *captureConfig) {
		if keep != nil {
			c.only = keep
		}
	}
}

// CaptureOnSinkError receives the sink's errors; the default drops them,
// since a sink failure must never fail the step. Nil is ignored.
func CaptureOnSinkError(fn func(ctx context.Context, out Output, err error)) CaptureOption {
	return func(c *captureConfig) {
		if fn != nil {
			c.onSinkErr = fn
		}
	}
}

// CaptureClock sets the clock Started, Finished and record times are read
// from; the default is the real clock. Nil is ignored.
func CaptureClock(clk clock.Clock) CaptureOption {
	return func(c *captureConfig) {
		if clk != nil {
			c.clk = clk
		}
	}
}

// captureKey is the ctx key of the active capture.
type captureKey struct{}

// capture collects one attempt's records.
type capture struct {
	clk   clock.Clock
	level slog.Leveler
	tee   slog.Handler

	mu   sync.Mutex
	recs []OutputRecord
}

// CaptureOutput is step middleware that captures each attempt's log
// records (written through OutputLogger) and outcome and delivers them to
// sink after the attempt, the way a scheduler sends a task's output to a
// file or by mail. It never changes the step's result. A nil sink makes it
// a no-op.
func CaptureOutput(sink OutputSink, opts ...CaptureOption) workflow.Middleware {
	cfg := captureConfig{clk: clock.Real{}, level: slog.LevelInfo, onSinkErr: func(context.Context, Output, error) {}}
	for _, o := range opts {
		o(&cfg)
	}
	return func(next workflow.Handler) workflow.Handler {
		if sink == nil {
			return next
		}
		return func(ctx context.Context, in []byte) ([]byte, error) {
			info, hasInfo := workflow.StepInfoFrom(ctx)
			if cfg.only != nil && !cfg.only(info) {
				return next(ctx, in)
			}
			cp := &capture{clk: cfg.clk, level: cfg.level, tee: cfg.tee}
			started := cfg.clk.Now()
			out, err := next(context.WithValue(ctx, captureKey{}, cp), in)
			if err == nil && cfg.failOnly {
				return out, err
			}
			o := Output{Info: info, HasInfo: hasInfo, Started: started, Finished: cfg.clk.Now(), Err: err}
			cp.mu.Lock()
			o.Records = cp.recs
			cp.mu.Unlock()
			if cfg.result && out != nil {
				o.Result = append([]byte(nil), out...)
			}
			if serr := sink.Deliver(context.WithoutCancel(ctx), o); serr != nil {
				cfg.onSinkErr(ctx, o, serr)
			}
			return out, err
		}
	}
}

// OutputLogger returns a logger whose records CaptureOutput captures for
// the step running on ctx (and passes to the CaptureTee handler). Outside
// a capture it returns fallback, or slog.Default() when fallback is nil,
// so a step can always log through it.
func OutputLogger(ctx context.Context, fallback *slog.Logger) *slog.Logger {
	if cp, ok := ctx.Value(captureKey{}).(*capture); ok {
		return slog.New(&captureHandler{cp: cp})
	}
	if fallback != nil {
		return fallback
	}
	return slog.Default()
}

// captureHandler is the slog.Handler behind OutputLogger.
type captureHandler struct {
	cp     *capture
	attrs  []slog.Attr
	groups []string
	tee    slog.Handler // cp.tee with this handler's attrs and groups applied
}

func (h *captureHandler) teeHandler() slog.Handler {
	if h.tee != nil {
		return h.tee
	}
	return h.cp.tee
}

func (h *captureHandler) Enabled(ctx context.Context, l slog.Level) bool {
	if t := h.teeHandler(); t != nil && t.Enabled(ctx, l) {
		return true
	}
	return l >= h.cp.level.Level()
}

func (h *captureHandler) Handle(ctx context.Context, r slog.Record) error {
	if t := h.teeHandler(); t != nil && t.Enabled(ctx, r.Level) {
		_ = t.Handle(ctx, r.Clone()) // the tee's failure must not lose the captured record
	}
	if r.Level < h.cp.level.Level() {
		return nil
	}
	rec := OutputRecord{Time: h.cp.clk.Now(), Level: r.Level, Message: r.Message}
	rec.Attrs = append(rec.Attrs, h.attrs...)
	prefix := strings.Join(h.groups, ".")
	r.Attrs(func(a slog.Attr) bool {
		if prefix != "" {
			a.Key = prefix + "." + a.Key
		}
		rec.Attrs = append(rec.Attrs, a)
		return true
	})
	h.cp.mu.Lock()
	h.cp.recs = append(h.cp.recs, rec)
	h.cp.mu.Unlock()
	return nil
}

func (h *captureHandler) WithAttrs(as []slog.Attr) slog.Handler {
	n := &captureHandler{cp: h.cp, groups: h.groups, attrs: append([]slog.Attr(nil), h.attrs...)}
	prefix := strings.Join(h.groups, ".")
	for _, a := range as {
		if prefix != "" {
			a.Key = prefix + "." + a.Key
		}
		n.attrs = append(n.attrs, a)
	}
	if t := h.teeHandler(); t != nil {
		n.tee = t.WithAttrs(as)
	}
	return n
}

func (h *captureHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	n := &captureHandler{cp: h.cp, attrs: h.attrs, groups: append(append([]string(nil), h.groups...), name)}
	if t := h.teeHandler(); t != nil {
		n.tee = t.WithGroup(name)
	}
	return n
}
