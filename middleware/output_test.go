package middleware_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/memory"
	"github.com/ubgo/workflow/middleware"
)

// outputs collects delivered Outputs.
type outputs struct {
	mu  sync.Mutex
	got []middleware.Output
}

func (o *outputs) sink() middleware.OutputSink {
	return middleware.OutputSinkFunc(func(_ context.Context, out middleware.Output) error {
		o.mu.Lock()
		o.got = append(o.got, out)
		o.mu.Unlock()
		return nil
	})
}

func (o *outputs) all() []middleware.Output {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]middleware.Output(nil), o.got...)
}

var outT0 = time.Date(2026, 10, 13, 9, 0, 0, 0, time.UTC)

func stepCtx() context.Context {
	return workflow.WithStepInfo(context.Background(), workflow.StepInfo{Workflow: "report", Step: "s", RunID: "r1", Attempt: 1})
}

func TestCaptureOutputRecordsAndHidesResult(t *testing.T) {
	var o outputs
	clk := clock.NewFake(outT0)
	h := middleware.CaptureOutput(o.sink(), middleware.CaptureClock(clk))(func(ctx context.Context, in []byte) ([]byte, error) {
		l := middleware.OutputLogger(ctx, nil).With("job", "nightly").WithGroup("g")
		l.Info("rows exported", "n", 42)
		l.Debug("below the default level")
		clk.Advance(time.Second)
		return []byte("secret result"), nil
	})
	out, err := h(stepCtx(), nil)
	if err != nil || string(out) != "secret result" {
		t.Fatalf("step result changed: %q, %v", out, err)
	}
	got := o.all()
	if len(got) != 1 {
		t.Fatalf("delivered %d outputs, want 1", len(got))
	}
	g := got[0]
	if !g.HasInfo || g.Info.RunID != "r1" || g.Result != nil || g.Err != nil || g.Finished.Sub(g.Started) != time.Second {
		t.Fatalf("output = %+v", g)
	}
	if len(g.Records) != 1 || g.Records[0].Message != "rows exported" || g.Records[0].Level != slog.LevelInfo {
		t.Fatalf("records = %+v", g.Records)
	}
	keys := []string{}
	for _, a := range g.Records[0].Attrs {
		keys = append(keys, a.Key)
	}
	if strings.Join(keys, ",") != "job,g.n" {
		t.Fatalf("attr keys = %v, want job,g.n", keys)
	}
}

func TestCaptureOutputResultAndFailures(t *testing.T) {
	var o outputs
	mw := middleware.CaptureOutput(o.sink(), middleware.CaptureResult(), middleware.CaptureOnlyFailures())
	ok := mw(func(context.Context, []byte) ([]byte, error) { return []byte("r"), nil })
	bad := mw(func(context.Context, []byte) ([]byte, error) { return []byte("partial"), errBoom })
	if _, err := ok(stepCtx(), nil); err != nil {
		t.Fatal(err)
	}
	if _, err := bad(stepCtx(), nil); !errors.Is(err, errBoom) {
		t.Fatalf("err = %v", err)
	}
	got := o.all()
	if len(got) != 1 || !errors.Is(got[0].Err, errBoom) || string(got[0].Result) != "partial" {
		t.Fatalf("got %+v, want only the failure with its result", got)
	}
}

func TestCaptureOutputSinkErrorNeverFailsStep(t *testing.T) {
	var seen error
	sinkErr := errors.New("sink down")
	h := middleware.CaptureOutput(middleware.OutputSinkFunc(func(context.Context, middleware.Output) error { return sinkErr }),
		middleware.CaptureOnSinkError(func(_ context.Context, _ middleware.Output, err error) { seen = err }))(
		func(context.Context, []byte) ([]byte, error) { return []byte("ok"), nil })
	out, err := h(stepCtx(), nil)
	if err != nil || string(out) != "ok" {
		t.Fatalf("step = %q, %v; a sink error must not fail it", out, err)
	}
	if !errors.Is(seen, sinkErr) {
		t.Fatalf("CaptureOnSinkError saw %v", seen)
	}
}

func TestCaptureOnlyAndNilSink(t *testing.T) {
	var o outputs
	mw := middleware.CaptureOutput(o.sink(), middleware.CaptureOnly(func(i workflow.StepInfo) bool { return i.Workflow == "other" }))
	if _, err := mw(func(context.Context, []byte) ([]byte, error) { return nil, nil })(stepCtx(), nil); err != nil {
		t.Fatal(err)
	}
	if n := len(o.all()); n != 0 {
		t.Fatalf("CaptureOnly delivered %d outputs for a filtered step", n)
	}
	called := false
	h := middleware.CaptureOutput(nil)(func(context.Context, []byte) ([]byte, error) { called = true; return nil, nil })
	if _, err := h(stepCtx(), nil); err != nil || !called {
		t.Fatal("nil sink must pass through")
	}
}

func TestCaptureTeeAndLevel(t *testing.T) {
	var o outputs
	var teeBuf bytes.Buffer
	tee := slog.NewTextHandler(&teeBuf, &slog.HandlerOptions{Level: slog.LevelDebug})
	h := middleware.CaptureOutput(o.sink(), middleware.CaptureTee(tee), middleware.CaptureLevel(slog.LevelWarn))(
		func(ctx context.Context, _ []byte) ([]byte, error) {
			l := middleware.OutputLogger(ctx, nil)
			l.Debug("dbg")
			l.Warn("careful")
			return nil, nil
		})
	if _, err := h(stepCtx(), nil); err != nil {
		t.Fatal(err)
	}
	if got := o.all()[0].Records; len(got) != 1 || got[0].Message != "careful" {
		t.Fatalf("records = %+v, want only the warning", got)
	}
	if s := teeBuf.String(); !strings.Contains(s, "dbg") || !strings.Contains(s, "careful") {
		t.Fatalf("tee got %q, want both records", s)
	}
}

func TestOutputLoggerFallback(t *testing.T) {
	l := slog.New(slog.DiscardHandler)
	if middleware.OutputLogger(context.Background(), l) != l {
		t.Fatal("outside a capture OutputLogger must return the fallback")
	}
	if middleware.OutputLogger(context.Background(), nil) != slog.Default() {
		t.Fatal("outside a capture with no fallback OutputLogger must return slog.Default")
	}
}

func TestFileSinkReplaceAndAppend(t *testing.T) {
	dir := t.TempDir()
	out := middleware.Output{Info: workflow.StepInfo{Workflow: "w", Step: "s", RunID: "r", Attempt: 1}, HasInfo: true, Finished: outT0,
		Records: []middleware.OutputRecord{{Time: outT0, Level: slog.LevelInfo, Message: "hello"}}}
	for _, tc := range []struct {
		mode  middleware.FileMode
		lines int
	}{{middleware.FileReplace, 2}, {middleware.FileAppend, 4}} {
		path := filepath.Join(dir, "out.log")
		_ = os.Remove(path)
		s := middleware.FileSink(path, tc.mode)
		for range 2 {
			if err := s.Deliver(context.Background(), out); err != nil {
				t.Fatal(err)
			}
		}
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if n := strings.Count(string(b), "\n"); n != tc.lines {
			t.Fatalf("mode %d: %d lines, want %d:\n%s", tc.mode, n, tc.lines, b)
		}
		if fi, _ := os.Stat(path); fi.Mode().Perm() != 0o600 {
			t.Fatalf("file mode %v, want 0600", fi.Mode().Perm())
		}
	}
	if err := middleware.FileSink(filepath.Join(dir, "missing", "x.log"), middleware.FileAppend).Deliver(context.Background(), out); err == nil {
		t.Fatal("FileSink into a missing directory succeeded")
	}
}

func TestWriterSinkFormat(t *testing.T) {
	var buf bytes.Buffer
	err := middleware.WriterSink(&buf).Deliver(context.Background(), middleware.Output{
		Info: workflow.StepInfo{Workflow: "w", Step: "s", RunID: "r9", Attempt: 2}, HasInfo: true, Finished: outT0,
		Records: []middleware.OutputRecord{{Time: outT0, Level: slog.LevelWarn, Message: "m", Attrs: []slog.Attr{slog.Int("n", 1)}}},
		Result:  []byte("res"), Err: errBoom,
	})
	if err != nil {
		t.Fatal(err)
	}
	want := "== 2026-10-13T09:00:00Z w/s attempt 2 [r9] error: boom\n2026-10-13T09:00:00Z WARN m n=1\nresult: res\n"
	if buf.String() != want {
		t.Fatalf("format =\n%q\nwant\n%q", buf.String(), want)
	}
}

func TestCaptureOutputOnMemoryDriver(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nightly.log")
	c := memory.New(memory.WithMiddleware(middleware.CaptureOutput(middleware.FileSink(path, middleware.FileAppend))))
	f := workflow.NewFunc("nightly", func(ctx context.Context, in string) error {
		middleware.OutputLogger(ctx, nil).Info("exported", "for", in)
		return nil
	})
	if err := workflow.RegisterFunc(c, f); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	ref, err := f.Enqueue(ctx, c, "acme")
	if err != nil {
		t.Fatal(err)
	}
	for {
		st, err := c.Status(ctx, ref)
		if err != nil {
			t.Fatal(err)
		}
		if st == workflow.StatusSucceeded {
			break
		}
		time.Sleep(time.Millisecond)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if s := string(b); !strings.Contains(s, "nightly/func attempt 1") || !strings.Contains(s, "exported for=acme") {
		t.Fatalf("file = %q", s)
	}
}
