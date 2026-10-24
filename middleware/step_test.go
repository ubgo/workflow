package middleware_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/middleware"
)

var errBoom = errors.New("boom")

func echo(_ context.Context, in []byte) ([]byte, error) { return in, nil }

func TestChain_OrderAndNil(t *testing.T) {
	var trace []string
	tag := func(name string) workflow.Middleware {
		return func(next workflow.Handler) workflow.Handler {
			return func(ctx context.Context, in []byte) ([]byte, error) {
				trace = append(trace, name)
				return next(ctx, in)
			}
		}
	}
	h := middleware.Chain(tag("a"), nil, tag("b"))(echo)
	if _, err := h(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(trace, ","); got != "a,b" {
		t.Fatalf("order = %s, want a,b", got)
	}
}

func TestChainEnqueue_OrderAndWrapClient(t *testing.T) {
	var trace []string
	tag := func(name string) workflow.EnqueueMiddleware {
		return func(next workflow.EnqueueFunc) workflow.EnqueueFunc {
			return func(ctx context.Context, job workflow.Job) (workflow.RunRef, error) {
				trace = append(trace, name)
				return next(ctx, job)
			}
		}
	}
	stub := &stubClient{caps: workflow.CapCancel}
	c := middleware.WrapClient(stub, tag("a"), tag("b"))
	if _, err := c.Enqueue(context.Background(), workflow.Job{Workflow: "w"}); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(trace, ","); got != "a,b" || len(stub.jobs) != 1 {
		t.Fatalf("order = %s, jobs = %d", got, len(stub.jobs))
	}
	if c.Capabilities() != workflow.CapCancel {
		t.Fatal("WrapClient changed the driver's capabilities")
	}
}

func TestLogging_NoPayloadInLog(t *testing.T) {
	var buf bytes.Buffer
	l := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	clk := clock.NewFake(time.Unix(0, 0))
	h := middleware.Logging(l, clk)(func(context.Context, []byte) ([]byte, error) {
		clk.Advance(2 * time.Second)
		return nil, errBoom
	})
	if _, err := h(context.Background(), []byte("secret-card-number")); !errors.Is(err, errBoom) {
		t.Fatalf("err = %v", err)
	}
	out := buf.String()
	if strings.Contains(out, "secret-card-number") {
		t.Fatal("payload leaked into the log")
	}
	for _, want := range []string{"step started", "step failed", "duration=2s", "error=boom"} {
		if !strings.Contains(out, want) {
			t.Errorf("log lacks %q:\n%s", want, out)
		}
	}
}

func TestRecover(t *testing.T) {
	var buf bytes.Buffer
	h := middleware.Recover(slog.New(slog.NewTextHandler(&buf, nil)))(func(context.Context, []byte) ([]byte, error) {
		panic("kaput")
	})
	_, err := h(context.Background(), nil)
	if !errors.Is(err, middleware.ErrPanic) || workflow.IsPermanent(err) {
		t.Fatalf("err = %v, want retryable ErrPanic", err)
	}
	if strings.Contains(err.Error(), "goroutine") {
		t.Fatal("stack leaked into the error string")
	}
	if !strings.Contains(buf.String(), "goroutine") {
		t.Fatal("stack missing from the log")
	}
}

func TestTimeout(t *testing.T) {
	h := middleware.Timeout(time.Millisecond)(func(ctx context.Context, _ []byte) ([]byte, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	})
	if _, err := h(context.Background(), nil); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v", err)
	}
	noop := middleware.Timeout(0)(func(ctx context.Context, _ []byte) ([]byte, error) {
		if _, ok := ctx.Deadline(); ok {
			return nil, errors.New("zero timeout set a deadline")
		}
		return nil, nil
	})
	if _, err := noop(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
}

func TestFailOn(t *testing.T) {
	mw := middleware.FailOn(func(err error) bool { return errors.Is(err, errBoom) })
	fail := func(e error) workflow.Handler {
		return func(context.Context, []byte) ([]byte, error) { return nil, e }
	}
	if _, err := mw(fail(errBoom))(context.Background(), nil); !workflow.IsPermanent(err) || !errors.Is(err, errBoom) {
		t.Fatalf("matched error not permanent: %v", err)
	}
	other := errors.New("other")
	if _, err := mw(fail(other))(context.Background(), nil); workflow.IsPermanent(err) {
		t.Fatal("unmatched error made permanent")
	}
	if _, err := mw(echo)(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
}

func TestSkip(t *testing.T) {
	ran := false
	h := middleware.Skip(func(_ context.Context, in []byte) bool { return string(in) == "skip" })(
		func(_ context.Context, in []byte) ([]byte, error) { ran = true; return in, nil })
	out, err := h(context.Background(), []byte("skip"))
	if ran || out != nil || err != nil {
		t.Fatalf("skipped step ran=%v out=%q err=%v", ran, out, err)
	}
	if out, _ := h(context.Background(), []byte("go")); !ran || string(out) != "go" {
		t.Fatal("unskipped step did not run")
	}
}

func TestThrottleErrors(t *testing.T) {
	clk := clock.NewFake(time.Unix(0, 0))
	mw, err := middleware.ThrottleErrors(middleware.ThrottleConfig{
		Clock: clk, MaxErrors: 2, Window: time.Minute, Cooldown: 5 * time.Minute,
		Key: func(_ context.Context, in []byte) string { return string(in) },
	})
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	failing := true
	h := mw(func(context.Context, []byte) ([]byte, error) {
		calls++
		if failing {
			return nil, errBoom
		}
		return nil, nil
	})
	ctx := context.Background()
	_, _ = h(ctx, []byte("api"))
	_, _ = h(ctx, []byte("api"))
	if _, err := h(ctx, []byte("api")); !errors.Is(err, middleware.ErrThrottled) || calls != 2 {
		t.Fatalf("circuit not open after 2 errors: err=%v calls=%d", err, calls)
	}
	if _, err := h(ctx, []byte("other")); !errors.Is(err, errBoom) {
		t.Fatalf("another key was throttled: %v", err)
	}
	clk.Advance(5 * time.Minute)
	failing = false
	if _, err := h(ctx, []byte("api")); err != nil {
		t.Fatalf("circuit still open after cooldown: %v", err)
	}

	// Errors spread wider than the window never open the circuit.
	failing = true
	_, _ = h(ctx, []byte("slow"))
	clk.Advance(2 * time.Minute)
	if _, err := h(ctx, []byte("slow")); !errors.Is(err, errBoom) {
		t.Fatalf("an error outside the window opened the circuit: %v", err)
	}
	_, _ = h(ctx, []byte("slow")) // second error inside the fresh window
	if _, err := h(ctx, []byte("slow")); !errors.Is(err, middleware.ErrThrottled) {
		t.Fatalf("two errors inside a fresh window should open: %v", err)
	}

	// Permanent errors are not counted.
	perm := mw(func(context.Context, []byte) ([]byte, error) { return nil, workflow.Permanent(errBoom) })
	for range 3 {
		if _, err := perm(ctx, []byte("perm")); errors.Is(err, middleware.ErrThrottled) {
			t.Fatal("permanent errors opened the circuit")
		}
	}

	if _, err := middleware.ThrottleErrors(middleware.ThrottleConfig{}); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("zero config accepted: %v", err)
	}
}

func TestLogging_StepInfoAttributes(t *testing.T) {
	var buf bytes.Buffer
	l := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	ctx := workflow.WithStepInfo(context.Background(), workflow.StepInfo{
		Workflow: "orders", Step: "charge", RunID: "r42", Attempt: 3, Labels: map[string]string{"tenant": "acme"},
	})
	if _, err := middleware.Logging(l, clock.NewFake(time.Unix(0, 0)))(echo)(ctx, []byte("secret")); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	for _, want := range []string{"workflow=orders", "step=charge", "run_id=r42", "attempt=3"} {
		if !strings.Contains(out, want) {
			t.Errorf("log lacks %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "secret") || strings.Contains(out, "acme") {
		t.Fatalf("payload or labels leaked:\n%s", out)
	}
}

func TestThrottleErrors_DefaultKeyIsWorkflow(t *testing.T) {
	mw, err := middleware.ThrottleErrors(middleware.ThrottleConfig{
		Clock: clock.NewFake(time.Unix(0, 0)), MaxErrors: 1, Window: time.Minute, Cooldown: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	h := mw(func(context.Context, []byte) ([]byte, error) { return nil, errBoom })
	a := workflow.WithStepInfo(context.Background(), workflow.StepInfo{Workflow: "a"})
	b := workflow.WithStepInfo(context.Background(), workflow.StepInfo{Workflow: "b"})
	_, _ = h(a, nil)
	if _, err := h(a, nil); !errors.Is(err, middleware.ErrThrottled) {
		t.Fatalf("workflow a not throttled: %v", err)
	}
	if _, err := h(b, nil); !errors.Is(err, errBoom) {
		t.Fatalf("workflow b throttled by a's errors: %v", err)
	}
}
