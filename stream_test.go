package workflow_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/memory"
)

var streamT0 = time.Date(2026, 11, 20, 9, 0, 0, 0, time.UTC)

// emitWorkflow emits each word of its payload as a chunk.
func emitWorkflow(name string) workflow.Workflow {
	return workflow.Workflow{Name: name, Steps: []workflow.Step{{Name: "answer", Run: func(ctx context.Context, in []byte) ([]byte, error) {
		buf := make([]byte, 0, 8)
		for _, w := range []string{"hello", " ", "world"} {
			buf = append(buf[:0], w...)
			if err := workflow.Emit(ctx, buf); err != nil {
				return nil, err
			}
		}
		return []byte("hello world"), nil
	}}}}
}

func chunkTexts(cs []workflow.StreamChunk) string {
	out := ""
	for _, c := range cs {
		out += fmt.Sprintf("[%d %s#%d.%d %q]", c.Pos, c.Step, c.Attempt, c.Seq, c.Data)
	}
	return out
}

// TestEmit_WithoutStreamIsUnsupported: no plugin, no middleware, or a
// nil sink, and Emit refuses instead of dropping the chunk.
func TestEmit_WithoutStreamIsUnsupported(t *testing.T) {
	if err := workflow.Emit(context.Background(), []byte("x")); !errors.Is(err, workflow.ErrUnsupported) {
		t.Fatalf("outside a step: %v, want ErrUnsupported", err)
	}
	for name, opts := range map[string][]workflow.InlineOption{
		"no middleware": nil,
		"nil sink":      {workflow.InlineMiddleware(workflow.StreamMiddleware(nil))},
	} {
		_, err := workflow.RunInline(context.Background(), emitWorkflow("e"), workflow.Job{}, opts...)
		if !errors.Is(err, workflow.ErrUnsupported) {
			t.Errorf("%s: %v, want ErrUnsupported", name, err)
		}
	}
	p := workflow.Stream(nil)
	if mw := p.(workflow.StepMiddlewareProvider).StepMiddleware(); len(mw) != 0 {
		t.Errorf("Stream(nil) adds %d middleware, want none", len(mw))
	}
}

// TestEmit_InlineNumbersAndCopies: chunks get a run-wide Pos and a
// per-attempt Seq, and Emit copies the caller's buffer (the step reuses
// one, so without the copy every chunk would read "world").
func TestEmit_InlineNumbersAndCopies(t *testing.T) {
	ms := workflow.NewMemoryStream(workflow.MemoryStreamOptions{Clock: clock.NewFake(streamT0)})
	_, err := workflow.RunInline(context.Background(), emitWorkflow("e"), workflow.Job{}, workflow.InlineMiddleware(workflow.StreamMiddleware(ms)))
	if err != nil {
		t.Fatal(err)
	}
	got, err := ms.Read(context.Background(), workflow.InlineRunID, 0)
	if err != nil {
		t.Fatal(err)
	}
	if want := `[1 answer#1.1 "hello"][2 answer#1.2 " "][3 answer#1.3 "world"]`; chunkTexts(got) != want {
		t.Fatalf("chunks %s, want %s", chunkTexts(got), want)
	}
	after, _ := ms.Read(context.Background(), workflow.InlineRunID, 2)
	if len(after) != 1 || after[0].Pos != 3 || !after[0].At.Equal(streamT0) {
		t.Fatalf("Read after 2: %+v", after)
	}
	got[0].Data[0] = 'X' // Read returns copies
	again, _ := ms.Read(context.Background(), workflow.InlineRunID, 0)
	if string(again[0].Data) != "hello" {
		t.Fatalf("Read handed out the store's own bytes: %q", again[0].Data)
	}
}

// TestStream_PluginOnMemory: the plugin on the memory driver, with
// StreamCloseOnFinish closing the run's stream when it ends; a closed
// stream refuses writes, subscribes closed, stays readable for Retain
// and is dropped after it.
func TestStream_PluginOnMemory(t *testing.T) {
	clk := clock.NewFake(streamT0)
	ms := workflow.NewMemoryStream(workflow.MemoryStreamOptions{Clock: clk, Retain: time.Minute})
	c, err := workflow.Use(memory.New(), workflow.Stream(ms, workflow.StreamCloseOnFinish()))
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Register(emitWorkflow("answer")); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "answer"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := workflow.AwaitResult(ctx, c, ref, workflow.AwaitOptions{Poll: time.Millisecond}); err != nil {
		t.Fatal(err)
	}
	// The run-finished observer runs after the status turns terminal.
	closed := false
	for i := 0; i < 1000 && !closed; i++ {
		sub, stop := ms.Subscribe(ctx, ref.RunID)
		select {
		case _, open := <-sub:
			closed = !open // a closed run's subscription comes back closed
		default:
			time.Sleep(time.Millisecond)
		}
		stop()
	}
	if !closed {
		t.Fatal("the stream was not closed when the run finished")
	}
	got, _ := ms.Read(ctx, ref.RunID, 0)
	if want := `[1 answer#1.1 "hello"][2 answer#1.2 " "][3 answer#1.3 "world"]`; chunkTexts(got) != want {
		t.Fatalf("chunks %s, want %s", chunkTexts(got), want)
	}
	if err := ms.Write(ctx, ref.RunID, "answer", 4, []byte("late")); !errors.Is(err, workflow.ErrWrongState) {
		t.Fatalf("write after Close: %v, want ErrWrongState", err)
	}
	clk.Advance(time.Minute)
	if runs := ms.Runs(); len(runs) != 0 {
		t.Fatalf("runs after Retain: %v, want none", runs)
	}
}

// TestStream_CloseOnFinishNeedsRunNotifier: the closing variant observes
// run ends, so Use refuses a client without RunNotifier; the plain
// plugin does not.
func TestStream_CloseOnFinishNeedsRunNotifier(t *testing.T) {
	bare := struct{ workflow.Client }{}
	if _, err := workflow.Use(bare, workflow.Stream(workflow.NewMemoryStream(workflow.MemoryStreamOptions{}), workflow.StreamCloseOnFinish())); !errors.Is(err, workflow.ErrUnsupported) {
		t.Fatalf("CloseOnFinish on a client without RunNotifier: %v, want ErrUnsupported", err)
	}
	if _, err := workflow.Use(bare, workflow.Stream(workflow.NewMemoryStream(workflow.MemoryStreamOptions{}))); err != nil {
		t.Fatalf("plain plugin: %v", err)
	}
}

// TestMemoryStream_Bounds: every documented limit, each checked so that
// removing it fails the case.
func TestMemoryStream_Bounds(t *testing.T) {
	ctx := context.Background()
	write := func(ms *workflow.MemoryStream, run string, data ...string) error {
		for i, d := range data {
			if err := ms.Write(ctx, workflow.RunID(run), "s", int64(i+1), []byte(d)); err != nil {
				return err
			}
		}
		return nil
	}
	cases := []struct {
		name  string
		opt   workflow.MemoryStreamOptions
		do    func(ms *workflow.MemoryStream) error
		want  error
		check func(t *testing.T, ms *workflow.MemoryStream)
	}{
		{name: "chunk over MaxChunk is refused", opt: workflow.MemoryStreamOptions{MaxChunk: 3},
			do: func(ms *workflow.MemoryStream) error { return write(ms, "r", "abcd") }, want: workflow.ErrInvalid},
		{name: "empty run ID is refused",
			do: func(ms *workflow.MemoryStream) error { return write(ms, "", "a") }, want: workflow.ErrInvalid},
		{name: "MaxRunChunks drops the oldest", opt: workflow.MemoryStreamOptions{MaxRunChunks: 2},
			do: func(ms *workflow.MemoryStream) error { return write(ms, "r", "a", "b", "c") },
			check: func(t *testing.T, ms *workflow.MemoryStream) {
				got, _ := ms.Read(ctx, "r", 0)
				if want := `[2 s#0.2 "b"][3 s#0.3 "c"]`; chunkTexts(got) != want {
					t.Errorf("chunks %s, want %s", chunkTexts(got), want)
				}
			}},
		{name: "MaxRunBytes drops the oldest", opt: workflow.MemoryStreamOptions{MaxRunBytes: 4},
			do: func(ms *workflow.MemoryStream) error { return write(ms, "r", "ab", "cd", "e") },
			check: func(t *testing.T, ms *workflow.MemoryStream) {
				got, _ := ms.Read(ctx, "r", 0)
				if want := `[2 s#0.2 "cd"][3 s#0.3 "e"]`; chunkTexts(got) != want {
					t.Errorf("chunks %s, want %s", chunkTexts(got), want)
				}
			}},
		{name: "MaxRuns full of open runs refuses a new run", opt: workflow.MemoryStreamOptions{MaxRuns: 1},
			do: func(ms *workflow.MemoryStream) error {
				if err := write(ms, "a", "x"); err != nil {
					return err
				}
				return write(ms, "b", "y")
			}, want: workflow.ErrStreamFull},
		{name: "MaxRuns evicts a closed run for a new one", opt: workflow.MemoryStreamOptions{MaxRuns: 1},
			do: func(ms *workflow.MemoryStream) error {
				if err := write(ms, "a", "x"); err != nil {
					return err
				}
				if err := ms.Close(ctx, "a"); err != nil {
					return err
				}
				return write(ms, "b", "y")
			},
			check: func(t *testing.T, ms *workflow.MemoryStream) {
				if runs := ms.Runs(); len(runs) != 1 || runs[0] != "b" {
					t.Errorf("runs %v, want [b]", runs)
				}
			}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ms := workflow.NewMemoryStream(tc.opt)
			err := tc.do(ms)
			if tc.want == nil && err != nil {
				t.Fatalf("err %v", err)
			}
			if tc.want != nil && !errors.Is(err, tc.want) {
				t.Fatalf("err %v, want %v", err, tc.want)
			}
			if tc.check != nil {
				tc.check(t, ms)
			}
		})
	}
}

// TestMemoryStream_TTL: a run nothing was written to for TTL is dropped,
// closed or not, and its subscribers' channels close.
func TestMemoryStream_TTL(t *testing.T) {
	clk := clock.NewFake(streamT0)
	ms := workflow.NewMemoryStream(workflow.MemoryStreamOptions{Clock: clk, TTL: time.Hour})
	ctx := context.Background()
	if err := ms.Write(ctx, "r", "s", 1, []byte("a")); err != nil {
		t.Fatal(err)
	}
	sub, _ := ms.Subscribe(ctx, "r")
	clk.Advance(59 * time.Minute)
	if len(ms.Runs()) != 1 {
		t.Fatal("dropped before the TTL")
	}
	clk.Advance(time.Minute)
	if len(ms.Runs()) != 0 {
		t.Fatal("kept past the TTL")
	}
	if _, open := <-sub; open {
		t.Fatal("subscriber channel still open after the run was dropped")
	}
}

// TestMemoryStream_Subscribe: a subscriber sees new chunks; one that
// falls Buffer behind is closed rather than blocking the writer; ctx and
// the stop function end a subscription; a subscription made before the
// first write sees it.
func TestMemoryStream_Subscribe(t *testing.T) {
	ms := workflow.NewMemoryStream(workflow.MemoryStreamOptions{Buffer: 2})
	ctx := context.Background()
	early, stopEarly := ms.Subscribe(ctx, "r")
	defer stopEarly()
	slowCtx, cancelSlow := context.WithCancel(ctx)
	defer cancelSlow()
	for i := range 3 {
		if err := ms.Write(ctx, "r", "s", int64(i+1), []byte{byte('a' + i)}); err != nil {
			t.Fatal(err)
		}
	}
	// early has Buffer 2 and was never read: the third write closed it.
	n := 0
	for range early {
		n++
	}
	if n != 2 {
		t.Fatalf("lagging subscriber got %d chunks before closing, want 2", n)
	}
	live, stop := ms.Subscribe(slowCtx, "r")
	if err := ms.Write(ctx, "r", "s", 4, []byte("d")); err != nil {
		t.Fatal(err)
	}
	if c := <-live; c.Pos != 4 || string(c.Data) != "d" {
		t.Fatalf("live got %+v", c)
	}
	cancelSlow()
	for range live { // closes once ctx ends
	}
	stop() // a second end is harmless
	_, stop2 := ms.Subscribe(ctx, "r")
	stop2()
	stop2()
}

// keepSink keeps the slices it is given, as a store that queues chunks
// for a later batch write might.
type keepSink struct{ got [][]byte }

func (k *keepSink) Write(_ context.Context, _ workflow.RunID, _ string, _ int64, chunk []byte) error {
	k.got = append(k.got, chunk)
	return nil
}

// TestEmit_CopiesForAnySink: the step reuses one buffer; a sink that
// keeps the slice still sees each chunk as emitted.
func TestEmit_CopiesForAnySink(t *testing.T) {
	k := &keepSink{}
	if _, err := workflow.RunInline(context.Background(), emitWorkflow("e"), workflow.Job{}, workflow.InlineMiddleware(workflow.StreamMiddleware(k))); err != nil {
		t.Fatal(err)
	}
	if got := fmt.Sprintf("%q", k.got); got != `["hello" " " "world"]` {
		t.Fatalf("sink kept %s", got)
	}
}
