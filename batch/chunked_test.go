package batch_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/batch"
	"github.com/ubgo/workflow/memory"
)

// flakyClient fails the Enqueue of one job after the inner client started
// it, the way a crash between a chunk's enqueue and its journal save looks
// to the step: the job runs, the step does not know.
type flakyClient struct {
	workflow.Client
	failAt string
	failed atomic.Bool
}

func (f *flakyClient) Enqueue(ctx context.Context, job workflow.Job) (workflow.RunRef, error) {
	ref, err := f.Client.Enqueue(ctx, job)
	if err == nil && string(job.Payload) == f.failAt && f.failed.CompareAndSwap(false, true) {
		return workflow.RunRef{}, errors.New("connection reset after the write")
	}
	return ref, err
}

// fanout is the test's world: a "send" workflow counting each recipient,
// held at a gate so its runs stay unfinished, and a "fanout" workflow
// whose step calls EnqueueChunked over n recipients.
type fanout struct {
	mu    sync.Mutex
	sent  map[string]int
	gate  chan struct{}
	pages map[int64]int
}

func (f *fanout) count() (map[string]int, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	total := 0
	for _, n := range f.sent {
		total += n
	}
	return f.sent, total
}

func startFanout(t *testing.T, n int64, opts batch.ChunkedOptions, failPage int64, wrap func(workflow.Client) workflow.Client) (*fanout, workflow.Client, workflow.RunRef) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	// Every send blocks on the gate, so leave workers for the fan-out.
	c, err := workflow.Use(memory.New(memory.WithWorkers(100)), workflow.Journal(workflow.NewMemoryJournal()))
	if err != nil {
		t.Fatal(err)
	}
	target := c
	if wrap != nil {
		target = wrap(c)
	}
	f := &fanout{sent: map[string]int{}, gate: make(chan struct{}), pages: map[int64]int{}}
	must(t, c.Register(workflow.Workflow{Name: "send", Steps: []workflow.Step{{Name: "mail", Run: func(ctx context.Context, in []byte) ([]byte, error) {
		<-f.gate
		f.mu.Lock()
		f.sent[string(in)]++
		f.mu.Unlock()
		return nil, nil
	}}}}))
	must(t, c.Register(workflow.Workflow{Name: "fanout", Retry: workflow.RetryPolicy{Max: 10, DelayFor: workflow.RetryAfterDelay}, Steps: []workflow.Step{{Name: "send-all", Run: func(ctx context.Context, _ []byte) ([]byte, error) {
		_, err := batch.EnqueueChunked(ctx, target, "recipients", opts, func(ctx context.Context, offset, size int64) ([]workflow.Job, error) {
			f.mu.Lock()
			f.pages[offset]++
			tries := f.pages[offset]
			f.mu.Unlock()
			if offset == failPage && tries == 1 {
				return nil, errors.New("recipient database timed out")
			}
			var jobs []workflow.Job
			for i := offset; i < min(offset+size, n); i++ {
				jobs = append(jobs, workflow.Job{Workflow: "send", Payload: fmt.Appendf(nil, "user-%d", i)})
			}
			return jobs, nil
		})
		return nil, err
	}}}}))
	must(t, c.Start(ctx))
	ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "fanout"})
	must(t, err)
	return f, c, ref
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

// awaitFanout waits for the fan-out run, opens the gate and waits until
// want sends are counted (or a little longer, to catch extra ones).
func awaitFanout(t *testing.T, f *fanout, c workflow.Client, ref workflow.RunRef, want int) map[string]int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	st, err := workflow.Await(ctx, c, ref, workflow.AwaitOptions{Poll: 2 * time.Millisecond})
	if err != nil || st != workflow.StatusSucceeded {
		_, rerr := workflow.AwaitResult(ctx, c, ref, workflow.AwaitOptions{})
		t.Fatalf("fan-out run ended %v, %v: %v", st, err, rerr)
	}
	close(f.gate)
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, total := f.count(); total >= want || time.Now().After(deadline) {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	time.Sleep(30 * time.Millisecond)
	sent, _ := f.count()
	return sent
}

func assertEachOnce(t *testing.T, sent map[string]int, n int) {
	t.Helper()
	if len(sent) != n {
		t.Fatalf("%d recipients reached, want %d", len(sent), n)
	}
	for r, k := range sent {
		if k != 1 {
			t.Fatalf("%s got %d messages", r, k)
		}
	}
}

// TestEnqueueChunked_ResumesWithoutDoubleSend: the page of the second
// chunk fails once, so the step is retried; it resumes at that chunk (the
// first is not paged again) and every recipient is sent exactly once.
func TestEnqueueChunked_ResumesWithoutDoubleSend(t *testing.T) {
	f, c, ref := startFanout(t, 25, batch.ChunkedOptions{ChunkOptions: workflow.ChunkOptions{Size: 10, Total: 25}}, 10, nil)
	sent := awaitFanout(t, f, c, ref, 25)
	assertEachOnce(t, sent, 25)
	if f.pages[0] != 1 || f.pages[10] != 2 || f.pages[20] != 1 {
		t.Fatalf("pages read %v; want the first chunk once, the failed one twice", f.pages)
	}
}

// TestEnqueueChunked_KeysFoldAResentChunk: the enqueue of a job fails
// after the job was started (an ambiguous failure), so the retried step
// sends its whole chunk again; the idempotency keys fold the jobs already
// started into their runs, and every recipient is still sent once.
func TestEnqueueChunked_KeysFoldAResentChunk(t *testing.T) {
	wrap := func(c workflow.Client) workflow.Client { return &flakyClient{Client: c, failAt: "user-14"} }
	f, c, ref := startFanout(t, 25, batch.ChunkedOptions{ChunkOptions: workflow.ChunkOptions{Size: 10, Total: 25}}, -1, wrap)
	sent := awaitFanout(t, f, c, ref, 25)
	assertEachOnce(t, sent, 25)
	if f.pages[10] != 2 {
		t.Fatalf("pages read %v; want the chunk at 10 sent twice", f.pages)
	}
}

// TestEnqueueChunked_UpstreamLimit: a limit of ten jobs with a fast leak;
// each chunk of ten fills it, so the next chunk waits for room (the step
// is retried after the limit's wait) and every recipient is still sent
// once.
func TestEnqueueChunked_UpstreamLimit(t *testing.T) {
	limit, err := workflow.NewUpstreamLimit(nil, workflow.BucketSpec{Capacity: 10, LeakPerSecond: 500}, nil)
	must(t, err)
	opts := batch.ChunkedOptions{ChunkOptions: workflow.ChunkOptions{Size: 10, Total: 35}, Limit: limit}
	f, c, ref := startFanout(t, 35, opts, -1, nil)
	sent := awaitFanout(t, f, c, ref, 35)
	assertEachOnce(t, sent, 35)
	if f.pages[10] < 2 {
		t.Fatalf("pages read %v; want the chunk at 10 paged again after waiting on the limit", f.pages)
	}
}

// TestEnqueueChunked_Refusals: a client without idempotency keys, a page
// returning more than a chunk, and a call outside a step are refused,
// Permanent.
func TestEnqueueChunked_Refusals(t *testing.T) {
	ctx := context.Background()
	page := func(context.Context, int64, int64) ([]workflow.Job, error) { return nil, nil }
	opts := batch.ChunkedOptions{ChunkOptions: workflow.ChunkOptions{Size: 2, MaxChunks: 3}}
	noKeys := &noKeyClient{Client: memory.New()}
	if _, err := batch.EnqueueChunked(ctx, noKeys, "x", opts, page); !errors.Is(err, workflow.ErrUnsupported) || !workflow.IsPermanent(err) {
		t.Fatalf("no CapIdempotencyKey: %v", err)
	}
	if _, err := batch.EnqueueChunked(ctx, memory.New(), "x", opts, page); !errors.Is(err, workflow.ErrInvalid) || !workflow.IsPermanent(err) {
		t.Fatalf("outside a step: %v", err)
	}
	wf := workflow.Workflow{Name: "w", Steps: []workflow.Step{{Name: "s", Run: func(ctx context.Context, _ []byte) ([]byte, error) {
		_, err := batch.EnqueueChunked(ctx, memory.New(), "x", opts, func(context.Context, int64, int64) ([]workflow.Job, error) {
			return make([]workflow.Job, 3), nil
		})
		return nil, err
	}}}}
	_, err := workflow.RunInline(workflow.WithStepInfo(ctx, workflow.StepInfo{RunID: "r1"}), wf, workflow.Job{})
	if !errors.Is(err, workflow.ErrNoJournal) {
		t.Fatalf("inline without a journal: %v", err)
	}
}

// noKeyClient hides CapIdempotencyKey.
type noKeyClient struct{ workflow.Client }

func (n *noKeyClient) Capabilities() workflow.Caps {
	return n.Client.Capabilities().Without(workflow.CapIdempotencyKey)
}
