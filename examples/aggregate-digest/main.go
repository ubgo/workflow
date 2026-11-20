// Command aggregate-digest shows two ways to change how many runs a burst
// of work becomes. Aggregation: comments on a post become one digest mail
// per user and window instead of a mail per comment, with
// middleware.Aggregate collecting the payloads in an AggregateStore and one
// flush run taking them. Chunked fan-out: one announcement goes to every
// recipient of a list a chunk at a time with batch.EnqueueChunked, and a
// failure halfway resumes at the chunk that failed without sending anyone
// a second mail. Everything runs on the in-memory driver.
//
// It is the program behind docs/reliability/aggregate.md: the guide
// copies its regions (marked "snippet:") and task docs:snippets fails when
// they drift.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/batch"
	"github.com/ubgo/workflow/memory"
	"github.com/ubgo/workflow/middleware"
)

func main() {
	if err := run(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, "aggregate-digest:", err)
		os.Exit(1)
	}
}

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if err := digest(ctx); err != nil {
		return err
	}
	return fanOut(ctx)
}

// comment is one job's payload: a comment on a post, for one user.
type comment struct {
	User string `json:"user"`
	Text string `json:"text"`
}

// mailbox collects what the steps send, safely across goroutines.
type mailbox struct {
	mu    sync.Mutex
	mails []string
}

func (m *mailbox) send(s string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.mails = append(m.mails, s)
}

func (m *mailbox) sorted() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return slices.Sorted(slices.Values(m.mails))
}

func digest(ctx context.Context) error {
	fmt.Println("-- Aggregate comments into digests")
	box := &mailbox{}

	// snippet:aggregate
	inner := memory.New()
	store := middleware.NewMemoryAggregate() // sqlstore.AggregateStore or redis.AggregateStore across processes
	agg, err := middleware.AggregatePlugin(inner, store, middleware.AggregateConfig{
		Workflows: []string{"comment-digest"},
		Group:     func(j workflow.Job) string { return j.Labels["user"] },
		Window:    50 * time.Millisecond, // one digest per user per window
		MaxItems:  3,                     // or sooner, once three comments are waiting
	})
	if err != nil {
		return err
	}
	// The Journal plugin comes first: a retried flush step gets back the
	// comments it took.
	c, err := workflow.Use(inner, workflow.Journal(workflow.NewMemoryJournal()), agg)
	if err != nil {
		return err
	}
	// snippet:end

	// snippet:flush
	err = c.Register(workflow.Workflow{Name: "comment-digest", Steps: []workflow.Step{{Name: "mail",
		Run: func(ctx context.Context, in []byte) ([]byte, error) {
			var comments []comment // AggregateInput turned the flush's items into a JSON array
			if err := json.Unmarshal(in, &comments); err != nil {
				return nil, workflow.Permanent(err)
			}
			if len(comments) == 0 {
				return nil, nil // an earlier flush took this chunk
			}
			texts := make([]string, len(comments))
			for i, cm := range comments {
				texts[i] = cm.Text
			}
			box.send(fmt.Sprintf("digest for %s: %s", comments[0].User, strings.Join(texts, ", ")))
			return nil, nil
		}}}})
	// snippet:end
	if err != nil {
		return err
	}
	if err := c.Start(ctx); err != nil {
		return err
	}

	// snippet:enqueue
	posts := []comment{{"alice", "c1"}, {"bob", "c1"}, {"alice", "c2"}, {"alice", "c3"}, {"alice", "c4"}, {"bob", "c2"}, {"alice", "c5"}}
	for _, cm := range posts {
		payload, _ := json.Marshal(cm)
		_, err := c.Enqueue(ctx, workflow.Job{Workflow: "comment-digest", Payload: payload,
			Labels: map[string]string{"user": cm.User}})
		if err != nil {
			return err
		}
	}
	// snippet:end

	for range 400 {
		if store.Pending("alice")+store.Pending("bob") == 0 && len(box.sorted()) == 3 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(80 * time.Millisecond) // the last empty flush runs and sends nothing
	fmt.Printf("%d comments became %d mails:\n", len(posts), len(box.sorted()))
	for _, m := range box.sorted() {
		fmt.Println("  " + m)
	}
	return nil
}

func fanOut(ctx context.Context) error {
	fmt.Println("-- Fan an announcement out in chunks")
	c, err := workflow.Use(memory.New(), workflow.Journal(workflow.NewMemoryJournal()))
	if err != nil {
		return err
	}
	box := &mailbox{}
	if err := c.Register(workflow.Workflow{Name: "announce-one", Steps: []workflow.Step{{Name: "mail",
		Run: func(ctx context.Context, in []byte) ([]byte, error) {
			box.send(string(in))
			return nil, nil
		}}}}); err != nil {
		return err
	}
	var mu sync.Mutex
	paged := map[int64]int{}
	recipients := 25

	// snippet:chunked
	wf := workflow.Workflow{Name: "announce", Retry: workflow.RetryPolicy{Max: 3}, Steps: []workflow.Step{{Name: "send-all",
		Run: func(ctx context.Context, _ []byte) ([]byte, error) {
			n, err := batch.EnqueueChunked(ctx, c, "recipients",
				batch.ChunkedOptions{ChunkOptions: workflow.ChunkOptions{Size: 10, Total: int64(recipients)}},
				func(ctx context.Context, offset, size int64) ([]workflow.Job, error) {
					mu.Lock()
					paged[offset]++
					first := paged[offset] == 1
					mu.Unlock()
					if offset == 10 && first {
						return nil, errors.New("recipient database timed out") // the step is retried from this chunk
					}
					var jobs []workflow.Job
					for i := offset; i < min(offset+size, int64(recipients)); i++ {
						jobs = append(jobs, workflow.Job{Workflow: "announce-one", Payload: fmt.Appendf(nil, "user-%02d", i)})
					}
					return jobs, nil
				})
			if err != nil {
				return nil, err
			}
			return json.Marshal(n)
		}}}}
	// snippet:end
	if err := c.Register(wf); err != nil {
		return err
	}
	if err := c.Start(ctx); err != nil {
		return err
	}
	ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "announce"})
	if err != nil {
		return err
	}
	res, err := workflow.AwaitResult(ctx, c, ref, workflow.AwaitOptions{Poll: 2 * time.Millisecond})
	if err != nil {
		return err
	}
	for range 400 {
		if len(box.sorted()) >= recipients {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	time.Sleep(20 * time.Millisecond)
	sent := box.sorted()
	distinct := len(slices.Compact(slices.Clone(sent)))
	fmt.Printf("enqueued %s jobs; %d mails to %d distinct recipients\n", res, len(sent), distinct)
	mu.Lock()
	fmt.Printf("pages read: offset 0 %dx, offset 10 %dx, offset 20 %dx\n", paged[0], paged[10], paged[20])
	mu.Unlock()
	return nil
}
