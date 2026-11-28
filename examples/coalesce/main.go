// Command coalesce runs a search reindex that must run once more for
// changes arriving while it runs, and only once however many arrive: a
// workflow with Coalesce keeps one run pending behind the running one, and
// further enqueues fold into it, the newest payload kept
// (CoalesceKeepLast).
//
// It is the program behind docs/reliability/coalesce.md: the guide copies
// its regions (marked "snippet:") and task docs:snippets fails when they
// drift.
package main

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/memory"
)

func main() {
	if err := run(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, "coalesce:", err)
		os.Exit(1)
	}
}

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context) error {
	started := make(chan struct{}, 4)
	release := make(chan struct{})
	var mu sync.Mutex
	var runs []string
	reindex := func(_ context.Context, in []byte) ([]byte, error) {
		mu.Lock()
		runs = append(runs, string(in))
		first := len(runs) == 1
		mu.Unlock()
		started <- struct{}{}
		if first {
			<-release // the first reindex runs while changes keep arriving
		}
		return nil, nil
	}
	c := memory.New()
	// snippet:workflow
	if err := c.Register(workflow.Workflow{
		Name:     "reindex",
		Steps:    []workflow.Step{{Name: "rebuild", Run: reindex}},
		Coalesce: &workflow.Coalesce{Payload: workflow.CoalesceKeepLast},
	}); err != nil {
		return err
	}
	// snippet:end
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if err := c.Start(ctx); err != nil {
		return err
	}
	// snippet:enqueue
	job := func(version string) workflow.Job {
		return workflow.Job{Workflow: "reindex", IdempotencyKey: "reindex:catalog", Payload: []byte(version)}
	}
	first, err := c.Enqueue(ctx, job("v1"))
	if err != nil {
		return err
	}
	<-started // v1 is running
	var pending workflow.RunRef
	for _, v := range []string{"v2", "v3", "v4"} { // changes arriving meanwhile
		if pending, err = c.Enqueue(ctx, job(v)); err != nil {
			return err
		}
	}
	st, err := c.Status(ctx, pending)
	if err != nil {
		return err
	}
	fmt.Println("while v1 runs, the pending run is:", st)
	// snippet:end
	close(release)
	for _, ref := range []workflow.RunRef{first, pending} {
		if _, err := workflow.Await(ctx, c, ref, workflow.AwaitOptions{Poll: 5 * time.Millisecond}); err != nil {
			return err
		}
	}
	mu.Lock()
	defer mu.Unlock()
	fmt.Println("runs:", len(runs), "payloads:", runs)
	return nil
}
