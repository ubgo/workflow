// Command chain-edits shows a chain of jobs (Job.Chain) on the in-memory
// driver and a running step changing what comes after it: a step that
// finds more work prepends and appends jobs, a step that fails and is
// retried adds its jobs once, a failed run ends the chain, and Hatchet
// refuses a chain because a run's shape is fixed when it registers.
//
// It is the program behind docs/workflows/chains.md.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/hatchet"
	"github.com/ubgo/workflow/memory"
)

func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// trail records which jobs ran, in order.
type trail struct {
	mu  sync.Mutex
	ran []string
}

func (t *trail) add(s string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.ran = append(t.ran, s)
}

func (t *trail) take() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	s := strings.Join(t.ran, " -> ")
	t.ran = nil
	return s
}

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context, out io.Writer) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	c := memory.New()

	var ran trail
	// record is a one-step workflow that notes its payload when it runs.
	record := func(name string) workflow.Workflow {
		return workflow.Workflow{Name: name, Steps: []workflow.Step{{Name: "run", Run: func(_ context.Context, in []byte) ([]byte, error) {
			ran.add(name + "(" + string(in) + ")")
			return nil, nil
		}}}}
	}
	var importAttempts atomic.Int64
	// snippet:edit
	importFiles := workflow.Workflow{
		Name:  "import-files",
		Retry: workflow.RetryPolicy{Max: 1},
		Steps: []workflow.Step{{Name: "scan", Run: func(ctx context.Context, in []byte) ([]byte, error) {
			ran.add("import-files(" + string(in) + ")")
			// The scan found a second file: import it next, before the
			// rest of the chain, and rebuild the search index at the end.
			if err := workflow.PrependToChain(ctx, workflow.Job{Workflow: "import-file", Payload: []byte("b.csv")}); err != nil {
				return nil, err
			}
			if err := workflow.AppendToChain(ctx, workflow.Job{Workflow: "reindex", Payload: []byte("all")}); err != nil {
				return nil, err
			}
			if importAttempts.Add(1) == 1 {
				// This attempt fails, so its edits are dropped; the retry
				// makes them again and they apply once.
				return nil, errors.New("storage timeout")
			}
			return nil, nil
		}}},
	}
	// snippet:end
	failing := workflow.Workflow{Name: "charge", Steps: []workflow.Step{{Name: "run", Run: func(_ context.Context, in []byte) ([]byte, error) {
		ran.add("charge(" + string(in) + ")")
		return nil, workflow.Permanent(errors.New("card declined"))
	}}}}
	for _, wf := range []workflow.Workflow{importFiles, record("import-file"), record("notify"), record("reindex"), failing} {
		if err := c.Register(wf); err != nil {
			return err
		}
	}
	ends := make(chan workflow.RunEvent, 16)
	if err := c.OnRunFinished(func(_ context.Context, ev workflow.RunEvent) { ends <- ev }); err != nil {
		return err
	}
	if err := c.Start(ctx); err != nil {
		return err
	}
	defer c.Stop()

	// snippet:chain
	_, err := c.Enqueue(ctx, workflow.Job{
		Workflow: "import-file",
		Payload:  []byte("a.csv"),
		// Each job runs after the one before it succeeds.
		Chain: []workflow.Job{
			{Workflow: "notify", Payload: []byte("imported")},
		},
	})
	if err != nil {
		return err
	}
	// snippet:end
	waitN(ends, 2)
	fmt.Fprintf(out, "chain: %s\n", ran.take())

	if _, err := c.Enqueue(ctx, workflow.Job{
		Workflow: "import-files",
		Payload:  []byte("inbox"),
		Chain:    []workflow.Job{{Workflow: "notify", Payload: []byte("imported")}},
	}); err != nil {
		return err
	}
	waitN(ends, 4)
	fmt.Fprintf(out, "edited while running (scan tried %d times): %s\n", importAttempts.Load(), ran.take())

	// snippet:failed
	if _, err := c.Enqueue(ctx, workflow.Job{
		Workflow: "charge",
		Payload:  []byte("o-1"),
		Chain:    []workflow.Job{{Workflow: "notify", Payload: []byte("charged")}},
	}); err != nil {
		return err
	}
	// snippet:end
	ev := <-ends
	fmt.Fprintf(out, "a failed run ends the chain: %s (%s)\n", ran.take(), ev.Status)

	// snippet:outside
	err = workflow.AppendToChain(ctx, workflow.Job{Workflow: "notify"})
	// snippet:end
	var unsupported *workflow.UnsupportedError
	fmt.Fprintf(out, "AppendToChain outside a step: %v, missing %s\n", errors.As(err, &unsupported), unsupported.Missing)

	// snippet:hatchet
	job := workflow.Job{Workflow: "import-file", Chain: []workflow.Job{{Workflow: "notify"}}}
	err = workflow.CheckJob(hatchet.New(hatchet.Config{}).Capabilities(), job)
	// snippet:end
	fmt.Fprintf(out, "on hatchet: ErrUnsupported %v (%v)\n", errors.Is(err, workflow.ErrUnsupported), err)
	return nil
}

// waitN takes n run ends.
func waitN(ends <-chan workflow.RunEvent, n int) {
	for range n {
		<-ends
	}
}
