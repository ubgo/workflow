// Command flow-stages shows the step-layout helpers of the core package:
// Stages (groups of parallel steps, one group after another), When (a
// step that runs only on a condition and otherwise passes its input on),
// Milestone (a step enabled only while the run is in a given state) and
// the SoftTimeout middleware (a warning before the hard timeout, so a
// long step can save its progress). It runs on the in-memory driver, so
// nothing needs to be installed.
//
// It is the program behind docs/workflows/stages.md: the guide copies its
// regions (marked "snippet:") and task docs:snippets fails when they
// drift.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"sync"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/memory"
	"github.com/ubgo/workflow/middleware"
)

func main() {
	if err := run(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, "flow-stages:", err)
		os.Exit(1)
	}
}

// await is how the program waits for a run.
var await = workflow.AwaitOptions{Poll: 2 * time.Millisecond}

// log records which steps ran, in order.
type log struct {
	mu    sync.Mutex
	steps []string
}

func (l *log) add(name string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.steps = append(l.steps, name)
}

func (l *log) index(name string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Index(l.steps, name)
}

// step returns a body that logs name and outputs it as a JSON string.
func (l *log) step(name string) workflow.Handler {
	return func(context.Context, []byte) ([]byte, error) {
		l.add(name)
		return json.Marshal(name)
	}
}

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var ran log
	// snippet:stages
	steps, err := workflow.Stages(
		[]workflow.Step{{Name: "fetch-prices", Run: ran.step("fetch-prices")}, {Name: "fetch-stock", Run: ran.step("fetch-stock")}},
		[]workflow.Step{{Name: "merge", Run: ran.step("merge")}},
		[]workflow.Step{{Name: "publish-web", Run: ran.step("publish-web")}, {Name: "publish-feed", Run: ran.step("publish-feed")}},
	)
	if err != nil {
		return err
	}
	// snippet:end
	sync := workflow.Workflow{Name: "catalogue-sync", Steps: steps}

	// snippet:when
	resize := func(_ context.Context, in []byte) ([]byte, error) {
		return append([]byte("resized "), in...), nil
	}
	isLarge := func(_ context.Context, in []byte) (bool, error) {
		return len(in) > 10, nil
	}
	images := workflow.Workflow{Name: "images", Steps: []workflow.Step{
		{Name: "resize", Run: workflow.When(isLarge, resize)},
		{Name: "store", After: []string{"resize"}, Run: func(_ context.Context, in []byte) ([]byte, error) {
			return in, nil
		}},
	}}
	// snippet:end

	release := make(chan struct{})
	// snippet:milestone
	order := workflow.Workflow{Name: "order", Steps: []workflow.Step{
		{Name: "pay", Run: func(ctx context.Context, _ []byte) ([]byte, error) {
			<-release // the payment provider answers
			return nil, workflow.SetRunData(ctx, "state", "paid")
		}},
		{Name: "ship", Run: workflow.Milestone("state", "paid", func(context.Context, []byte) ([]byte, error) {
			return []byte(`"shipped"`), nil
		}, workflow.MilestoneOptions{Poll: 5 * time.Millisecond})},
	}}
	// snippet:end

	// snippet:soft
	soft := middleware.SoftTimeout(middleware.SoftTimeoutConfig{After: 20 * time.Millisecond})
	export := workflow.Workflow{Name: "export", Steps: []workflow.Step{{
		Name:    "rows",
		Timeout: 50 * time.Millisecond,
		Run: soft(func(ctx context.Context, _ []byte) ([]byte, error) {
			done, err := workflow.Memo(ctx, "progress", func(context.Context) (int, error) { return 0, nil })
			if err != nil {
				return nil, err
			}
			for done < 1_000_000 {
				select {
				case <-middleware.SoftDone(ctx):
					return json.Marshal(fmt.Sprintf("stopped early, %d rows saved for the next attempt", done))
				case <-time.After(time.Millisecond):
					done += 1000
				}
			}
			return json.Marshal("all rows")
		}),
	}}}
	// snippet:end

	c, err := workflow.Use(memory.New(), workflow.Journal(workflow.NewMemoryJournal()))
	if err != nil {
		return err
	}
	for _, wf := range []workflow.Workflow{sync, images, order, export} {
		if err := c.Register(wf); err != nil {
			return err
		}
	}
	if err := c.Start(ctx); err != nil {
		return err
	}

	fmt.Println("stages:")
	ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "catalogue-sync"})
	if err != nil {
		return err
	}
	out, err := workflow.AwaitResult(ctx, c, ref, await)
	if err != nil {
		return err
	}
	fmt.Println("  merge after both fetches:", ran.index("merge") > ran.index("fetch-prices") && ran.index("merge") > ran.index("fetch-stock"))
	fmt.Println("  publishing after merge:", ran.index("publish-web") > ran.index("merge") && ran.index("publish-feed") > ran.index("merge"))
	fmt.Println("  run result:", string(out))

	fmt.Println("when:")
	for _, img := range []string{`"icon.png"`, `"poster-large.png"`} {
		ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "images", Payload: []byte(img)})
		if err != nil {
			return err
		}
		out, err := workflow.AwaitResult(ctx, c, ref, await)
		if err != nil {
			return err
		}
		fmt.Printf("  %s -> %s\n", img, out)
	}

	fmt.Println("milestone:")
	ref, err = c.Enqueue(ctx, workflow.Job{Workflow: "order"})
	if err != nil {
		return err
	}
	time.Sleep(20 * time.Millisecond)
	st, err := c.Status(ctx, ref)
	if err != nil {
		return err
	}
	fmt.Println("  before payment the run has ended:", st.IsTerminal())
	close(release)
	out, err = workflow.AwaitResult(ctx, c, ref, await)
	if err != nil {
		return err
	}
	fmt.Println("  after payment:", string(out))

	fmt.Println("soft timeout:")
	ref, err = c.Enqueue(ctx, workflow.Job{Workflow: "export"})
	if err != nil {
		return err
	}
	out, err = workflow.AwaitResult(ctx, c, ref, await)
	if err != nil {
		return err
	}
	var msg string
	if err := json.Unmarshal(out, &msg); err != nil {
		return err
	}
	fmt.Println("  step returned before its hard timeout:", len(msg) > 0 && msg != "all rows")
	return nil
}
