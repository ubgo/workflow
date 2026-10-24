// Command flow-memo shows the flow helpers that keep work inside a step
// from repeating when the step is retried: Memo, MapEach, Loop and
// ProcessChunks over the Journal plugin, run-scoped data with SetRunData
// and RunData, and an exclusive choice built with Choice. Everything runs
// on the in-memory driver, so nothing needs to be installed.
//
// It is the program behind docs/workflows/memoisation.md and
// docs/workflows/branching.md: the guides copy its regions (marked
// "snippet:") and task docs:snippets fails when they drift.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/memory"
)

func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "flow-memo:", err)
		os.Exit(1)
	}
}

// await is how the program waits for a run: poll its status until it is
// terminal, then read its result.
var await = workflow.AwaitOptions{Poll: 2 * time.Millisecond}

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context, out io.Writer) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// snippet:client
	journal := workflow.NewMemoryJournal()
	c, err := workflow.Use(memory.New(), workflow.Journal(journal))
	if err != nil {
		return err
	}
	// snippet:end

	var calls counter
	wfs := []workflow.Workflow{billWorkflow(&calls), mapWorkflow(&calls), loopWorkflow(&calls), chunkWorkflow(&calls), dataWorkflow()}
	choice, err := choiceWorkflow()
	if err != nil {
		return err
	}
	for _, wf := range append(wfs, choice) {
		if err := c.Register(wf); err != nil {
			return err
		}
	}
	if err := c.Start(ctx); err != nil {
		return err
	}

	sections := []func(context.Context, workflow.Client, io.Writer, *counter) error{memo, mapEach, loop, chunks, runData, choose}
	for _, s := range sections {
		if err := s(ctx, c, out, &calls); err != nil {
			return err
		}
	}
	noJournal(ctx, out)

	// The plugin forgets a run's entries when the run finishes.
	for range 500 {
		if len(journal.Runs()) == 0 {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	fmt.Fprintf(out, "journal:\n  runs holding entries after every run finished: %d\n", len(journal.Runs()))
	return nil
}

// counter counts calls by name, safely across goroutines.
type counter struct {
	mu sync.Mutex
	n  map[string]int
}

// add counts one call of name and returns how many there have been.
func (c *counter) add(name string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.n == nil {
		c.n = map[string]int{}
	}
	c.n[name]++
	return c.n[name]
}

// get returns how many calls of name there have been.
func (c *counter) get(name string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.n[name]
}

// billWorkflow charges a card and then mails a receipt. The mail fails on
// the first attempt, so the step is retried; Memo keeps the charge from
// running twice.
func billWorkflow(calls *counter) workflow.Workflow {
	// snippet:memo
	return workflow.Workflow{
		Name:  "bill",
		Retry: workflow.RetryPolicy{Max: 2},
		Steps: []workflow.Step{{Name: "charge", Run: func(ctx context.Context, _ []byte) ([]byte, error) {
			chargeID, err := workflow.Memo(ctx, "charge-card", func(ctx context.Context) (string, error) {
				calls.add("charge")
				return "ch_1", nil // the payment provider's charge ID
			})
			if err != nil {
				return nil, err
			}
			if calls.add("mail") == 1 {
				return nil, errors.New("mail server timed out") // retried; the charge is not repeated
			}
			return []byte(chargeID), nil
		}}},
	}
	// snippet:end
}

func memo(ctx context.Context, c workflow.Client, out io.Writer, calls *counter) error {
	res, err := enqueueAndWait(ctx, c, workflow.Job{Workflow: "bill"})
	if err != nil {
		return err
	}
	fmt.Fprintln(out, "memo:")
	fmt.Fprintf(out, "  result %s, charge ran %d time(s), step ran %d time(s)\n", res, calls.get("charge"), calls.get("mail"))
	return nil
}

// mapWorkflow resizes three images; the second fails on its first try.
func mapWorkflow(calls *counter) workflow.Workflow {
	return workflow.Workflow{
		Name:  "resize-all",
		Retry: workflow.RetryPolicy{Max: 2},
		Steps: []workflow.Step{{Name: "resize", Run: func(ctx context.Context, in []byte) ([]byte, error) {
			var images []string
			if err := json.Unmarshal(in, &images); err != nil {
				return nil, workflow.Permanent(err)
			}
			// snippet:map
			sizes, err := workflow.MapEach(ctx, "resize", images, func(ctx context.Context, i int, image string) (string, error) {
				if calls.add("resize "+image) == 1 && image == "b.png" {
					return "", errors.New("image service busy") // only this item runs again
				}
				return image + "@2x", nil
			}, workflow.MapConcurrency(2))
			// snippet:end
			if err != nil {
				return nil, err
			}
			return json.Marshal(sizes)
		}}},
	}
}

func mapEach(ctx context.Context, c workflow.Client, out io.Writer, calls *counter) error {
	res, err := enqueueAndWait(ctx, c, workflow.Job{Workflow: "resize-all", Payload: []byte(`["a.png","b.png","c.png"]`)})
	if err != nil {
		return err
	}
	fmt.Fprintln(out, "map:")
	fmt.Fprintf(out, "  result %s\n", res)
	fmt.Fprintf(out, "  calls: a.png %d, b.png %d, c.png %d\n", calls.get("resize a.png"), calls.get("resize b.png"), calls.get("resize c.png"))
	return nil
}

// loopWorkflow walks a paged API by cursor; page 2 fails once.
func loopWorkflow(calls *counter) workflow.Workflow {
	return workflow.Workflow{
		Name:  "sync-pages",
		Retry: workflow.RetryPolicy{Max: 2},
		Steps: []workflow.Step{{Name: "sync", Run: func(ctx context.Context, _ []byte) ([]byte, error) {
			// snippet:loop
			cursor, err := workflow.Loop(ctx, "pages", 0, 10, func(ctx context.Context, i int, cursor int) (int, bool, error) {
				if calls.add(fmt.Sprintf("page %d", cursor)) == 1 && cursor == 2 {
					return cursor, false, errors.New("API returned 502") // pages 0 and 1 are not fetched again
				}
				next := cursor + 1
				return next, next == 4, nil // done after page 3
			})
			// snippet:end
			if err != nil {
				return nil, err
			}
			return json.Marshal(cursor)
		}}},
	}
}

func loop(ctx context.Context, c workflow.Client, out io.Writer, calls *counter) error {
	res, err := enqueueAndWait(ctx, c, workflow.Job{Workflow: "sync-pages"})
	if err != nil {
		return err
	}
	fmt.Fprintln(out, "loop:")
	fmt.Fprintf(out, "  final cursor %s\n", res)
	fmt.Fprintf(out, "  fetches: page 0 %d, page 1 %d, page 2 %d, page 3 %d\n", calls.get("page 0"), calls.get("page 1"), calls.get("page 2"), calls.get("page 3"))
	return nil
}

// chunkWorkflow exports 25 rows in chunks of 10; the chunk at offset 10
// fails once.
func chunkWorkflow(calls *counter) workflow.Workflow {
	return workflow.Workflow{
		Name:  "export",
		Retry: workflow.RetryPolicy{Max: 2},
		Steps: []workflow.Step{{Name: "rows", Run: func(ctx context.Context, _ []byte) ([]byte, error) {
			// snippet:chunks
			n, err := workflow.ProcessChunks(ctx, "rows", workflow.ChunkOptions{Total: 25, Size: 10},
				func(ctx context.Context, offset, size int64) (int64, error) {
					if calls.add(fmt.Sprintf("chunk %d", offset)) == 1 && offset == 10 {
						return 0, errors.New("disk full") // chunk 0 is not written again
					}
					return min(size, 25-offset), nil // rows written; fewer than size ends the loop
				})
			// snippet:end
			if err != nil {
				return nil, err
			}
			return json.Marshal(n)
		}}},
	}
}

func chunks(ctx context.Context, c workflow.Client, out io.Writer, calls *counter) error {
	ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "export"})
	if err != nil {
		return err
	}
	res, err := workflow.AwaitResult(ctx, c, ref, await)
	if err != nil {
		return err
	}
	fmt.Fprintln(out, "chunks:")
	fmt.Fprintf(out, "  rows %s; calls: offset 0 %d, offset 10 %d, offset 20 %d\n", res, calls.get("chunk 0"), calls.get("chunk 10"), calls.get("chunk 20"))
	// snippet:progress
	page, err := c.(workflow.RunQuerier).Runs(ctx, workflow.RunQuery{Ref: ref, Limit: 1})
	if err != nil {
		return err
	}
	p := page.Runs[0].Progress // the latest report: Percent, Message, and ChunkProgress as JSON in Data
	var cp workflow.ChunkProgress
	if err := json.Unmarshal(p.Data, &cp); err != nil {
		return err
	}
	fmt.Fprintf(out, "  progress %.0f%% %q, done %d of %d\n", p.Percent, p.Message, cp.Done, cp.Total)
	// snippet:end
	return nil
}

// dataWorkflow sets a value in its first step and reads it two steps
// later, without threading it through the step in between.
func dataWorkflow() workflow.Workflow {
	return workflow.Workflow{
		Name: "onboard",
		Steps: []workflow.Step{
			// snippet:rundata
			{Name: "lookup", Run: func(ctx context.Context, in []byte) ([]byte, error) {
				err := workflow.SetRunData(ctx, "plan", "pro") // visible to every later step of this run
				return []byte(`"account created"`), err
			}},
			{Name: "provision", After: []string{"lookup"}, Run: func(ctx context.Context, in []byte) ([]byte, error) {
				return []byte(`"workspace ready"`), nil // does not pass the plan on
			}},
			{Name: "welcome", After: []string{"provision"}, Run: func(ctx context.Context, in []byte) ([]byte, error) {
				plan, ok, err := workflow.RunData[string](ctx, "plan")
				if err != nil {
					return nil, err
				}
				if !ok {
					return nil, workflow.Permanent(errors.New("no plan was set"))
				}
				return json.Marshal("welcome mail for the " + plan + " plan")
			}},
			// snippet:end
		},
	}
}

func runData(ctx context.Context, c workflow.Client, out io.Writer, _ *counter) error {
	res, err := enqueueAndWait(ctx, c, workflow.Job{Workflow: "onboard"})
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "run data:\n  %s\n", res)
	return nil
}

// choiceWorkflow routes a refund to automatic approval or to review.
func choiceWorkflow() (workflow.Workflow, error) {
	// snippet:choice
	steps, err := workflow.Choice(workflow.ChoiceSpec{
		Name:  "route",
		After: []string{"load"},
		Decide: func(ctx context.Context, in []byte) (string, error) {
			var amount int
			if err := json.Unmarshal(in, &amount); err != nil {
				return "", workflow.Permanent(err)
			}
			if amount < 100 {
				return "auto", nil
			}
			return "review", nil
		},
		Branches: []workflow.Branch{
			{Name: "auto", Run: func(ctx context.Context, in []byte) ([]byte, error) {
				return []byte(`"refunded ` + string(in) + ` automatically"`), nil
			}},
			{Name: "review", Run: func(ctx context.Context, in []byte) ([]byte, error) {
				return []byte(`"refund of ` + string(in) + ` sent to review"`), nil
			}},
		},
	})
	if err != nil {
		return workflow.Workflow{}, err
	}
	load := workflow.Step{Name: "load", Run: func(ctx context.Context, in []byte) ([]byte, error) {
		return in, nil // the refund amount, from the payload
	}}
	notify := workflow.Step{Name: "notify", After: []string{"route"}, Run: func(ctx context.Context, in []byte) ([]byte, error) {
		return in, nil // the chosen branch's output, unchanged
	}}
	wf := workflow.Workflow{Name: "refund", Steps: append(append([]workflow.Step{load}, steps...), notify)}
	// snippet:end
	return wf, nil
}

func choose(ctx context.Context, c workflow.Client, out io.Writer, _ *counter) error {
	fmt.Fprintln(out, "choice:")
	wf, err := choiceWorkflow()
	if err != nil {
		return err
	}
	names := make([]string, len(wf.Steps))
	for i, s := range wf.Steps {
		names[i] = s.Name
	}
	fmt.Fprintf(out, "  steps: %s\n", strings.Join(names, ", "))
	for _, amount := range []string{"40", "250"} {
		res, err := enqueueAndWait(ctx, c, workflow.Job{Workflow: "refund", Payload: []byte(amount)})
		if err != nil {
			return err
		}
		fmt.Fprintf(out, "  %s: %s\n", amount, res)
	}
	return nil
}

// noJournal shows the refusal when no journal is wired in: a handler run
// inline without the Journal middleware.
func noJournal(ctx context.Context, out io.Writer) {
	wf := workflow.Workflow{Name: "plain", Steps: []workflow.Step{{Name: "s", Run: func(ctx context.Context, _ []byte) ([]byte, error) {
		_, err := workflow.Memo(ctx, "x", func(context.Context) (int, error) { return 1, nil })
		return nil, err
	}}}}
	_, err := workflow.RunInline(ctx, wf, workflow.Job{})
	fmt.Fprintf(out, "no journal:\n  ErrNoJournal=%v permanent=%v\n", errors.Is(err, workflow.ErrNoJournal), workflow.IsPermanent(err))
}

// enqueueAndWait enqueues job and returns the run's result.
func enqueueAndWait(ctx context.Context, c workflow.Client, job workflow.Job) ([]byte, error) {
	ref, err := c.Enqueue(ctx, job)
	if err != nil {
		return nil, err
	}
	return workflow.AwaitResult(ctx, c, ref, await)
}
