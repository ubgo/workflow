// Command named-queues shows named queues on the in-memory driver: a
// workflow's default queue, a per-dispatch override, a worker cap and a
// priority per queue, and pausing one queue while the others keep running.
//
// One worker serves every queue, so the order the steps run in is the
// order the queues hand them out, and the output is the same on every run.
// It is the program behind docs/jobs/queues.md: the guide copies its
// regions (marked "snippet:") and task docs:snippets fails when they drift.
package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/memory"
)

func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context, out io.Writer) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	ran := make(chan string, 16)
	release := make(chan struct{})
	step := func(ctx context.Context, in []byte) ([]byte, error) {
		info, _ := workflow.StepInfoFrom(ctx)
		ran <- info.Workflow + " " + string(in) + " on " + info.Queue
		if string(in) == "hold" {
			<-release // keeps the one worker busy while the others queue up
		}
		return nil, nil
	}

	// snippet:workflows
	emails := workflow.Workflow{Name: "send-email", Queue: "mail", Steps: []workflow.Step{{Name: "send", Run: step}}}
	reports := workflow.Workflow{Name: "build-report", Queue: "reports", Steps: []workflow.Step{{Name: "build", Run: step}}}
	// snippet:end

	// snippet:client
	c := memory.New(
		memory.WithWorkers(1),
		memory.WithQueues(
			workflow.QueueConfig{Name: "urgent", Priority: 10}, // drains first
			workflow.QueueConfig{Name: "mail", Priority: 5},
			workflow.QueueConfig{Name: "reports", Workers: 1}, // at most one report at a time
		),
	)
	// snippet:end
	for _, wf := range []workflow.Workflow{emails, reports} {
		if err := c.Register(wf); err != nil {
			return err
		}
	}
	if err := c.Start(ctx); err != nil {
		return err
	}
	defer c.Stop()

	enqueue := func(job workflow.Job) (workflow.RunRef, error) { return c.Enqueue(ctx, job) }
	if _, err := enqueue(workflow.Job{Workflow: "build-report", Payload: []byte("hold")}); err != nil {
		return err
	}
	fmt.Fprintln(out, <-ran)

	// snippet:enqueue
	refs := make([]workflow.RunRef, 0, 3)
	for _, job := range []workflow.Job{
		{Workflow: "build-report", Payload: []byte("monthly")},
		{Workflow: "send-email", Payload: []byte("newsletter")},
		{Workflow: "send-email", Payload: []byte("password-reset"), Queue: "urgent"}, // this one run jumps the line
	} {
		ref, err := c.Enqueue(ctx, job)
		if err != nil {
			return err
		}
		refs = append(refs, ref)
	}
	// snippet:end
	close(release)
	for range refs {
		fmt.Fprintln(out, <-ran)
	}
	for _, ref := range refs {
		if err := waitDone(ctx, c, ref); err != nil {
			return err
		}
	}

	// snippet:pause
	if err := c.Pause(ctx, workflow.PauseQueue("mail")); err != nil {
		return err
	}
	held, err := c.Enqueue(ctx, workflow.Job{Workflow: "send-email", Payload: []byte("digest")})
	if err != nil {
		return err
	}
	report, err := c.Enqueue(ctx, workflow.Job{Workflow: "build-report", Payload: []byte("weekly")})
	if err != nil {
		return err
	}
	fmt.Fprintln(out, <-ran) // the report runs; the paused mail queue holds the digest
	if err := waitDone(ctx, c, report); err != nil {
		return err
	}
	st, err := c.Status(ctx, held)
	if err != nil {
		return err
	}
	fmt.Fprintln(out, "digest while mail is paused:", st)
	if err := c.Resume(ctx, workflow.PauseQueue("mail")); err != nil {
		return err
	}
	// snippet:end
	fmt.Fprintln(out, <-ran)
	if err := waitDone(ctx, c, held); err != nil {
		return err
	}

	// snippet:refuse
	_, err = c.Enqueue(ctx, workflow.Job{Workflow: "send-email", Queue: "sms"})
	fmt.Fprintln(out, "enqueue to a queue this client does not serve:", err)
	// snippet:end
	return nil
}

// waitDone polls until ref has finished. An app would rather observe run
// ends (workflow.RunNotifier); polling keeps the example short.
func waitDone(ctx context.Context, c *memory.Client, ref workflow.RunRef) error {
	for {
		st, err := c.Status(ctx, ref)
		if err != nil {
			return err
		}
		if st.IsTerminal() {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Millisecond):
		}
	}
}
