// Command encrypted-payloads seals every payload with AES-GCM before it
// reaches the driver and opens it inside the worker, so the engine only
// ever stores ciphertext: a two-step workflow whose step output is sealed
// too, a key rotation with an old key kept for decrypting, and the
// refusals for plaintext and for a key dropped too early.
//
// It is the program behind docs/middleware/codecs.md: the guide copies its
// regions (marked "snippet:") and task docs:snippets fails when they
// drift.
package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/memory"
	"github.com/ubgo/workflow/middleware"
)

// order is the job's payload; workflow.Define decodes it from JSON after
// DecodeStep has opened it.
type order struct {
	ID   string
	Card string
}

// Keys are 16, 24 or 32 bytes. A real app loads them from its secret
// store; these fixed ones keep the example self-contained.
var (
	oldKey = bytes.Repeat([]byte{1}, 32)
	newKey = bytes.Repeat([]byte{2}, 32)
)

func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "encrypted-payloads:", err)
		os.Exit(1)
	}
}

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context, out io.Writer) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// snippet:codec
	sealing, err := middleware.Encrypt(oldKey)
	if err != nil {
		return err
	}
	// After the rotation: seal with newKey, still open what oldKey sealed.
	rotated, err := middleware.Encrypt(newKey, oldKey)
	if err != nil {
		return err
	}
	// snippet:end

	// snippet:worker
	c := memory.New(memory.WithMiddleware(middleware.DecodeStep(rotated)))
	err = c.Register(workflow.Workflow{
		Name: "charge",
		Steps: []workflow.Step{
			{Name: "authorize", Run: workflow.Define(func(ctx context.Context, o order) (string, error) {
				return "auth-" + o.ID, nil // sealed again by DecodeStep before the engine keeps it
			})},
			{Name: "capture", After: []string{"authorize"}, Run: workflow.Define(func(ctx context.Context, auth string) (string, error) {
				fmt.Fprintln(out, "capture got", auth)
				return "", nil
			})},
		},
	})
	if err != nil {
		return err
	}
	// snippet:end

	ends := make(chan workflow.RunEvent, 4)
	if err := c.OnRunFinished(func(_ context.Context, ev workflow.RunEvent) { ends <- ev }); err != nil {
		return err
	}
	if err := c.Start(ctx); err != nil {
		return err
	}
	wait := func(label string, ref workflow.RunRef) {
		for ev := range ends {
			if ev.Ref.RunID == ref.RunID {
				fmt.Fprintf(out, "%s: %s, decode error: %v\n", label, ev.Status, errors.Is(ev.Err, middleware.ErrDecode))
				return
			}
		}
	}

	// snippet:client
	// stored runs after sealing, so it sees exactly what the driver keeps.
	stored := func(next workflow.EnqueueFunc) workflow.EnqueueFunc {
		return func(ctx context.Context, job workflow.Job) (workflow.RunRef, error) {
			fmt.Fprintf(out, "driver stores a sealed payload: %v, card visible: %v\n",
				strings.HasPrefix(string(job.Payload), `"wfenc1:`), bytes.Contains(job.Payload, []byte("4242")))
			return next(ctx, job)
		}
	}
	app := middleware.WithCodec(c, rotated, stored)
	ref, err := app.Enqueue(ctx, workflow.Job{Workflow: "charge", Payload: []byte(`{"ID":"o-1","Card":"4242"}`)})
	if err != nil {
		return err
	}
	// snippet:end
	wait("sealed with the new key", ref)

	// An app instance not yet rotated still seals with the old key; the
	// worker opens it with the old key it kept.
	ref, err = middleware.WithCodec(c, sealing).Enqueue(ctx, workflow.Job{Workflow: "charge", Payload: []byte(`{"ID":"o-2"}`)})
	if err != nil {
		return err
	}
	wait("sealed with the old key", ref)

	// snippet:plaintext
	ref, err = c.Enqueue(ctx, workflow.Job{Workflow: "charge", Payload: []byte(`{"ID":"o-3"}`)}) // not through WithCodec
	if err != nil {
		return err
	}
	// snippet:end
	wait("plaintext", ref)

	return droppedTooEarly(ctx, out, sealing)
}

// droppedTooEarly runs a worker that knows only the new key against a
// payload sealed with the old one: the step fails for good with ErrDecode.
func droppedTooEarly(ctx context.Context, out io.Writer, sealing middleware.Codec) error {
	newOnly, err := middleware.Encrypt(newKey)
	if err != nil {
		return err
	}
	c := memory.New(memory.WithMiddleware(middleware.DecodeStep(newOnly)))
	if err := c.Register(workflow.Workflow{
		Name:  "charge",
		Retry: workflow.RetryPolicy{Max: 3}, // not spent: a decode error is permanent
		Steps: []workflow.Step{{Name: "authorize", Run: func(context.Context, []byte) ([]byte, error) { return nil, nil }}},
	}); err != nil {
		return err
	}
	ends := make(chan workflow.RunEvent, 1)
	if err := c.OnRunFinished(func(_ context.Context, ev workflow.RunEvent) { ends <- ev }); err != nil {
		return err
	}
	if err := c.Start(ctx); err != nil {
		return err
	}
	if _, err := middleware.WithCodec(c, sealing).Enqueue(ctx, workflow.Job{Workflow: "charge", Payload: []byte(`{"ID":"o-4"}`)}); err != nil {
		return err
	}
	ev := <-ends
	fmt.Fprintf(out, "old key dropped: %s, decode error: %v, permanent: %v\n", ev.Status, errors.Is(ev.Err, middleware.ErrDecode), workflow.IsPermanent(ev.Err))
	return nil
}
