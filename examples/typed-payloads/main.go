// Command typed-payloads shows payloads as Go types: a job built from a
// struct (JobOf), a result decoded back (DecodePayload), payload size
// limits set by the app (middleware.MaxPayload) and by the driver
// (memory.WithPayloadLimit), and a payload whose shape changed between
// releases, read by new code through upcasters (middleware.PayloadVersion
// with workflow.DefineVersioned).
//
// It runs on the in-memory driver, so nothing needs to be installed and
// the output never varies. It is the program behind docs/jobs/payloads.md:
// the guide copies its regions (marked "snippet:") and task docs:snippets
// fails when they drift.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/memory"
	"github.com/ubgo/workflow/middleware"
)

// snippet:types
// Welcome is the payload of the "send-welcome" workflow.
type Welcome struct {
	UserID string `json:"user_id"`
	Plan   string `json:"plan"`
}

// Receipt is what its step returns.
type Receipt struct {
	MessageID string `json:"message_id"`
}

// snippet:end

// snippet:versions
// GreetV1 is how release 1 wrote the "greet" payload.
type GreetV1 struct {
	Name string `json:"name"` // "Ann Lee"
}

// GreetV2 is the shape this code reads: the name split in two.
type GreetV2 struct {
	First string `json:"first"`
	Last  string `json:"last"`
}

// upcasters turn each old shape into the next one: 1 into 2.
var upcasters = map[int]workflow.Upcaster{
	1: func(raw []byte) ([]byte, error) {
		var v1 GreetV1
		if err := json.Unmarshal(raw, &v1); err != nil {
			return nil, err
		}
		first, last, _ := strings.Cut(v1.Name, " ")
		return json.Marshal(GreetV2{First: first, Last: last})
	},
}

// greet reads every version: older payloads are upcast to 2 first.
var greet = workflow.DefineVersioned(2, upcasters, func(_ context.Context, in GreetV2) (string, error) {
	return "Hello " + in.First + " " + in.Last, nil
})

// snippet:end

func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "typed-payloads:", err)
		os.Exit(1)
	}
}

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context, out io.Writer) error {
	clk := clock.NewFake(time.Date(2026, 3, 2, 9, 0, 0, 0, time.UTC))
	// snippet:handler
	sendWelcome := workflow.Define(func(_ context.Context, in Welcome) (Receipt, error) {
		return Receipt{MessageID: "msg-" + in.UserID}, nil // in is already decoded
	})
	// snippet:end

	// snippet:driver-limit
	c := memory.New(memory.WithClock(clk), memory.WithPayloadLimit(4096)) // what production's engine takes
	// snippet:end
	for _, wf := range []workflow.Workflow{
		{Name: "send-welcome", Steps: []workflow.Step{{Name: "send", Run: sendWelcome}}},
		{Name: "greet", Steps: []workflow.Step{{Name: "greet", Run: greet}}},
	} {
		if err := c.Register(wf); err != nil {
			return err
		}
	}
	if err := c.Start(ctx); err != nil {
		return err
	}
	defer c.Stop()
	wait := workflow.AwaitOptions{Poll: 10 * time.Millisecond}

	fmt.Fprintln(out, "typed:")
	// snippet:job-of
	job, err := workflow.JobOf("send-welcome", Welcome{UserID: "u-7", Plan: "pro"})
	if err != nil {
		return err
	}
	job.IdempotencyKey = "welcome:u-7" // the rest of the Job is set as usual
	ref, err := c.Enqueue(ctx, job)
	if err != nil {
		return err
	}
	raw, err := workflow.AwaitResult(ctx, c, ref, wait)
	if err != nil {
		return err
	}
	receipt, err := workflow.DecodePayload[Receipt](raw)
	if err != nil {
		return err
	}
	// snippet:end
	fmt.Fprintf(out, "  payload %s\n  receipt %s\n", job.Payload, receipt.MessageID)
	_, err = workflow.DecodePayload[Welcome]([]byte(`{"user_id": 7}`))
	fmt.Fprintf(out, "  bad payload: permanent %t\n", workflow.IsPermanent(err))

	fmt.Fprintln(out, "size limits:")
	// snippet:max-payload
	app := middleware.WrapClient(c, middleware.MaxPayload(1024)) // the app's own cap
	big := workflow.Job{Workflow: "send-welcome", Payload: []byte(`"` + strings.Repeat("x", 2000) + `"`)}
	_, err = app.Enqueue(ctx, big)
	var tooBig *workflow.PayloadTooLargeError
	if errors.As(err, &tooBig) {
		fmt.Fprintf(out, "  %d bytes over the %s limit of %d\n", tooBig.Size, tooBig.Source, tooBig.Limit)
	}
	// snippet:end
	fmt.Fprintf(out, "  ErrPayloadTooLarge %t, ErrInvalid %t\n", errors.Is(err, workflow.ErrPayloadTooLarge), errors.Is(err, workflow.ErrInvalid))
	huge := workflow.Job{Workflow: "send-welcome", Payload: []byte(`"` + strings.Repeat("x", 5000) + `"`)}
	_, err = c.Enqueue(ctx, huge)
	if errors.As(err, &tooBig) {
		fmt.Fprintf(out, "  %d bytes over the %s limit of %d\n", tooBig.Size, tooBig.Source, tooBig.Limit)
	}
	// snippet:limiter
	if l, ok := any(c).(workflow.PayloadLimiter); ok {
		fmt.Fprintf(out, "  driver limit: %d bytes\n", l.PayloadLimit())
	}
	// snippet:end

	fmt.Fprintln(out, "versions:")
	// snippet:stamp
	release1 := middleware.WrapClient(c, middleware.PayloadVersion(1)) // the old release, still enqueuing
	release2 := middleware.WrapClient(c, middleware.PayloadVersion(2)) // this release
	// snippet:end
	for _, e := range []struct {
		client  workflow.Client
		payload any
	}{
		{release1, GreetV1{Name: "Ann Lee"}},
		{release2, GreetV2{First: "Bo", Last: "Chen"}},
	} {
		job, err := workflow.JobOf("greet", e.payload)
		if err != nil {
			return err
		}
		ref, err := e.client.Enqueue(ctx, job)
		if err != nil {
			return err
		}
		raw, err := workflow.AwaitResult(ctx, c, ref, wait)
		if err != nil {
			return err
		}
		page, err := c.Runs(ctx, workflow.RunQuery{Ref: ref})
		if err != nil {
			return err
		}
		v, err := workflow.PayloadVersionOf(page.Runs[0].Labels)
		if err != nil {
			return err
		}
		fmt.Fprintf(out, "  version %d %s -> %s\n", v, job.Payload, raw)
	}
	newer := workflow.Job{Workflow: "greet", Payload: []byte(`{}`), Labels: map[string]string{workflow.LabelPayloadVersion: "3"}}
	_, err = release2.Enqueue(ctx, newer)
	fmt.Fprintf(out, "  version 3 sent to release 2: ErrInvalid %t\n", errors.Is(err, workflow.ErrInvalid))
	return nil
}
