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

// main runs the example and exits non-zero on error.
func main() {
	if err := run(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, "typed-payloads:", err)
		os.Exit(1)
	}
}

const (
	// awaitPoll is how often the program looks at a run it waits for: the
	// in-memory driver ends a run in microseconds.
	awaitPoll = 10 * time.Millisecond
	// hugeChars is the size of the payload sent past the app's cap: more
	// than the driver's 4096-byte limit, so the driver refuses it.
	hugeChars = 5000
)

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context) error {
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
	wait := workflow.AwaitOptions{Poll: awaitPoll}

	fmt.Println("typed:")
	// snippet:job-of
	// snippet:build-job
	job, err := workflow.JobOf("send-welcome", Welcome{UserID: "u-7", Plan: "pro"})
	if err != nil {
		return err
	}
	job.IdempotencyKey = "welcome:u-7" // the rest of the Job is set as usual
	// snippet:end
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
	fmt.Printf("  payload %s\n  receipt %s\n", job.Payload, receipt.MessageID)
	_, err = workflow.DecodePayload[Welcome]([]byte(`{"user_id": 7}`))
	fmt.Printf("  bad payload: permanent %t\n", workflow.IsPermanent(err))

	fmt.Println("size limits:")
	// snippet:max-payload
	app := middleware.WrapClient(c, middleware.MaxPayload(1024)) // the app's own cap
	// big is a payload of 2,000 characters, over the app's cap.
	big := workflow.Job{Workflow: "send-welcome", Payload: []byte(`"` + strings.Repeat("x", 2000) + `"`)}
	_, err = app.Enqueue(ctx, big)
	var tooBig *workflow.PayloadTooLargeError
	if errors.As(err, &tooBig) {
		fmt.Printf("  %d bytes over the %s limit of %d\n", tooBig.Size, tooBig.Source, tooBig.Limit)
	}
	// snippet:end
	fmt.Printf("  ErrPayloadTooLarge %t, ErrInvalid %t\n", errors.Is(err, workflow.ErrPayloadTooLarge), errors.Is(err, workflow.ErrInvalid))
	huge := workflow.Job{Workflow: "send-welcome", Payload: []byte(`"` + strings.Repeat("x", hugeChars) + `"`)}
	_, err = c.Enqueue(ctx, huge)
	if errors.As(err, &tooBig) {
		fmt.Printf("  %d bytes over the %s limit of %d\n", tooBig.Size, tooBig.Source, tooBig.Limit)
	}
	// snippet:limiter
	if l, ok := any(c).(workflow.PayloadLimiter); ok {
		fmt.Printf("  driver limit: %d bytes\n", l.PayloadLimit())
	}
	// snippet:end

	fmt.Println("versions:")
	// snippet:stamp
	release1 := middleware.WrapClient(c, middleware.PayloadVersion(1)) // the old release, still enqueuing
	release2 := middleware.WrapClient(c, middleware.PayloadVersion(2)) // this release
	// snippet:end
	oldJob, err := workflow.JobOf("greet", GreetV1{Name: "Ann Lee"})
	if err != nil {
		return err
	}
	newJob, err := workflow.JobOf("greet", GreetV2{First: "Bo", Last: "Chen"})
	if err != nil {
		return err
	}
	for _, e := range []struct {
		client workflow.Client
		job    workflow.Job
	}{
		{release1, oldJob},
		{release2, newJob},
	} {
		job := e.job
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
		fmt.Printf("  version %d %s -> %s\n", v, job.Payload, raw)
	}
	newer := workflow.Job{Workflow: "greet", Payload: []byte(`{}`), Labels: map[string]string{workflow.LabelPayloadVersion: "3"}} // newer than this release reads
	_, err = release2.Enqueue(ctx, newer)
	fmt.Printf("  version 3 sent to release 2: ErrInvalid %t\n", errors.Is(err, workflow.ErrInvalid))
	return nil
}
