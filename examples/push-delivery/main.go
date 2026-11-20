// Command push-delivery sends jobs to an HTTP endpoint instead of running
// them in the worker: a step built with push.Deliver POSTs each job,
// signed with Standard Webhooks, and push.Handler on the receiving side
// (a serverless function in a real app; an httptest server here) checks
// the signature and runs its own workflow on the payload. The queue keeps
// the retries: a receiver that answers 5xx is retried, one that refuses
// for good fails the run at once, and a forged request never runs.
//
// It is the program behind docs/jobs/push-delivery.md: the guide copies
// its regions (marked "snippet:") and task docs:snippets fails when they
// drift.
package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/events"
	"github.com/ubgo/workflow/memory"
	"github.com/ubgo/workflow/push"
)

func main() {
	if err := run(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, "push-delivery:", err)
		os.Exit(1)
	}
}

// secret is shared by the sender and the receiver. A real app reads it
// from its secret store; "whsec_" followed by base64 is the Standard
// Webhooks format. This one is made up for the example.
var secret = "whsec_" + base64.StdEncoding.EncodeToString([]byte("example-only-secret-not-real"))

// snippet:receiver
// sendEmail is the workflow the endpoint runs. It fails once for
// "flaky@example.com" (a cold start) and refuses "bounced@example.com"
// for good.
func sendEmail(coldStarts *atomic.Int32) workflow.Workflow {
	return workflow.Workflow{Name: "send-email", Steps: []workflow.Step{{Name: "send", Run: func(ctx context.Context, in []byte) ([]byte, error) {
		var to string
		if err := json.Unmarshal(in, &to); err != nil {
			return nil, workflow.Permanent(err) // 422: the sender stops
		}
		switch {
		case to == "bounced@example.com":
			return nil, workflow.Permanent(errors.New("address bounced")) // 422
		case to == "flaky@example.com" && coldStarts.Add(1) == 1:
			return nil, errors.New("cold start") // 500: the sender retries
		}
		return json.Marshal(map[string]string{"sent_to": to})
	}}}}
}

// snippet:end

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// snippet:handler
	verifier, err := events.NewStandardWebhooks([]string{secret}, 5*time.Minute, nil)
	if err != nil {
		return err
	}
	var coldStarts atomic.Int32
	endpoint := httptest.NewServer(push.Handler(sendEmail(&coldStarts), verifier))
	defer endpoint.Close()
	// snippet:end

	// snippet:deliver
	signer, err := events.NewStandardWebhooks([]string{secret}, 0, nil)
	if err != nil {
		return err
	}
	dispatch := workflow.Workflow{
		Name:  "dispatch-email",
		Retry: workflow.RetryPolicy{Max: 3, DelayFor: workflow.RetryAfterDelay}, // honour a 429/503 Retry-After
		Steps: []workflow.Step{{Name: "deliver", Run: push.Deliver(endpoint.URL, signer,
			push.WithClient(endpoint.Client()), push.WithTimeout(10*time.Second))}},
	}
	c := memory.New()
	if err := c.Register(dispatch); err != nil {
		return err
	}
	if err := c.Start(ctx); err != nil {
		return err
	}
	// snippet:end

	fmt.Println("deliveries:")
	for _, to := range []string{"ann@example.com", "flaky@example.com", "bounced@example.com"} {
		payload, _ := json.Marshal(to)
		ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "dispatch-email", Payload: payload})
		if err != nil {
			return err
		}
		res, err := workflow.AwaitResult(ctx, c, ref, workflow.AwaitOptions{Poll: 2 * time.Millisecond})
		page, qerr := c.Runs(ctx, workflow.RunQuery{Ref: ref, Limit: 1})
		if qerr != nil {
			return qerr
		}
		info := page.Runs[0]
		if err != nil {
			fmt.Printf("  %s: %s after %d attempt(s): %s\n", to, info.Status, info.Attempts, info.Error)
			continue
		}
		fmt.Printf("  %s: %s after %d attempt(s), result %s\n", to, info.Status, info.Attempts, res)
	}

	fmt.Println("forged request:")
	body := []byte(`{"workflow":"dispatch-email","run":"x","step":"deliver","attempt":1,"payload":"ImV2ZUBleGFtcGxlLmNvbSI="}`)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.URL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	resp, err := endpoint.Client().Do(req)
	if err != nil {
		return err
	}
	_ = resp.Body.Close()
	fmt.Printf("  unsigned: %d\n", resp.StatusCode)
	return nil
}
