// Command events-webhooks shows the events package: a Bus that turns one
// published event into a run per subscription, deduplicated by the event's
// ID; an inbound webhook handler that verifies a signature and enqueues;
// and the HMAC and Standard Webhooks signing schemes. It runs on the
// in-memory driver and calls the handler through httptest, so nothing
// listens on a port and nothing needs to be installed.
//
// It is the program behind docs/events/: the guides copy its regions
// (marked "snippet:") and task docs:snippets fails when they drift.
package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/events"
	"github.com/ubgo/workflow/memory"
)

func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "events-webhooks:", err)
		os.Exit(1)
	}
}

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context, out io.Writer) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Every run holds until gate closes, so a republished event finds its
	// first runs unfinished and the deduplication is visible.
	gate := make(chan struct{})
	hold := func(ctx context.Context, _ []byte) ([]byte, error) {
		select {
		case <-gate:
			return nil, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	c := memory.New()
	for _, name := range []string{"send-receipt", "update-crm", "github-push"} {
		if err := c.Register(workflow.Workflow{Name: name, Steps: []workflow.Step{{Name: "run", Run: hold}}}); err != nil {
			return err
		}
	}
	if err := c.Start(ctx); err != nil {
		return err
	}
	defer close(gate)

	if err := bus(ctx, c, out); err != nil {
		return err
	}
	if err := webhook(ctx, c, out); err != nil {
		return err
	}
	return standard(out)
}

func bus(ctx context.Context, c workflow.Client, out io.Writer) error {
	fmt.Fprintln(out, "bus:")
	// snippet:bus
	b, err := events.New(c)
	if err != nil {
		return err
	}
	err = errors.Join(
		b.Subscribe(events.Subscription{Name: "receipt", Event: "order.paid", Workflow: "send-receipt"}),
		b.Subscribe(events.Subscription{Name: "crm", Event: "order.paid", Workflow: "update-crm",
			Filter: func(m events.Message) bool { return m.Labels["channel"] != "pos" }}),
	)
	if err != nil {
		return err
	}
	msg := events.Message{ID: "evt_1", Name: "order.paid", Payload: []byte(`{"order":42}`),
		Labels: map[string]string{"channel": "web"}}
	first, err := b.Publish(ctx, msg)
	if err != nil {
		return err
	}
	// snippet:end
	for _, d := range first.Deliveries {
		fmt.Fprintf(out, "  %s started a run: %v\n", d.Subscription, !d.Ref.RunID.IsZero())
	}
	// snippet:republish
	again, err := b.Publish(ctx, msg) // a retry after a timeout: same ID
	if err != nil {
		return err
	}
	for i, d := range again.Deliveries {
		fmt.Fprintf(out, "  republish of %s returned the first run: %v\n", d.Subscription, d.Ref == first.Deliveries[i].Ref)
	}
	// snippet:end
	pos, err := b.Publish(ctx, events.Message{ID: "evt_2", Name: "order.paid", Labels: map[string]string{"channel": "pos"}})
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "  a pos order matches %d subscription(s)\n", len(pos.Deliveries))
	page, err := c.(workflow.RunQuerier).Runs(ctx, workflow.RunQuery{Workflow: "update-crm", Limit: 1})
	if err != nil {
		return err
	}
	l := page.Runs[0].Labels
	fmt.Fprintf(out, "  run labels: event=%s id=%s subscription=%s channel=%s\n",
		l[events.LabelEvent], l[events.LabelEventID], l[events.LabelSubscription], l["channel"])
	return nil
}

func webhook(ctx context.Context, c workflow.Client, out io.Writer) error {
	fmt.Fprintln(out, "webhook:")
	// snippet:webhook
	signer := events.HMAC{
		Header:   "X-Hub-Signature-256",
		Prefix:   "sha256=",
		Secrets:  [][]byte{[]byte("new-secret"), []byte("old-secret")}, // the first signs, all verify
		IDHeader: "X-GitHub-Delivery",
	}
	h, err := events.NewWebhook(events.WebhookConfig{
		Verifier: signer,
		Client:   c,
		Workflow: "github-push",
		MaxBody:  1 << 10,
	})
	if err != nil {
		return err
	}
	// snippet:end
	body := []byte(`{"ref":"refs/heads/main"}`)
	sig, err := signer.Sign(body)
	if err != nil {
		return err
	}
	send := func(label, method string, body []byte, headers map[string]string) {
		req := httptest.NewRequestWithContext(ctx, method, "/hooks/github", bytes.NewReader(body))
		for k, v := range headers {
			req.Header.Set(k, v)
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		fmt.Fprintf(out, "  %-22s %d %s", label, rec.Code, rec.Body.String())
	}
	signed := map[string]string{"X-Hub-Signature-256": sig, "X-GitHub-Delivery": "d-1"}
	send("GET", http.MethodGet, nil, nil)
	send("no signature", http.MethodPost, body, nil)
	send("wrong signature", http.MethodPost, body, map[string]string{"X-Hub-Signature-256": "sha256=00"})
	send("too large", http.MethodPost, bytes.Repeat([]byte("x"), 2<<10), signed)
	send("signed", http.MethodPost, body, signed)
	send("redelivered", http.MethodPost, body, signed)
	page, err := c.(workflow.RunQuerier).Runs(ctx, workflow.RunQuery{Workflow: "github-push"})
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "  runs of github-push: %d\n", len(page.Runs))

	// snippet:unavailable
	down, err := events.NewWebhook(events.WebhookConfig{Verifier: signer, Client: unreachable{c}, Workflow: "github-push"})
	if err != nil {
		return err
	}
	// snippet:end
	h = down
	send("backend down", http.MethodPost, body, signed)
	_, err = events.NewWebhook(events.WebhookConfig{Client: c, Workflow: "github-push"})
	fmt.Fprintf(out, "  no Verifier refused: %v\n", errors.Is(err, workflow.ErrInvalid))
	return nil
}

// unreachable is a client whose backend cannot be reached: every Enqueue
// fails with an error marked workflow.Unavailable.
type unreachable struct{ workflow.Client }

func (unreachable) Enqueue(context.Context, workflow.Job) (workflow.RunRef, error) {
	return workflow.RunRef{}, workflow.Unavailable(errors.New("connection refused"))
}

func standard(out io.Writer) error {
	fmt.Fprintln(out, "standard webhooks:")
	clk := clock.NewFake(time.Date(2026, 10, 14, 9, 0, 0, 0, time.UTC))
	// snippet:standard
	sw, err := events.NewStandardWebhooks([]string{"whsec_c2VjcmV0LWtleS0x"}, 5*time.Minute, clk)
	if err != nil {
		return err
	}
	body := []byte(`{"type":"invoice.paid"}`)
	req := httptest.NewRequest(http.MethodPost, "/hooks/billing", bytes.NewReader(body))
	req.Header = sw.Headers("msg_1", body) // webhook-id, webhook-timestamp, webhook-signature
	id, err := sw.Verify(req, body)
	// snippet:end
	fmt.Fprintf(out, "  verified id=%s err=%v\n", id, err)
	_, err = sw.Verify(req, []byte(`{"type":"invoice.voided"}`))
	fmt.Fprintf(out, "  changed body: ErrSignature=%v\n", errors.Is(err, events.ErrSignature))
	clk.Advance(10 * time.Minute) // the same request replayed ten minutes later
	_, err = sw.Verify(req, body)
	fmt.Fprintf(out, "  replayed after 10m: ErrSignature=%v\n", errors.Is(err, events.ErrSignature))
	return nil
}
