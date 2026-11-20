package events_test

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/events"
)

// counting counts the enqueues that reach the client and can fail them.
type counting struct {
	workflow.Client
	n    atomic.Int32
	fail atomic.Bool
}

func (c *counting) Enqueue(ctx context.Context, job workflow.Job) (workflow.RunRef, error) {
	c.n.Add(1)
	if c.fail.Load() {
		return workflow.RunRef{}, workflow.Unavailable(errors.New("db down"))
	}
	return c.Client.Enqueue(ctx, job)
}

// TestWebhookInbox (MQ-15): with an Inbox a redelivered delivery ID never
// reaches the client again; a failed enqueue is released, so the sender's
// retry enqueues; a delivery held by another request gets 503.
func TestWebhookInbox(t *testing.T) {
	rec := &recorder{}
	c := &counting{Client: started(t, rec.workflow("ingest"))}
	in := workflow.NewMemoryInbox()
	hook, err := events.NewWebhook(events.WebhookConfig{Unsigned: true, Client: c, Workflow: "ingest",
		ID:    func(r *http.Request, _ []byte) string { return r.Header.Get("Delivery") },
		Inbox: &workflow.InboxConsumer{Inbox: in, Consumer: "shop-hooks"}})
	if err != nil {
		t.Fatal(err)
	}
	post := func(id string) int {
		r := httptest.NewRequest(http.MethodPost, "/hook", bytes.NewReader([]byte(`{"order":7}`)))
		r.Header.Set("Delivery", id)
		w := httptest.NewRecorder()
		hook.ServeHTTP(w, r)
		return w.Code
	}
	for range 3 {
		if code := post("d-1"); code != http.StatusOK {
			t.Fatalf("delivery = %d", code)
		}
	}
	if n := c.n.Load(); n != 1 {
		t.Fatalf("%d enqueues for one delivery ID posted 3 times; want 1", n)
	}
	c.fail.Store(true)
	if code := post("d-2"); code != http.StatusServiceUnavailable {
		t.Fatalf("failed enqueue = %d; want 503", code)
	}
	c.fail.Store(false)
	if code := post("d-2"); code != http.StatusOK {
		t.Fatalf("retry after a failed enqueue = %d", code)
	}
	if n := c.n.Load(); n != 3 {
		t.Fatalf("enqueues = %d; want 3 (d-1 once, d-2 failed then retried)", n)
	}
	if st, _, _ := in.Claim(context.Background(), workflow.InboxKey{Consumer: "shop-hooks", ID: "d-3"}, time.Minute, time.Now()); st != workflow.InboxClaimed {
		t.Fatal(st)
	}
	if code := post("d-3"); code != http.StatusServiceUnavailable {
		t.Fatalf("delivery held elsewhere = %d; want 503", code)
	}
	if _, err := events.NewWebhook(events.WebhookConfig{Unsigned: true, Client: c, Workflow: "ingest", Inbox: &workflow.InboxConsumer{Inbox: in}}); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("Inbox without a Consumer = %v; want ErrInvalid", err)
	}
}
