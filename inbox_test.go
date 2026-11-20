package workflow_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/workflowtest"
)

func TestMemoryInboxContract(t *testing.T) {
	in := workflow.NewMemoryInbox()
	workflowtest.InboxContract(t, in, in)
}

// TestInboxConsumerOnce: a redelivered message runs the work once; a
// failed attempt is released so the redelivery runs it again; a message
// held by another worker is busy (retryable); the keep window ends.
func TestInboxConsumerOnce(t *testing.T) {
	ctx := context.Background()
	clk := clock.NewFake(time.Date(2026, 11, 20, 9, 0, 0, 0, time.UTC))
	in := workflow.NewMemoryInbox()
	c := workflow.InboxConsumer{Inbox: in, Consumer: "mailer", Keep: time.Hour, Clock: clk}
	runs := 0
	send := func(context.Context) error { runs++; return nil }
	for range 3 {
		if _, err := c.Once(ctx, "msg-1", send); err != nil {
			t.Fatal(err)
		}
	}
	if runs != 1 {
		t.Fatalf("work ran %d times for one message delivered 3 times", runs)
	}
	boom := errors.New("smtp down")
	if ran, err := c.Once(ctx, "msg-2", func(context.Context) error { return boom }); !ran || !errors.Is(err, boom) {
		t.Fatalf("failing work = %v, %v", ran, err)
	}
	if ran, err := c.Once(ctx, "msg-2", send); !ran || err != nil {
		t.Fatalf("redelivery after a failure = %v, %v; want it to run", ran, err)
	}

	// Another worker holds msg-3: busy, not permanent.
	if st, _, _ := in.Claim(ctx, workflow.InboxKey{Consumer: "mailer", ID: "msg-3"}, time.Minute, clk.Now()); st != workflow.InboxClaimed {
		t.Fatal(st)
	}
	if ran, err := c.Once(ctx, "msg-3", send); ran || !errors.Is(err, workflow.ErrInboxBusy) || workflow.IsPermanent(err) {
		t.Fatalf("held message = %v, %v; want a retryable ErrInboxBusy", ran, err)
	}
	// Another consumer reads msg-1 on its own.
	other := workflow.InboxConsumer{Inbox: in, Consumer: "indexer", Clock: clk}
	if ran, err := other.Once(ctx, "msg-1", send); !ran || err != nil {
		t.Fatalf("second consumer = %v, %v", ran, err)
	}
	clk.Advance(2 * time.Hour)
	if ran, _ := c.Once(ctx, "msg-1", send); !ran {
		t.Fatal("msg-1 still dropped after its keep ended")
	}
	if n := in.Prune(clk.Now().Add(8 * 24 * time.Hour)); n == 0 {
		t.Fatal("Prune removed nothing")
	}
	if keys := in.Keys(); len(keys) != 0 {
		t.Fatalf("after Prune: %v", keys)
	}
	if _, err := (workflow.InboxConsumer{Inbox: in, Consumer: "mailer"}).Once(ctx, "", send); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("empty ID = %v", err)
	}
	if _, err := (workflow.InboxConsumer{Consumer: "mailer"}).Once(ctx, "x", send); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("no inbox = %v", err)
	}
}

func TestInboxKeyIsUnambiguous(t *testing.T) {
	a := workflow.InboxKey{Consumer: "a/b", ID: "c"}.String()
	b := workflow.InboxKey{Consumer: "a", ID: "b/c"}.String()
	if a == b {
		t.Fatalf("keys collide: %q", a)
	}
	if got := (workflow.InboxKey{Consumer: "mailer", ID: "m-1"}).String(); got != "6:mailer/m-1" {
		t.Fatalf("String = %q; the stored form changed", got)
	}
}
