// Command consumer-inbox shows idempotent consumption (MQ-15): a message
// delivered more than once is processed once per consumer. An
// InboxConsumer drops redeliveries and lets a failed attempt run again;
// sqlstore's Inbox.MarkTx records the message inside the app's own
// transaction, so concurrent deliveries write one order exactly once; and
// an inbound webhook with an Inbox answers a redelivery without enqueueing
// again. SQL parts run on SQLite in a temporary file.
//
// It is the program behind the "Drop redelivered messages with an inbox"
// section of docs/jobs/idempotency.md: the guide copies its regions
// (marked "snippet:") and task docs:snippets fails when they drift.
package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/ubgo/sqlb"
	driversqlite "github.com/ubgo/sqlb/contrib/driver-sqlite"
	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/events"
	"github.com/ubgo/workflow/memory"
	"github.com/ubgo/workflow/sqlstore"
)

func main() {
	if err := run(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, "consumer-inbox:", err)
		os.Exit(1)
	}
}

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context) error {
	if err := consumer(ctx); err != nil {
		return err
	}
	if err := transactional(ctx); err != nil {
		return err
	}
	return webhook(ctx)
}

// consumer processes a message delivered three times, then one whose
// first attempt fails.
func consumer(ctx context.Context) error {
	fmt.Println("consumer:")
	sent := 0
	sendReceipt := func(context.Context) error { sent++; return nil }
	// snippet:consumer
	mailer := workflow.InboxConsumer{
		Inbox:    workflow.NewMemoryInbox(), // sqlstore.Inbox or redis.Inbox across processes
		Consumer: "receipt-mailer",
		Keep:     7 * 24 * time.Hour, // remember processed IDs longer than any redelivery
	}
	for range 3 { // an at-least-once source delivers msg-1 three times
		ran, err := mailer.Once(ctx, "msg-1", sendReceipt)
		if err != nil {
			return err // nack: the message comes back
		}
		fmt.Println("  msg-1 ran:", ran)
	}
	// snippet:end
	fmt.Println("  receipts sent:", sent)
	_, err := mailer.Once(ctx, "msg-2", func(context.Context) error { return errors.New("smtp down") })
	fmt.Println("  msg-2 first attempt:", err)
	ran, err := mailer.Once(ctx, "msg-2", sendReceipt)
	fmt.Println("  msg-2 redelivered, ran:", ran, err)
	return nil
}

// transactional delivers one message to four workers at once; each writes
// an order and marks the message in one transaction.
func transactional(ctx context.Context) error {
	fmt.Println("transactional, four concurrent deliveries:")
	dir, err := os.MkdirTemp("", "consumer-inbox-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	db, err := driversqlite.Open(filepath.Join(dir, "app.db"))
	if err != nil {
		return err
	}
	defer db.Close()
	if _, err := db.ExecContext(ctx, "CREATE TABLE orders (msg_id TEXT, total INTEGER)"); err != nil {
		return err
	}
	inbox, err := sqlstore.NewInbox(db, sqlb.SQLiteDialect{})
	if err != nil {
		return err
	}
	if err := inbox.Migrate(ctx); err != nil {
		return err
	}
	// snippet:marktx
	handle := func(ctx context.Context, msgID string, total int) error {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback() // a no-op after Commit
		key := workflow.InboxKey{Consumer: "order-writer", ID: msgID}
		fresh, err := inbox.MarkTx(ctx, tx, key, 7*24*time.Hour, time.Now())
		if err != nil || !fresh {
			return err // seen before: nothing to do
		}
		if _, err := tx.ExecContext(ctx, "INSERT INTO orders (msg_id, total) VALUES (?, ?)", msgID, total); err != nil {
			return err
		}
		return tx.Commit() // the order and the record commit together
	}
	// snippet:end
	var wg sync.WaitGroup
	errs := make(chan error, 4)
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for attempt := 0; ; attempt++ { // SQLite allows one writer: retry a busy transaction
				err := handle(ctx, "msg-9", 120)
				if err == nil || attempt == 50 {
					errs <- err
					return
				}
				time.Sleep(5 * time.Millisecond)
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			return err
		}
	}
	var n int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM orders").Scan(&n); err != nil {
		return err
	}
	fmt.Println("  orders written:", n)
	return nil
}

// webhook posts one delivery three times to a handler with an Inbox.
func webhook(ctx context.Context) error {
	fmt.Println("webhook:")
	c := memory.New()
	runs := make(chan struct{}, 10)
	if err := c.Register(workflow.Workflow{Name: "ingest", Steps: []workflow.Step{{Name: "s", Run: func(context.Context, []byte) ([]byte, error) {
		runs <- struct{}{}
		return nil, nil
	}}}}); err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if err := c.Start(ctx); err != nil {
		return err
	}
	// snippet:webhook
	hook, err := events.NewWebhook(events.WebhookConfig{
		Unsigned: true, // a real endpoint sets a Verifier
		Client:   c, Workflow: "ingest",
		ID:    func(r *http.Request, _ []byte) string { return r.Header.Get("Webhook-Id") },
		Inbox: &workflow.InboxConsumer{Inbox: workflow.NewMemoryInbox(), Consumer: "shop-webhooks"},
	})
	// snippet:end
	if err != nil {
		return err
	}
	for range 3 {
		r := httptest.NewRequest(http.MethodPost, "/hook", bytes.NewReader([]byte(`{"order":7}`)))
		r.Header.Set("Webhook-Id", "wh-1")
		w := httptest.NewRecorder()
		hook.ServeHTTP(w, r)
		fmt.Println("  status:", w.Code)
	}
	<-runs
	time.Sleep(20 * time.Millisecond)
	fmt.Println("  runs started:", 1+len(runs))
	return nil
}
