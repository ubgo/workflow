// Command complete-in-tx-any-driver completes a step together with the
// app's own write through the core workflow.TxCompleter interface, so the
// same handler runs on the SQL driver in production (one database
// transaction) and on the memory driver in tests (a function-scoped unit
// that records the step only when the write succeeded). A declined write
// records nothing and the handler's own result stands.
//
// It is the program behind docs/reliability/complete-in-transaction.md's
// section on the core interface: the guide copies its regions (marked
// "snippet:") and task docs:snippets fails when they drift.
package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/memory"
)

func main() {
	if err := run(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, "complete-in-tx-any-driver:", err)
		os.Exit(1)
	}
}

// ledger is the app's own store: a SQL table in production, a map here.
type ledger struct {
	mu   sync.Mutex
	rows map[string]bool
}

// errDeclined is the app's refusal to write.
var errDeclined = errors.New("ledger: card declined")

// snippet:write
// record writes an invoice row in the step's unit of work: inside the
// sqlstore transaction when there is one, in the map otherwise.
func (l *ledger) record(ctx context.Context, invoice string) error {
	if tx, ok := workflow.CompletionTx[*sql.Tx](ctx); ok {
		_, err := tx.ExecContext(ctx, "INSERT INTO invoices (id) VALUES ($1)", invoice)
		return err
	}
	if invoice == "invoice-declined" {
		return errDeclined
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.rows[invoice] = true
	return nil
}

// snippet:end

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context) error {
	var c workflow.Client = memory.New() // sqlstore.New(db, dialect) in production
	l := &ledger{rows: map[string]bool{}}

	// snippet:step
	invoice := func(ctx context.Context, in []byte) ([]byte, error) {
		id := string(in)
		tc, ok := c.(workflow.TxCompleter)
		if !ok {
			return nil, workflow.Permanent(errors.New("this driver cannot complete in a transaction"))
		}
		err := tc.CompleteInTx(ctx, []byte(`"`+id+`"`), func(ctx context.Context) error {
			return l.record(ctx, id)
		})
		if err != nil {
			return []byte(`"not invoiced"`), nil // nothing was recorded: this result stands
		}
		return nil, nil // ignored: the step completed with the invoice id
	}
	// snippet:end

	if err := c.Register(workflow.Workflow{Name: "invoice", Steps: []workflow.Step{{Name: "write", Run: invoice}}}); err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if err := c.Start(ctx); err != nil {
		return err
	}
	fmt.Println("supports CompleteInTx:", workflow.Supports(c, workflow.OpCompleteInTx))
	for _, id := range []string{"invoice-42", "invoice-declined"} {
		ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "invoice", Payload: []byte(id)})
		if err != nil {
			return err
		}
		out, err := workflow.AwaitResult(ctx, c, ref, workflow.AwaitOptions{Poll: 5 * time.Millisecond})
		if err != nil {
			return err
		}
		l.mu.Lock()
		written := l.rows[id]
		l.mu.Unlock()
		fmt.Printf("%s: output %s, row written: %v\n", id, out, written)
	}
	return nil
}
