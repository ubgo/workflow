// Command scoped-data shows data shared beyond one run (WX-01): a folder
// of runs that belong together (one customer), every run of one workflow,
// and every run of every workflow. The values live in a DataStore the app
// picks; here the in-memory one, then sqlstore's DataStore on SQLite with
// two clients over one database, as two processes would hold it, so a
// counter both update loses no write.
//
// It is the program behind the "Share data across runs" section of
// docs/workflows/memoisation.md: the guide copies its regions (marked
// "snippet:") and task docs:snippets fails when they drift.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/ubgo/sqlb"
	driversqlite "github.com/ubgo/sqlb/contrib/driver-sqlite"
	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/memory"
	"github.com/ubgo/workflow/sqlstore"
)

func main() {
	if err := run(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, "scoped-data:", err)
		os.Exit(1)
	}
}

var await = workflow.AwaitOptions{Poll: 2 * time.Millisecond}

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if err := inMemory(ctx); err != nil {
		return err
	}
	return onSQLite(ctx)
}

// order is the payload of both workflows: which customer the run is for.
type order struct {
	Customer string `json:"customer"`
	Amount   int    `json:"amount"`
}

// workflows are one signup and one order workflow that share a
// customer's folder; each counts its own runs and every run is counted
// globally.
func workflows() []workflow.Workflow {
	// snippet:steps
	signup := workflow.Workflow{Name: "signup", Steps: []workflow.Step{{Name: "create", Run: func(ctx context.Context, in []byte) ([]byte, error) {
		var o order
		if err := json.Unmarshal(in, &o); err != nil {
			return nil, workflow.Permanent(err)
		}
		// Every run that names this customer's folder sees the plan.
		if err := workflow.SetData(ctx, workflow.Folder("customer/"+o.Customer), "plan", "pro"); err != nil {
			return nil, err
		}
		return nil, count(ctx)
	}}}}
	place := workflow.Workflow{Name: "order", Steps: []workflow.Step{{Name: "price", Run: func(ctx context.Context, in []byte) ([]byte, error) {
		var o order
		if err := json.Unmarshal(in, &o); err != nil {
			return nil, workflow.Permanent(err)
		}
		folder := workflow.Folder("customer/" + o.Customer)
		plan, ok, err := workflow.GetData[string](ctx, folder, "plan")
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, workflow.Permanent(errors.New("customer has no plan"))
		}
		// UpdateData reads, changes and writes back only if nobody wrote
		// in between, so concurrent runs never lose a total.
		total, err := workflow.UpdateData(ctx, folder, "spent", func(sum int, _ bool) (int, error) { return sum + o.Amount, nil })
		if err != nil {
			return nil, err
		}
		if err := count(ctx); err != nil {
			return nil, err
		}
		return json.Marshal(fmt.Sprintf("%s plan, %d spent so far", plan, total))
	}}}}
	// snippet:end
	return []workflow.Workflow{signup, place}
}

// count adds one to the running workflow's counter and to the global one.
func count(ctx context.Context) error {
	// snippet:count
	inc := func(n int, _ bool) (int, error) { return n + 1, nil }
	if _, err := workflow.UpdateData(ctx, workflow.WorkflowScope(""), "runs", inc); err != nil { // "" = this run's workflow
		return err
	}
	_, err := workflow.UpdateData(ctx, workflow.GlobalScope(), "runs", inc)
	// snippet:end
	return err
}

func inMemory(ctx context.Context) error {
	fmt.Println("memory:")
	// snippet:client
	store := workflow.NewMemoryData() // sqlstore.DataStore or redis.DataStore for a fleet
	c, err := workflow.Use(memory.New(), workflow.Data(store))
	if err != nil {
		return err
	}
	// snippet:end
	for _, wf := range workflows() {
		if err := c.Register(wf); err != nil {
			return err
		}
	}
	if err := c.Start(ctx); err != nil {
		return err
	}
	if _, err := runOne(ctx, c, "signup", order{Customer: "acme"}); err != nil {
		return err
	}
	for _, amount := range []int{40, 60} {
		out, err := runOne(ctx, c, "order", order{Customer: "acme", Amount: amount})
		if err != nil {
			return err
		}
		fmt.Println("  order:", out)
	}
	if _, err := runOne(ctx, c, "order", order{Customer: "globex", Amount: 5}); err != nil {
		var failed *workflow.RunFailedError
		fmt.Println("  globex order failed:", errors.As(err, &failed))
	}
	// snippet:outside
	rc := workflow.ContextWithData(ctx, store) // outside a step: name the workflow
	signups, _, err := workflow.GetData[int](rc, workflow.WorkflowScope("signup"), "runs")
	if err != nil {
		return err
	}
	// snippet:end
	orders, _, _ := workflow.GetData[int](rc, workflow.WorkflowScope("order"), "runs")
	all, _, _ := workflow.GetData[int](rc, workflow.GlobalScope(), "runs")
	fmt.Printf("  runs: signup %d, order %d, all %d\n", signups, orders, all)
	return nil
}

func runOne(ctx context.Context, c workflow.Client, wf string, o order) (string, error) {
	payload, err := json.Marshal(o)
	if err != nil {
		return "", err
	}
	ref, err := c.Enqueue(ctx, workflow.Job{Workflow: wf, Payload: payload})
	if err != nil {
		return "", err
	}
	raw, err := workflow.AwaitResult(ctx, c, ref, await)
	if err != nil {
		return "", err
	}
	var out string
	if len(raw) > 0 {
		err = json.Unmarshal(raw, &out)
	}
	return out, err
}

// onSQLite runs two clients over one SQLite database, as two processes
// would, both updating one folder total through sqlstore.DataStore.
func onSQLite(ctx context.Context) error {
	fmt.Println("sqlite, two clients:")
	dir, err := os.MkdirTemp("", "scoped-data-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	db, err := driversqlite.Open(filepath.Join(dir, "wf.db"))
	if err != nil {
		return err
	}
	defer db.Close()
	var clients []workflow.Client
	for i := range 2 {
		sc, err := sqlstore.New(db, sqlb.SQLiteDialect{}, sqlstore.WithPollInterval(5*time.Millisecond))
		if err != nil {
			return err
		}
		if i == 0 {
			if err := sc.Migrate(ctx); err != nil { // creates the data table too
				return err
			}
		}
		// snippet:sql
		c, err := workflow.Use(sc, workflow.Data(sc.DataStore()))
		// snippet:end
		if err != nil {
			return err
		}
		for _, wf := range workflows() {
			if err := c.Register(wf); err != nil {
				return err
			}
		}
		if err := c.Start(ctx); err != nil {
			return err
		}
		defer sc.Stop()
		clients = append(clients, c)
	}
	if _, err := runOne(ctx, clients[0], "signup", order{Customer: "acme"}); err != nil {
		return err
	}
	refs := make([]workflow.RunRef, 0, 10)
	for i := range 10 {
		payload, _ := json.Marshal(order{Customer: "acme", Amount: 10})
		ref, err := clients[i%2].Enqueue(ctx, workflow.Job{Workflow: "order", Payload: payload})
		if err != nil {
			return err
		}
		refs = append(refs, ref)
	}
	for _, ref := range refs {
		if _, err := workflow.AwaitResult(ctx, clients[0], ref, await); err != nil {
			return err
		}
	}
	store, err := sqlstore.NewDataStore(db, sqlb.SQLiteDialect{})
	if err != nil {
		return err
	}
	rc := workflow.ContextWithData(ctx, store)
	spent, _, err := workflow.GetData[int](rc, workflow.Folder("customer/acme"), "spent")
	if err != nil {
		return err
	}
	all, _, _ := workflow.GetData[int](rc, workflow.GlobalScope(), "runs")
	fmt.Printf("  acme spent %d over 10 orders; runs counted %d\n", spent, all)
	return nil
}
