// Command workflow-json runs a workflow whose definition is data. It
// first checks a misspelled document with dsl.Parse, which refuses it
// with a *dsl.FieldError naming the field's JSON path. A JSON
// document names its steps, their order, a wait for an event, retries and
// timeouts, and the Go handlers it runs by name. The program registers
// those handlers in a dsl.Registry, loads the document with dsl.Load,
// registers the result on the in-memory driver like any workflow, and runs
// two orders through it. It then shows the refusal of a definition naming
// a handler nobody registered, and a Marshal and Parse round trip.
//
// It is the program behind docs/workflows/definitions-as-data.md.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/dsl"
	"github.com/ubgo/workflow/memory"
)

// runTimeout bounds the whole program, generous for a slow CI host.
const runTimeout = 10 * time.Second

// pollEvery is how often the program re-reads a run's status while it
// waits: a millisecond keeps the in-memory demo quick.
const pollEvery = time.Millisecond

// orders are the IDs of the two orders the program runs through the
// workflow, in the order they are enqueued and printed.
var orders = []string{"A-1", "A-2"}

// snippet:definition
// orderJSON is the definition as an app would keep it in a file, a
// database row or a DataStore entry. Handler names refer to Go code the
// program registers; the document itself holds no code.
const orderJSON = `{
  "name": "order",
  "timeout": "30s",
  "retry": {"max": 2, "backoff": {"initial": "1s", "factor": 2}},
  "steps": [
    {"name": "reserve", "handler": "reserve-stock"},
    {"name": "await-payment", "after": ["reserve"],
     "wait": {"event": "payment.succeeded", "key_label": "order", "timeout": "72h"}},
    {"name": "ship", "after": ["await-payment"], "handler": "ship-parcel",
     "retry": {"max": 5, "backoff": {"initial": "10s", "factor": 2, "max": "5m"}}}
  ]
}`

// snippet:end

// payment is the payload of the payment.succeeded event, which the wait
// step passes on to "ship".
type payment struct {
	Order string
	Paid  int
}

// main runs the example and exits non-zero on error.
func main() {
	if err := run(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, "workflow-json:", err)
		os.Exit(1)
	}
}

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, runTimeout)
	defer cancel()

	// snippet:check
	// Parse checks a document without any handler: a typo is refused with
	// the JSON path of the field, which a form can show next to it.
	_, err := dsl.Parse([]byte(`{"name": "order", "steps": [{"name": "reserve", "handlr": "reserve-stock"}]}`))
	var fe *dsl.FieldError
	if errors.As(err, &fe) {
		fmt.Printf("checked: %s: %s\n", fe.Path, fe.Reason)
	}
	// snippet:end
	if fe == nil {
		return fmt.Errorf("a misspelled field was not refused with a FieldError: %v", err)
	}

	// snippet:registry
	reg := dsl.NewRegistry()
	if err := reg.Register("reserve-stock", workflow.Define(func(ctx context.Context, order string) (string, error) {
		return order, nil // reserve the stock here
	})); err != nil {
		return err
	}
	if err := reg.Register("ship-parcel", workflow.Define(func(ctx context.Context, p payment) (string, error) {
		return fmt.Sprintf("shipped %s, paid %d", p.Order, p.Paid), nil
	})); err != nil {
		return err
	}
	// snippet:end

	// snippet:load
	wf, err := dsl.Load([]byte(orderJSON), reg) // Parse, then Build against reg
	if err != nil {
		return err // wraps workflow.ErrInvalid and names the place
	}
	c := memory.New()
	defer c.Stop()
	if err := c.Register(wf); err != nil { // like a workflow written in Go
		return err
	}
	// snippet:end
	fmt.Printf("loaded %q:", wf.Name)
	for _, s := range wf.Steps {
		fmt.Printf(" %s (%s)", s.Name, s.Kind())
	}
	fmt.Println()
	if err := c.Start(ctx); err != nil {
		return err
	}

	refs := map[string]workflow.RunRef{}
	for _, id := range orders {
		ref, err := c.Enqueue(ctx, workflow.Job{
			Workflow: "order",
			Payload:  fmt.Appendf(nil, "%q", id),
			Labels:   map[string]string{"order": id}, // the wait's key_label
		})
		if err != nil {
			return err
		}
		refs[id] = ref
	}
	for _, id := range orders {
		if err := untilWaiting(ctx, c, refs[id]); err != nil {
			return err
		}
	}
	fmt.Println("both orders waiting for payment")
	for _, p := range []payment{{"A-2", 25}, {"A-1", 40}} { // paid out of order: each wait takes only its own order's payment
		if err := c.Signal(ctx, workflow.Event{Name: "payment.succeeded", Key: p.Order, Payload: fmt.Appendf(nil, `{"Order":%q,"Paid":%d}`, p.Order, p.Paid)}); err != nil {
			return err
		}
	}
	for _, id := range orders {
		out, err := workflow.AwaitResult(ctx, c, refs[id], workflow.AwaitOptions{Poll: pollEvery})
		if err != nil {
			return err
		}
		fmt.Printf("%s: %s\n", id, out)
	}

	// snippet:refusal
	_, err = dsl.Load([]byte(`{"name": "order", "steps": [
	  {"name": "reserve", "handler": "reserve-stock"},
	  {"name": "ship", "after": ["reserve"], "handler": "ship-order"}]}`), reg)
	fmt.Println("refused:", err)
	// snippet:end

	// snippet:round-trip
	def, err := dsl.Parse([]byte(orderJSON))
	if err != nil {
		return err
	}
	stored, err := dsl.Marshal(def) // indented JSON, ready to store
	if err != nil {
		return err
	}
	back, err := dsl.Parse(stored)
	if err != nil {
		return err
	}
	fmt.Println("round trip equal:", reflect.DeepEqual(def, back))
	// snippet:end
	return nil
}

// untilWaiting polls until the run is parked on its wait step: steps move
// on their own goroutines.
func untilWaiting(ctx context.Context, c workflow.Client, ref workflow.RunRef) error {
	for {
		st, err := c.Status(ctx, ref)
		if err != nil {
			return err
		}
		if st == workflow.StatusWaiting {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("run %s never reached waiting: %w", ref.RunID, ctx.Err())
		case <-time.After(pollEvery):
		}
	}
}
