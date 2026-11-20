// Command saga-choreography runs a saga with no orchestrator: three
// services (order, payment, inventory) that only react to events and
// publish their own, compensation events included, on one events.Bus with
// events.WithCorrelation. A paid order runs the happy path; a declined
// card makes the payment service publish payment.failed, the order
// service compensate with order.cancelled, and the inventory service
// release what it reserved. events.Trail then lists each order's runs as
// one unit, and the causation labels show which event led to which.
// Everything runs on the in-memory driver.
//
// It is the program behind docs/events/choreography.md: the guide copies
// its regions (marked "snippet:") and task docs:snippets fails when they
// drift.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"
	"sync/atomic"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/events"
	"github.com/ubgo/workflow/memory"
)

func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "saga-choreography:", err)
		os.Exit(1)
	}
}

// order is the payload every event of the choreography carries.
type order struct {
	ID   int    `json:"id"`
	Card string `json:"card"`
}

// stock counts what the inventory service reserved and released.
type stock struct{ reserved, released atomic.Int64 }

// service is a one-step workflow that does its work and returns the name
// of the event to publish next ("" for none).
type service func(ctx context.Context, o order) (next string, err error)

// snippet:service
// react turns a service into a workflow whose step publishes the next
// event through the bus. The event's ID is derived from the event that
// started this run, so a retried step publishes the same ID again and the
// bus starts nothing twice; WithCorrelation reads the chain from ctx.
func react(bus *events.Bus, name string, do service) workflow.Workflow {
	return workflow.Workflow{Name: name, Steps: []workflow.Step{{Name: "react", Run: func(ctx context.Context, p []byte) ([]byte, error) {
		var o order
		if err := json.Unmarshal(p, &o); err != nil {
			return nil, workflow.Permanent(err)
		}
		next, err := do(ctx, o)
		if err != nil || next == "" {
			return nil, err
		}
		info, _ := workflow.StepInfoFrom(ctx)
		id := next + "/" + info.Labels[events.LabelEventID]
		_, err = bus.Publish(ctx, events.Message{ID: id, Name: next, Payload: p})
		return nil, err
	}}}}
}

// snippet:end

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context, w io.Writer) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	c := memory.New()

	// snippet:bus
	bus, err := events.New(c, events.WithCorrelation()) // every run gets the chain's labels
	if err != nil {
		return err
	}
	subs := []events.Subscription{
		{Name: "payment-charge", Event: "order.placed", Workflow: "charge"},
		{Name: "inventory-reserve", Event: "order.placed", Workflow: "reserve"},
		{Name: "order-confirm", Event: "payment.captured", Workflow: "confirm-order"},
		{Name: "order-compensate", Event: "payment.failed", Workflow: "cancel-order"},
		{Name: "inventory-release", Event: "order.cancelled", Workflow: "release"},
	}
	for _, s := range subs {
		if err := bus.Subscribe(s); err != nil {
			return err
		}
	}
	// snippet:end

	var inv stock
	services := []workflow.Workflow{
		// The payment service: charge the card, say how it went.
		react(bus, "charge", func(_ context.Context, o order) (string, error) {
			if o.Card == "declined" {
				return "payment.failed", nil
			}
			return "payment.captured", nil
		}),
		// The order service: confirm, or compensate a failed payment.
		react(bus, "confirm-order", func(context.Context, order) (string, error) { return "", nil }),
		react(bus, "cancel-order", func(context.Context, order) (string, error) { return "order.cancelled", nil }),
		// The inventory service: reserve on placement, release on cancel.
		react(bus, "reserve", func(context.Context, order) (string, error) { inv.reserved.Add(1); return "", nil }),
		react(bus, "release", func(context.Context, order) (string, error) { inv.released.Add(1); return "", nil }),
	}
	for _, wf := range services {
		if err := c.Register(wf); err != nil {
			return err
		}
	}
	if err := c.Start(ctx); err != nil {
		return err
	}

	for _, o := range []order{{ID: 1001, Card: "ok"}, {ID: 1002, Card: "declined"}} {
		payload, err := json.Marshal(o)
		if err != nil {
			return err
		}
		// snippet:publish
		_, err = bus.Publish(ctx, events.Message{
			ID:          fmt.Sprintf("o%d", o.ID),
			Name:        "order.placed",
			Correlation: fmt.Sprintf("order-%d", o.ID), // optional: the chain's ID; the message ID by default
			Payload:     payload,
		})
		if err != nil {
			return err
		}
		// snippet:end
		rep, err := c.RunAll(ctx) // every service reacts, hop by hop
		if err != nil {
			return err
		}
		fmt.Fprintf(w, "order %d (card %s): %d runs succeeded, %d failed\n", o.ID, o.Card, rep.Succeeded, rep.Failed)
	}
	fmt.Fprintf(w, "inventory: reserved %d, released %d\n", inv.reserved.Load(), inv.released.Load())

	for _, corr := range []string{"order-1001", "order-1002"} {
		// snippet:trail
		runs, err := events.Trail(ctx, c, corr, workflow.RunQuery{})
		if err != nil {
			return err
		}
		// snippet:end
		fmt.Fprintf(w, "trail %s (%d runs):\n", corr, len(runs))
		printTree(w, runs)
	}
	return nil
}

// printTree prints a trail as the events it went through: each event with
// the runs it started, and under it the events those runs published,
// linked by LabelCausation. Runs and events are sorted, so the output does
// not depend on the order the runs finished in.
func printTree(w io.Writer, runs []workflow.RunInfo) {
	byEvent := map[string][]workflow.RunInfo{} // event ID -> runs it started
	children := map[string][]string{}          // event ID -> events published by its runs
	for _, r := range runs {
		id := r.Labels[events.LabelEventID]
		if len(byEvent[id]) == 0 {
			cause := r.Labels[events.LabelCausation]
			children[cause] = append(children[cause], id)
		}
		byEvent[id] = append(byEvent[id], r)
	}
	var walk func(cause string, depth int)
	walk = func(cause string, depth int) {
		for _, id := range slices.Sorted(slices.Values(children[cause])) {
			started := byEvent[id]
			names := make([]string, len(started))
			for i, r := range started {
				names[i] = r.Workflow + " " + string(r.Status)
			}
			slices.Sort(names)
			fmt.Fprintf(w, "%s%s %s -> %s\n", strings.Repeat("  ", depth+1), started[0].Labels[events.LabelEvent], id, strings.Join(names, ", "))
			walk(id, depth+1)
		}
	}
	walk("", 0)
}
