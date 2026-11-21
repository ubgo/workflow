// Command events-signal shows the parts of an events.Bus that report what
// a publish did beyond starting runs: a bus built with events.WithSignal
// that also wakes a run already waiting for the event, the ReplayResult
// and its ReplayDelivery entries (a failed delivery, then the same replay
// ID run again), the Cancellation of a cancel subscription with the
// default events.UnfinishedStatuses (including the refusal of a message
// with no key), and events.ValidExtensionName, the rule that decides which
// labels cross over into a CloudEvent. Everything runs on the in-memory
// driver, so nothing needs to be installed.
//
// It is the program behind docs/events/publish-subscribe.md,
// docs/events/replay-and-cancel.md and docs/events/cloudevents.md: the
// guides copy its regions (marked "snippet:") and task docs:snippets fails
// when they drift.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/events"
	"github.com/ubgo/workflow/memory"
)

// main runs the example and exits non-zero on error.
func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "events-signal:", err)
		os.Exit(1)
	}
}

const (
	// pollEvery is how often a wait checks a run: every run here finishes
	// in microseconds on the memory driver, and a slow poll would only make
	// the program slower.
	pollEvery = 2 * time.Millisecond

	// waitLimit bounds every wait for a status, so a broken build fails the
	// test instead of hanging it.
	waitLimit = 5 * time.Second
)

// awaitOpts is the AwaitOptions every wait for a finished run uses.
var awaitOpts = workflow.AwaitOptions{Poll: pollEvery}

// The workflow names, shared by the registrations and the subscriptions
// that start them.
const (
	wfOrder   = "order"
	wfReceipt = "send-receipt"
	wfLedger  = "post-ledger"
	wfRemind  = "remind"
)

// The event names the program publishes.
const (
	evPaid       = "payment.succeeded"
	evInvoice    = "invoice.issued"
	evCheckedOut = "cart.checked-out"
)

// labelOrder is the run label a waiting order run is found by; labelCart
// is the label the cart reminders carry and the cancel subscription
// matches.
const (
	labelOrder = "order"
	labelCart  = "cart"
)

// program holds what the sections share: the client and where they print.
type program struct {
	c workflow.Client
	w io.Writer

	// shipped receives what the order run's last step got from the wait
	// step: the payload of the signal that woke it.
	shipped chan string

	// ledgerDown makes the ledger subscription's Job fail, as a ledger
	// service that is down would, so a replay has a failed delivery.
	ledgerDown atomic.Bool
}

// run is main without the process exit, so the test drives the same code
// and compares what it prints.
func run(ctx context.Context, w io.Writer) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	p := &program{c: memory.New(), w: w, shipped: make(chan string, 1)}
	// noop is the body of the steps whose only job is to finish.
	noop := func(context.Context, []byte) ([]byte, error) { return nil, nil }
	wfs := []workflow.Workflow{
		p.orderWorkflow(),
		{Name: wfReceipt, Steps: []workflow.Step{{Name: "send", Run: noop}}},
		{Name: wfLedger, Steps: []workflow.Step{{Name: "post", Run: noop}}},
		// A reminder holds until it is cancelled or the program ends.
		{Name: wfRemind, Steps: []workflow.Step{{Name: "hold", Run: func(ctx context.Context, _ []byte) ([]byte, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		}}}},
	}
	for _, wf := range wfs {
		if err := p.c.Register(wf); err != nil {
			return err
		}
	}
	if err := p.c.Start(ctx); err != nil {
		return err
	}
	for _, section := range []func(context.Context) error{p.signal, p.replay, p.cancelOnEvent, p.extensionNames} {
		if err := section(ctx); err != nil {
			return err
		}
	}
	return nil
}

// orderWorkflow waits for the payment of its own order, then ships it.
func (p *program) orderWorkflow() workflow.Workflow {
	return workflow.Workflow{Name: wfOrder, Steps: []workflow.Step{
		// snippet:wait-step
		{
			Name: "await-payment",
			Wait: &workflow.WaitSpec{
				Event:   evPaid,
				KeyFrom: workflow.KeyFunc(func(j workflow.Job) string { return j.Labels[labelOrder] }), // its own order
			},
		},
		// snippet:end
		{
			Name:  "ship",
			After: []string{"await-payment"},
			Run: func(_ context.Context, paid []byte) ([]byte, error) {
				p.shipped <- string(paid) // the wait step passes the signal's payload on
				return nil, nil
			},
		},
	}}
}

// signal publishes one event that both starts a subscriber's run and
// wakes the order run waiting for it.
func (p *program) signal(ctx context.Context) error {
	fmt.Fprintln(p.w, "signal:")
	// snippet:signal-bus
	bus, err := events.New(p.c, events.WithSignal()) // Publish also signals waiting runs
	if err != nil {
		return err
	}
	err = bus.Subscribe(events.Subscription{Name: "receipt", Event: evPaid, Workflow: wfReceipt})
	if err != nil {
		return err
	}
	// snippet:end
	a1, err := p.c.Enqueue(ctx, workflow.Job{Workflow: wfOrder, Labels: map[string]string{labelOrder: "A-1"}})
	if err != nil {
		return err
	}
	a2, err := p.c.Enqueue(ctx, workflow.Job{Workflow: wfOrder, Labels: map[string]string{labelOrder: "A-2"}})
	if err != nil {
		return err
	}
	for _, ref := range []workflow.RunRef{a1, a2} {
		if err := p.awaitStatus(ctx, ref, workflow.StatusWaiting); err != nil {
			return err
		}
	}
	// snippet:signal-publish
	pub, err := bus.Publish(ctx, events.Message{
		ID:      "pay_1",
		Name:    evPaid,
		Key:     "A-1", // the key the waiting run matches: its order
		Payload: []byte(`{"order":"A-1","cents":4000}`),
	})
	if err != nil {
		return err // a failed signal is joined in here; Signalled is then false
	}
	fmt.Fprintf(p.w, "  signalled: %v, subscriber runs started: %d\n", pub.Signalled, len(pub.Deliveries))
	// snippet:end
	if _, err := workflow.Await(ctx, p.c, pub.Deliveries[0].Ref, awaitOpts); err != nil {
		return err
	}
	if _, err := workflow.Await(ctx, p.c, a1, awaitOpts); err != nil {
		return err
	}
	fmt.Fprintf(p.w, "  order A-1 woke and shipped with %s\n", <-p.shipped)
	st, err := p.c.Status(ctx, a2)
	if err != nil {
		return err
	}
	fmt.Fprintf(p.w, "  order A-2 is still %s: the event's key was A-1\n", st)
	return nil
}

// replay replays logged events while one subscriber is down, then again
// under the same replay ID once it is back.
func (p *program) replay(ctx context.Context) error {
	fmt.Fprintln(p.w, "replay result:")
	bus, err := events.New(p.c, events.WithLog(events.NewMemoryLog()))
	if err != nil {
		return err
	}
	err = errors.Join(
		bus.Subscribe(events.Subscription{Name: "receipt", Event: evInvoice, Workflow: wfReceipt}),
		bus.Subscribe(events.Subscription{Name: "ledger", Event: evInvoice, Job: func(m events.Message) (workflow.Job, error) {
			if p.ledgerDown.Load() {
				return workflow.Job{}, errors.New("ledger unavailable")
			}
			return workflow.Job{Workflow: wfLedger, Payload: m.Payload}, nil
		}}),
	)
	if err != nil {
		return err
	}
	for _, id := range []string{"inv_1", "inv_2"} {
		if _, err := bus.Publish(ctx, events.Message{ID: id, Name: evInvoice}); err != nil {
			return err
		}
	}
	p.ledgerDown.Store(true)
	// snippet:replay-result
	r, err := bus.Replay(ctx, events.ReplayOptions{ID: "backfill-1"}) // zero From and To: the whole log
	fmt.Fprintf(p.w, "  replay %s: %d messages, %d deliveries, error: %v\n", r.ID, r.Messages, len(r.Deliveries), err != nil)
	for _, d := range r.Deliveries {
		fmt.Fprintf(p.w, "    %s -> %-7s existing=%-5v failed=%v\n", d.MessageID, d.Subscription, d.Existing, d.Err != nil)
	}
	// snippet:end
	if err := p.awaitDeliveries(ctx, r); err != nil {
		return err
	}
	p.ledgerDown.Store(false)
	fmt.Fprintln(p.w, "  the ledger is back; the same replay ID again:")
	// snippet:replay-again
	again, err := bus.Replay(ctx, events.ReplayOptions{ID: "backfill-1"}) // retries only what failed
	if err != nil {
		return err
	}
	for _, d := range again.Deliveries {
		fmt.Fprintf(p.w, "    %s -> %-7s existing=%-5v failed=%v\n", d.MessageID, d.Subscription, d.Existing, d.Err != nil)
	}
	// snippet:end
	return p.awaitDeliveries(ctx, again)
}

// cancelOnEvent cancels a running reminder through a cancel subscription
// whose key comes from the message's labels, then publishes a message
// without that label.
func (p *program) cancelOnEvent(ctx context.Context) error {
	fmt.Fprintln(p.w, "cancellation:")
	names := make([]string, len(events.UnfinishedStatuses))
	for i, s := range events.UnfinishedStatuses {
		names[i] = string(s)
	}
	fmt.Fprintf(p.w, "  default statuses: %s\n", strings.Join(names, ", "))
	bus, err := events.New(p.c)
	if err != nil {
		return err
	}
	reminder, err := p.c.Enqueue(ctx, workflow.Job{Workflow: wfRemind, Labels: map[string]string{labelCart: "c1"}})
	if err != nil {
		return err
	}
	if err := p.awaitStatus(ctx, reminder, workflow.StatusRunning); err != nil {
		return err
	}
	// snippet:cancellation
	err = bus.SubscribeCancel(events.CancelSubscription{
		Name:  "stop-reminders",
		Event: evCheckedOut,
		Label: labelCart,
		Key:   func(m events.Message) string { return m.Labels[labelCart] }, // not m.Key
	})
	if err != nil {
		return err
	}
	pub, err := bus.Publish(ctx, events.Message{ID: "chk_1", Name: evCheckedOut, Labels: map[string]string{labelCart: "c1"}})
	if err != nil {
		return err
	}
	cn := pub.Cancellations[0]
	fmt.Fprintf(p.w, "  %s: cancelled %d, diverted %v, err %v\n", cn.Subscription, cn.Cancelled, !cn.Diverted.IsZero(), cn.Err)
	// snippet:end
	// snippet:cancellation-nokey
	pub, err = bus.Publish(ctx, events.Message{ID: "chk_2", Name: evCheckedOut}) // no cart label
	fmt.Fprintf(p.w, "  no key: cancelled %d, Cancellation.Err is ErrInvalid: %v, Publish error: %v\n",
		pub.Cancellations[0].Cancelled, errors.Is(pub.Cancellations[0].Err, workflow.ErrInvalid), err != nil)
	// snippet:end
	return nil
}

// extensionNames shows which names may name a CloudEvents extension, and
// so which message labels FromMessage carries over.
func (p *program) extensionNames(context.Context) error {
	fmt.Fprintln(p.w, "extension names:")
	// snippet:extension-names
	for _, name := range []string{"tenant", "region2", "tenant-id", "Tenant", "subject", ""} {
		fmt.Fprintf(p.w, "  %-12q valid=%v\n", name, events.ValidExtensionName(name))
	}
	ce := events.FromMessage(events.Message{ID: "evt_1", Name: "order.paid",
		Labels: map[string]string{"tenant": "acme", "tenant-id": "7", "subject": "x"}}, "https://shop.example/orders")
	fmt.Fprintf(p.w, "  FromMessage kept: %v\n", ce.Extensions) // the other two labels have no CloudEvents spelling
	// snippet:end
	return nil
}

// awaitDeliveries waits for every run a replay started or found.
func (p *program) awaitDeliveries(ctx context.Context, r events.ReplayResult) error {
	for _, d := range r.Deliveries {
		if d.Err != nil {
			continue
		}
		if _, err := workflow.Await(ctx, p.c, d.Ref, awaitOpts); err != nil {
			return err
		}
	}
	return nil
}

// awaitStatus polls ref until it reaches want or waitLimit passes.
func (p *program) awaitStatus(ctx context.Context, ref workflow.RunRef, want workflow.Status) error {
	ctx, cancel := context.WithTimeout(ctx, waitLimit)
	defer cancel()
	for {
		st, err := p.c.Status(ctx, ref)
		if err == nil && st == want {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("run %v did not reach %s (last %s, %v)", ref, want, st, err)
		case <-time.After(awaitOpts.Poll):
		}
	}
}
