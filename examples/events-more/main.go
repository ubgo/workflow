// Command events-more shows what sits around an events.Bus: CloudEvents
// in and out over HTTP, payload versions upcast before subscribers see
// them, an event log with replay, a subscription that cancels runs or
// diverts them to another workflow, and workflow.Then, which starts a
// workflow when another finishes. Everything runs on the in-memory
// driver, so nothing needs to be installed.
//
// It is the program behind docs/events/cloudevents.md,
// docs/events/replay-and-cancel.md and docs/workflows/then.md: the guides
// copy its regions (marked "snippet:") and task docs:snippets fails when
// they drift.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/events"
	"github.com/ubgo/workflow/memory"
)

const (
	// pollEvery is how often the waits below look at a run again: short,
	// so the program finishes quickly on the in-memory driver.
	pollEvery = 2 * time.Millisecond
	// waitFor bounds every wait, so a run that never reaches the state the
	// program expects fails the program instead of hanging it.
	waitFor = 5 * time.Second
)

// main runs the example and exits non-zero on error.
func main() {
	if err := run(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, "events-more:", err)
		os.Exit(1)
	}
}

// inbox records what each workflow ran with, in order.
type inbox struct {
	mu  sync.Mutex
	got map[string][]string
}

// add records that workflow wf ran with payload.
func (in *inbox) add(wf, payload string) {
	in.mu.Lock()
	defer in.mu.Unlock()
	if in.got == nil {
		in.got = map[string][]string{}
	}
	in.got[wf] = append(in.got[wf], payload)
}

// of returns a copy of the payloads workflow wf ran with, oldest first.
func (in *inbox) of(wf string) []string {
	in.mu.Lock()
	defer in.mu.Unlock()
	return slices.Clone(in.got[wf])
}

// record is a one-step workflow that records its payload; it fails when
// the payload says so.
func record(in *inbox, name string) workflow.Workflow {
	return workflow.Workflow{Name: name, Steps: []workflow.Step{{Name: "s", Run: func(ctx context.Context, p []byte) ([]byte, error) {
		in.add(name, string(p))
		if strings.Contains(string(p), "bad") {
			return nil, workflow.Permanent(errors.New("unreadable file"))
		}
		return nil, nil
	}}}}
}

// hold is a workflow whose runs keep running until the program ends, as
// a long fulfilment would.
func hold(name string) workflow.Workflow {
	return workflow.Workflow{Name: name, Steps: []workflow.Step{{Name: "s", Run: func(ctx context.Context, _ []byte) ([]byte, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}}}}
}

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	in := &inbox{}

	// snippet:then
	then, err := workflow.Then([]workflow.ThenRule{
		{When: "import", Start: func(ev workflow.RunEvent) (workflow.Job, bool, error) {
			return workflow.Job{Workflow: "index", Payload: []byte(ev.Labels["file"])}, true, nil
		}},
		{Name: "import-failed", When: "import", On: []workflow.Status{workflow.StatusFailed},
			Start: func(ev workflow.RunEvent) (workflow.Job, bool, error) {
				return workflow.Job{Workflow: "alert", Payload: []byte(ev.Labels["file"])}, true, nil // ev.Err says why
			}},
	})
	if err != nil {
		return err
	}
	c, err := workflow.Use(memory.New(), then) // before Register and Start
	if err != nil {
		return err
	}
	// snippet:end

	wfs := []workflow.Workflow{record(in, "receipt"), record(in, "import"), record(in, "index"), record(in, "alert"),
		record(in, "refund"), record(in, "crm"), hold("fulfil"), hold("remind")}
	for _, wf := range wfs {
		if err := c.Register(wf); err != nil {
			return err
		}
	}
	if err := c.Start(ctx); err != nil {
		return err
	}
	// sections run in order on the one client, each printing its block.
	sections := []func(context.Context, workflow.Client, *inbox) error{cloudEvents, versions, replay, cancelOnEvent, divert, thenSection}
	for _, s := range sections {
		if err := s(ctx, c, in); err != nil {
			return err
		}
	}
	return nil
}

// cloudEvents sends a CloudEvent over HTTP in binary mode, publishes it on
// the bus and writes the message back out in structured JSON.
func cloudEvents(ctx context.Context, c workflow.Client, in *inbox) error {
	b, err := events.New(c)
	if err != nil {
		return err
	}
	if err := b.Subscribe(events.Subscription{Name: "receipt", Event: "order.paid", Workflow: "receipt"}); err != nil {
		return err
	}
	// snippet:cloudevents-send
	ce := events.CloudEvent{SpecVersion: events.CloudEventsSpecVersion, ID: "evt_1", Source: "https://shop.example/orders",
		Type: "order.paid", Subject: "42", DataContentType: "application/json",
		Data: []byte(`{"order":42,"cents":1999,"currency":"EUR"}`), Extensions: map[string]string{"tenant": "acme"}}
	h := http.Header{}
	body, err := ce.WriteHTTP(h) // binary mode: ce-* headers, Data as the body
	if err != nil {
		return err
	}
	req := httptest.NewRequest(http.MethodPost, "/events", bytes.NewReader(body))
	req.Header = h
	// snippet:end
	fmt.Println("cloudevents:")
	fmt.Printf("  headers: ce-id=%s ce-type=%s ce-subject=%s ce-tenant=%s\n", h.Get("ce-id"), h.Get("ce-type"), h.Get("ce-subject"), h.Get("ce-tenant"))

	// snippet:cloudevents-receive
	got, err := events.ReadHTTP(req) // binary or structured mode; refusals wrap ErrInvalid
	if err != nil {
		return err
	}
	m := events.ToMessage(got) // ID, Type→Name, Subject→Key, Data→Payload, extensions→Labels
	p, err := b.Publish(ctx, m)
	if err != nil {
		return err
	}
	// snippet:end
	if err := awaitAll(ctx, c, p.Deliveries); err != nil {
		return err
	}
	fmt.Printf("  message: name=%s key=%s labels=%v\n", m.Name, m.Key, m.Labels)
	fmt.Printf("  receipt ran with %s\n", in.of("receipt")[0])

	// snippet:cloudevents-json
	out, err := json.Marshal(events.FromMessage(m, "https://shop.example/orders"))
	// snippet:end
	if err != nil {
		return err
	}
	fmt.Printf("  structured: %s\n", out)
	return nil
}

// versions upcasts an old order.paid payload before the subscriber sees it
// and shows a version newer than the bus knows refused. orderV0 had
// "amount"; v1 renamed it "cents"; v2 added "currency".
func versions(ctx context.Context, c workflow.Client, in *inbox) error {
	// snippet:versions
	b, err := events.New(c, events.WithUpcasters("order.paid", 2, map[int]workflow.Upcaster{ // 2: the current version
		0: func(raw []byte) ([]byte, error) { // v0 -> v1: "amount" is now "cents"
			return bytes.Replace(raw, []byte(`"amount"`), []byte(`"cents"`), 1), nil
		},
		1: func(raw []byte) ([]byte, error) { // v1 -> v2: add the currency, EUR before v2
			return bytes.Replace(raw, []byte(`}`), []byte(`,"currency":"EUR"}`), 1), nil
		},
	}))
	if err != nil {
		return err
	}
	// snippet:end
	if err := b.Subscribe(events.Subscription{Name: "receipt", Event: "order.paid", Workflow: "receipt"}); err != nil {
		return err
	}
	// snippet:versions-publish
	old := events.Message{ID: "evt_v0", Name: "order.paid", Version: 0, Payload: []byte(`{"order":7,"amount":500}`)}
	p, err := b.Publish(ctx, old) // the receipt run gets {"order":7,"cents":500,"currency":"EUR"}
	if err != nil {
		return err
	}
	_, future := b.Publish(ctx, events.Message{ID: "evt_v3", Name: "order.paid", Version: 3}) // newer than the current version
	// snippet:end
	if err := awaitAll(ctx, c, p.Deliveries); err != nil {
		return err
	}
	fmt.Println("versions:")
	receipts := in.of("receipt")
	fmt.Printf("  v0 message reached the receipt run as %s\n", receipts[len(receipts)-1])
	fmt.Printf("  v3 message refused: ErrInvalid=%v\n", errors.Is(future, workflow.ErrInvalid))
	return nil
}

// replay logs three invoices an hour apart and replays the two from the
// hour the receipt mailer was down, then shows the same replay ID start
// nothing twice.
func replay(ctx context.Context, c workflow.Client, in *inbox) error {
	clk := clock.NewFake(time.Date(2026, 11, 20, 9, 0, 0, 0, time.UTC))
	// snippet:log
	log := events.NewMemoryLog() // in-process; a durable EventLog for a fleet
	b, err := events.New(c, events.WithLog(log), events.WithClock(clk))
	if err != nil {
		return err
	}
	// snippet:end
	err = errors.Join(
		b.Subscribe(events.Subscription{Name: "receipt", Event: "invoice.issued", Workflow: "receipt"}),
		b.Subscribe(events.Subscription{Name: "crm", Event: "invoice.issued", Workflow: "crm"}),
	)
	if err != nil {
		return err
	}
	var refs []workflow.RunRef
	invoices := []string{"inv_1", "inv_2", "inv_3"}
	for i, id := range invoices {
		p, err := b.Publish(ctx, events.Message{ID: id, Name: "invoice.issued", Payload: []byte(`"` + id + `"`)})
		if err != nil {
			return err
		}
		for _, d := range p.Deliveries {
			refs = append(refs, d.Ref)
		}
		if i < len(invoices)-1 { // an hour between invoices, none after the last
			clk.Advance(time.Hour)
		}
	}
	if err := awaitRefs(ctx, c, refs); err != nil {
		return err
	}
	before := len(in.of("receipt"))
	// snippet:replay
	from := time.Date(2026, 11, 20, 10, 0, 0, 0, time.UTC) // the hour the receipt mailer was down
	const outage = 2 * time.Hour                           // how long it stayed down
	r, err := b.Replay(ctx, events.ReplayOptions{
		ID:            "mailer-outage-2026-11-20",
		From:          from,
		To:            from.Add(outage),
		Subscriptions: []string{"receipt"},
	})
	if err != nil {
		return err
	}
	// snippet:end
	refs = refs[:0]
	for _, d := range r.Deliveries {
		refs = append(refs, d.Ref)
	}
	if err := awaitRefs(ctx, c, refs); err != nil {
		return err
	}
	fmt.Println("replay:")
	replayed := in.of("receipt")[before:]
	slices.Sort(replayed) // the two runs ran concurrently
	fmt.Printf("  log holds %d messages; replayed %d to receipt: %v\n", log.Len(), r.Messages, replayed)
	again, err := b.Replay(ctx, events.ReplayOptions{ID: "mailer-outage-2026-11-20", From: from, To: from.Add(outage), Subscriptions: []string{"receipt"}})
	if err != nil {
		return err
	}
	existing := 0
	for _, d := range again.Deliveries {
		if d.Existing {
			existing++
		}
	}
	fmt.Printf("  same replay ID again: %d of %d already started, new receipt runs %d\n", existing, len(again.Deliveries), len(in.of("receipt"))-before-len(replayed))
	return nil
}

// cancelOnEvent cancels the reminder runs of the cart a checkout event
// names and leaves the other cart's reminder running.
func cancelOnEvent(ctx context.Context, c workflow.Client, _ *inbox) error {
	b, err := events.New(c)
	if err != nil {
		return err
	}
	// snippet:cancel
	err = b.SubscribeCancel(events.CancelSubscription{
		Name:  "stop-cart-reminders",
		Event: "cart.checked-out",
		Label: "cart", // cancels every unfinished run labelled cart=<the message's Key>
	})
	if err != nil {
		return err
	}
	// snippet:end
	r1, err := c.Enqueue(ctx, workflow.Job{Workflow: "remind", Labels: map[string]string{"cart": "c1"}})
	if err != nil {
		return err
	}
	r2, err := c.Enqueue(ctx, workflow.Job{Workflow: "remind", Labels: map[string]string{"cart": "c2"}})
	if err != nil {
		return err
	}
	// Both reminders must be running before the checkout, so the c2 line
	// below reads "running" and not a status it held for a moment before.
	for _, r := range []workflow.RunRef{r1, r2} {
		if err := awaitStatus(ctx, c, r, workflow.StatusRunning); err != nil {
			return err
		}
	}
	// snippet:cancel-publish
	p, err := b.Publish(ctx, events.Message{ID: "chk_1", Name: "cart.checked-out", Key: "c1"})
	if err != nil {
		return err
	}
	fmt.Println("cancel on event:")
	fmt.Printf("  %s cancelled %d run(s)\n", p.Cancellations[0].Subscription, p.Cancellations[0].Cancelled)
	// snippet:end
	if err := awaitStatus(ctx, c, r1, workflow.StatusCancelled); err != nil {
		return err
	}
	st, err := c.Status(ctx, r2)
	if err != nil {
		return err
	}
	fmt.Printf("  cart c1 reminder: cancelled; cart c2 reminder: %s\n", st)
	return nil
}

// divert cancels an order's fulfilment run when the order is cancelled,
// starts a refund in its place, and shows a republish start no second one.
func divert(ctx context.Context, c workflow.Client, in *inbox) error {
	b, err := events.New(c)
	if err != nil {
		return err
	}
	// snippet:divert
	err = b.SubscribeCancel(events.CancelSubscription{
		Name:   "cancel-to-refund",
		Event:  "order.cancelled",
		Label:  "order",
		Divert: "refund", // after the cancel, start refund with the message's payload
	})
	if err != nil {
		return err
	}
	// snippet:end
	run, err := c.Enqueue(ctx, workflow.Job{Workflow: "fulfil", Labels: map[string]string{"order": "42"}})
	if err != nil {
		return err
	}
	if err := awaitStatus(ctx, c, run, workflow.StatusRunning); err != nil {
		return err
	}
	msg := events.Message{ID: "cnl_1", Name: "order.cancelled", Key: "42", Payload: []byte(`{"order":42}`), Labels: map[string]string{"order": "42"}}
	p, err := b.Publish(ctx, msg)
	if err != nil {
		return err
	}
	cn := p.Cancellations[0]
	if err := awaitRefs(ctx, c, []workflow.RunRef{cn.Diverted}); err != nil {
		return err
	}
	fmt.Println("divert:")
	fmt.Printf("  cancelled %d fulfilment run(s); refund ran with %s\n", cn.Cancelled, in.of("refund")[0])
	again, err := b.Publish(ctx, msg) // a retried publish
	if err != nil {
		return err
	}
	fmt.Printf("  republish: cancelled %d, same refund run %v, refund runs %d\n", again.Cancellations[0].Cancelled, again.Cancellations[0].Diverted == cn.Diverted, len(in.of("refund")))
	return nil
}

// thenSection shows workflow.Then starting index after a successful import
// and alert after a failed one.
func thenSection(ctx context.Context, c workflow.Client, in *inbox) error {
	for _, file := range []string{"ok.csv", "bad.csv"} {
		if _, err := c.Enqueue(ctx, workflow.Job{Workflow: "import", Payload: []byte(file), Labels: map[string]string{"file": file}}); err != nil {
			return err
		}
	}
	if err := waitUntil(ctx, func() bool { return len(in.of("index")) == 1 && len(in.of("alert")) == 1 }); err != nil {
		return err
	}
	fmt.Println("then:")
	idx := in.of("index")
	fmt.Printf("  import ok.csv succeeded -> index ran with %s\n", idx[len(idx)-1])
	fmt.Printf("  import bad.csv failed -> alert ran with %s\n", in.of("alert")[0])
	return nil
}

// awaitAll waits for every delivery's run to finish.
func awaitAll(ctx context.Context, c workflow.Client, ds []events.Delivery) error {
	refs := make([]workflow.RunRef, len(ds))
	for i, d := range ds {
		refs[i] = d.Ref
	}
	return awaitRefs(ctx, c, refs)
}

// awaitRefs waits for every run to finish.
func awaitRefs(ctx context.Context, c workflow.Client, refs []workflow.RunRef) error {
	for _, ref := range refs {
		if _, err := workflow.Await(ctx, c, ref, workflow.AwaitOptions{Poll: pollEvery}); err != nil {
			return err
		}
	}
	return nil
}

// awaitStatus waits for ref to reach want.
func awaitStatus(ctx context.Context, c workflow.Client, ref workflow.RunRef, want workflow.Status) error {
	return waitUntil(ctx, func() bool {
		st, err := c.Status(ctx, ref)
		return err == nil && st == want
	})
}

// waitUntil polls cond every pollEvery for up to waitFor.
func waitUntil(ctx context.Context, cond func() bool) error {
	for range int(waitFor / pollEvery) {
		if cond() {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(pollEvery):
		}
	}
	return errors.New("timed out")
}
