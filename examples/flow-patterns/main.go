// Command flow-patterns shows the control-flow building blocks made of
// ordinary steps: trigger rules over tolerated failures with an exit
// handler, a saga that compensates, a multi-choice, a race decided by the
// first signal, a polling sensor, a human approval with a signed link, a
// step group, an early successful end, and a recursion limit; then a
// saga across parallel steps, a local merge over multi-choice branches,
// a race whose default branch a delayed deadline job answers, a durable
// sleep until a time, a typed enqueue and result, and a shape guard
// catching a renamed step during a rolling deploy. Everything runs on the
// in-memory driver.
//
// It is the program behind docs/workflows/trigger-rules.md,
// docs/workflows/sagas.md, docs/workflows/multi-choice-and-race.md,
// docs/workflows/polling-and-approval.md and
// docs/workflows/groups-and-guards.md: the guides copy its regions
// (marked "snippet:") and task docs:snippets fails when they drift.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/memory"
)

// main runs the example and exits non-zero on error.
func main() {
	if err := run(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, "flow-patterns:", err)
		os.Exit(1)
	}
}

const (
	// pollEvery is how often the program checks a run or the log: every
	// run here finishes in microseconds on the memory driver, so a slow
	// poll would only make the program slower.
	pollEvery = 2 * time.Millisecond

	// recursionPolls bounds the wait for the deepest walk run to log its
	// refusal: that many polls of pollEvery is a second, far more than
	// four nested runs need, and a broken build still ends.
	recursionPolls = 500

	// recursionSettle is the pause after the refusal is logged, so a
	// shallower run that is still finishing adds its line before the log
	// is printed.
	recursionSettle = 10 * time.Millisecond
)

// await is how the program waits for a run.
var await = workflow.AwaitOptions{Poll: pollEvery}

// log collects what the steps did, in order, safely across goroutines.
type log struct {
	mu    sync.Mutex
	lines []string
}

// add appends one line under the lock; steps call it from the driver's
// goroutines.
func (l *log) add(s string) { l.mu.Lock(); l.lines = append(l.lines, s); l.mu.Unlock() }

// has reports whether any line so far contains sub, under the lock, so
// the program can poll the log while steps still write to it.
func (l *log) has(sub string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return strings.Contains(strings.Join(l.lines, ","), sub)
}

// take returns the lines so far joined with commas and empties the log,
// so each section prints only what its own runs did.
func (l *log) take() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	s := strings.Join(l.lines, ", ")
	l.lines = nil
	return s
}

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var did log

	// snippet:client
	c, err := workflow.Use(memory.New(),
		workflow.Journal(workflow.NewMemoryJournal()), // Saga, Poll and EndRun keep their state here
		workflow.EndRuns(),         // honours EndRun
		workflow.RecursionLimit(3), // a step's own enqueues nest at most 3 deep
	)
	if err != nil {
		return err
	}
	// snippet:end

	wfs := []func(*log) (workflow.Workflow, error){orderWorkflow, tripWorkflow, notifyWorkflow, offerWorkflow, sensorWorkflow, refundWorkflow, importWorkflow, checkWorkflow,
		bookingWorkflow, alertsWorkflow, renewalWorkflow, priceWorkflow, listingWorkflow}
	for _, build := range wfs {
		wf, err := build(&did)
		if err != nil {
			return err
		}
		if err := c.Register(wf); err != nil {
			return err
		}
	}
	if err := c.Register(walkWorkflow(c, &did)); err != nil {
		return err
	}
	if err := c.Register(deadlineWorkflow(c)); err != nil {
		return err
	}
	if err := c.Start(ctx); err != nil {
		return err
	}

	// start enqueues one run of wf with payload and labels.
	start := func(wf string, payload string, labels map[string]string) (workflow.RunRef, error) {
		return c.Enqueue(ctx, workflow.Job{Workflow: wf, Payload: []byte(payload), Labels: labels})
	}
	// result waits for ref and returns its output, or its error as text.
	result := func(ref workflow.RunRef) string {
		out, err := workflow.AwaitResult(ctx, c, ref, await)
		if err != nil {
			return "error: " + err.Error()
		}
		return string(out)
	}

	fmt.Println("rules:")
	for _, card := range []string{`"ok"`, `"declined"`} {
		ref, err := start("order", card, nil)
		if err != nil {
			return err
		}
		res := result(ref)
		fmt.Printf("  card %s: %s\n    ran: %s\n", card, shorten(res), did.take())
	}

	fmt.Println("saga:")
	ref, err := start("trip", `null`, nil)
	if err != nil {
		return err
	}
	fmt.Printf("  %s\n    ran: %s\n", shorten(result(ref)), did.take())

	fmt.Println("multi-choice:")
	ref, err = start("notify", `{"email":true,"sms":true}`, nil)
	if err != nil {
		return err
	}
	fmt.Printf("  %s\n", result(ref))

	fmt.Println("race:")
	ref, err = start("offer", `"offer-7"`, nil)
	if err != nil {
		return err
	}
	// snippet:signal
	if err := workflow.SignalChoice(ctx, c, "offer.reply", "offer-7", "decline", []byte(`"too expensive"`)); err != nil {
		return err
	}
	// A later answer loses: the run already took the first one.
	if err := workflow.SignalChoice(ctx, c, "offer.reply", "offer-7", "accept", nil); err != nil {
		return err
	}
	// snippet:end
	fmt.Printf("  %s\n", result(ref))

	fmt.Println("poll:")
	ref, err = start("sensor", `null`, nil)
	if err != nil {
		return err
	}
	fmt.Printf("  %s after %s\n", result(ref), did.take())

	fmt.Println("approval:")
	if err := approve(ctx, c, start, result); err != nil {
		return err
	}

	fmt.Println("group:")
	ref, err = start("import", `null`, nil)
	if err != nil {
		return err
	}
	fmt.Printf("  %s\n", result(ref))

	fmt.Println("end run:")
	ref, err = start("check", `"blocked@example.com"`, nil)
	if err != nil {
		return err
	}
	st, err := workflow.Await(ctx, c, ref, await)
	if err != nil {
		return err
	}
	fmt.Printf("  status %s, ran: %s\n", st, did.take())

	fmt.Println("recursion:")
	ref, err = start("walk", `null`, nil)
	if err != nil {
		return err
	}
	if _, err := workflow.Await(ctx, c, ref, await); err != nil {
		return err
	}
	for range recursionPolls {
		if did.has("stopped") {
			break
		}
		time.Sleep(pollEvery)
	}
	time.Sleep(recursionSettle)
	fmt.Printf("  %s\n", did.take())

	fmt.Println("saga across steps:")
	ref, err = start("booking", `null`, nil)
	if err != nil {
		return err
	}
	fmt.Printf("  %s\n    ran: %s\n", shorten(result(ref)), did.take())

	fmt.Println("local merge:")
	ref, err = start("alerts", `{"email":true,"sms":true}`, nil)
	if err != nil {
		return err
	}
	if _, err := workflow.Await(ctx, c, ref, await); err != nil {
		return err
	}
	fmt.Printf("  %s\n", did.take())

	fmt.Println("race deadline:")
	if err := listingDeadline(ctx, c, result); err != nil {
		return err
	}

	fmt.Println("sleep until:")
	renewal := time.Now().Add(renewIn)
	ref, err = start("renewal", strconv.Quote(renewal.Format(time.RFC3339Nano)), nil)
	if err != nil {
		return err
	}
	fmt.Printf("  %s\n    renewed on time: %v\n", result(ref), !time.Now().Before(renewal))

	fmt.Println("typed:")
	if err := typed(ctx, c); err != nil {
		return err
	}

	fmt.Println("shape guard:")
	return shapeGuard(ctx)
}

// shorten keeps the part of a run error after the driver's prefix.
func shorten(s string) string {
	if i := strings.Index(s, "workflow: steps failed"); i >= 0 {
		return "failed: " + s[i:]
	}
	if i := strings.Index(s, "trip compensated"); i >= 0 {
		return "failed: " + s[i:]
	}
	if i := strings.Index(s, "workflow: saga"); i >= 0 {
		return "failed: " + s[i:]
	}
	return s
}

// orderWorkflow charges a card; a declined card goes to a branch that
// mails the customer, and the receipt is skipped. A cleanup step always
// runs last.
func orderWorkflow(did *log) (workflow.Workflow, error) {
	// step builds a step that logs its name, then runs fn.
	step := func(name string, after []string, fn func(in []byte) ([]byte, error)) workflow.Step {
		return workflow.Step{Name: name, After: after, Run: func(ctx context.Context, in []byte) ([]byte, error) {
			did.add(name)
			return fn(in)
		}}
	}
	// errDeclined is the charge's failure the Catch route recognises.
	errDeclined := errors.New("card declined")
	// snippet:rules
	wf := workflow.Workflow{Name: "order", Steps: []workflow.Step{
		step("charge", nil, func(in []byte) ([]byte, error) {
			if string(in) == `"declined"` {
				return nil, workflow.Permanent(errDeclined)
			}
			return []byte(`"ch_1"`), nil
		}),
		step("receipt", []string{"charge"}, func(in []byte) ([]byte, error) { return []byte(`"mailed"`), nil }),
		step("tell-customer", []string{"charge"}, func(in []byte) ([]byte, error) { return []byte(`"asked for another card"`), nil }),
		step("cleanup", nil, func(in []byte) ([]byte, error) { return []byte(`"cart released"`), nil }),
	}}
	return workflow.WithRules(wf, workflow.RulesSpec{
		Catch: map[string][]workflow.CatchRoute{"charge": {{Kind: "declined", Match: func(err error) bool {
			return errors.Is(err, errDeclined) // errors.Is works: it runs in the failing attempt
		}}}},
		Rules: map[string]workflow.TriggerRule{
			"tell-customer": workflow.RuleOneFailedOf("declined"), // runs only for a decline
		},
		Finally: []string{"cleanup"}, // after every leaf, whatever happened; keeps the run failed on a failure
	})
	// snippet:end
}

// tripWorkflow books a car, a flight and a hotel; the hotel has no
// rooms, so the flight and the car are cancelled, newest first.
func tripWorkflow(did *log) (workflow.Workflow, error) {
	// act returns a saga action or compensation that only logs s.
	act := func(s string) func(context.Context) error {
		return func(context.Context) error { did.add(s); return nil }
	}
	// noRooms is the hotel booking, which always fails.
	noRooms := func(context.Context) error { did.add("book hotel"); return errors.New("no rooms") }
	return workflow.Workflow{Name: "trip", Steps: []workflow.Step{{Name: "book", Run: func(ctx context.Context, _ []byte) ([]byte, error) {
		// snippet:saga
		saga := workflow.NewSaga("trip")
		if err := saga.Do(ctx, "car", act("book car"), act("cancel car")); err != nil {
			return nil, saga.Abort(ctx, err)
		}
		if err := saga.Do(ctx, "flight", act("book flight"), act("cancel flight")); err != nil {
			return nil, saga.Abort(ctx, err)
		}
		if err := saga.Do(ctx, "hotel", noRooms, act("cancel hotel")); err != nil {
			return nil, saga.Abort(ctx, err) // cancels the flight, then the car; Permanent
		}
		return []byte(`"booked"`), nil
		// snippet:end
	}}}}, nil
}

// notifyWorkflow sends on every channel the payload turns on.
func notifyWorkflow(*log) (workflow.Workflow, error) {
	// send returns a branch body that reports it sent on channel.
	send := func(channel string) workflow.Handler {
		return func(context.Context, []byte) ([]byte, error) { return json.Marshal("sent by " + channel) }
	}
	// snippet:multichoice
	steps, err := workflow.MultiChoice(workflow.MultiChoiceSpec{
		Name: "send",
		Decide: func(ctx context.Context, in []byte) ([]string, error) {
			var want map[string]bool
			if err := json.Unmarshal(in, &want); err != nil {
				return nil, workflow.Permanent(err)
			}
			var picked []string
			for _, ch := range []string{"email", "sms", "push"} {
				if want[ch] {
					picked = append(picked, ch)
				}
			}
			return picked, nil
		},
		Branches: []workflow.Branch{{Name: "email", Run: send("email")}, {Name: "sms", Run: send("sms")}, {Name: "push", Run: send("push")}},
	})
	// snippet:end
	return workflow.Workflow{Name: "notify", Steps: steps}, err
}

// offerWorkflow waits for the customer's reply on any channel; the first
// reply decides.
func offerWorkflow(*log) (workflow.Workflow, error) {
	// snippet:race
	steps, err := workflow.Race(workflow.RaceSpec{
		Name:  "reply",
		Event: "offer.reply",
		KeyFrom: workflow.KeyFunc(func(j workflow.Job) string {
			var id string
			_ = json.Unmarshal(j.Payload, &id) // the offer ID
			return id
		}),
		Timeout: 72 * time.Hour, // how long the customer has to reply
		Branches: []workflow.Branch{
			{Name: "accept", Run: func(ctx context.Context, in []byte) ([]byte, error) { return []byte(`"order placed"`), nil }},
			{Name: "decline", Run: func(ctx context.Context, in []byte) ([]byte, error) {
				var reason string
				if err := json.Unmarshal(in, &reason); err != nil { // in is the signal's payload
					return nil, workflow.Permanent(err)
				}
				return json.Marshal("offer closed: " + reason)
			}},
		},
	})
	// snippet:end
	return workflow.Workflow{Name: "offer", Steps: steps}, err
}

// sensorWorkflow waits until a partition file exists.
func sensorWorkflow(did *log) (workflow.Workflow, error) {
	looks := 0 // how many times the sensor has looked so far
	return workflow.Workflow{Name: "sensor", Steps: []workflow.Step{{Name: "wait-partition", Run: func(ctx context.Context, _ []byte) ([]byte, error) {
		// snippet:poll
		// First look after 1ms, the gap doubling to at most 8ms, for up to an hour.
		path, err := workflow.Poll(ctx, "partition", workflow.PollSpec{Every: time.Millisecond, Factor: 2, Max: 8 * time.Millisecond, Timeout: time.Hour},
			func(ctx context.Context) (string, bool, error) {
				looks++ // a real sensor would stat the file or list the bucket; here it is there on the third look
				return "s3://lake/2026-11-20/", looks == 3, nil
			})
		if err != nil {
			return nil, err // a Snooze until the next look, ErrPollTimeout at the end
		}
		// snippet:end
		did.add(fmt.Sprintf("%d looks", looks))
		return json.Marshal(path)
	}}}}, nil
}

// refundWorkflow pays a refund once a person approved it.
func refundWorkflow(*log) (workflow.Workflow, error) {
	// snippet:approval
	approval, err := workflow.ApprovalStep(workflow.ApprovalSpec{
		Name:    "approve",
		KeyFrom: workflow.KeyFunc(func(j workflow.Job) string { return j.Labels["refund"] }),
		Timeout: 7 * 24 * time.Hour, // a week to answer
	})
	if err != nil {
		return workflow.Workflow{}, err
	}
	pay := workflow.Step{Name: "pay", After: []string{"approve"}, Run: func(ctx context.Context, in []byte) ([]byte, error) {
		d, err := workflow.DecisionOf(in)
		if err != nil {
			return nil, err
		}
		if !d.Approved {
			return json.Marshal("refund refused by " + d.By)
		}
		return json.Marshal("refund paid, approved by " + d.By)
	}}
	// snippet:end
	return workflow.Workflow{Name: "refund", Steps: []workflow.Step{approval, pay}}, nil
}

// approve starts a refund, signs a link for the approver, and answers it
// as the link's handler would.
func approve(ctx context.Context, c workflow.Client, start func(string, string, map[string]string) (workflow.RunRef, error), result func(workflow.RunRef) string) error {
	ref, err := start("refund", `null`, map[string]string{"refund": "r-19"})
	if err != nil {
		return err
	}
	now := time.Date(2026, 11, 20, 9, 0, 0, 0, time.UTC) // a fixed send time, so the expiry is the same on every run
	// snippet:signer
	signer := workflow.ApprovalSigner{Keys: [][]byte{[]byte("a-secret-of-32-bytes-from-config")}}
	token, err := signer.Sign("r-19", now.Add(48*time.Hour)) // valid for two days; put it in the email's link
	if err != nil {
		return err
	}
	// In the link's handler: check the token, then answer.
	key, err := signer.Verify(token, now.Add(time.Hour))
	if err != nil {
		return err // forged or expired: ErrApprovalToken
	}
	if err := workflow.Approve(ctx, c, key, "ana@example.com", "looks right"); err != nil {
		return err
	}
	// snippet:end
	_, err = signer.Verify(token, now.Add(49*time.Hour)) // an hour after the link expired
	fmt.Printf("  %s\n  the same link two days later: %v\n", result(ref), err)
	return nil
}

// importWorkflow fetches, then cleans and counts in parallel, as one
// group the rest of the workflow sees as a single step.
func importWorkflow(*log) (workflow.Workflow, error) {
	// out returns a step body whose output is s.
	out := func(s string) workflow.Handler {
		return func(context.Context, []byte) ([]byte, error) { return json.Marshal(s) }
	}
	// snippet:group
	group, err := workflow.Group("import", nil, []workflow.Step{
		{Name: "fetch", Run: out("rows")},
		{Name: "clean", After: []string{"fetch"}, Run: out("120 clean rows")},
		{Name: "count", After: []string{"fetch"}, Run: out("3 rejected")},
	})
	if err != nil {
		return workflow.Workflow{}, err
	}
	report := workflow.Step{Name: "report", After: []string{"import"}, Run: func(ctx context.Context, in []byte) ([]byte, error) {
		return in, nil // {"clean": ..., "count": ...}: the group's last steps, by their names inside it
	}}
	wf := workflow.Workflow{Name: "import", Steps: append(group, report)}
	// snippet:end
	return wf, nil
}

// checkWorkflow ends early, successfully, when the address is blocked.
func checkWorkflow(did *log) (workflow.Workflow, error) {
	// step builds a step that logs its name, then runs fn.
	step := func(name string, after []string, fn workflow.Handler) workflow.Step {
		return workflow.Step{Name: name, After: after, Run: func(ctx context.Context, in []byte) ([]byte, error) {
			did.add(name)
			return fn(ctx, in)
		}}
	}
	// snippet:endrun
	lookup := step("lookup", nil, func(ctx context.Context, in []byte) ([]byte, error) {
		if strings.Contains(string(in), "blocked") {
			if err := workflow.EndRun(ctx); err != nil { // the steps below are skipped
				return nil, err
			}
			return []byte(`"blocked, nothing to do"`), nil
		}
		return in, nil
	})
	// snippet:end
	enrich := step("enrich", []string{"lookup"}, func(ctx context.Context, in []byte) ([]byte, error) { return in, nil })
	save := step("save", []string{"enrich"}, func(ctx context.Context, in []byte) ([]byte, error) { return in, nil })
	return workflow.Workflow{Name: "check", Steps: []workflow.Step{lookup, enrich, save}}, nil
}

// walkWorkflow enqueues itself from its step until the recursion limit
// refuses.
func walkWorkflow(c workflow.Client, did *log) workflow.Workflow {
	return workflow.Workflow{Name: "walk", Steps: []workflow.Step{{Name: "visit", Run: func(ctx context.Context, in []byte) ([]byte, error) {
		info, _ := workflow.StepInfoFrom(ctx)
		depth := info.Labels[workflow.LabelDepth]
		if depth == "" {
			depth = "0"
		}
		// snippet:recursion
		_, err := c.Enqueue(ctx, workflow.Job{Workflow: "walk"}) // a child of this run, one level deeper
		if errors.Is(err, workflow.ErrDepth) {
			did.add("depth " + depth + " stopped: limit reached")
			return nil, nil
		}
		// snippet:end
		did.add("depth " + depth)
		return nil, err
	}}}}
}

// bookingWorkflow books a car, a flight and a hotel as three parallel
// steps. The hotel is full, so the undo step cancels the bookings that
// went through and fails the run; the confirm step is skipped.
func bookingWorkflow(did *log) (workflow.Workflow, error) {
	// book builds a booking step for what that fails for good with err,
	// or succeeds with a booking reference when err is nil.
	book := func(what string, err error) workflow.Step {
		return workflow.Step{Name: what, Run: func(context.Context, []byte) ([]byte, error) {
			if err != nil {
				return nil, workflow.Permanent(err)
			}
			return json.Marshal(what + "-booking-1")
		}}
	}
	// snippet:sagasteps
	order := []string{"car", "flight", "hotel"} // booking order: undo runs it backwards
	wf := workflow.Workflow{Name: "booking", Steps: []workflow.Step{
		book("car", nil), book("flight", nil), book("hotel", errors.New("no rooms")),
		{Name: "confirm", After: order, Run: func(context.Context, []byte) ([]byte, error) {
			did.add("confirm")
			return []byte(`"trip booked"`), nil
		}},
		{Name: "undo", After: order, Run: func(ctx context.Context, in []byte) ([]byte, error) {
			var booked map[string]*string // a failed booking's output is null
			if err := json.Unmarshal(in, &booked); err != nil {
				return nil, workflow.Permanent(err)
			}
			for _, what := range slices.Backward(order) {
				if booked[what] != nil {
					did.add("cancel " + *booked[what])
				}
			}
			f := workflow.ParentFailures(ctx)[0]
			return nil, workflow.Permanent(fmt.Errorf("trip compensated: %s: %s", f.Step, f.Error))
		}},
	}}
	return workflow.WithRules(wf, workflow.RulesSpec{
		Tolerate: order, // a failed booking is recorded, not fatal
		Rules:    map[string]workflow.TriggerRule{"undo": workflow.RuleOneFailed},
	})
	// snippet:end
}

// alertsWorkflow pages on the channels the payload turns on, and an
// audit step lists the branch steps themselves in After: a local merge
// that runs once, after the branches taken, and reads what they stored.
func alertsWorkflow(did *log) (workflow.Workflow, error) {
	channels := []string{"email", "sms", "push"}
	branches := make([]workflow.Branch, len(channels))
	for i, ch := range channels {
		branches[i] = workflow.Branch{Name: ch, Run: func(ctx context.Context, _ []byte) ([]byte, error) {
			return nil, workflow.SetRunData(ctx, "sent."+ch, ch+"-receipt-1") // one name per branch
		}}
	}
	steps, err := workflow.MultiChoice(workflow.MultiChoiceSpec{
		Name: "page",
		Decide: func(ctx context.Context, in []byte) ([]string, error) {
			var want map[string]bool
			if err := json.Unmarshal(in, &want); err != nil {
				return nil, workflow.Permanent(err)
			}
			var picked []string
			for _, ch := range channels {
				if want[ch] {
					picked = append(picked, ch)
				}
			}
			return picked, nil
		},
		Branches: branches,
	})
	if err != nil {
		return workflow.Workflow{}, err
	}
	// snippet:merge
	steps = append(steps, workflow.Step{
		Name:  "audit",
		After: []string{"page.email", "page.sms", "page.push"}, // the branch steps, not the join "page"
		Run: func(ctx context.Context, _ []byte) ([]byte, error) {
			var sent []string
			for _, ch := range []string{"email", "sms", "push"} {
				receipt, ok, err := workflow.RunData[string](ctx, "sent."+ch)
				if err != nil {
					return nil, err
				}
				if ok { // a branch not taken stored nothing
					sent = append(sent, receipt)
				}
			}
			did.add("audit after " + strings.Join(sent, ", "))
			return nil, nil
		},
	})
	// snippet:end
	return workflow.Workflow{Name: "alerts", Steps: steps}, nil
}

// renewIn is how far ahead the renewal step sleeps to: short, so the
// program ends at once.
const renewIn = 20 * time.Millisecond

// renewalWorkflow renews a subscription at the time its payload names.
func renewalWorkflow(*log) (workflow.Workflow, error) {
	return workflow.Workflow{Name: "renewal", Steps: []workflow.Step{{Name: "renew", Run: func(ctx context.Context, in []byte) ([]byte, error) {
		// snippet:sleepuntil
		var at time.Time
		if err := json.Unmarshal(in, &at); err != nil {
			return nil, workflow.Permanent(err)
		}
		if err := workflow.SleepUntil(clock.Real{}, at); err != nil {
			return nil, err // a Snooze for the time left; the step comes back at `at`
		}
		return json.Marshal("renewed") // runs once `at` has passed
		// snippet:end
	}}}}, nil
}

// quote and priced are the typed payload and result of the price
// workflow.
type (
	quote struct {
		SKU string
		Qty int
	}
	priced struct {
		SKU   string
		Total float64
	}
)

// unitPrice is the price of one item in the typed example.
const unitPrice = 2.5

// priceWorkflow prices an order with a typed handler.
func priceWorkflow(*log) (workflow.Workflow, error) {
	return workflow.Workflow{Name: "price", Steps: []workflow.Step{{Name: "total", Run: workflow.Define(func(_ context.Context, q quote) (priced, error) {
		return priced{SKU: q.SKU, Total: float64(q.Qty) * unitPrice}, nil
	})}}}, nil
}

// typed enqueues a typed payload and reads a typed result.
func typed(ctx context.Context, c workflow.Client) error {
	// snippet:typed
	ref, err := workflow.EnqueueOf(ctx, c, "price", quote{SKU: "pen", Qty: 4}, func(j *workflow.Job) {
		j.Labels = map[string]string{"customer": "c-7"} // edit the job before it is sent
	})
	if err != nil {
		return err
	}
	p, err := workflow.AwaitResultOf[priced](ctx, c, ref, await)
	if err != nil {
		return err
	}
	fmt.Printf("  %s costs %.2f\n", p.SKU, p.Total)
	// snippet:end
	return nil
}

// shapeGuard plays a rolling deploy that renamed a step: a worker on the
// new build runs v2 while an API server on the old build still enqueues
// under v1, and the guard fails that run before any handler runs.
func shapeGuard(ctx context.Context) error {
	// ok is the body of every sync step: the guard fails the run before
	// any of them would run.
	ok := func(context.Context, []byte) ([]byte, error) { return []byte(`"done"`), nil }
	v1 := workflow.Workflow{Name: "sync", Steps: []workflow.Step{{Name: "fetch", Run: ok}, {Name: "save", After: []string{"fetch"}, Run: ok}}}
	v2 := workflow.Workflow{Name: "sync", Steps: []workflow.Step{{Name: "fetch", Run: ok}, {Name: "store", After: []string{"fetch"}, Run: ok}}}
	fmt.Printf("  shape v1 %s, v2 %s\n", workflow.Shape(v1), workflow.Shape(v2))

	backend := memory.New() // one backend both deploys share
	// snippet:shape
	worker, err := workflow.Use(backend, workflow.ShapeGuard(v2)) // the new deploy: "save" renamed "store"
	if err != nil {
		return err
	}
	if err := worker.Register(v2); err != nil {
		return err
	}
	if err := worker.Start(ctx); err != nil {
		return err
	}
	api, err := workflow.Use(backend, workflow.ShapeGuard(v1)) // an API server still on the old build
	if err != nil {
		return err
	}
	ref, err := api.Enqueue(ctx, workflow.Job{Workflow: "sync"}) // stamped with Shape(v1)
	if err != nil {
		return err
	}
	_, err = workflow.AwaitResult(ctx, worker, ref, await)
	msg := fmt.Sprint(err) // ErrShapeChanged, Permanent, before any handler ran
	fmt.Println("  run from the old build failed:", msg[max(strings.Index(msg, "was enqueued"), 0):])
	// snippet:end
	return nil
}

// expireIn is the deadline of a listing nobody answers: short, so the
// program ends at once; a real app would use days.
const expireIn = 20 * time.Millisecond

// listingWorkflow races on the buyer's answer to a listing; the expire
// branch is the default a delayed deadline job answers for.
func listingWorkflow(*log) (workflow.Workflow, error) {
	// answer returns a branch body whose output is s.
	answer := func(s string) workflow.Handler {
		return func(context.Context, []byte) ([]byte, error) { return json.Marshal(s) }
	}
	steps, err := workflow.Race(workflow.RaceSpec{
		Name:  "answer",
		Event: "listing.answer",
		KeyFrom: workflow.KeyFunc(func(j workflow.Job) string {
			var id string
			_ = json.Unmarshal(j.Payload, &id) // the listing ID
			return id
		}),
		Branches: []workflow.Branch{{Name: "buy", Run: answer("sold")}, {Name: "expire", Run: answer("listing expired")}},
	})
	return workflow.Workflow{Name: "listing", Steps: steps}, err
}

// deadlineWorkflow answers "expire" for the listing its payload names.
func deadlineWorkflow(c workflow.Client) workflow.Workflow {
	// snippet:deadline
	return workflow.Workflow{Name: "listing.deadline", Steps: []workflow.Step{{Name: "expire",
		Run: func(ctx context.Context, in []byte) ([]byte, error) {
			var id string
			if err := json.Unmarshal(in, &id); err != nil { // the listing ID, as the listing run's payload
				return nil, workflow.Permanent(err)
			}
			return nil, workflow.SignalChoice(ctx, c, "listing.answer", id, "expire", nil) // loses quietly if someone answered
		},
	}}}
	// snippet:end
}

// listingDeadline starts a listing and its deadline job; nobody answers,
// so the deadline's expire branch wins.
func listingDeadline(ctx context.Context, c workflow.Client, result func(workflow.RunRef) string) error {
	// snippet:deadline-start
	ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "listing", Payload: []byte(`"listing-3"`)})
	if err != nil {
		return err
	}
	_, err = c.Enqueue(ctx, workflow.Job{Workflow: "listing.deadline", Payload: []byte(`"listing-3"`), RunAt: time.Now().Add(expireIn)})
	if err != nil {
		return err
	}
	// snippet:end
	fmt.Printf("  nobody answered: %s\n", result(ref))
	return nil
}
