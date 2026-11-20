// Command flow-patterns shows the control-flow building blocks made of
// ordinary steps: trigger rules over tolerated failures with an exit
// handler, a saga that compensates, a multi-choice, a race decided by the
// first signal, a polling sensor, a human approval with a signed link, a
// step group, an early successful end, and a recursion limit. Everything
// runs on the in-memory driver.
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
	"strings"
	"sync"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/memory"
)

func main() {
	if err := run(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, "flow-patterns:", err)
		os.Exit(1)
	}
}

// await is how the program waits for a run.
var await = workflow.AwaitOptions{Poll: 2 * time.Millisecond}

// log collects what the steps did, in order, safely across goroutines.
type log struct {
	mu    sync.Mutex
	lines []string
}

func (l *log) add(s string) { l.mu.Lock(); l.lines = append(l.lines, s); l.mu.Unlock() }

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

	wfs := []func(*log) (workflow.Workflow, error){orderWorkflow, tripWorkflow, notifyWorkflow, offerWorkflow, sensorWorkflow, refundWorkflow, importWorkflow, checkWorkflow}
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
	if err := c.Start(ctx); err != nil {
		return err
	}

	start := func(wf string, payload string, labels map[string]string) (workflow.RunRef, error) {
		return c.Enqueue(ctx, workflow.Job{Workflow: wf, Payload: []byte(payload), Labels: labels})
	}
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
	for range 500 {
		if strings.Contains(strings.Join(did.lines, ","), "stopped") {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	time.Sleep(10 * time.Millisecond)
	fmt.Printf("  %s\n", did.take())
	return nil
}

// shorten keeps the part of a run error after the driver's prefix.
func shorten(s string) string {
	if i := strings.Index(s, "workflow: steps failed"); i >= 0 {
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
	step := func(name string, after []string, fn func(in []byte) ([]byte, error)) workflow.Step {
		return workflow.Step{Name: name, After: after, Run: func(ctx context.Context, in []byte) ([]byte, error) {
			did.add(name)
			return fn(in)
		}}
	}
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
	act := func(s string) func(context.Context) error {
		return func(context.Context) error { did.add(s); return nil }
	}
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
		Timeout: 72 * time.Hour,
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
	looks := 0
	return workflow.Workflow{Name: "sensor", Steps: []workflow.Step{{Name: "wait-partition", Run: func(ctx context.Context, _ []byte) ([]byte, error) {
		// snippet:poll
		path, err := workflow.Poll(ctx, "partition", workflow.PollSpec{Every: time.Millisecond, Factor: 2, Max: 8 * time.Millisecond, Timeout: time.Hour},
			func(ctx context.Context) (string, bool, error) {
				looks++ // a real sensor would stat the file or list the bucket
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
		Timeout: 7 * 24 * time.Hour,
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
	now := time.Date(2026, 11, 20, 9, 0, 0, 0, time.UTC)
	// snippet:signer
	signer := workflow.ApprovalSigner{Keys: [][]byte{[]byte("a-secret-of-32-bytes-from-config")}}
	token, err := signer.Sign("r-19", now.Add(48*time.Hour)) // put it in the email's link
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
	_, err = signer.Verify(token, now.Add(49*time.Hour))
	fmt.Printf("  %s\n  the same link two days later: %v\n", result(ref), err)
	return nil
}

// importWorkflow fetches, then cleans and counts in parallel, as one
// group the rest of the workflow sees as a single step.
func importWorkflow(*log) (workflow.Workflow, error) {
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
