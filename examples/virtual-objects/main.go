// Command virtual-objects shows a keyed virtual object: a shopping cart
// per customer whose calls run one at a time per cart and share the
// cart's state, kept in the journal store, with no table of the app's
// own. It also finds a cart's calls by their key label and shows that a
// call of a method the object does not have fails its run. It runs on the
// in-memory driver, so nothing needs to be installed.
//
// It is the program behind docs/workflows/objects.md: the guide copies its
// regions (marked "snippet:") and task docs:snippets fails when they
// drift.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/memory"
)

// main runs the example and exits non-zero on error.
func main() {
	if err := run(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, "virtual-objects:", err)
		os.Exit(1)
	}
}

const (
	// pollEvery is how often the program checks a run or a cart's state:
	// short, because every call finishes in microseconds in memory.
	pollEvery = 2 * time.Millisecond
	// stateWait bounds the wait for bob's cart to appear, so a broken
	// driver fails the program instead of hanging it.
	stateWait = 5 * time.Second
)

// await is how the program waits for a run.
var await = workflow.AwaitOptions{Poll: pollEvery}

// snippet:define
// Cart is the state one cart keeps between calls.
type Cart struct {
	Items []string `json:"items"`
}

// cartObject defines the cart object: its state lives in store and its
// methods add an item or clear the cart.
func cartObject(store workflow.JournalStore) (workflow.Workflow, error) {
	return workflow.DefineObject(workflow.ObjectSpec[Cart]{
		Name:  "cart",
		Store: store,
		Methods: map[string]workflow.ObjectMethod[Cart]{
			"add": func(_ context.Context, c Cart, args []byte) (Cart, []byte, error) {
				var item string
				if err := json.Unmarshal(args, &item); err != nil {
					return c, nil, workflow.Permanent(err)
				}
				c.Items = append(c.Items, item)
				out, err := json.Marshal(len(c.Items))
				return c, out, err
			},
			"clear": func(context.Context, Cart, []byte) (Cart, []byte, error) {
				return Cart{}, nil, nil
			},
		},
	})
}

// snippet:end

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	store := workflow.NewMemoryJournal()
	cart, err := cartObject(store)
	if err != nil {
		return err
	}
	c := memory.New()
	if err := c.Register(cart); err != nil {
		return err
	}
	if err := c.Start(ctx); err != nil {
		return err
	}

	// snippet:call
	var refs []workflow.RunRef
	for _, item := range []string{"socks", "hat", "scarf"} {
		args, err := json.Marshal(item)
		if err != nil {
			return err
		}
		ref, err := workflow.CallObject(ctx, c, "cart", "alice", "add", args, workflow.Job{})
		if err != nil {
			return err
		}
		refs = append(refs, ref)
	}
	// snippet:end
	if _, err := workflow.CallObject(ctx, c, "cart", "bob", "add", []byte(`"gloves"`), workflow.Job{}); err != nil {
		return err
	}
	for _, ref := range refs {
		if _, err := workflow.AwaitResult(ctx, c, ref, await); err != nil {
			return err
		}
	}

	// snippet:state
	alice, _, err := workflow.ObjectState[Cart](ctx, store, "cart", "alice")
	if err != nil {
		return err
	}
	// snippet:end
	fmt.Println("alice's cart holds", len(alice.Items), "items")
	deadline := time.Now().Add(stateWait)
	for {
		bob, ok, err := workflow.ObjectState[Cart](ctx, store, "cart", "bob")
		if err != nil {
			return err
		}
		if ok || time.Now().After(deadline) {
			fmt.Println("bob's cart holds", len(bob.Items), "item")
			break
		}
		time.Sleep(pollEvery)
	}
	ref, err := workflow.CallObject(ctx, c, "cart", "alice", "clear", nil, workflow.Job{})
	if err != nil {
		return err
	}
	if _, err := workflow.AwaitResult(ctx, c, ref, await); err != nil {
		return err
	}
	alice, _, err = workflow.ObjectState[Cart](ctx, store, "cart", "alice")
	if err != nil {
		return err
	}
	fmt.Println("after clear, alice's cart holds", len(alice.Items), "items")

	// snippet:calls
	page, err := c.Runs(ctx, workflow.RunQuery{ // c implements workflow.RunQuerier
		Workflow: "cart",
		Labels:   map[string]string{workflow.LabelObjectKey: "alice"}, // set by CallObject
	})
	if err != nil {
		return err
	}
	fmt.Println("calls of alice's cart so far:", len(page.Runs))
	// snippet:end

	// snippet:unknown-method
	ref, err = workflow.CallObject(ctx, c, "cart", "alice", "remove", []byte(`"hat"`), workflow.Job{})
	if err != nil {
		return err // CallObject does not know the methods; the call's run does
	}
	_, err = workflow.AwaitResult(ctx, c, ref, await)
	var failed *workflow.RunFailedError
	if !errors.As(err, &failed) {
		return fmt.Errorf("a call of an unknown method did not fail: %v", err)
	}
	fmt.Printf("unknown method: the call %s, naming the methods: %v\n",
		failed.Status, strings.Contains(failed.Err, "(methods: [add clear])"))
	// snippet:end
	return nil
}
