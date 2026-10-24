// Command deferred-dispatch shows enqueues held until a request succeeds:
// an HTTP handler enqueues a receipt job, and workflow.DeferHTTP sends it
// only when the handler answers with a success status. A handler that
// answers 500 or panics drops it. Outside HTTP, a Deferred is flushed or
// discarded by hand.
//
// It is the program behind docs/jobs/deferred.md. Requests go through
// httptest, so no port is opened.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sort"
	"strings"
	"sync"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/memory"
	"github.com/ubgo/workflow/middleware"
)

func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// receipts records the orders a receipt job ran for.
type receipts struct {
	mu   sync.Mutex
	sent []string
}

func (r *receipts) add(order string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sent = append(r.sent, order)
}

func (r *receipts) list() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	s := append([]string(nil), r.sent...)
	sort.Strings(s)
	return strings.Join(s, ",")
}

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context, out io.Writer) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var sent receipts
	mem := memory.New()
	if err := mem.Register(workflow.Workflow{
		Name: "send-receipt",
		Steps: []workflow.Step{{Name: "send", Run: func(_ context.Context, in []byte) ([]byte, error) {
			sent.add(string(in))
			return nil, nil
		}}},
	}); err != nil {
		return err
	}
	ends := make(chan workflow.RunEvent, 16)
	if err := mem.OnRunFinished(func(_ context.Context, ev workflow.RunEvent) { ends <- ev }); err != nil {
		return err
	}
	if err := mem.Start(ctx); err != nil {
		return err
	}
	defer mem.Stop()

	// snippet:client
	// The app enqueues through this client everywhere. Inside a request
	// wrapped by DeferHTTP its jobs are held; anywhere else they are sent
	// at once.
	c := middleware.WrapClient(mem, workflow.DeferEnqueue())
	// snippet:end

	// snippet:handler
	mux := http.NewServeMux()
	mux.HandleFunc("POST /orders/{id}", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		// Held, not sent: the run exists only if this request succeeds.
		if _, err := c.Enqueue(r.Context(), workflow.Job{Workflow: "send-receipt", Payload: []byte(id)}); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		switch r.URL.Query().Get("then") {
		case "fail":
			http.Error(w, "payment declined", http.StatusPaymentRequired)
		case "panic":
			panic("order handler bug")
		default:
			w.WriteHeader(http.StatusCreated)
		}
	})
	h := workflow.DeferHTTP(mux, workflow.DeferOptions{})
	// snippet:end

	for _, target := range []string{"/orders/o-1", "/orders/o-2?then=fail", "/orders/o-3?then=panic"} {
		status, panicked := serve(h, target)
		fmt.Fprintf(out, "POST %s: status %d, panicked %v\n", target, status, panicked)
	}
	<-ends // only o-1 ran
	fmt.Fprintf(out, "receipts after the requests: %s\n", sent.list())

	// snippet:manual
	d := workflow.NewDeferred()
	jobCtx := workflow.WithDeferred(ctx, d)
	for _, id := range []string{"o-4", "o-5"} {
		if _, err := c.Enqueue(jobCtx, workflow.Job{Workflow: "send-receipt", Payload: []byte(id)}); err != nil {
			return err
		}
	}
	fmt.Fprintf(out, "held before flush: %d\n", d.Len())
	refs, err := d.Flush(ctx) // the work this belongs to succeeded
	if err != nil {
		return err
	}
	// snippet:end
	fmt.Fprintf(out, "flushed: %d runs\n", len(refs))
	<-ends
	<-ends

	// snippet:discard
	d = workflow.NewDeferred()
	if _, err := c.Enqueue(workflow.WithDeferred(ctx, d), workflow.Job{Workflow: "send-receipt", Payload: []byte("o-6")}); err != nil {
		return err
	}
	dropped := d.Discard() // the work failed: nothing is sent
	_, err = c.Enqueue(workflow.WithDeferred(ctx, d), workflow.Job{Workflow: "send-receipt", Payload: []byte("o-7")})
	// snippet:end
	fmt.Fprintf(out, "discarded: %d, enqueue after discard: ErrDeferredClosed %v\n", dropped, errors.Is(err, workflow.ErrDeferredClosed))
	fmt.Fprintf(out, "receipts at the end: %s\n", sent.list())
	return nil
}

// serve sends one POST through h and reports the status and whether the
// handler panicked (DeferHTTP drops the held jobs, then lets the panic go
// on up, here into this recover).
func serve(h http.Handler, target string) (status int, panicked bool) {
	rec := httptest.NewRecorder()
	defer func() {
		if recover() != nil {
			status, panicked = http.StatusInternalServerError, true
		}
	}()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, target, nil))
	return rec.Code, false
}
