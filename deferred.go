package workflow

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
)

// ErrDeferredClosed is returned by an enqueue made through a Deferred
// that was already flushed or discarded, such as a goroutine of a request
// handler enqueueing after the response was written. Nothing is sent.
var ErrDeferredClosed = errors.New("workflow: deferred enqueues already flushed or discarded")

// Deferred holds enqueues until the work that made them is known to have
// succeeded: Flush sends them in order, Discard drops them. It is how a
// request handler enqueues "send the welcome email" without sending it
// when the request then fails. Put one in a ctx with WithDeferred (DeferHTTP
// does this per request) and enqueue through a client wrapped with
// DeferEnqueue.
//
// It is not a transaction: a Flush after a database commit can still fail
// or the process can die between the two. When the job must be enqueued
// if and only if a database transaction commits, use the SQL driver's
// EnqueueTx or its transactional outbox instead.
//
// A Deferred is safe for concurrent use. Use NewDeferred; it is single
// use: after Flush or Discard every Add returns ErrDeferredClosed.
type Deferred struct {
	mu     sync.Mutex
	held   []heldJob
	closed bool
}

type heldJob struct {
	enqueue EnqueueFunc
	job     Job
}

// NewDeferred returns an empty, open Deferred.
func NewDeferred() *Deferred { return &Deferred{} }

// Add holds job, to be sent with enqueue by Flush. It returns
// ErrDeferredClosed once the Deferred was flushed or discarded. The job is
// copied, so the caller may reuse it.
func (d *Deferred) Add(enqueue EnqueueFunc, job Job) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return ErrDeferredClosed
	}
	d.held = append(d.held, heldJob{enqueue: enqueue, job: CloneJob(job)})
	return nil
}

// Len is the number of jobs held.
func (d *Deferred) Len() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.held)
}

// take closes d and returns what it held; a second call returns nothing.
func (d *Deferred) take() []heldJob {
	d.mu.Lock()
	defer d.mu.Unlock()
	held := d.held
	d.held, d.closed = nil, true
	return held
}

// Flush sends every held job, in the order held, and closes d. It sends
// all of them even when one fails, and returns the refs of the ones sent
// (the zero RunRef for a failure, so refs[i] is the i-th held job) with
// every failure joined. A second Flush, or a Flush after Discard, sends
// nothing.
func (d *Deferred) Flush(ctx context.Context) ([]RunRef, error) {
	held := d.take()
	refs := make([]RunRef, len(held))
	var errs []error
	for i, h := range held {
		ref, err := h.enqueue(ctx, h.job)
		if err != nil {
			errs = append(errs, fmt.Errorf("deferred job %d for %q: %w", i, h.job.Workflow, err))
			continue
		}
		refs[i] = ref
	}
	return refs, errors.Join(errs...)
}

// Discard drops every held job, closes d and returns how many it dropped.
func (d *Deferred) Discard() int { return len(d.take()) }

// deferredKey is the unexported context key for a Deferred.
type deferredKey struct{}

// WithDeferred returns a ctx carrying d, so enqueues made with it through
// a DeferEnqueue client are held by d.
func WithDeferred(ctx context.Context, d *Deferred) context.Context {
	return context.WithValue(ctx, deferredKey{}, d)
}

// DeferredFrom returns the Deferred on ctx, and false when there is none.
func DeferredFrom(ctx context.Context) (*Deferred, bool) {
	d, ok := ctx.Value(deferredKey{}).(*Deferred)
	return d, ok
}

// DeferEnqueue is enqueue middleware that holds a job in the ctx's
// Deferred instead of sending it, returning the zero RunRef and nil: the
// run does not exist yet, so there is no ref to give. Without a Deferred
// on the ctx it sends the job at once, so the same client works inside and
// outside requests. Validation and capability checks happen when the job
// is flushed, by the client underneath; a caller that needs them up
// front, or needs the RunRef, enqueues without a Deferred on its ctx.
// Middleware listed after it in the chain runs at flush time.
func DeferEnqueue() EnqueueMiddleware {
	return func(next EnqueueFunc) EnqueueFunc {
		return func(ctx context.Context, job Job) (RunRef, error) {
			d, ok := DeferredFrom(ctx)
			if !ok {
				return next(ctx, job)
			}
			return RunRef{}, d.Add(next, job)
		}
	}
}

// DeferOptions tunes DeferHTTP. The zero value is the default.
type DeferOptions struct {
	// Succeeded decides from the response status whether the request
	// succeeded, so its held jobs are flushed rather than discarded. Nil
	// means DeferSucceeded (any status below 400). A handler that writes
	// no status counts as 200.
	Succeeded func(status int) bool

	// OnError receives a flush that failed for some jobs. The response is
	// already written, so it cannot reach the client. Nil logs it with
	// Logger.
	OnError func(r *http.Request, err error)

	// Logger is used by the default OnError. Nil means slog.Default().
	Logger *slog.Logger
}

// DeferSucceeded is DeferOptions.Succeeded's default: a status below
// 400.
func DeferSucceeded(status int) bool { return status < http.StatusBadRequest }

// DeferHTTP wraps next so every request gets its own Deferred on its
// context: jobs enqueued during the request through a DeferEnqueue client
// are sent after next returns, when opts.Succeeded accepts the response
// status, and dropped when it does not or when next panics (the panic
// goes on up after the drop). The flush runs on the request's context
// without its cancellation, so a client that hung up after a successful
// response does not cancel the jobs.
func DeferHTTP(next http.Handler, opts DeferOptions) http.Handler {
	succeeded := opts.Succeeded
	if succeeded == nil {
		succeeded = DeferSucceeded
	}
	onError := opts.OnError
	if onError == nil {
		logger := opts.Logger
		if logger == nil {
			logger = slog.Default()
		}
		onError = func(r *http.Request, err error) {
			logger.ErrorContext(r.Context(), "workflow: deferred enqueue failed", "method", r.Method, "path", r.URL.Path, "error", err)
		}
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		d := NewDeferred()
		sw := &statusWriter{ResponseWriter: w}
		completed := false
		defer func() {
			if !completed { // next panicked (or called runtime.Goexit)
				d.Discard()
			}
		}()
		next.ServeHTTP(sw, r.WithContext(WithDeferred(r.Context(), d)))
		completed = true
		if !succeeded(sw.Status()) {
			d.Discard()
			return
		}
		if _, err := d.Flush(context.WithoutCancel(r.Context())); err != nil {
			onError(r, err)
		}
	})
}

// statusWriter records the status a handler wrote.
type statusWriter struct {
	http.ResponseWriter
	status int
}

func (s *statusWriter) WriteHeader(code int) {
	if s.status == 0 {
		s.status = code
	}
	s.ResponseWriter.WriteHeader(code)
}

func (s *statusWriter) Write(b []byte) (int, error) {
	if s.status == 0 {
		s.status = http.StatusOK
	}
	return s.ResponseWriter.Write(b)
}

// Flush passes a streaming handler's flush through, so wrapping a
// handler in DeferHTTP does not stop it streaming.
func (s *statusWriter) Flush() {
	if s.status == 0 {
		s.status = http.StatusOK
	}
	_ = http.NewResponseController(s.ResponseWriter).Flush() // an unflushable writer has nothing to flush
}

// Status is the status written, 200 when none was.
func (s *statusWriter) Status() int {
	if s.status == 0 {
		return http.StatusOK
	}
	return s.status
}

// Unwrap lets http.ResponseController reach the underlying writer's
// Flush, Hijack and deadlines.
func (s *statusWriter) Unwrap() http.ResponseWriter { return s.ResponseWriter }
