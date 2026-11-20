package admin

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"sync"
	"time"
)

// AuditEntry is one write action taken through the admin HTTP handler:
// who did what to which resource, how it ended and when. A write is any
// Action whose ReadOnly is false (retry, delete, cancel, cancel-all,
// pause and resume, schedule pause, resume and trigger, force complete
// and fail, the bulk retries and deletes, reschedule, cancel-step, and
// any write a later version adds), dry runs included (Resource.DryRun
// says so).
type AuditEntry struct {
	// Who is the principal Auth.Identify returned for the request (the
	// handler's P), as an any so one sink serves every handler.
	Who any

	// Action and Resource are what Authorize was asked about.
	Action   Action
	Resource Resource

	// Allowed is false when Authorize refused the request (403); the
	// operation then never ran. A refused write is recorded too, since
	// who tried what is what an audit log is read for.
	Allowed bool

	// Status is the HTTP status the handler answered.
	Status int

	// Err is why the action did not succeed: the operation's error (a
	// conflict, an unsupported capability, a driver failure, with its
	// full detail even when the response said only "internal error"), or
	// the refusal. Nil on success.
	Err error

	// At is when the action ended, on the Admin's clock (WithClock).
	At time.Time
}

// AuditSink records audit entries: an app's audit table, a log line, a
// message to a security channel. Record runs on the request's goroutine
// after the response is written, so a slow sink delays only the end of
// that request. An error is logged (WithLogger) and never changes the
// response, which has already been sent.
type AuditSink interface {
	Record(ctx context.Context, e AuditEntry) error
}

// AuditFunc adapts a function to AuditSink.
type AuditFunc func(ctx context.Context, e AuditEntry) error

// Record calls f.
func (f AuditFunc) Record(ctx context.Context, e AuditEntry) error { return f(ctx, e) }

// WithAudit calls sink once for every write request that reached
// Authorize, after the action ran (or was refused). Read-only requests
// are not recorded, nor are requests refused before Authorize (401
// anonymous, 415 media type, 400 malformed), since no action was asked
// for. Nil turns auditing off, the default.
func WithAudit(sink AuditSink) HandlerOption { return func(c *handlerConfig) { c.audit = sink } }

// MemoryAudit is an in-memory AuditSink for tests and small tools. Every
// method is safe for concurrent use. It keeps every entry, so it is not
// for a long-running server.
type MemoryAudit struct {
	mu      sync.Mutex
	entries []AuditEntry
}

// Record appends e.
func (m *MemoryAudit) Record(_ context.Context, e AuditEntry) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.entries = append(m.entries, e)
	return nil
}

// Entries returns a copy of the entries, oldest first.
func (m *MemoryAudit) Entries() []AuditEntry {
	m.mu.Lock()
	defer m.mu.Unlock()
	return slices.Clone(m.entries)
}

// auditRecord is what one request collects for its entry. The gate puts
// it on the request's ctx; authorize, fail and writeError fill it.
type auditRecord struct {
	action  Action
	res     Resource
	asked   bool // Authorize was called
	allowed bool
	err     error
}

type auditKey struct{}

// auditWriter is the ResponseWriter the gate hands the routes when
// auditing is on: it remembers the status and the error the route
// answered with.
type auditWriter struct {
	http.ResponseWriter
	rec    *auditRecord
	status int
}

func (w *auditWriter) WriteHeader(code int) {
	if w.status == 0 {
		w.status = code
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *auditWriter) Write(b []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.ResponseWriter.Write(b)
}

// Unwrap lets http.ResponseController reach the real writer.
func (w *auditWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

// startAudit wraps w and ctx for one request when auditing is on.
func (h *handler[P]) startAudit(w http.ResponseWriter, ctx context.Context) (http.ResponseWriter, context.Context, *auditWriter) {
	if h.cfg.audit == nil {
		return w, ctx, nil
	}
	aw := &auditWriter{ResponseWriter: w, rec: &auditRecord{}}
	return aw, context.WithValue(ctx, auditKey{}, aw.rec), aw
}

// finishAudit records the request's entry when it asked for a write.
func (h *handler[P]) finishAudit(ctx context.Context, who P, aw *auditWriter) {
	if aw == nil || !aw.rec.asked || aw.rec.action.ReadOnly() {
		return
	}
	e := AuditEntry{Who: who, Action: aw.rec.action, Resource: aw.rec.res, Allowed: aw.rec.allowed, Status: aw.status, Err: aw.rec.err, At: h.a.clk.Now()}
	if e.Status == 0 {
		e.Status = http.StatusOK
	}
	if e.Err == nil && e.Status >= 400 {
		e.Err = fmt.Errorf("admin: answered %d", e.Status)
	}
	err := func() (err error) {
		defer func() {
			if p := recover(); p != nil {
				err = fmt.Errorf("admin: audit sink panicked: %v", p)
			}
		}()
		return h.cfg.audit.Record(context.WithoutCancel(ctx), e)
	}()
	if err != nil {
		h.cfg.logger.Error("admin: audit sink failed", "action", string(e.Action), "err", err)
	}
}

// noteAuthorize records what Authorize was asked and its answer.
func noteAuthorize(ctx context.Context, action Action, res Resource, err error) {
	rec, ok := ctx.Value(auditKey{}).(*auditRecord)
	if !ok {
		return
	}
	rec.action, rec.res, rec.asked, rec.allowed = action, res, true, err == nil
	if err != nil {
		rec.err = errors.New("admin: not allowed: " + string(action))
	}
}

// noteError records the error a route answered with, the first one only
// (fail records the operation's error before writeError sends it).
func noteError(w http.ResponseWriter, err error) {
	aw, ok := w.(*auditWriter)
	if !ok || aw.rec.err != nil || err == nil {
		return
	}
	aw.rec.err = err
}
