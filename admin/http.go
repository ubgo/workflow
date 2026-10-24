package admin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"mime"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/ubgo/workflow"
)

// Action is what an HTTP request asks to do, as the Authorizer sees it.
// The names are stable, so an app can store them in its own permission
// tables.
type Action string

// The actions, one per route.
const (
	ActionCapabilities  Action = "capabilities.read" // GET  /capabilities
	ActionListRuns      Action = "runs.list"         // GET  /runs
	ActionShowRun       Action = "runs.show"         // GET  /runs/{id}
	ActionListFailures  Action = "failures.list"     // GET  /failures
	ActionListDiscarded Action = "discarded.list"    // GET  /discarded
	ActionRetryRun      Action = "runs.retry"        // POST /runs/{id}/retry
	ActionCancelRun     Action = "runs.cancel"       // POST /runs/{id}/cancel
	ActionDeleteRun     Action = "runs.delete"       // DELETE /runs/{id}
	ActionCancelRuns    Action = "runs.cancel_all"   // POST /runs/cancel
	ActionPause         Action = "pauses.create"     // POST /pause
	ActionResume        Action = "pauses.delete"     // POST /resume
	ActionListPauses    Action = "pauses.list"       // GET  /pauses
	ActionListSchedules Action = "schedules.list"    // GET  /schedules
	ActionQueueStats    Action = "queues.list"       // GET  /queues
	// ActionPauseSchedule and ActionResumeSchedule pause and resume one
	// schedule by ID.
	ActionPauseSchedule  Action = "schedules.pause"  // POST /schedules/{id}/pause
	ActionResumeSchedule Action = "schedules.resume" // POST /schedules/{id}/resume
	// ActionShowSchedule reads one schedule with its last and next fire;
	// ActionTriggerSchedule runs it now.
	ActionShowSchedule    Action = "schedules.show"    // GET  /schedules/{id}
	ActionTriggerSchedule Action = "schedules.trigger" // POST /schedules/{id}/trigger
	// ActionForceComplete and ActionForceFail end a run from outside
	// (workflow.RunForcer).
	ActionForceComplete Action = "runs.force_complete" // POST /runs/{id}/complete
	ActionForceFail     Action = "runs.force_fail"     // POST /runs/{id}/fail
	// ActionShowOutput reads a run's captured step output (WithOutputs).
	ActionShowOutput Action = "runs.output" // GET /runs/{id}/output
	// ActionRetryRuns and ActionDeleteRuns retry or forget every run a
	// filter matches (Admin.RetryAll, Admin.DeleteAll), dry runs included.
	ActionRetryRuns  Action = "runs.retry_all"  // POST /runs/retry
	ActionDeleteRuns Action = "runs.delete_all" // POST /runs/delete
	// ActionMetrics reads run-end history for the charts (WithMetrics).
	ActionMetrics Action = "metrics.read" // GET /metrics
	// ActionListNodes lists the online nodes; ActionHealth reads the
	// client's health report.
	ActionListNodes Action = "nodes.list"  // GET /nodes
	ActionHealth    Action = "health.read" // GET /health
	// ActionRescheduleRun moves a delayed run; ActionCancelStep withdraws
	// one step of a run.
	ActionRescheduleRun Action = "runs.reschedule"  // POST /runs/{id}/reschedule
	ActionCancelStep    Action = "runs.cancel_step" // POST /runs/{id}/steps/{step}/cancel
	// ActionUI is loading the web UI's page and its files. The UI's data
	// comes from the routes above, each authorized with its own action,
	// so a caller allowed only ActionUI sees an empty screen.
	ActionUI Action = "ui.read" // GET /, GET /ui, GET /ui/{file}
)

// Actions lists every action, so a test or a permission screen can walk
// them.
var Actions = []Action{
	ActionCapabilities, ActionListRuns, ActionShowRun, ActionListFailures, ActionListDiscarded,
	ActionRetryRun, ActionCancelRun, ActionDeleteRun, ActionCancelRuns,
	ActionPause, ActionResume, ActionListPauses, ActionListSchedules, ActionQueueStats,
	ActionPauseSchedule, ActionResumeSchedule, ActionShowSchedule, ActionTriggerSchedule,
	ActionForceComplete, ActionForceFail, ActionShowOutput, ActionUI,
	ActionRetryRuns, ActionDeleteRuns, ActionMetrics,
	ActionListNodes, ActionHealth, ActionRescheduleRun, ActionCancelStep,
}

// ReadOnly reports whether the action only reads. An Authorizer that gives
// some users a read-only view allows exactly these.
func (a Action) ReadOnly() bool {
	switch a {
	case ActionCapabilities, ActionListRuns, ActionShowRun, ActionListFailures, ActionListDiscarded,
		ActionListPauses, ActionListSchedules, ActionQueueStats, ActionShowSchedule, ActionShowOutput, ActionUI, ActionMetrics,
		ActionListNodes, ActionHealth:
		return true
	}
	return false
}

// Resource is what an action is about. The field the action uses is set;
// the others are zero.
type Resource struct {
	// Run is the run for ActionShowRun, ActionRetryRun, ActionCancelRun,
	// ActionDeleteRun, ActionForceComplete, ActionForceFail,
	// ActionShowOutput, ActionRescheduleRun and ActionCancelStep.
	Run workflow.RunRef
	// Query is the filter for ActionListRuns, ActionListFailures,
	// ActionCancelRuns, ActionRetryRuns and ActionDeleteRuns, so an
	// Authorizer can scope a caller to its own workflows, queues or labels
	// (a tenant label, for example).
	Query *workflow.RunQuery
	// DryRun is true when a bulk action (ActionCancelRuns,
	// ActionRetryRuns, ActionDeleteRuns) only previews, so an Authorizer
	// can let more people preview than act.
	DryRun bool
	// Metrics is the query for ActionMetrics.
	Metrics *MetricsQuery
	// Step is the step for ActionCancelStep.
	Step string
	// At is the new time for ActionRescheduleRun.
	At time.Time
	// Pause is the target for ActionPause and ActionResume.
	Pause *workflow.PauseTarget
	// Schedule is the schedule for ActionPauseSchedule,
	// ActionResumeSchedule, ActionShowSchedule and ActionTriggerSchedule.
	Schedule workflow.ScheduleID
	// Reason is the operator's reason for ActionForceFail, so an
	// Authorizer can require one or write it to an audit log.
	Reason string
	// File is the UI file for ActionUI ("index.html", "app.js", "app.css").
	File string
}

// Request is one HTTP call as the Authorizer sees it: who, which action,
// which resource. P is the app's own principal type, whatever Identify
// returns.
type Request[P any] struct {
	Who      P
	Action   Action
	Resource Resource
	// HTTP is the request, for headers an Authorizer needs (a tenant
	// header, the remote address). Its body has been read; do not read it.
	HTTP *http.Request
}

// Auth is how the handler decides who may do what. Both functions are
// required: a handler with either one nil refuses every request, so the
// zero Auth is closed.
type Auth[P any] struct {
	// Identify returns the caller of r, from a session cookie or a bearer
	// token. An error means anonymous: the request is refused with 401
	// before any of it is parsed.
	Identify func(r *http.Request) (P, error)
	// Authorize allows req by returning nil. Any error, or a panic, refuses
	// it with 403, so an Authorizer that fails, fails closed.
	Authorize func(ctx context.Context, req Request[P]) error
}

// HandlerOption configures NewHandler.
type HandlerOption func(*handlerConfig)

type handlerConfig struct {
	payloads bool
	stacks   bool
	logger   *slog.Logger
	maxBody  int64
}

// WithPayloads includes each run's payload (base64) in run responses. By
// default it is left out, because payloads can carry personal data and an
// admin screen is seen by more people than the code that wrote them.
func WithPayloads() HandlerOption { return func(c *handlerConfig) { c.payloads = true } }

// WithStacks includes the stack trace of each attempt error
// (workflow.AttemptError.Stack) in run responses. By default it is left
// out: a stack names source files and functions, which an admin screen
// does not need to show to everyone who can read run errors.
func WithStacks() HandlerOption { return func(c *handlerConfig) { c.stacks = true } }

// WithLogger logs internal errors (the ones answered with a bare 500) and
// authorization panics. Nil, the default, discards them.
func WithLogger(l *slog.Logger) HandlerOption { return func(c *handlerConfig) { c.logger = l } }

// DefaultMaxBody is the largest request body the handler reads.
const DefaultMaxBody = 64 << 10

// mediaTypeJSON is the only request and response body type the API speaks.
// A write must declare it (a CSRF guard: a cross-site form post cannot set
// it without a preflight), and every response carries it.
const mediaTypeJSON = "application/json"

// The error codes in an error response's "code" field.
const (
	CodeUnauthorized = "unauthorized"           // 401: Identify failed
	CodeForbidden    = "forbidden"              // 403: no Auth configured, or Authorize refused
	CodeInvalid      = "invalid"                // 400: a malformed request or workflow.ErrInvalid
	CodeNotFound     = "not_found"              // 404: workflow.ErrNotFound, or no such route
	CodeUnsupported  = "unsupported"            // 501: workflow.ErrUnsupported
	CodeConflict     = "conflict"               // 409: workflow.ErrWrongState, workflow.ErrDuplicate or workflow.ErrOverlap
	CodeMediaType    = "unsupported_media_type" // 415: a write without Content-Type: application/json
	CodeInternal     = "internal"               // 500: anything else; the detail is logged, not sent
)

// principalKey carries the identified principal from the gate to a route.
type principalKey[P any] struct{}

// handler serves the admin API for one principal type.
type handler[P any] struct {
	a    *Admin
	auth Auth[P]
	cfg  handlerConfig
	mux  *http.ServeMux
}

// NewHandler returns the admin JSON API over a. It is closed by default:
// with a nil Auth.Identify or Auth.Authorize every request, on every path,
// is refused with 403 before anything else happens. With both set, each
// request is identified first (401 on error, also before routing, so an
// anonymous caller learns nothing about the routes), then parsed, then
// authorized with its action and resource (403 on any error or panic),
// and only then run.
//
// Writes (POST, DELETE) must send Content-Type: application/json, which a
// browser cannot send cross-site without a CORS preflight the handler
// never answers, so a forged form post from another site is refused (415).
// Mount it under a prefix with http.StripPrefix.
//
// Routes (JSON in and out; times are RFC 3339):
//
//	GET    /capabilities      the operations the client supports, and what it lacks
//	GET    /runs              list and search runs; filters below; paginated
//	GET    /runs/{id}         one run (?scheduled=1 when id is a ScheduledID)
//	GET    /failures          failed runs; same filters but status
//	GET    /discarded         runs a step discarded; same filters but status
//	POST   /runs/{id}/retry   retry a failed run from its failed step
//	POST   /runs/{id}/cancel  cancel one run
//	DELETE /runs/{id}         forget a finished run
//	POST   /runs/cancel       cancel every run a filter body matches; returns the count; {"dry_run":true} counts only
//	POST   /pause             body {"workflow":..} | {"queue":..} | {"all":true}
//	POST   /resume            same body
//	GET    /pauses            the active pauses
//	GET    /schedules         schedules with next fire times; ?limit= and ?cursor=
//	GET    /queues            queue depth and oldest wait (workers and running when the driver scales workers)
//	POST   /schedules/{id}/pause   stop one schedule firing
//	POST   /schedules/{id}/resume  let it fire again
//	GET    /schedules/{id}         one schedule: next fire, overlap policy, last fire and its run
//	POST   /schedules/{id}/trigger run it now; returns the run's ref (409 when its overlap policy skips)
//	POST   /runs/{id}/complete     force-complete a run that is not terminal, or is failed
//	POST   /runs/{id}/fail         force-fail a run that is not terminal; body {"reason":".."}
//	GET    /runs/{id}/output       captured step output, oldest attempt first (needs WithOutputs, else 501)
//	POST   /runs/retry             retry every failed run a filter body matches; {"dry_run":true} previews
//	POST   /runs/delete            forget every finished run a filter body matches; same body
//	GET    /metrics                run ends per step by status (needs WithMetrics, else 501); ?from=&to= or ?window=, ?step=, ?workflow=
//	GET    /nodes                  the online nodes (workflow.NodeLister)
//	GET    /health                 the client's health report: live, ready and each check
//	POST   /runs/{id}/reschedule   move a delayed run; body {"at":"RFC 3339"}
//	POST   /runs/{id}/steps/{step}/cancel  withdraw one step of a run
//	GET    /ui/               the web UI (see below); GET / and GET /ui redirect to it
//
// Run filters: workflow, queue, tenant (CapTenant), due_from and due_to
// (RunInfo.RunAt, CapDueQuery), status (repeat or comma-separated),
// label=key=value (repeat; every pair must match; a value ending in "*"
// is a prefix, needing CapLabelPrefix, else 501), from and to (enqueue
// time), limit and cursor (from the previous page's "next"). On /runs,
// /failures and /discarded, count=1 adds "total", "truncated" and
// "cursors" (the cursor that starts each page at this limit), read by
// Admin.CountRuns within the max scan, so a screen can show numbered
// pages over the driver's opaque cursors.
//
// The web UI is three files embedded in this package (a page, a style
// sheet and a script; no third-party code, nothing fetched at run time),
// served behind the same gate as the API: Identify, then Authorize with
// ActionUI. Every call the UI makes is a request to the routes above and
// is authorized on its own. A browser sends no Authorization header when
// it loads a page, so Identify must accept the app's session cookie for
// the UI to open. Pages are sent with a Content-Security-Policy that
// allows only these files and same-origin requests.
func NewHandler[P any](a *Admin, auth Auth[P], opts ...HandlerOption) http.Handler {
	h := &handler[P]{a: a, auth: auth, cfg: handlerConfig{maxBody: DefaultMaxBody}}
	for _, o := range opts {
		if o != nil {
			o(&h.cfg)
		}
	}
	if h.cfg.logger == nil {
		h.cfg.logger = slog.New(slog.DiscardHandler)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /capabilities", h.capabilities)
	mux.HandleFunc("GET /runs", h.listRuns)
	mux.HandleFunc("GET /runs/{id}", h.showRun)
	mux.HandleFunc("GET /failures", h.listFailures)
	mux.HandleFunc("GET /discarded", h.listDiscarded)
	mux.HandleFunc("POST /runs/{id}/retry", h.retryRun)
	mux.HandleFunc("POST /runs/{id}/cancel", h.cancelRun)
	mux.HandleFunc("DELETE /runs/{id}", h.deleteRun)
	mux.HandleFunc("POST /runs/cancel", h.cancelRuns)
	mux.HandleFunc("POST /pause", h.pause)
	mux.HandleFunc("POST /resume", h.resume)
	mux.HandleFunc("GET /pauses", h.listPauses)
	mux.HandleFunc("GET /schedules", h.listSchedules)
	mux.HandleFunc("GET /queues", h.queues)
	mux.HandleFunc("POST /schedules/{id}/pause", h.pauseSchedule)
	mux.HandleFunc("POST /schedules/{id}/resume", h.resumeSchedule)
	mux.HandleFunc("GET /schedules/{id}", h.showSchedule)
	mux.HandleFunc("POST /schedules/{id}/trigger", h.triggerSchedule)
	mux.HandleFunc("POST /runs/{id}/complete", h.forceComplete)
	mux.HandleFunc("POST /runs/{id}/fail", h.forceFail)
	mux.HandleFunc("GET /runs/{id}/output", h.showOutput)
	mux.HandleFunc("POST /runs/retry", h.retryRuns)
	mux.HandleFunc("POST /runs/delete", h.deleteRuns)
	mux.HandleFunc("GET /metrics", h.showMetrics)
	mux.HandleFunc("GET /nodes", h.listNodes)
	mux.HandleFunc("GET /health", h.health)
	mux.HandleFunc("POST /runs/{id}/reschedule", h.reschedule)
	mux.HandleFunc("POST /runs/{id}/steps/{step}/cancel", h.cancelStep)
	mux.HandleFunc("GET /{$}", h.uiRedirect)
	mux.HandleFunc("GET /ui", h.uiRedirect)
	mux.HandleFunc("GET /ui/{file...}", h.uiFile)
	h.mux = mux
	return h
}

// ServeHTTP is the gate: closed without Auth, 401 without an identity,
// 415 for a write without a JSON content type; then the routes.
func (h *handler[P]) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if h.auth.Identify == nil || h.auth.Authorize == nil {
		writeError(w, http.StatusForbidden, CodeForbidden, "admin API is closed: no Auth.Identify and Auth.Authorize configured")
		return
	}
	who, err := h.identify(r)
	if err != nil {
		writeError(w, http.StatusUnauthorized, CodeUnauthorized, "authentication required")
		return
	}
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		mt, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
		if err != nil || mt != mediaTypeJSON {
			writeError(w, http.StatusUnsupportedMediaType, CodeMediaType, "writes need Content-Type: application/json")
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, h.cfg.maxBody)
	}
	ctx := context.WithValue(r.Context(), principalKey[P]{}, who)
	h.mux.ServeHTTP(w, r.WithContext(ctx))
}

// identify calls Identify, turning a panic into an error so it fails
// closed.
func (h *handler[P]) identify(r *http.Request) (who P, err error) {
	defer func() {
		if p := recover(); p != nil {
			h.cfg.logger.Error("admin: Identify panicked", "panic", fmt.Sprint(p))
			err = errors.New("admin: Identify panicked")
		}
	}()
	return h.auth.Identify(r)
}

// authorize asks Authorize about action and res, writing 403 and returning
// false on refusal. A panic refuses too.
func (h *handler[P]) authorize(w http.ResponseWriter, r *http.Request, action Action, res Resource) (ok bool) {
	who, _ := r.Context().Value(principalKey[P]{}).(P)
	err := func() (err error) {
		defer func() {
			if p := recover(); p != nil {
				h.cfg.logger.Error("admin: Authorize panicked", "action", string(action), "panic", fmt.Sprint(p))
				err = errors.New("admin: Authorize panicked")
			}
		}()
		return h.auth.Authorize(r.Context(), Request[P]{Who: who, Action: action, Resource: res, HTTP: r})
	}()
	if err != nil {
		writeError(w, http.StatusForbidden, CodeForbidden, "not allowed: "+string(action))
		return false
	}
	return true
}

// fail maps an operation error to a status and code. Errors with a known
// sentinel are sent with their message, which names only the run, the
// filter field or the capability; anything else is logged and answered
// with a bare 500, so a database error never reaches the browser.
func (h *handler[P]) fail(w http.ResponseWriter, action Action, err error) {
	switch {
	case errors.Is(err, workflow.ErrInvalid):
		writeError(w, http.StatusBadRequest, CodeInvalid, err.Error())
	case errors.Is(err, workflow.ErrNotFound):
		writeError(w, http.StatusNotFound, CodeNotFound, err.Error())
	case errors.Is(err, workflow.ErrUnsupported):
		writeError(w, http.StatusNotImplemented, CodeUnsupported, err.Error())
	case errors.Is(err, workflow.ErrWrongState), errors.Is(err, workflow.ErrDuplicate), errors.Is(err, workflow.ErrOverlap):
		writeError(w, http.StatusConflict, CodeConflict, err.Error())
	default:
		h.cfg.logger.Error("admin: operation failed", "action", string(action), "err", err)
		writeError(w, http.StatusInternalServerError, CodeInternal, "internal error")
	}
}

// --- routes ---

func (h *handler[P]) capabilities(w http.ResponseWriter, r *http.Request) {
	if !h.authorize(w, r, ActionCapabilities, Resource{}) {
		return
	}
	type op struct {
		Operation Operation `json:"operation"`
		Supported bool      `json:"supported"`
		Missing   string    `json:"missing,omitempty"`
		Reason    string    `json:"reason,omitempty"`
	}
	sup := h.a.Support()
	out := struct {
		Capabilities string `json:"capabilities"`
		Operations   []op   `json:"operations"`
		// Output is true when captured step output can be read
		// (WithOutputs), so a screen hides the output panel otherwise.
		Output bool `json:"output"`
		// Metrics is true when run-end history can be read (WithMetrics),
		// so a screen hides the charts otherwise.
		Metrics bool `json:"metrics"`
	}{Capabilities: h.a.c.Capabilities().String(), Operations: make([]op, 0, len(sup)), Output: h.a.HasOutputs(), Metrics: h.a.HasMetrics()}
	for _, s := range sup {
		o := op{Operation: s.Operation, Supported: s.Supported, Reason: s.Reason}
		if s.Missing != 0 {
			o.Missing = s.Missing.String()
		}
		out.Operations = append(out.Operations, o)
	}
	writeJSON(w, http.StatusOK, out)
}

func (h *handler[P]) listRuns(w http.ResponseWriter, r *http.Request) {
	q, err := parseRunQuery(r, true)
	if err != nil {
		writeError(w, http.StatusBadRequest, CodeInvalid, err.Error())
		return
	}
	if !h.authorize(w, r, ActionListRuns, Resource{Query: &q}) {
		return
	}
	page, err := h.a.Runs(r.Context(), q)
	if err != nil {
		h.fail(w, ActionListRuns, err)
		return
	}
	out := h.pageJSON(page)
	if wantCount(r) {
		if err := h.addCount(r.Context(), &out, q); err != nil {
			h.fail(w, ActionListRuns, err)
			return
		}
	}
	writeJSON(w, http.StatusOK, out)
}

// wantCount reports whether a list request asked for count=1.
func wantCount(r *http.Request) bool {
	v := r.URL.Query().Get("count")
	return v == "1" || v == "true"
}

// addCount fills the page's count fields for q, counted from the first
// page whatever cursor the request carried.
func (h *handler[P]) addCount(ctx context.Context, out *pageJSON, q workflow.RunQuery) error {
	q.Cursor = ""
	c, err := h.a.CountRuns(ctx, q)
	if err != nil {
		return err
	}
	out.Total, out.Truncated, out.Cursors = &c.Total, c.Truncated, c.Cursors
	return nil
}

func (h *handler[P]) listFailures(w http.ResponseWriter, r *http.Request) {
	h.listStatus(w, r, ActionListFailures, workflow.StatusFailed, h.a.Failures)
}

func (h *handler[P]) listDiscarded(w http.ResponseWriter, r *http.Request) {
	h.listStatus(w, r, ActionListDiscarded, workflow.StatusDiscarded, h.a.Discarded)
}

// listStatus serves /failures and /discarded: run filters without a
// status, the action authorized, then list.
func (h *handler[P]) listStatus(w http.ResponseWriter, r *http.Request, action Action, st workflow.Status, list func(context.Context, workflow.RunQuery) (workflow.RunPage, error)) {
	q, err := parseRunQuery(r, false)
	if err != nil {
		writeError(w, http.StatusBadRequest, CodeInvalid, err.Error())
		return
	}
	if !h.authorize(w, r, action, Resource{Query: &q}) {
		return
	}
	page, err := list(r.Context(), q)
	if err != nil {
		h.fail(w, action, err)
		return
	}
	out := h.pageJSON(page)
	if wantCount(r) {
		// list succeeded, so the operation is supported; count the same
		// single status it listed.
		q.Statuses = []workflow.Status{st}
		if err := h.addCount(r.Context(), &out, q); err != nil {
			h.fail(w, action, err)
			return
		}
	}
	writeJSON(w, http.StatusOK, out)
}

func (h *handler[P]) showRun(w http.ResponseWriter, r *http.Request) {
	ref := pathRef(r)
	if !h.authorize(w, r, ActionShowRun, Resource{Run: ref}) {
		return
	}
	info, err := h.a.Run(r.Context(), ref)
	if err != nil {
		h.fail(w, ActionShowRun, err)
		return
	}
	writeJSON(w, http.StatusOK, h.runJSON(info))
}

func (h *handler[P]) retryRun(w http.ResponseWriter, r *http.Request) {
	ref := pathRef(r)
	if !h.authorize(w, r, ActionRetryRun, Resource{Run: ref}) {
		return
	}
	out, err := h.a.Retry(r.Context(), ref)
	if err != nil {
		h.fail(w, ActionRetryRun, err)
		return
	}
	writeJSON(w, http.StatusOK, struct {
		Ref refJSON `json:"ref"`
	}{toRefJSON(out)})
}

func (h *handler[P]) cancelRun(w http.ResponseWriter, r *http.Request) {
	ref := pathRef(r)
	if !h.authorize(w, r, ActionCancelRun, Resource{Run: ref}) {
		return
	}
	if err := h.a.Cancel(r.Context(), ref); err != nil {
		h.fail(w, ActionCancelRun, err)
		return
	}
	writeJSON(w, http.StatusOK, okJSON{OK: true})
}

func (h *handler[P]) deleteRun(w http.ResponseWriter, r *http.Request) {
	ref := pathRef(r)
	if !h.authorize(w, r, ActionDeleteRun, Resource{Run: ref}) {
		return
	}
	if err := h.a.Delete(r.Context(), ref); err != nil {
		h.fail(w, ActionDeleteRun, err)
		return
	}
	writeJSON(w, http.StatusOK, okJSON{OK: true})
}

func (h *handler[P]) cancelRuns(w http.ResponseWriter, r *http.Request) {
	var body cancelJSON
	if err := decodeBody(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, CodeInvalid, err.Error())
		return
	}
	q, err := body.query()
	if err != nil {
		writeError(w, http.StatusBadRequest, CodeInvalid, err.Error())
		return
	}
	if !h.authorize(w, r, ActionCancelRuns, Resource{Query: &q, DryRun: body.DryRun}) {
		return
	}
	if body.DryRun {
		res, err := h.a.PreviewCancelAll(r.Context(), q)
		if err != nil {
			h.fail(w, ActionCancelRuns, err)
			return
		}
		writeJSON(w, http.StatusOK, toBulkJSON(res, true))
		return
	}
	n, err := h.a.CancelAll(r.Context(), q)
	if err != nil {
		h.fail(w, ActionCancelRuns, err)
		return
	}
	writeJSON(w, http.StatusOK, struct {
		Cancelled int `json:"cancelled"`
	}{n})
}

func (h *handler[P]) pause(w http.ResponseWriter, r *http.Request) {
	h.pauseOrResume(w, r, ActionPause, h.a.Pause)
}

func (h *handler[P]) resume(w http.ResponseWriter, r *http.Request) {
	h.pauseOrResume(w, r, ActionResume, h.a.Resume)
}

func (h *handler[P]) pauseOrResume(w http.ResponseWriter, r *http.Request, action Action, op func(context.Context, workflow.PauseTarget) error) {
	var body pauseJSON
	if err := decodeBody(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, CodeInvalid, err.Error())
		return
	}
	t := workflow.PauseTarget{Workflow: body.Workflow, Queue: body.Queue, All: body.All}
	if err := workflow.ValidatePauseTarget(t); err != nil {
		writeError(w, http.StatusBadRequest, CodeInvalid, err.Error())
		return
	}
	if !h.authorize(w, r, action, Resource{Pause: &t}) {
		return
	}
	if err := op(r.Context(), t); err != nil {
		h.fail(w, action, err)
		return
	}
	writeJSON(w, http.StatusOK, okJSON{OK: true})
}

func (h *handler[P]) listPauses(w http.ResponseWriter, r *http.Request) {
	if !h.authorize(w, r, ActionListPauses, Resource{}) {
		return
	}
	ps, err := h.a.Paused(r.Context())
	if err != nil {
		h.fail(w, ActionListPauses, err)
		return
	}
	out := make([]pauseJSON, 0, len(ps))
	for _, p := range ps {
		out = append(out, pauseJSON{Workflow: p.Workflow, Queue: p.Queue, All: p.All})
	}
	writeJSON(w, http.StatusOK, struct {
		Pauses []pauseJSON `json:"pauses"`
	}{out})
}

// DefaultSchedulesLimit and MaxSchedulesLimit bound a page of GET
// /schedules.
const (
	DefaultSchedulesLimit = 100
	MaxSchedulesLimit     = 1000
)

func (h *handler[P]) listSchedules(w http.ResponseWriter, r *http.Request) {
	limit, offset, err := parsePage(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, CodeInvalid, err.Error())
		return
	}
	if !h.authorize(w, r, ActionListSchedules, Resource{}) {
		return
	}
	all, err := h.a.Schedules(r.Context())
	if err != nil {
		h.fail(w, ActionListSchedules, err)
		return
	}
	out := struct {
		Schedules []scheduleJSON `json:"schedules"`
		Next      string         `json:"next,omitempty"`
		Total     int            `json:"total"`
	}{Schedules: []scheduleJSON{}, Total: len(all)}
	end := min(offset+limit, len(all))
	for _, s := range all[min(offset, len(all)):end] {
		out.Schedules = append(out.Schedules, toScheduleJSON(s))
	}
	if end < len(all) {
		out.Next = strconv.Itoa(end)
	}
	writeJSON(w, http.StatusOK, out)
}

func (h *handler[P]) queues(w http.ResponseWriter, r *http.Request) {
	if !h.authorize(w, r, ActionQueueStats, Resource{}) {
		return
	}
	rep, err := h.a.QueueStats(r.Context())
	if err != nil {
		h.fail(w, ActionQueueStats, err)
		return
	}
	type q struct {
		Queue            string    `json:"queue"`
		Depth            int       `json:"depth"`
		OldestEnqueuedAt time.Time `json:"oldest_enqueued_at"`
		OldestWaitSecs   float64   `json:"oldest_wait_seconds"`
		Workers          *int      `json:"workers,omitempty"`
		Running          *int      `json:"running,omitempty"`
	}
	out := struct {
		Queues    []q       `json:"queues"`
		Truncated bool      `json:"truncated"`
		At        time.Time `json:"at"`
	}{Queues: []q{}, Truncated: rep.Truncated, At: rep.At}
	for _, s := range rep.Queues {
		row := q{Queue: s.Queue, Depth: s.Depth, OldestEnqueuedAt: s.OldestEnqueuedAt, OldestWaitSecs: s.OldestWait.Seconds()}
		if rep.Workers {
			// Pointers so a zero count is shown on a scaling driver and
			// the fields are absent on one that cannot report them.
			row.Workers, row.Running = &s.Workers, &s.Running
		}
		out.Queues = append(out.Queues, row)
	}
	writeJSON(w, http.StatusOK, out)
}

func (h *handler[P]) pauseSchedule(w http.ResponseWriter, r *http.Request) {
	h.scheduleOp(w, r, ActionPauseSchedule, h.a.PauseSchedule)
}

func (h *handler[P]) resumeSchedule(w http.ResponseWriter, r *http.Request) {
	h.scheduleOp(w, r, ActionResumeSchedule, h.a.ResumeSchedule)
}

// scheduleOp is pause and resume of one schedule: the path's ID,
// authorized, then the operation.
func (h *handler[P]) scheduleOp(w http.ResponseWriter, r *http.Request, action Action, op func(context.Context, workflow.ScheduleID) error) {
	id := workflow.ScheduleID(r.PathValue("id"))
	if !h.authorize(w, r, action, Resource{Schedule: id}) {
		return
	}
	if err := op(r.Context(), id); err != nil {
		h.fail(w, action, err)
		return
	}
	writeJSON(w, http.StatusOK, okJSON{OK: true})
}

func (h *handler[P]) showSchedule(w http.ResponseWriter, r *http.Request) {
	id := workflow.ScheduleID(r.PathValue("id"))
	if !h.authorize(w, r, ActionShowSchedule, Resource{Schedule: id}) {
		return
	}
	s, err := h.a.Schedule(r.Context(), id)
	if err != nil {
		h.fail(w, ActionShowSchedule, err)
		return
	}
	writeJSON(w, http.StatusOK, toScheduleJSON(s))
}

func (h *handler[P]) triggerSchedule(w http.ResponseWriter, r *http.Request) {
	id := workflow.ScheduleID(r.PathValue("id"))
	if !h.authorize(w, r, ActionTriggerSchedule, Resource{Schedule: id}) {
		return
	}
	ref, err := h.a.TriggerSchedule(r.Context(), id)
	if err != nil {
		h.fail(w, ActionTriggerSchedule, err)
		return
	}
	writeJSON(w, http.StatusOK, struct {
		Ref refJSON `json:"ref"`
	}{toRefJSON(ref)})
}

func (h *handler[P]) forceComplete(w http.ResponseWriter, r *http.Request) {
	ref := pathRef(r)
	if !h.authorize(w, r, ActionForceComplete, Resource{Run: ref}) {
		return
	}
	if err := h.a.ForceComplete(r.Context(), ref); err != nil {
		h.fail(w, ActionForceComplete, err)
		return
	}
	writeJSON(w, http.StatusOK, okJSON{OK: true})
}

// forceJSON is the body of POST /runs/{id}/fail.
type forceJSON struct {
	Reason string `json:"reason"`
}

func (h *handler[P]) forceFail(w http.ResponseWriter, r *http.Request) {
	var body forceJSON
	if err := decodeBody(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, CodeInvalid, err.Error())
		return
	}
	ref := pathRef(r)
	if !h.authorize(w, r, ActionForceFail, Resource{Run: ref, Reason: body.Reason}) {
		return
	}
	if err := h.a.ForceFail(r.Context(), ref, body.Reason); err != nil {
		h.fail(w, ActionForceFail, err)
		return
	}
	writeJSON(w, http.StatusOK, okJSON{OK: true})
}

type outputJSON struct {
	Step      string    `json:"step"`
	Attempt   int       `json:"attempt"`
	Started   time.Time `json:"started,omitzero"`
	Finished  time.Time `json:"finished,omitzero"`
	Text      string    `json:"text"`
	Truncated bool      `json:"truncated,omitempty"`
	Error     string    `json:"error,omitempty"`
}

func (h *handler[P]) showOutput(w http.ResponseWriter, r *http.Request) {
	ref := pathRef(r)
	if !h.authorize(w, r, ActionShowOutput, Resource{Run: ref}) {
		return
	}
	outs, err := h.a.RunOutput(r.Context(), ref)
	if err != nil {
		h.fail(w, ActionShowOutput, err)
		return
	}
	res := struct {
		Attempts []outputJSON `json:"attempts"`
	}{Attempts: make([]outputJSON, 0, len(outs))}
	for _, o := range outs {
		res.Attempts = append(res.Attempts, outputJSON(o))
	}
	writeJSON(w, http.StatusOK, res)
}

func (h *handler[P]) retryRuns(w http.ResponseWriter, r *http.Request) {
	h.bulk(w, r, ActionRetryRuns, h.a.RetryAll)
}

func (h *handler[P]) deleteRuns(w http.ResponseWriter, r *http.Request) {
	h.bulk(w, r, ActionDeleteRuns, h.a.DeleteAll)
}

// bulk serves POST /runs/retry and /runs/delete: the filter body,
// authorized with the dry-run flag, then the operation.
func (h *handler[P]) bulk(w http.ResponseWriter, r *http.Request, action Action, op func(context.Context, workflow.RunQuery, BulkOptions) (BulkResult, error)) {
	var body bulkBodyJSON
	if err := decodeBody(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, CodeInvalid, err.Error())
		return
	}
	q := body.runQuery()
	if err := workflow.ValidateRunQuery(q); err != nil {
		writeError(w, http.StatusBadRequest, CodeInvalid, err.Error())
		return
	}
	if !h.authorize(w, r, action, Resource{Query: &q, DryRun: body.DryRun}) {
		return
	}
	res, err := op(r.Context(), q, BulkOptions{DryRun: body.DryRun, Max: body.Max})
	if err != nil {
		h.fail(w, action, err)
		return
	}
	writeJSON(w, http.StatusOK, toBulkJSON(res, body.DryRun))
}

type nodeJSON struct {
	ID        string            `json:"id"`
	Mode      workflow.NodeMode `json:"mode"`
	Queues    []string          `json:"queues"`
	Workers   int               `json:"workers"`
	Running   int               `json:"running"`
	StartedAt time.Time         `json:"started_at,omitzero"`
	LastSeen  time.Time         `json:"last_seen,omitzero"`
}

func (h *handler[P]) listNodes(w http.ResponseWriter, r *http.Request) {
	if !h.authorize(w, r, ActionListNodes, Resource{}) {
		return
	}
	ns, err := h.a.Nodes(r.Context())
	if err != nil {
		h.fail(w, ActionListNodes, err)
		return
	}
	out := struct {
		Nodes []nodeJSON `json:"nodes"`
	}{Nodes: make([]nodeJSON, 0, len(ns))}
	for _, n := range ns {
		row := nodeJSON(n)
		if row.Queues == nil {
			row.Queues = []string{}
		}
		out.Nodes = append(out.Nodes, row)
	}
	writeJSON(w, http.StatusOK, out)
}

type healthCheckJSON struct {
	Name   string `json:"name"`
	OK     bool   `json:"ok"`
	Detail string `json:"detail,omitempty"`
}

// health answers 200 whatever the verdict: this is a screen's read of the
// report, not a probe (a probe needs no auth and answers 503 when not
// ready; that is the app's route, built on workflow.CheckHealth).
func (h *handler[P]) health(w http.ResponseWriter, r *http.Request) {
	if !h.authorize(w, r, ActionHealth, Resource{}) {
		return
	}
	rep := h.a.Health(r.Context())
	out := struct {
		Live   bool              `json:"live"`
		Ready  bool              `json:"ready"`
		Checks []healthCheckJSON `json:"checks"`
	}{Live: rep.Live, Ready: rep.Ready, Checks: make([]healthCheckJSON, 0, len(rep.Checks))}
	for _, c := range rep.Checks {
		out.Checks = append(out.Checks, healthCheckJSON(c))
	}
	writeJSON(w, http.StatusOK, out)
}

// rescheduleJSON is the body of POST /runs/{id}/reschedule.
type rescheduleJSON struct {
	At time.Time `json:"at"`
}

func (h *handler[P]) reschedule(w http.ResponseWriter, r *http.Request) {
	var body rescheduleJSON
	if err := decodeBody(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, CodeInvalid, err.Error())
		return
	}
	ref := pathRef(r)
	if !h.authorize(w, r, ActionRescheduleRun, Resource{Run: ref, At: body.At}) {
		return
	}
	out, err := h.a.Reschedule(r.Context(), ref, body.At)
	if err != nil {
		h.fail(w, ActionRescheduleRun, err)
		return
	}
	writeJSON(w, http.StatusOK, struct {
		Ref refJSON `json:"ref"`
	}{toRefJSON(out)})
}

func (h *handler[P]) cancelStep(w http.ResponseWriter, r *http.Request) {
	ref := pathRef(r)
	step := r.PathValue("step")
	if !h.authorize(w, r, ActionCancelStep, Resource{Run: ref, Step: step}) {
		return
	}
	if err := h.a.CancelStep(r.Context(), ref, step); err != nil {
		h.fail(w, ActionCancelStep, err)
		return
	}
	writeJSON(w, http.StatusOK, okJSON{OK: true})
}

// DefaultMetricsWindow is the span GET /metrics covers without from or
// window, and DefaultMetricsPoints how many steps it splits a span into
// without step.
const (
	DefaultMetricsWindow = time.Hour
	DefaultMetricsPoints = 60
)

type metricsPointJSON struct {
	At        time.Time `json:"at"`
	Succeeded int64     `json:"succeeded"`
	Failed    int64     `json:"failed"`
	Cancelled int64     `json:"cancelled"`
	Discarded int64     `json:"discarded"`
}

func (h *handler[P]) showMetrics(w http.ResponseWriter, r *http.Request) {
	q, err := parseMetricsQuery(r, h.a.clk.Now())
	if err != nil {
		writeError(w, http.StatusBadRequest, CodeInvalid, err.Error())
		return
	}
	if !h.authorize(w, r, ActionMetrics, Resource{Metrics: &q}) {
		return
	}
	s, err := h.a.Metrics(r.Context(), q)
	if err != nil {
		h.fail(w, ActionMetrics, err)
		return
	}
	out := struct {
		From        time.Time          `json:"from"`
		To          time.Time          `json:"to"`
		StepSeconds float64            `json:"step_seconds"`
		Since       time.Time          `json:"since,omitzero"`
		Points      []metricsPointJSON `json:"points"`
	}{From: q.From, To: q.To, StepSeconds: s.Step.Seconds(), Since: s.Since, Points: make([]metricsPointJSON, 0, len(s.Points))}
	for _, p := range s.Points {
		out.Points = append(out.Points, metricsPointJSON(p))
	}
	writeJSON(w, http.StatusOK, out)
}

// parseMetricsQuery reads from, to, window, step and workflow. Without to
// the span ends now; without from it starts window (DefaultMetricsWindow)
// before to; without step it is split into DefaultMetricsPoints steps.
func parseMetricsQuery(r *http.Request, now time.Time) (MetricsQuery, error) {
	v := r.URL.Query()
	q := MetricsQuery{Workflow: v.Get("workflow")}
	var err error
	if q.To, err = parseTime(v.Get("to"), "to"); err != nil {
		return q, err
	}
	if q.To.IsZero() {
		q.To = now
	}
	if q.From, err = parseTime(v.Get("from"), "from"); err != nil {
		return q, err
	}
	if q.From.IsZero() {
		window := DefaultMetricsWindow
		if s := v.Get("window"); s != "" {
			if window, err = time.ParseDuration(s); err != nil || window <= 0 {
				return q, fmt.Errorf("window %q: want a positive duration such as 1h", s)
			}
		}
		q.From = q.To.Add(-window)
	} else if v.Get("window") != "" {
		return q, errors.New("give from or window, not both")
	}
	if s := v.Get("step"); s != "" {
		if q.Step, err = time.ParseDuration(s); err != nil || q.Step <= 0 {
			return q, fmt.Errorf("step %q: want a positive duration such as 1m", s)
		}
	} else {
		q.Step = max(q.To.Sub(q.From)/DefaultMetricsPoints, time.Second)
	}
	return q, ValidateMetricsQuery(q)
}

// --- parsing ---

// pathRef reads the {id} path value: a RunID, or a ScheduledID with
// ?scheduled=1.
func pathRef(r *http.Request) workflow.RunRef {
	id := r.PathValue("id")
	if v := r.URL.Query().Get("scheduled"); v == "1" || v == "true" {
		return workflow.RunRef{ScheduledID: workflow.ScheduledID(id)}
	}
	return workflow.RunRef{RunID: workflow.RunID(id)}
}

// parseRunQuery reads run filters from the query string. withStatus is
// false for /failures, which refuses a status filter.
func parseRunQuery(r *http.Request, withStatus bool) (workflow.RunQuery, error) {
	v := r.URL.Query()
	q := workflow.RunQuery{Workflow: v.Get("workflow"), Queue: v.Get("queue"), Tenant: v.Get("tenant"), Cursor: v.Get("cursor")}
	for _, raw := range v["status"] {
		if !withStatus {
			return q, errors.New("this route lists one status; use /runs to filter by status")
		}
		for _, s := range strings.Split(raw, ",") {
			if s = strings.TrimSpace(s); s != "" {
				q.Statuses = append(q.Statuses, workflow.Status(s))
			}
		}
	}
	for _, raw := range v["label"] {
		k, val, ok := strings.Cut(raw, "=")
		if !ok || k == "" {
			return q, fmt.Errorf("label %q: want key=value or key=prefix*", raw)
		}
		if prefix, isPrefix := strings.CutSuffix(val, labelPrefixMark); isPrefix {
			if q.LabelPrefixes == nil {
				q.LabelPrefixes = map[string]string{}
			}
			q.LabelPrefixes[k] = prefix
			continue
		}
		if q.Labels == nil {
			q.Labels = map[string]string{}
		}
		q.Labels[k] = val
	}
	var err error
	if q.From, err = parseTime(v.Get("from"), "from"); err != nil {
		return q, err
	}
	if q.To, err = parseTime(v.Get("to"), "to"); err != nil {
		return q, err
	}
	if q.DueFrom, err = parseTime(v.Get("due_from"), "due_from"); err != nil {
		return q, err
	}
	if q.DueTo, err = parseTime(v.Get("due_to"), "due_to"); err != nil {
		return q, err
	}
	if s := v.Get("limit"); s != "" {
		if q.Limit, err = strconv.Atoi(s); err != nil {
			return q, fmt.Errorf("limit %q is not a number", s)
		}
	}
	return q, workflow.ValidateRunQuery(q)
}

// labelPrefixMark ends a label filter value that is a prefix:
// label=tenant=ac* matches tenant values starting with "ac", and
// label=tenant=* matches any run that has the key. Only a trailing mark
// counts, so an exact value cannot end in "*" through this API.
const labelPrefixMark = "*"

func parseTime(s, name string) (time.Time, error) {
	if s == "" {
		return time.Time{}, nil
	}
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return time.Time{}, fmt.Errorf("%s %q is not an RFC 3339 time", name, s)
	}
	return t, nil
}

// parsePage reads ?limit= and ?cursor= for /schedules, whose cursor is an
// offset.
func parsePage(r *http.Request) (limit, offset int, err error) {
	limit = DefaultSchedulesLimit
	if s := r.URL.Query().Get("limit"); s != "" {
		if limit, err = strconv.Atoi(s); err != nil || limit < 1 || limit > MaxSchedulesLimit {
			return 0, 0, fmt.Errorf("limit %q: want 1 to %d", s, MaxSchedulesLimit)
		}
	}
	if s := r.URL.Query().Get("cursor"); s != "" {
		if offset, err = strconv.Atoi(s); err != nil || offset < 0 {
			return 0, 0, fmt.Errorf("cursor %q is not one this API returned", s)
		}
	}
	return limit, offset, nil
}

// decodeBody reads one JSON object, refusing unknown fields and trailing
// data, so a typo in a filter is an error rather than a wider cancel.
func decodeBody(r *http.Request, v any) error {
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return fmt.Errorf("body: %w", err)
	}
	if dec.More() {
		return errors.New("body: more than one JSON value")
	}
	return nil
}

// --- JSON shapes ---

type okJSON struct {
	OK bool `json:"ok"`
}

type refJSON struct {
	RunID       string `json:"run_id,omitempty"`
	ScheduledID string `json:"scheduled_id,omitempty"`
}

func toRefJSON(r workflow.RunRef) refJSON {
	return refJSON{RunID: r.RunID.String(), ScheduledID: r.ScheduledID.String()}
}

// filterJSON is the run filter in the body of POST /runs/cancel,
// /runs/retry and /runs/delete.
type filterJSON struct {
	Workflow string            `json:"workflow,omitempty"`
	Queue    string            `json:"queue,omitempty"`
	Tenant   string            `json:"tenant,omitempty"`
	Statuses []workflow.Status `json:"statuses,omitempty"`
	Labels   map[string]string `json:"labels,omitempty"`
	// LabelPrefixes is RunQuery.LabelPrefixes: key to value prefix.
	LabelPrefixes map[string]string `json:"label_prefixes,omitempty"`
	From          time.Time         `json:"from,omitzero"`
	To            time.Time         `json:"to,omitzero"`
}

// runQuery is the filter as a RunQuery, unvalidated.
func (f filterJSON) runQuery() workflow.RunQuery {
	return workflow.RunQuery{Workflow: f.Workflow, Queue: f.Queue, Tenant: f.Tenant, Statuses: f.Statuses, Labels: f.Labels, LabelPrefixes: f.LabelPrefixes, From: f.From, To: f.To}
}

// query is the filter as a cancel query, validated for CancelAll.
func (f filterJSON) query() (workflow.RunQuery, error) {
	q := f.runQuery()
	return q, workflow.ValidateCancelQuery(q)
}

// cancelJSON is the body of POST /runs/cancel: the filter, and dry_run to
// count without cancelling.
type cancelJSON struct {
	filterJSON
	DryRun bool `json:"dry_run,omitempty"`
}

// bulkBodyJSON is the body of POST /runs/retry and /runs/delete: the
// filter, dry_run to preview, and max to act on fewer runs than the
// Admin's bound.
type bulkBodyJSON struct {
	filterJSON
	DryRun bool `json:"dry_run,omitempty"`
	Max    int  `json:"max,omitempty"`
}

// bulkJSON is the answer of a bulk call or a preview.
type bulkJSON struct {
	Matched   int       `json:"matched"`
	Done      int       `json:"done"`
	Skipped   int       `json:"skipped"`
	Truncated bool      `json:"truncated"`
	DryRun    bool      `json:"dry_run"`
	Sample    []refJSON `json:"sample"`
}

func toBulkJSON(r BulkResult, dry bool) bulkJSON {
	out := bulkJSON{Matched: r.Matched, Done: r.Done, Skipped: r.Skipped, Truncated: r.Truncated, DryRun: dry, Sample: make([]refJSON, 0, len(r.Sample))}
	for _, ref := range r.Sample {
		out.Sample = append(out.Sample, toRefJSON(ref))
	}
	return out
}

type pauseJSON struct {
	Workflow string `json:"workflow,omitempty"`
	Queue    string `json:"queue,omitempty"`
	All      bool   `json:"all,omitempty"`
}

type runJSON struct {
	Ref        refJSON           `json:"ref"`
	Workflow   string            `json:"workflow"`
	Queue      string            `json:"queue"`
	Status     workflow.Status   `json:"status"`
	Error      string            `json:"error,omitempty"`
	FailedStep string            `json:"failed_step,omitempty"`
	Labels     map[string]string `json:"labels,omitempty"`
	Payload    []byte            `json:"payload,omitempty"`
	Priority   int               `json:"priority"`
	EnqueuedAt time.Time         `json:"enqueued_at,omitzero"`
	StartedAt  time.Time         `json:"started_at,omitzero"`
	FinishedAt time.Time         `json:"finished_at,omitzero"`
	Attempts   int               `json:"attempts"`
	// NextAttempt and NextAttemptAt are set only while the run is
	// retrying (workflow.RunInfo.NextAttempt).
	NextAttempt   int       `json:"next_attempt,omitempty"`
	NextAttemptAt time.Time `json:"next_attempt_at,omitzero"`
	// Errors is the per-attempt error history, oldest first, on a driver
	// with CapErrorHistory; each stack only with WithStacks.
	Errors []attemptErrorJSON `json:"errors,omitempty"`
	// Tenant is RunInfo.Tenant (CapTenant).
	Tenant string `json:"tenant,omitempty"`
	// RunAt is when a delayed run is or was due (CapDueQuery).
	RunAt time.Time `json:"run_at,omitzero"`
	// Progress is the latest ReportProgress; its data only with
	// WithPayloads.
	Progress *progressJSON `json:"progress,omitempty"`
	// Output is the run's result once it succeeded (CapResult), base64,
	// only with WithPayloads: a result is app data like a payload.
	Output []byte `json:"output,omitempty"`
	// HasOutput is true when the run has a result, shown or not, so a
	// screen can say it is hidden.
	HasOutput bool `json:"has_output,omitempty"`
	// AttemptedBy is the nodes that ran its attempts (CapNodeIdentity).
	AttemptedBy []string `json:"attempted_by,omitempty"`
}

type progressJSON struct {
	Percent float64   `json:"percent"`
	Message string    `json:"message,omitempty"`
	Step    string    `json:"step,omitempty"`
	At      time.Time `json:"at,omitzero"`
	Data    []byte    `json:"data,omitempty"`
}

type attemptErrorJSON struct {
	Step    string    `json:"step"`
	Attempt int       `json:"attempt"`
	Error   string    `json:"error"`
	At      time.Time `json:"at,omitzero"`
	Stack   string    `json:"stack,omitempty"`
	Node    string    `json:"node,omitempty"`
}

func (h *handler[P]) runJSON(r workflow.RunInfo) runJSON {
	out := runJSON{
		Ref: toRefJSON(r.Ref), Workflow: r.Workflow, Queue: r.Queue, Status: r.Status,
		Error: r.Error, FailedStep: r.FailedStep, Labels: r.Labels, Priority: r.Priority,
		EnqueuedAt: r.EnqueuedAt, StartedAt: r.StartedAt, FinishedAt: r.FinishedAt, Attempts: r.Attempts,
		NextAttempt: r.NextAttempt, NextAttemptAt: r.NextAttemptAt,
		Tenant: r.Tenant, RunAt: r.RunAt, AttemptedBy: r.AttemptedBy, HasOutput: r.Output != nil,
	}
	if h.cfg.payloads {
		out.Payload = r.Payload
		out.Output = r.Output
	}
	if p := r.Progress; p != nil {
		out.Progress = &progressJSON{Percent: p.Percent, Message: p.Message, Step: p.Step, At: p.At}
		if h.cfg.payloads {
			out.Progress.Data = p.Data
		}
	}
	for _, e := range r.Errors {
		row := attemptErrorJSON{Step: e.Step, Attempt: e.Attempt, Error: e.Error, At: e.At, Node: e.Node}
		if h.cfg.stacks {
			row.Stack = e.Stack
		}
		out.Errors = append(out.Errors, row)
	}
	return out
}

type pageJSON struct {
	Runs []runJSON `json:"runs"`
	Next string    `json:"next,omitempty"`
	// Total, Truncated and Cursors are set with count=1 (Admin.CountRuns).
	Total     *int     `json:"total,omitempty"`
	Truncated bool     `json:"truncated,omitempty"`
	Cursors   []string `json:"cursors,omitempty"`
}

func (h *handler[P]) pageJSON(p workflow.RunPage) pageJSON {
	out := pageJSON{Runs: make([]runJSON, 0, len(p.Runs)), Next: p.Next}
	for _, r := range p.Runs {
		out.Runs = append(out.Runs, h.runJSON(r))
	}
	return out
}

type windowJSON struct {
	From string `json:"from"`
	To   string `json:"to"`
}

type scheduleJSON struct {
	ID       string `json:"id"`
	Name     string `json:"name,omitempty"`
	Workflow string `json:"workflow"`
	// Tenant is the tenant each fired run carries (Schedule.Tenant).
	Tenant   string            `json:"tenant,omitempty"`
	Cron     string            `json:"cron"`
	Location string            `json:"location"`
	Declared bool              `json:"declared"`
	Paused   bool              `json:"paused"`
	Next     time.Time         `json:"next,omitzero"`
	Between  []windowJSON      `json:"between,omitempty"`
	Unless   []windowJSON      `json:"unless,omitempty"`
	Labels   map[string]string `json:"labels,omitempty"`
	// Overlap is the schedule's overlap policy ("allow" when unset).
	Overlap workflow.OverlapPolicy `json:"overlap"`
	// Last is the last fire (tick or trigger), on a driver with
	// CapScheduleHistory that has fired it; absent otherwise.
	Last *scheduleLastJSON `json:"last,omitempty"`
}

type scheduleLastJSON struct {
	Fire   time.Time       `json:"fire,omitzero"`
	Run    refJSON         `json:"run"`
	Status workflow.Status `json:"status,omitempty"`
}

func toScheduleJSON(s workflow.ScheduleInfo) scheduleJSON {
	out := scheduleJSON{ID: s.ID.String(), Name: s.Name, Workflow: s.Workflow, Tenant: s.Tenant, Cron: s.Cron,
		Declared: s.Declared, Paused: s.Paused, Next: s.Next, Labels: s.Labels, Overlap: s.Overlap}
	if out.Overlap == "" {
		out.Overlap = workflow.OverlapAllow
	}
	if !s.Last.Fire.IsZero() || !s.Last.Run.IsZero() {
		out.Last = &scheduleLastJSON{Fire: s.Last.Fire, Run: toRefJSON(s.Last.Run), Status: s.Last.Status}
	}
	if s.Location != nil {
		out.Location = s.Location.String()
	}
	for _, w := range s.Windows.Between {
		out.Between = append(out.Between, windowJSON(w))
	}
	for _, w := range s.Windows.Unless {
		out.Unless = append(out.Unless, windowJSON(w))
	}
	return out
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", mediaTypeJSON)
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, code, msg string) {
	writeJSON(w, status, struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}{Error: struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	}{code, msg}})
}
