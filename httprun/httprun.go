// Package httprun puts a workflow client behind three HTTP routes a
// frontend or another service calls: start a run, read its status and
// result, and follow it live with Server-Sent Events, streamed output
// included. It is the asynchronous request-reply pattern: the start route
// answers 202 Accepted with a Location to poll, so a request never waits
// for the work.
//
// Routes, relative to where the handler is mounted (http.StripPrefix):
//
//	POST /{workflow}        enqueue the body as the payload; 202 with Location: runs/{id}
//	GET  /runs/{id}         the run's status, and its result once it succeeded
//	GET  /runs/{id}/events  Server-Sent Events: status changes, streamed chunks, then "end"
//
// A run whose Enqueue returned only a ScheduledID (a delayed start) is
// addressed with ?scheduled=1, which the Location carries.
//
// # Closed by default
//
// The handler copies the admin module's gate: with a nil Auth.Identify or
// Auth.Authorize every request, on every path, is refused with 403 before
// anything else happens. With both set, each request is identified first
// (401 on error, before routing, so an anonymous caller learns nothing
// about the routes), then parsed, then authorized with its typed Action
// and Resource (403 on any error or panic), and only then run. Only the
// workflows named in Config.Workflows can be started; any other name is a
// 404, so adding a workflow to the client never exposes it here by
// accident. A start must send Content-Type: application/json (or a type in
// Config.MediaTypes), which a browser cannot send cross-site without a
// CORS preflight the handler never answers, so a forged form post from
// another site is refused (415). Payloads, run errors and labels are
// never sent back unless an option says so; the result of a succeeded
// run is, since returning it is what the status route is for.
//
// # Drivers
//
// Starting and polling status run on every driver. The result needs
// CapResult (workflow.ResultReader). Live status events come from
// workflow.LifecycleNotifier when the client implements it and New runs
// before Start; otherwise, and always as a safety net (lifecycle events
// are local to the process that ran the step), the route polls Status on
// Config.Poll. Streamed output needs a workflow.StreamReader in
// Config.Stream: workflow.MemoryStream serves one process; a fleet needs a
// shared store implementing the interface.
//
// It needs nothing beyond the standard library and the core module.
package httprun

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
)

// Action is what a request asks to do, as the Authorizer sees it. The
// names are stable, so an app can store them in its permission tables.
type Action string

// The actions, one per route.
const (
	ActionStart  Action = "runs.start"  // POST /{workflow}
	ActionStatus Action = "runs.status" // GET  /runs/{id}
	ActionEvents Action = "runs.events" // GET  /runs/{id}/events
)

// Actions lists every action, so a test or a permission screen can walk
// them.
var Actions = []Action{ActionStart, ActionStatus, ActionEvents}

// Resource is what an action is about.
type Resource struct {
	// Workflow is the workflow to start (ActionStart), or the run's
	// workflow when it is known (Info set).
	Workflow string

	// Job is the job about to be enqueued (ActionStart), after
	// Config.Prepare, so an Authorizer can check its tenant or labels.
	// It must not be modified.
	Job *workflow.Job

	// Run is the run for ActionStatus and ActionEvents.
	Run workflow.RunRef

	// Info is the run as RunQuerier reported it (ActionStatus,
	// ActionEvents) when the client has CapRunQuery and the run exists;
	// nil otherwise. An Authorizer that lets each user see only the runs
	// they started checks a label or the tenant here (Config.Prepare is
	// where the app puts it on the job). Authorize is called even when
	// the run is not found, with Info nil, so a refused caller gets 403
	// whether or not the run exists.
	Info *workflow.RunInfo
}

// Request is one HTTP call as the Authorizer sees it. P is the app's own
// principal type, whatever Identify returns.
type Request[P any] struct {
	Who      P
	Action   Action
	Resource Resource
	// HTTP is the request, for headers an Authorizer needs. Its body has
	// been read; do not read it.
	HTTP *http.Request
}

// Auth decides who may do what. Both functions are required: with either
// one nil the handler refuses every request, so the zero Auth is closed.
type Auth[P any] struct {
	// Identify returns the caller of r (a session cookie, a bearer
	// token). An error means anonymous: 401 before anything is parsed.
	Identify func(r *http.Request) (P, error)
	// Authorize allows req by returning nil. Any error, or a panic,
	// refuses it with 403, so an Authorizer that fails, fails closed.
	Authorize func(ctx context.Context, req Request[P]) error
}

// The defaults of Config's zero fields.
const (
	//ds:def id=httprun-defaultmaxbody-fwge93c7 owner=@khanakia stability=stable
	DefaultMaxBody = 1 << 20
	//ds:def id=httprun-defaultpoll-xu2qjbxs owner=@khanakia stability=stable
	DefaultPoll = time.Second
	//ds:def id=httprun-defaultheartbeat-evx2n8yc owner=@khanakia stability=stable
	DefaultHeartbeat = 15 * time.Second
)

// MediaTypeJSON is the default and only built-in start content type.
const MediaTypeJSON = "application/json"

// Config configures New. Only Auth and Workflows are needed.
//
//ds:def id=httprun-config-jnyk8j5f owner=@khanakia stability=stable
type Config[P any] struct {
	// Auth gates every request (see the package doc). Required.
	Auth Auth[P]

	// Workflows are the workflow names POST /{workflow} may start. Any
	// other name answers 404. Empty means none can be started.
	Workflows []string

	// MaxBody is the largest payload accepted (DefaultMaxBody, 1 MiB);
	// a larger body answers 413.
	MaxBody int64

	// MediaTypes are the Content-Types a start may send (default only
	// application/json, whose body must then be valid JSON). The three a
	// browser sends cross-site without a preflight
	// (application/x-www-form-urlencoded, multipart/form-data,
	// text/plain) are ignored here, so the CSRF guard cannot be turned
	// off by accident.
	MediaTypes []string

	// Prepare edits the job before it is authorized and enqueued: set the
	// tenant or an owner label from the caller, a queue, a priority. An
	// error refuses the request (400 for one matching workflow.ErrInvalid,
	// 403 otherwise). It may read the identity with PrincipalFrom.
	Prepare func(r *http.Request, job *workflow.Job) error

	// Mount is the path the handler is mounted at ("/api/jobs"); when
	// set, Location is Mount + "/runs/{id}". Empty means a relative
	// Location ("runs/{id}"), which resolves under the mount as long as
	// the start path is directly below it.
	Mount string

	// Payloads includes the run's payload in GET /runs/{id} (needs
	// CapRunQuery). Off by default: a payload can carry personal data.
	Payloads bool

	// Errors includes the run's error text in status responses and
	// events. Off by default: an error message can name internal hosts
	// or data, and the caller of a public route is not an operator.
	Errors bool

	// Stream, when set, is where GET /runs/{id}/events reads the chunks
	// steps sent with workflow.Emit.
	Stream workflow.StreamReader

	// Poll is how often the events route asks Status when no lifecycle
	// event arrives (DefaultPoll).
	Poll time.Duration

	// Heartbeat is how often the events route sends an SSE comment so
	// proxies keep the connection open (DefaultHeartbeat).
	Heartbeat time.Duration

	// Clock drives Poll and Heartbeat; nil means clock.Real.
	Clock clock.Clock

	// Logger logs internal errors (answered with a bare 500), panics in
	// Auth, and why live lifecycle events are off. Nil discards them.
	Logger *slog.Logger
}

// The error codes in an error response's "code" field.
const (
	CodeUnauthorized = "unauthorized"           // 401
	CodeForbidden    = "forbidden"              // 403
	CodeInvalid      = "invalid"                // 400
	CodeNotFound     = "not_found"              // 404
	CodeTooLarge     = "too_large"              // 413
	CodeMediaType    = "unsupported_media_type" // 415
	CodeConflict     = "conflict"               // 409
	CodeUnsupported  = "unsupported"            // 501
	CodeUnavailable  = "unavailable"            // 503
	CodeInternal     = "internal"               // 500
)

type principalKey[P any] struct{}

// PrincipalFrom returns the identity Identify gave for the request whose
// ctx this is (inside Prepare, for example).
func PrincipalFrom[P any](ctx context.Context) (P, bool) {
	p, ok := ctx.Value(principalKey[P]{}).(P)
	return p, ok
}

// corsSimple are the content types a cross-site form can send without a
// preflight; never accepted.
var corsSimple = []string{"application/x-www-form-urlencoded", "multipart/form-data", "text/plain"}

type handler[P any] struct {
	c         workflow.Client
	cfg       Config[P]
	workflows map[string]bool
	media     map[string]bool
	mux       *http.ServeMux
	hub       *hub // nil when no lifecycle events
}

// New returns the handler over c. It is closed until cfg.Auth has both
// functions (see the package doc). When c implements
// workflow.LifecycleNotifier, New registers an observer for live status
// events; that only works before c.Start (afterwards the client answers
// ErrAlreadyStarted, logged, and the events route polls).
//
//ds:def id=httprun-new-hzhj9wnu owner=@khanakia stability=stable
func New[P any](c workflow.Client, cfg Config[P]) http.Handler {
	if cfg.MaxBody <= 0 {
		cfg.MaxBody = DefaultMaxBody
	}
	if cfg.Poll <= 0 {
		cfg.Poll = DefaultPoll
	}
	if cfg.Heartbeat <= 0 {
		cfg.Heartbeat = DefaultHeartbeat
	}
	if cfg.Clock == nil {
		cfg.Clock = clock.Real{}
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.DiscardHandler)
	}
	h := &handler[P]{c: c, cfg: cfg, workflows: map[string]bool{}, media: map[string]bool{}}
	for _, w := range cfg.Workflows {
		h.workflows[w] = true
	}
	types := cfg.MediaTypes
	if len(types) == 0 {
		types = []string{MediaTypeJSON}
	}
	for _, t := range types {
		t = strings.ToLower(strings.TrimSpace(t))
		if t != "" && !slices.Contains(corsSimple, t) {
			h.media[t] = true
		}
	}
	if ln, ok := c.(workflow.LifecycleNotifier); ok {
		hb := newHub()
		if err := ln.OnLifecycle(hb.observe); err != nil {
			cfg.Logger.Info("httprun: live lifecycle events are off; the events route polls Status", "err", err)
		} else {
			h.hub = hb
		}
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /{workflow}", h.start)
	mux.HandleFunc("GET /runs/{id}", h.status)
	mux.HandleFunc("GET /runs/{id}/events", h.events)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		writeError(w, http.StatusNotFound, CodeNotFound, "no such route")
	})
	h.mux = mux
	return h
}

// ServeHTTP is the gate: closed without Auth, 401 without an identity,
// then the routes.
//
//ds:def id=httprun-handler-p-servehttp-53jzjthw owner=@khanakia stability=stable
func (h *handler[P]) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if h.cfg.Auth.Identify == nil || h.cfg.Auth.Authorize == nil {
		writeError(w, http.StatusForbidden, CodeForbidden, "httprun is closed: no Auth.Identify and Auth.Authorize configured")
		return
	}
	who, err := h.identify(r)
	if err != nil {
		writeError(w, http.StatusUnauthorized, CodeUnauthorized, "authentication required")
		return
	}
	ctx := context.WithValue(r.Context(), principalKey[P]{}, who)
	h.mux.ServeHTTP(w, r.WithContext(ctx))
}

func (h *handler[P]) identify(r *http.Request) (who P, err error) {
	defer func() {
		if p := recover(); p != nil {
			h.cfg.Logger.Error("httprun: Identify panicked", "panic", fmt.Sprint(p))
			err = errors.New("httprun: Identify panicked")
		}
	}()
	return h.cfg.Auth.Identify(r)
}

// authorize asks Authorize, writing 403 and returning false on refusal or
// panic.
func (h *handler[P]) authorize(w http.ResponseWriter, r *http.Request, action Action, res Resource) bool {
	who, _ := PrincipalFrom[P](r.Context())
	err := func() (err error) {
		defer func() {
			if p := recover(); p != nil {
				h.cfg.Logger.Error("httprun: Authorize panicked", "action", string(action), "panic", fmt.Sprint(p))
				err = errors.New("httprun: Authorize panicked")
			}
		}()
		return h.cfg.Auth.Authorize(r.Context(), Request[P]{Who: who, Action: action, Resource: res, HTTP: r})
	}()
	if err != nil {
		writeError(w, http.StatusForbidden, CodeForbidden, "not allowed: "+string(action))
		return false
	}
	return true
}

// fail maps an error to a status. Known sentinels are sent with their
// message; anything else is logged and answered with a bare 500.
func (h *handler[P]) fail(w http.ResponseWriter, action Action, err error) {
	switch {
	case errors.Is(err, workflow.ErrInvalid):
		writeError(w, http.StatusBadRequest, CodeInvalid, err.Error())
	case errors.Is(err, workflow.ErrNotFound):
		writeError(w, http.StatusNotFound, CodeNotFound, err.Error())
	case errors.Is(err, workflow.ErrUnsupported):
		writeError(w, http.StatusNotImplemented, CodeUnsupported, err.Error())
	case errors.Is(err, workflow.ErrWrongState), errors.Is(err, workflow.ErrDuplicate):
		writeError(w, http.StatusConflict, CodeConflict, err.Error())
	case errors.Is(err, workflow.ErrNotStarted), workflow.IsUnavailable(err):
		h.cfg.Logger.Warn("httprun: backend unavailable", "action", string(action), "err", err)
		writeError(w, http.StatusServiceUnavailable, CodeUnavailable, "the job backend is unavailable")
	default:
		h.cfg.Logger.Error("httprun: operation failed", "action", string(action), "err", err)
		writeError(w, http.StatusInternalServerError, CodeInternal, "internal error")
	}
}

// --- POST /{workflow} ---

// HeaderIdempotencyKey is the request header whose value becomes the
// job's IdempotencyKey (needs CapIdempotencyKey; the driver refuses it
// otherwise, answered 501).
//
//ds:def id=httprun-headeridempotencykey-nz6c4mt2 owner=@khanakia stability=stable
const HeaderIdempotencyKey = "Idempotency-Key"

//ds:def id=httprun-handler-p-start-gzefdf7z owner=@khanakia stability=stable
func (h *handler[P]) start(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("workflow")
	if !h.workflows[name] {
		writeError(w, http.StatusNotFound, CodeNotFound, "no such workflow")
		return
	}
	mt, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || !h.media[strings.ToLower(mt)] {
		writeError(w, http.StatusUnsupportedMediaType, CodeMediaType, "a start must send one of the accepted Content-Types")
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, h.cfg.MaxBody))
	if err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			writeError(w, http.StatusRequestEntityTooLarge, CodeTooLarge, fmt.Sprintf("payload is larger than %d bytes", h.cfg.MaxBody))
			return
		}
		writeError(w, http.StatusBadRequest, CodeInvalid, "could not read the body")
		return
	}
	if strings.EqualFold(mt, MediaTypeJSON) && len(body) > 0 && !json.Valid(body) {
		writeError(w, http.StatusBadRequest, CodeInvalid, "the body is not valid JSON")
		return
	}
	job := workflow.Job{Workflow: name, IdempotencyKey: r.Header.Get(HeaderIdempotencyKey)}
	if len(body) > 0 {
		job.Payload = body
	}
	if h.cfg.Prepare != nil {
		if err := h.cfg.Prepare(r, &job); err != nil {
			if errors.Is(err, workflow.ErrInvalid) {
				writeError(w, http.StatusBadRequest, CodeInvalid, err.Error())
			} else {
				writeError(w, http.StatusForbidden, CodeForbidden, "refused by Prepare")
			}
			return
		}
		job.Workflow = name // Prepare may not redirect the start to another workflow
	}
	if !h.authorize(w, r, ActionStart, Resource{Workflow: name, Job: &job}) {
		return
	}
	ref, err := h.c.Enqueue(r.Context(), job)
	if err != nil {
		h.fail(w, ActionStart, err)
		return
	}
	loc := h.location(ref)
	w.Header().Set("Location", loc)
	writeJSON(w, http.StatusAccepted, struct {
		Ref       refJSON `json:"ref"`
		StatusURL string  `json:"status_url"`
		EventsURL string  `json:"events_url"`
	}{toRefJSON(ref), loc, eventsURL(loc)})
}

// location is where the run's status is read.
//
//ds:def id=httprun-handler-p-location-pt4m8y6s owner=@khanakia stability=stable
func (h *handler[P]) location(ref workflow.RunRef) string {
	base := "runs/"
	if h.cfg.Mount != "" {
		base = strings.TrimSuffix(h.cfg.Mount, "/") + "/runs/"
	}
	if ref.RunID.IsZero() {
		return base + url.PathEscape(ref.ScheduledID.String()) + "?scheduled=1"
	}
	return base + url.PathEscape(ref.RunID.String())
}

// eventsURL turns a status URL into its events URL, keeping the query.
func eventsURL(loc string) string {
	path, query, ok := strings.Cut(loc, "?")
	if ok {
		return path + "/events?" + query
	}
	return path + "/events"
}

// --- GET /runs/{id} ---

func pathRef(r *http.Request) workflow.RunRef {
	id := r.PathValue("id")
	if v := r.URL.Query().Get("scheduled"); v == "1" || v == "true" {
		return workflow.RunRef{ScheduledID: workflow.ScheduledID(id)}
	}
	return workflow.RunRef{RunID: workflow.RunID(id)}
}

// lookup reads the run's RunInfo when the client can list runs. found is
// false for an unknown run; err is set only for a real failure.
//
//ds:def id=httprun-handler-p-lookup-qr4zv6ub owner=@khanakia stability=stable
func (h *handler[P]) lookup(ctx context.Context, ref workflow.RunRef) (info *workflow.RunInfo, err error) {
	q, ok := h.c.(workflow.RunQuerier)
	if !ok || !h.c.Capabilities().Has(workflow.CapRunQuery) {
		return nil, nil
	}
	page, err := q.Runs(ctx, workflow.RunQuery{Ref: ref, Limit: 1})
	if err != nil {
		if errors.Is(err, workflow.ErrNotFound) || errors.Is(err, workflow.ErrUnsupported) {
			return nil, nil
		}
		return nil, err
	}
	if len(page.Runs) == 0 {
		return nil, nil
	}
	return &page.Runs[0], nil
}

// open reads the run and authorizes action on it. It writes the response
// and returns false when the request ends here.
//
//ds:def id=httprun-handler-p-open-uj2bzwjj owner=@khanakia stability=stable
func (h *handler[P]) open(w http.ResponseWriter, r *http.Request, action Action) (workflow.RunRef, *workflow.RunInfo, bool) {
	ref := pathRef(r)
	if ref.IsZero() {
		writeError(w, http.StatusBadRequest, CodeInvalid, "empty run ID")
		return ref, nil, false
	}
	info, err := h.lookup(r.Context(), ref)
	if err != nil {
		h.fail(w, action, err)
		return ref, nil, false
	}
	res := Resource{Run: ref, Info: info}
	if info != nil {
		res.Workflow = info.Workflow
	}
	if !h.authorize(w, r, action, res) {
		return ref, nil, false
	}
	return ref, info, true
}

//ds:def id=httprun-handler-p-status-xjcaa4jd owner=@khanakia stability=stable
func (h *handler[P]) status(w http.ResponseWriter, r *http.Request) {
	ref, info, ok := h.open(w, r, ActionStatus)
	if !ok {
		return
	}
	st, err := h.c.Status(r.Context(), ref)
	if err != nil {
		h.fail(w, ActionStatus, err)
		return
	}
	out := h.statusJSON(ref, st, info)
	if st == workflow.StatusSucceeded && h.hasResults() {
		res, err := h.c.(workflow.ResultReader).Result(r.Context(), ref)
		switch {
		case err == nil:
			out.setResult(res)
		case errors.Is(err, workflow.ErrNotFound), errors.Is(err, workflow.ErrWrongState):
			// pruned between the two calls: the status still stands
		default:
			h.fail(w, ActionStatus, err)
			return
		}
	}
	writeJSON(w, http.StatusOK, out)
}

func (h *handler[P]) hasResults() bool {
	_, ok := h.c.(workflow.ResultReader)
	return ok && h.c.Capabilities().Has(workflow.CapResult)
}

type refJSON struct {
	RunID       string `json:"run_id,omitempty"`
	ScheduledID string `json:"scheduled_id,omitempty"`
}

func toRefJSON(r workflow.RunRef) refJSON {
	return refJSON{RunID: r.RunID.String(), ScheduledID: r.ScheduledID.String()}
}

type progressJSON struct {
	Percent float64 `json:"percent"`
	Message string  `json:"message,omitempty"`
	Step    string  `json:"step,omitempty"`
}

// statusJSON is the body of GET /runs/{id}.
type statusJSON struct {
	Ref      refJSON         `json:"ref"`
	Workflow string          `json:"workflow,omitempty"`
	Status   workflow.Status `json:"status"`
	// Done is true once the status is terminal.
	Done       bool          `json:"done"`
	Error      string        `json:"error,omitempty"`
	Progress   *progressJSON `json:"progress,omitempty"`
	EnqueuedAt time.Time     `json:"enqueued_at,omitzero"`
	StartedAt  time.Time     `json:"started_at,omitzero"`
	FinishedAt time.Time     `json:"finished_at,omitzero"`
	// Result is the run's result when it is valid JSON; ResultBase64
	// holds any other result.
	Result       json.RawMessage `json:"result,omitempty"`
	ResultBase64 []byte          `json:"result_base64,omitempty"`
	// Payload and PayloadBase64 likewise, only with Config.Payloads.
	Payload       json.RawMessage `json:"payload,omitempty"`
	PayloadBase64 []byte          `json:"payload_base64,omitempty"`
}

func (s *statusJSON) setResult(b []byte) {
	if len(b) == 0 {
		return
	}
	if json.Valid(b) {
		s.Result = json.RawMessage(b)
	} else {
		s.ResultBase64 = b
	}
}

//ds:def id=httprun-handler-p-statusjson-8accxucp owner=@khanakia stability=stable
func (h *handler[P]) statusJSON(ref workflow.RunRef, st workflow.Status, info *workflow.RunInfo) statusJSON {
	out := statusJSON{Ref: toRefJSON(ref), Status: st, Done: st.IsTerminal()}
	if info == nil {
		return out
	}
	out.Ref = toRefJSON(info.Ref)
	out.Workflow = info.Workflow
	out.EnqueuedAt, out.StartedAt, out.FinishedAt = info.EnqueuedAt, info.StartedAt, info.FinishedAt
	if h.cfg.Errors {
		out.Error = info.Error
	}
	if p := info.Progress; p != nil {
		out.Progress = &progressJSON{Percent: p.Percent, Message: p.Message, Step: p.Step}
	}
	if h.cfg.Payloads && len(info.Payload) > 0 {
		if json.Valid(info.Payload) {
			out.Payload = json.RawMessage(info.Payload)
		} else {
			out.PayloadBase64 = info.Payload
		}
	}
	return out
}

// --- lifecycle hub ---

// hub fans lifecycle events out to the events routes following each run.
type hub struct {
	mu   sync.Mutex
	subs map[workflow.RunID]map[chan workflow.LifecycleEvent]struct{}
}

func newHub() *hub {
	return &hub{subs: map[workflow.RunID]map[chan workflow.LifecycleEvent]struct{}{}}
}

// observe is the LifecycleFunc. It never blocks: a follower whose buffer
// is full misses the event and catches up on its next poll.
func (b *hub) observe(_ context.Context, ev workflow.LifecycleEvent) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for ch := range b.subs[ev.Ref.RunID] {
		select {
		case ch <- ev:
		default:
		}
	}
}

func (b *hub) follow(run workflow.RunID) (<-chan workflow.LifecycleEvent, func()) {
	ch := make(chan workflow.LifecycleEvent, 32)
	b.mu.Lock()
	if b.subs[run] == nil {
		b.subs[run] = map[chan workflow.LifecycleEvent]struct{}{}
	}
	b.subs[run][ch] = struct{}{}
	b.mu.Unlock()
	return ch, func() {
		b.mu.Lock()
		delete(b.subs[run], ch)
		if len(b.subs[run]) == 0 {
			delete(b.subs, run)
		}
		b.mu.Unlock()
	}
}

// --- responses ---

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", MediaTypeJSON)
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

type errorJSON struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

func writeError(w http.ResponseWriter, status int, code, msg string) {
	var e errorJSON
	e.Error.Code, e.Error.Message = code, msg
	writeJSON(w, status, e)
}
