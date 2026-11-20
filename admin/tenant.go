package admin

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/ubgo/workflow"
)

// AllTenants is what a TenantScope returns for a caller who sees every
// tenant (an operator of the whole system): the request is then served
// exactly as without WithTenantScope. It is not a valid tenant ID
// (workflow.ValidateTenant refuses "*"), so it cannot collide with one.
const AllTenants = "*"

// TenantScope returns the tenant the caller who of r is confined to, or
// AllTenants for a caller who sees every tenant. It runs after Identify,
// on every request. An error, a panic, an empty string or a value that
// fails workflow.ValidateTenant refuses the request with 403, so a scope
// that cannot decide fails closed.
type TenantScope[P any] func(r *http.Request, who P) (tenant string, err error)

// WithTenantScope confines each caller to one tenant (workflow.Job.Tenant),
// for a dashboard a SaaS app shows its own customers. P must be the
// handler's principal type, the P of its Auth; a handler whose P differs
// refuses every request with 403, since it cannot ask the scope.
//
// For a caller scoped to tenant T:
//
//   - GET /runs, /failures and /discarded list only T's runs, counts
//     included: the tenant filter is set to T before the request is
//     parsed and authorized, so Resource.Query carries it. A request
//     naming another tenant is refused with 403.
//   - POST /runs/cancel, /runs/retry and /runs/delete act only on T's
//     runs, the same way, dry runs included.
//   - Every route on one run (show, retry, cancel, delete, complete,
//     fail, output, reschedule, cancel a step) reads the run first and
//     answers 404, the answer for a run that does not exist, when it
//     belongs to another tenant or to none.
//   - GET /schedules lists only the schedules whose Schedule.Tenant is T,
//     and every route on one schedule answers 404 for any other, so a
//     declared Cron (which has no tenant) is hidden.
//   - The routes that act on the whole system rather than on runs (POST
//     /pause, POST /resume, GET /pauses, GET /queues, GET /metrics, GET
//     /nodes, GET /health) are refused with 403: a queue, a pause, a node
//     or a chart is shared by every tenant. GET /capabilities reports
//     them as unsupported, with the tenant, so the web UI hides them.
//   - Any route this list does not name is refused with 403, so a route
//     added later is closed to scoped callers until it is scoped.
//
// Give an operator who must use those routes AllTenants. The client (and
// the WithRunQuerier querier, when set) must declare workflow.CapTenant:
// without it a run's tenant cannot be stored or filtered, and every
// request is refused with 501 and an UnsupportedError naming it.
//
// Authorize still runs on every request a scope lets through; TenantFrom
// reads the caller's tenant from its ctx.
func WithTenantScope[P any](fn TenantScope[P]) HandlerOption {
	return func(c *handlerConfig) {
		if fn != nil {
			c.tenant = fn
		}
	}
}

// tenantKey carries the caller's tenant from the gate to a route.
type tenantKey struct{}

// TenantFrom returns the tenant a request's ctx is scoped to, and false
// for an unscoped request (no WithTenantScope, or a caller given
// AllTenants). An Authorizer reads it from Request.HTTP.Context().
func TenantFrom(ctx context.Context) (string, bool) {
	t, ok := ctx.Value(tenantKey{}).(string)
	return t, ok
}

// tenantRule is what a scoped request to one route goes through.
type tenantRule uint8

const (
	tenantOpen      tenantRule = iota // nothing tenant-specific: served as is
	tenantList                        // the tenant query parameter is pinned
	tenantBulk                        // the body's tenant filter is pinned
	tenantRun                         // the run is read first: 404 unless it is the tenant's
	tenantSchedule                    // the schedule is read first: 404 unless it is the tenant's
	tenantSchedules                   // the list is filtered by the route (scopeSchedules)
	tenantRefused                     // a system-wide route: 403
)

// tenantRoute is one route as WithTenantScope treats it.
type tenantRoute struct {
	rule   tenantRule
	action Action
}

// tenantRoutes covers every route NewHandler registers, by the same
// pattern; TestTenantRoutes_CoverEveryRoute keeps the two lists equal.
var tenantRoutes = map[string]tenantRoute{
	"GET /capabilities":                   {tenantOpen, ActionCapabilities},
	"GET /runs":                           {tenantList, ActionListRuns},
	"GET /runs/{id}":                      {tenantRun, ActionShowRun},
	"GET /failures":                       {tenantList, ActionListFailures},
	"GET /discarded":                      {tenantList, ActionListDiscarded},
	"POST /runs/{id}/retry":               {tenantRun, ActionRetryRun},
	"POST /runs/{id}/cancel":              {tenantRun, ActionCancelRun},
	"DELETE /runs/{id}":                   {tenantRun, ActionDeleteRun},
	"POST /runs/cancel":                   {tenantBulk, ActionCancelRuns},
	"POST /pause":                         {tenantRefused, ActionPause},
	"POST /resume":                        {tenantRefused, ActionResume},
	"GET /pauses":                         {tenantRefused, ActionListPauses},
	"GET /schedules":                      {tenantSchedules, ActionListSchedules},
	"GET /queues":                         {tenantRefused, ActionQueueStats},
	"POST /schedules/{id}/pause":          {tenantSchedule, ActionPauseSchedule},
	"POST /schedules/{id}/resume":         {tenantSchedule, ActionResumeSchedule},
	"GET /schedules/{id}":                 {tenantSchedule, ActionShowSchedule},
	"POST /schedules/{id}/trigger":        {tenantSchedule, ActionTriggerSchedule},
	"POST /runs/{id}/complete":            {tenantRun, ActionForceComplete},
	"POST /runs/{id}/fail":                {tenantRun, ActionForceFail},
	"GET /runs/{id}/output":               {tenantRun, ActionShowOutput},
	"POST /runs/retry":                    {tenantBulk, ActionRetryRuns},
	"POST /runs/delete":                   {tenantBulk, ActionDeleteRuns},
	"GET /metrics":                        {tenantRefused, ActionMetrics},
	"GET /nodes":                          {tenantRefused, ActionListNodes},
	"GET /health":                         {tenantRefused, ActionHealth},
	"POST /runs/{id}/reschedule":          {tenantRun, ActionRescheduleRun},
	"POST /runs/{id}/steps/{step}/cancel": {tenantRun, ActionCancelStep},
	"GET /{$}":                            {tenantOpen, ActionUI},
	"GET /ui":                             {tenantOpen, ActionUI},
	"GET /ui/{file...}":                   {tenantOpen, ActionUI},
}

// refusedOps are the operations of the tenantRefused routes, reported as
// unsupported to a scoped caller by GET /capabilities.
var refusedOps = map[Operation]bool{OpQueues: true, OpPause: true, OpNodes: true, OpHealth: true}

// tenantGate is a handler's tenant scope, built by NewHandler.
type tenantGate[P any] struct {
	scope TenantScope[P]
	// closed, when set, refuses every request: the scope's principal type
	// does not match (403), or the client cannot store tenants (501).
	closed error
	mux    *http.ServeMux // the scoped routes, by tenantRoutes' patterns
}

// errScopeType closes a handler whose WithTenantScope was built for
// another principal type.
var errScopeType = errors.New("admin API is closed: WithTenantScope's principal type is not the handler's")

// newTenantGate builds the gate for cfg's scope, or returns nil when the
// handler has none.
func (h *handler[P]) newTenantGate() *tenantGate[P] {
	if h.cfg.tenant == nil {
		return nil
	}
	g := &tenantGate[P]{}
	scope, ok := h.cfg.tenant.(TenantScope[P])
	if !ok {
		g.closed = errScopeType
		return g
	}
	g.scope = scope
	missing := h.a.c.Capabilities().Missing(workflow.CapTenant)
	if h.a.rq != nil {
		missing = missing.With(h.a.rqCaps.Missing(workflow.CapTenant))
	}
	if !missing.IsZero() {
		g.closed = &workflow.UnsupportedError{Field: "admin.WithTenantScope", Missing: missing, Reason: "the client cannot store or filter a run's tenant"}
		return g
	}
	g.mux = http.NewServeMux()
	for pattern, tr := range tenantRoutes {
		g.mux.HandleFunc(pattern, h.scoped(tr))
	}
	// Default deny: a path no pattern matches is refused rather than
	// served unscoped.
	g.mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		writeError(w, http.StatusForbidden, CodeForbidden, "not allowed for a tenant-scoped caller")
	})
	return g
}

// route serves an identified request: through the routes as they are
// without a tenant scope or for an AllTenants caller, else through the
// scoped routes with the tenant in the ctx.
func (h *handler[P]) route(w http.ResponseWriter, r *http.Request, who P) {
	g := h.tenant
	if g == nil {
		h.mux.ServeHTTP(w, r)
		return
	}
	if g.closed != nil {
		if errors.Is(g.closed, workflow.ErrUnsupported) {
			writeError(w, http.StatusNotImplemented, CodeUnsupported, g.closed.Error())
			return
		}
		h.cfg.logger.Error("admin: " + g.closed.Error())
		writeError(w, http.StatusForbidden, CodeForbidden, g.closed.Error())
		return
	}
	tenant, err := h.callScope(g.scope, r, who)
	if err != nil {
		writeError(w, http.StatusForbidden, CodeForbidden, "not allowed: no tenant for this caller")
		return
	}
	if tenant == AllTenants {
		h.mux.ServeHTTP(w, r)
		return
	}
	g.mux.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), tenantKey{}, tenant)))
}

// callScope asks the scope, turning a panic, an empty answer and an
// invalid tenant into an error so each fails closed.
func (h *handler[P]) callScope(scope TenantScope[P], r *http.Request, who P) (tenant string, err error) {
	defer func() {
		if p := recover(); p != nil {
			h.cfg.logger.Error("admin: TenantScope panicked", "panic", fmt.Sprint(p))
			err = errors.New("admin: TenantScope panicked")
		}
	}()
	tenant, err = scope(r, who)
	switch {
	case err != nil:
		return "", err
	case tenant == AllTenants:
		return tenant, nil
	}
	return tenant, workflow.ValidateTenant(tenant)
}

// scoped is the handler of one route for a scoped caller: it applies the
// route's rule, then hands the request to the routes.
func (h *handler[P]) scoped(tr tenantRoute) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		tenant, _ := TenantFrom(r.Context())
		switch tr.rule {
		case tenantRefused:
			writeError(w, http.StatusForbidden, CodeForbidden, "not allowed for a tenant-scoped caller: "+string(tr.action))
			return
		case tenantList:
			v := r.URL.Query()
			for _, t := range v["tenant"] {
				if t != tenant {
					writeError(w, http.StatusForbidden, CodeForbidden, "not allowed: another tenant")
					return
				}
			}
			v.Set("tenant", tenant)
			r = r.Clone(r.Context())
			r.URL.RawQuery = v.Encode()
		case tenantBulk:
			body, status, msg := pinBodyTenant(r.Body, tenant)
			if status != 0 {
				code := CodeInvalid
				if status == http.StatusForbidden {
					code = CodeForbidden
				}
				writeError(w, status, code, msg)
				return
			}
			r = r.Clone(r.Context())
			r.Body, r.ContentLength = io.NopCloser(bytes.NewReader(body)), int64(len(body))
		case tenantRun:
			ref := pathRef(r)
			info, err := h.a.Run(r.Context(), ref)
			if err == nil && info.Tenant != tenant {
				// The answer for a run that does not exist, word for word,
				// so a caller cannot tell another tenant's run from none.
				err = fmt.Errorf("%w: admin: run %s", workflow.ErrNotFound, refString(ref))
			}
			if err != nil {
				h.fail(w, tr.action, err)
				return
			}
		case tenantSchedule:
			id := workflow.ScheduleID(r.PathValue("id"))
			s, err := h.a.Schedule(r.Context(), id)
			if err == nil && s.Tenant != tenant {
				err = fmt.Errorf("%w: admin: schedule %s", workflow.ErrNotFound, id)
			}
			if err != nil {
				h.fail(w, tr.action, err)
				return
			}
		}
		h.mux.ServeHTTP(w, r)
	}
}

// pinBodyTenant returns a bulk route's JSON body with its tenant filter
// set to tenant. Go's JSON decoding matches field names without regard to
// case, so every key that spells "tenant" in any case is checked and
// dropped before the one key is set. A body that is not one JSON object
// is a 400; a body naming another tenant is a 403.
func pinBodyTenant(body io.Reader, tenant string) (out []byte, status int, msg string) {
	dec := json.NewDecoder(body)
	var obj map[string]json.RawMessage
	if err := dec.Decode(&obj); err != nil || obj == nil {
		return nil, http.StatusBadRequest, "body: want one JSON object"
	}
	if dec.More() {
		return nil, http.StatusBadRequest, "body: more than one JSON value"
	}
	for k, raw := range obj {
		if !strings.EqualFold(k, "tenant") {
			continue
		}
		var t string
		if json.Unmarshal(raw, &t) != nil || (t != "" && t != tenant) {
			return nil, http.StatusForbidden, "not allowed: another tenant"
		}
		delete(obj, k)
	}
	obj["tenant"], _ = json.Marshal(tenant) // a string always marshals
	out, _ = json.Marshal(obj)              // raw JSON values and string keys always marshal
	return out, 0, ""
}

// scopeSchedules keeps the schedules of the ctx's tenant, or all of them
// for an unscoped request.
func scopeSchedules(ctx context.Context, all []workflow.ScheduleInfo) []workflow.ScheduleInfo {
	tenant, ok := TenantFrom(ctx)
	if !ok {
		return all
	}
	out := make([]workflow.ScheduleInfo, 0, len(all))
	for _, s := range all {
		if s.Tenant == tenant {
			out = append(out, s)
		}
	}
	return out
}

// scopeSupport reports the operations of the system-wide routes as
// unsupported to a scoped caller, so the web UI hides them.
func scopeSupport(ctx context.Context, sup []OperationSupport) []OperationSupport {
	if _, ok := TenantFrom(ctx); !ok {
		return sup
	}
	for i, s := range sup {
		if refusedOps[s.Operation] {
			sup[i] = OperationSupport{Operation: s.Operation, Reason: "not available to a tenant-scoped caller"}
		}
	}
	return sup
}
