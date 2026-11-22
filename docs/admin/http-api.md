# Admin JSON API

[`admin.NewHandler`](ds:block?id=admin-newhandler-np68g63j) serves the [admin operations](operations.md) as a JSON API, the base for a dashboard or a support tool. It is closed by default: it answers nothing until the app says who the caller is and what they may do, and every way that decision can fail ends in a refusal ([`ServeHTTP`](ds:block?id=admin-handler-servehttp-zmec7tcc), [`TestHandler_ClosedByDefault`](ds:block?id=admin-testhandler-closedbydefault-6z67u6yx)).

## Before you start

- An `*admin.Admin` over your client ([Admin operations](operations.md)).
- Your app's own login: a session cookie, a bearer token or an API key. The handler never authenticates anyone itself: it asks your [`Auth.Identify`](ds:block?id=admin-auth-sq9pywcz).
- The runnable programs for this page are [examples/admin](../../examples/admin/) (a login with two roles) and [examples/admin-routes](../../examples/admin-routes/), which calls every route once and makes the handler answer each error code.

## Step 1: say who may do what

[`admin.Auth[P]`](ds:block?id=admin-auth-sq9pywcz) has two functions, and `P` is your own principal type (a user struct, a claims struct, an ID):

- `Identify(r) (P, error)` returns the caller from the request. An error means anonymous.
- `Authorize(ctx, admin.Request[P]) error` allows a request by returning nil. [`Request`](ds:block?id=admin-request-4dc2jxur) carries `Who` (what `Identify` returned), `Action` (an [`admin.Action`](ds:block?id=admin-actions-fvf66fmr) such as `runs.retry`) and [`Resource`](ds:block?id=admin-resource-6fktgbm8) (the run, the parsed run filter or the pause target the request is about), plus the `*http.Request` for headers.

<!-- snippet: examples/admin/main.go#auth -->
```go
// user is the app's own principal type; admin.Auth is generic over it.
type user struct {
	Name string
	Role string // "admin" may do anything, "viewer" may only read
}

// tokens stands in for the app's session or API-token store.
var tokens = map[string]user{"t-ann": {"ann", "admin"}, "t-vic": {"vic", "viewer"}}

var auth = admin.Auth[user]{
	Identify: func(r *http.Request) (user, error) {
		u, ok := tokens[strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")]
		if !ok {
			return user{}, errors.New("no session")
		}
		return u, nil
	},
	Authorize: func(_ context.Context, req admin.Request[user]) error {
		if req.Who.Role == "admin" || (req.Who.Role == "viewer" && req.Action.ReadOnly()) {
			return nil
		}
		return fmt.Errorf("%s may not %s", req.Who.Name, req.Action)
	},
}
```

[`Action.ReadOnly()`](ds:block?id=admin-action-readonly-fgxr6xzq) is true for the reads, which makes a read-only role one line. Because `Resource.Query` is the parsed filter, an `Authorize` can confine a caller to its own runs, for example by refusing any list whose `Labels` lack the caller's tenant label. When the runs carry `Job.Tenant`, [`admin.WithTenantScope`](ds:block?id=admin-withtenantscope-cbtmkxnu) does that confinement for every route instead ([Per-tenant dashboard](tenants.md)).

## Step 2: mount the handler

<!-- snippet: examples/admin/main.go#mount -->
```go
adm := admin.New(c)
mux := http.NewServeMux()
mux.Handle("/admin/", http.StripPrefix("/admin", admin.NewHandler(adm, auth)))
```

What happens to each request, in order ([`ServeHTTP`](ds:block?id=admin-handler-servehttp-zmec7tcc) is the gate):

1. With `Auth.Identify` or `Auth.Authorize` nil, every request on every path is refused with 403. The zero `Auth` is closed ([`TestHandler_ClosedByDefault`](ds:block?id=admin-testhandler-closedbydefault-6z67u6yx)).
2. `Identify` runs before routing. An error or a panic is a 401 ([`identify`](ds:block?id=admin-handler-identify-f3xep7c4)), so an anonymous caller learns nothing about the routes, not even which exist ([`TestHandler_AnonymousRefused`](ds:block?id=admin-testhandler-anonymousrefused-vy9kxkyh)).
3. A write (`POST`, `DELETE`) without `Content-Type: application/json` is a 415. A browser cannot send that header cross-site without a CORS preflight the handler never answers, so a forged form post from another site is refused.
4. The request is parsed; a malformed filter or body is a 400 ([`parseRunQuery`](ds:block?id=admin-parserunquery-6qprr62n), [`decodeBody`](ds:block?id=admin-decodebody-vz6fvar2)).
5. `Authorize` runs with the action and resource. An error or a panic is a 403: an authorizer that fails, fails closed ([`authorize`](ds:block?id=admin-handler-authorize-yub6n963), [`TestHandler_AuthorizeFailsClosed`](ds:block?id=admin-testhandler-authorizefailsclosed-ftys37mz)).
6. The operation runs.

## Routes

Each route is registered by [`NewHandler`](ds:block?id=admin-newhandler-np68g63j) and asks `Authorize` about one of the [`admin.Action`](ds:block?id=admin-actions-fvf66fmr) constants; [`TestRoutes_CoverEveryAction`](ds:block?id=admin-testroutes-covereveryaction-qbbzvu2k) checks that every action in [`admin.Actions`](ds:block?id=admin-actions-3pefsteu) has its route.

| Method and path | Action | What it does |
|---|---|---|
| `GET /capabilities` | `capabilities.read` | Every operation ([`admin.Operations`](ds:block?id=admin-operations-98ugx4jk)), whether the client supports it, and the capabilities it lacks |
| `GET /runs` | `runs.list` | List and search runs, paginated |
| `GET /runs/{id}` | `runs.show` | One run; add `?scheduled=1` when `id` is a `ScheduledID` |
| `GET /failures` | `failures.list` | Failed runs, the same filters without `status` |
| `GET /discarded` | `discarded.list` | Discarded runs, the same filters without `status`; needs `CapRunStates` |
| `POST /runs/{id}/retry` | `runs.retry` | Retry a failed run from its failed step; returns `{"ref": ...}` |
| `POST /runs/{id}/cancel` | `runs.cancel` | Cancel one run |
| `DELETE /runs/{id}` | `runs.delete` | Forget a finished run |
| `POST /runs/cancel` | `runs.cancel_all` | Cancel every run a filter body matches; returns `{"cancelled": n}`. With `"dry_run": true` it cancels nothing and returns the preview below |
| `POST /runs/retry` | `runs.retry_all` | Retry every failed run a filter body matches (`statuses` empty or `["failed"]`); `dry_run` and `max` as below; needs `CapRunQuery` and `CapRetryRun` |
| `POST /runs/delete` | `runs.delete_all` | Forget every finished run a filter body matches; an empty filter is a 400; `dry_run` and `max` as below; needs `CapRunQuery` and `CapDeleteRun` |
| `POST /runs/{id}/reschedule` | `runs.reschedule` | Body `{"at": "<RFC 3339>"}`: move a delayed run that has not started; returns `{"ref": ...}`; 409 when the run is not delayed; the time reaches `Authorize` as `Resource.At` |
| `POST /runs/{id}/steps/{step}/cancel` | `runs.cancel_step` | Withdraw one step of a run; the step reaches `Authorize` as `Resource.Step` |
| `POST /runs/{id}/edit` | `runs.edit` | Body `{"payload": <JSON>, "priority": n, "run_at": "<RFC 3339>"}`, each optional (a field left out is not changed): change a delayed run that has not started; returns `{"ref": ...}`; 409 for a run that is not delayed, 400 for an edit that changes nothing. The change reaches `Authorize` as `Resource.Edit`, so a role may move a run and not change its payload ([Edit a run before it starts](../jobs/edit-runs.md)) |
| `POST /runs/{id}/reset` | `runs.reset` | Body `{"step": "...", "succeeded": true}` (`succeeded` optional): re-run a failed run in place from `step`, the steps before it keeping their outputs; with `succeeded` also a run that succeeded. Returns `{"ref": ...}`, the same run; 409 for a run in another state, 400 for a step the workflow does not have. `Authorize` sees `Resource.Step` and `Resource.Reset`, so a role may reset failed runs and not redo succeeded ones ([Manage runs › Re-run a failed run from an earlier step](../jobs/manage-runs.md#re-run-a-failed-run-from-an-earlier-step)) |
| `POST /runs/{id}/fork` | `runs.fork` | Body `{"step": "...", "idempotency_key": "...", "labels": {...}}` (key and labels optional): start a new run from a finished one at `step`; the original is not changed. Returns `{"ref": ...}`, the new run, with `"duplicate": true` when an unfinished run held the key and `ref` names it (nothing started); 409 for an unfinished run, 400 for a reserved label. `Authorize` sees `Resource.Step` and `Resource.Fork` ([Manage runs › Fork a run into a new run](../jobs/manage-runs.md#fork-a-run-into-a-new-run)) |
| `POST /signals/waiting` | `signals.waiting` | Body `{"name": "...", "key": "...", "payload": <JSON>}` (key and payload optional): deliver the event only to the runs waiting for it now; returns `{"resumed": n}`, zero included. `Authorize` sees `Resource.Event` ([Signals › Deliver only to runs waiting now](../workflows/signals.md#deliver-only-to-runs-waiting-now)) |
| `POST /pause` | `pauses.create` | Body `{"workflow": "..."}`, `{"queue": "..."}` or `{"all": true}` |
| `POST /resume` | `pauses.delete` | Same body |
| `GET /pauses` | `pauses.list` | The active pauses |
| `GET /schedules` | `schedules.list` | Schedules with next fire times ([`listSchedules`](ds:block?id=admin-handler-listschedules-6esmjgnq)); `?limit=` (default 100, at most 1,000: [`DefaultSchedulesLimit`, `MaxSchedulesLimit`](ds:block?id=admin-scheduleslimit-4thht8bb)) and `?cursor=` |
| `GET /queues` | `queues.list` | Depth and oldest wait per queue, and whether the scan was truncated; `workers` and `running` per queue on a client with `CapWorkerScale` |
| `POST /schedules/{id}/pause` | `schedules.pause` | Pause one schedule; needs `CapScheduleList` and `CapSchedulePause` |
| `POST /schedules/{id}/resume` | `schedules.resume` | Resume one schedule |
| `GET /schedules/{id}` | `schedules.show` | One schedule: cron, zone, overlap policy, next fire and its last fire with that run's ref and status |
| `POST /schedules/{id}/trigger` | `schedules.trigger` | Run the schedule now with its input and labels; returns `{"ref": ...}`; answers 409 when its overlap policy skips; needs `CapScheduleList` and `CapScheduleTrigger` |
| `POST /runs/{id}/complete` | `runs.force_complete` | Mark an unfinished or failed run succeeded; needs `CapForceRun` |
| `POST /runs/{id}/fail` | `runs.force_fail` | Body `{"reason": "..."}` (optional, at most 1,024 bytes, [`admin.MaxForceReason`](ds:block?id=admin-maxforcereason-x2gm25rb)): mark an unfinished run failed without a retry; the reason reaches `Authorize` as `Resource.Reason` |
| `GET /runs/{id}/output` | `runs.output` | Captured output of each attempt, when the handler's `Admin` was built with `admin.WithOutputs`; 501 without it |
| `GET /metrics` | `metrics.read` | Run ends per step by status for the charts, when the handler's `Admin` was built with `admin.WithMetrics`; 501 without it |
| `GET /nodes` | `nodes.list` | The online nodes: `id`, `mode`, `queues`, `workers`, `running`, `started_at`, `last_seen`; needs a client for which `workflow.Supports(c, workflow.OpNodes)` holds |
| `GET /health` | `health.read` | `{"live", "ready", "checks": [{"name", "ok", "detail"}]}`, always status 200 ([`health`](ds:block?id=admin-handler-health-czcfdgkm)): a screen's read, not a probe |
| `GET /`, `GET /ui`, `GET /ui/{file}` | `ui.read` | The [web UI](ui.md): the first two redirect to `ui/` |

Run filters in the query string ([`parseRunQuery`](ds:block?id=admin-parserunquery-6qprr62n)): `workflow`, `queue`, `tenant` (`CapTenant`), `status` (repeat it or separate with commas), `label=key=value` (repeat it; every pair must match), `label=key=prefix*` for a value that starts with a prefix and `label=key=*` for "has the key" (a trailing `*` only; it needs `CapLabelPrefix`, else 501), `from` and `to` (RFC 3339 enqueue times), `due_from` and `due_to` (RFC 3339 due times of delayed runs, `[due_from, due_to)`, `CapDueQuery`), `limit` (at most 1,000, [`workflow.MaxRunsLimit`](ds:block?id=maxrunslimit-ycr4zjnb)), `cursor` (the previous page's `next`), and `count=1` on `/runs`, `/failures` and `/discarded`, which adds `total`, `truncated` (the count stopped at the max scan) and `cursors` (the cursor that starts each page at this limit, for numbered pages; the request's own `cursor` must be empty). The `POST /runs/cancel`, `/runs/retry` and `/runs/delete` bodies take the same filters as JSON ([`filterJSON`](ds:block?id=admin-filterjson-bzv46dqr): `workflow`, `queue`, `tenant`, `statuses`, `labels`, `label_prefixes`, `from`, `to`) and refuse unknown fields ([`decodeBody`](ds:block?id=admin-decodebody-vz6fvar2)), so a typo is an error rather than a wider cancel. They also take `"dry_run": true`, and the retry and delete bodies `"max": n` to act on fewer runs than the handler's bound ([`admin.WithMaxBulk`](ds:block?id=admin-withmaxbulk-ety2e74q), 1,000 by default: [`admin.DefaultMaxBulk`](ds:block?id=admin-defaultmaxbulk-dtm2uzhk)). A preview or a bulk retry or delete answers ([`toBulkJSON`](ds:block?id=admin-tobulkjson-s3d6vea4)):

```json
{"matched": 5, "done": 0, "skipped": 0, "truncated": false, "dry_run": true, "sample": [{"run_id": "run-14"}]}
```

`matched` is how many runs the filter matched, up to the bound; `done` how many were retried or deleted (zero on a dry run); `skipped` how many changed between the listing and the action; `truncated` that more match, so the same call should be repeated; `sample` the first 20 refs ([`admin.BulkSample`](ds:block?id=admin-bulksample-2h3ycdxx); the fields are [`admin.BulkResult`](ds:block?id=admin-bulkresult-729ufgs8)). `Resource.DryRun` tells `Authorize` that a call only previews, so a role may be allowed to preview a bulk action it may not run ([Admin operations › Act on many runs at once](operations.md#act-on-many-runs-at-once)).

`GET /metrics` reads `window` (a duration such as `24h`, default one hour, ending now) or `from` and `to` (RFC 3339), `step` (a duration; default the span split into 60 steps, at least one second) and `workflow` ([`parseMetricsQuery`](ds:block?id=admin-parsemetricsquery-jxuy4y5r); the defaults are [`DefaultMetricsWindow` and `DefaultMetricsPoints`](ds:block?id=admin-defaultmetricswindow-s97e4m2j)); `from` together with `window` is a 400, and so is a query of more than 1,440 points ([`admin.MaxMetricsPoints`](ds:block?id=admin-maxmetricspoints-h8muc4a2)). It answers `{"from", "to", "step_seconds", "since", "points": [{"at", "succeeded", "failed", "cancelled", "discarded"}]}` ([Admin operations › Run-end history for charts](operations.md#run-end-history-for-charts)).

A run in a response ([`runJSON`](ds:block?id=admin-handler-runjson-3xmkf4pw)) also carries, where the driver fills them: `tenant`, `run_at` (when a delayed run is or was due), `progress` (`percent`, `message`, `step`, `at`), `attempted_by` (the nodes that ran its attempts), `has_output` (it has a result) and, on each failed attempt in `errors`, `node`. The `GET /capabilities` answer ([`capabilities`](ds:block?id=admin-handler-capabilities-x9rsrpru)) lists `reason` for an operation that is unsupported only because an interface is missing, and `metrics`, true when charts can be read; `nodes`, `reschedule`, `cancel_step`, `edit_run`, `reset_run`, `reset_succeeded`, `fork_run` and `signal_waiting` are reported by `workflow.Supports`, so behind a wrapper (`workflow.Forward`, `failover`) they are supported only when the client underneath does them. A schedule in `/schedules` and `/schedules/{id}` carries `tenant` when it has one (omitted when empty).

Errors are `{"error": {"code": "...", "message": "..."}}`, mapped from the operation's error by [`fail`](ds:block?id=admin-handler-fail-nf9d8sqp) ([`TestHandler_ErrorMapping`](ds:block?id=admin-testhandler-errormapping-zzqkqr76)):

| Status | Code | When |
|---|---|---|
| 400 | `invalid` | A malformed request, or the operation returned `workflow.ErrInvalid` |
| 401 | `unauthorized` | `Identify` failed |
| 403 | `forbidden` | No `Auth`, or `Authorize` refused |
| 404 | `not_found` | `workflow.ErrNotFound` |
| 409 | `conflict` | `workflow.ErrWrongState`, `workflow.ErrDuplicate`, or `workflow.ErrOverlap` (a trigger the schedule's overlap policy skipped) |
| 415 | `unsupported_media_type` | A write without a JSON content type |
| 501 | `unsupported` | The client lacks the operation (`workflow.ErrUnsupported`) |
| 500 | `internal` | Anything else; the detail goes to `admin.WithLogger`, never to the caller |

## Step 3: handle the error codes

The codes are the constants [`admin.CodeInvalid`, `CodeUnauthorized`, `CodeForbidden`, `CodeNotFound`, `CodeConflict`, `CodeMediaType`, `CodeUnsupported` and `CodeInternal`](ds:block?id=admin-codes-95dffahc). They are stable, so a dashboard or a script switches on the code and shows the message to a person:

<!-- snippet: examples/admin-routes/main.go#codes -->
```go
// apiError is the body of every error answer: {"error": {"code", "message"}}.
type apiError struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

// nextStep says what a client of the API does about each error code. The
// codes are stable, so a client switches on them rather than on the
// message, which is for people.
func nextStep(code string) string {
	switch code {
	case admin.CodeInvalid:
		return "fix the request"
	case admin.CodeUnauthorized:
		return "log in again"
	case admin.CodeForbidden:
		return "ask for a role that allows it"
	case admin.CodeNotFound:
		return "refresh the list: the run or schedule is gone"
	case admin.CodeConflict:
		return "reload the run: its state changed"
	case admin.CodeMediaType:
		return "send Content-Type: application/json"
	case admin.CodeUnsupported:
		return "hide the button: the server cannot do it"
	case admin.CodeInternal:
		return "try again later: the server logged the cause"
	}
	return "unknown code " + code
}
```

[`examples/admin-routes`](../../examples/admin-routes/) makes the handler answer each one: an empty bulk-delete filter, no token, a viewer's retry, a run that does not exist, a retry of a run that succeeded, a write without the JSON content type, and, on a second handler with no metrics source and an output store that is down, `/metrics` and a run's output:

```text
400 POST /runs/delete  invalid: fix the request
401 GET /failures  unauthorized: log in again
403 POST /runs/run-6/retry  forbidden: ask for a role that allows it
404 GET /runs/no-such-run  not_found: refresh the list: the run or schedule is gone
409 POST /runs/run-3/retry  conflict: reload the run: its state changed
415 POST /runs/run-6/retry  unsupported_media_type: send Content-Type: application/json
501 GET /metrics  unsupported: hide the button: the server cannot do it
500 GET /runs/run-6/output  internal: try again later: the server logged the cause
```

The 500 says nothing of the cause; the handler logs it to [`admin.WithLogger`](ds:block?id=admin-withlogger-dy7a7bwx) ([`fail`](ds:block?id=admin-handler-fail-nf9d8sqp)) and an [`admin.WithAudit`](ds:block?id=admin-withaudit-qj2q5pss) entry carries it in `Err`.

## What is left out

Payloads are not in run responses unless the handler is built with [`admin.WithPayloads()`](ds:block?id=admin-withpayloads-zq6ghfjc), and neither are a run's result (`output`) and its progress `data` ([`runJSON`](ds:block?id=admin-handler-runjson-3xmkf4pw)), which are app data in the same way: they can carry personal data, and an admin screen is seen by more people than the code that wrote them. Labels and error texts are included. Responses carry `Cache-Control: no-store` ([`writeJSON`](ds:block?id=admin-writejson-gpfs6kq4)).

## Try it

```sh
cd examples
go run ./admin
```

```text
anonymous GET /failures: 401
viewer GET /failures: 200, 1 failed run, step "send": memory: step "send" failed after 1 attempt(s): smtp: connection refused
viewer POST retry: 403 {"error":{"code":"forbidden","message":"not allowed: runs.retry"}}
admin POST retry: 200, run now succeeded
admin POST /pause emails: 200
ALERT queue_depth queue=emails depth=3 (max 2)
workflow.queue.depth{workflow.queue=emails} = 3
admin POST /resume emails: 200
resolved queue_depth queue=emails depth=0 (max 2)
```

Every route once, with a summary of each answer, then each error code. The handler is built with captured output and run-end history, so `/runs/{id}/output` and `/metrics` answer:

<!-- snippet: examples/admin-routes/main.go#mount -->
```go
adm := admin.New(f.c,
	admin.WithClock(clk),    // queue waits measured on the app's clock
	admin.WithOutputs(outs), // GET /runs/{id}/output
	admin.WithMetrics(mm),   // GET /metrics
)
srv := httptest.NewServer(http.StripPrefix("/admin", admin.NewHandler(adm, auth)))
```

```sh
go run ./admin-routes
```

```text
-- read
200 GET /capabilities  25 operations, output=true metrics=true
200 GET /runs?workflow=etl&count=1&limit=1  1 run on this page, total 2, pages 2
200 GET /runs?label=team=data*  2 runs with a team label starting data
200 GET /runs/run-3  etl failed, failed step transform
200 GET /failures  3 failed runs
200 GET /discarded  1 discarded run of cleanup
200 GET /runs/run-6/output  2 attempts captured, last: step transform, error "transform: bad date in row 7"
-- act on many runs
200 POST /runs/cancel  matched 4, done 0, dry run true
200 POST /runs/retry  matched 2, done 0, dry run true
200 POST /runs/delete  matched 1, done 1, dry run false, more match true
-- act on one run
200 POST /runs/run-12/retry  ref run-12
200 POST /runs/run-23/cancel  ok true
200 DELETE /runs/run-9  ok true
200 POST /runs/run-25/steps/await/cancel  ok true
200 POST /runs/run-3/reset  ref run-3
200 POST /runs/run-3/fork  ref run-36
200 POST /runs/run-27/complete  ok true
200 POST /signals/waiting  resumed 1
-- pauses and queues
200 POST /pause  ok true
200 GET /pauses  1 active, queue emails
200 POST /resume  ok true
200 GET /queues  2 queues: default workers 2, emails workers 1
-- schedules
200 GET /schedules  1 schedule, next fire 2026-10-15T02:00:00Z
200 GET /schedules/schedule-1  nightly-report "0 2 * * *" in UTC, paused false
200 POST /schedules/schedule-1/pause  ok true
200 POST /schedules/schedule-1/resume  ok true
200 POST /schedules/schedule-1/trigger  ref run-41
-- a delayed run
200 GET /runs/delayed-44?scheduled=1  scheduled, due 2026-10-14T10:00:00Z
200 POST /runs/delayed-44/reschedule?scheduled=1  ref delayed-44
200 POST /runs/delayed-44/edit?scheduled=1  ref delayed-44
200 POST /runs/delayed-44/fail?scheduled=1  ok true
-- charts, system and the UI
200 GET /metrics?from=2026-10-14T09:00:00Z&to=2026-10-14T09:10:00Z&step=5m  2 points, first: succeeded 8 failed 5 cancelled 1 discarded 1
200 GET /nodes  worker-1, 2 workers
200 GET /health  live true ready true, 1 checks
302 GET / -> ui/
302 GET /ui -> ui/
200 GET /ui/
-- error codes
400 POST /runs/delete  invalid: fix the request
401 GET /failures  unauthorized: log in again
403 POST /runs/run-6/retry  forbidden: ask for a role that allows it
404 GET /runs/no-such-run  not_found: refresh the list: the run or schedule is gone
409 POST /runs/run-3/retry  conflict: reload the run: its state changed
415 POST /runs/run-6/retry  unsupported_media_type: send Content-Type: application/json
501 GET /metrics  unsupported: hide the button: the server cannot do it
500 GET /runs/run-6/output  internal: try again later: the server logged the cause
```

The delayed run's routes carry `?scheduled=1`: until it starts, its ID is a `ScheduledID`.

## Pitfalls

- **Identify is your login, not a password check of its own.** Compare tokens with `crypto/subtle` or look them up by hash; the example's map lookup stands in for a session store.
- **Scope by resource when tenants share a deployment.** An `Authorize` that checks only the role lets a tenant admin list another tenant's runs; check `Resource.Query.Labels` (lists, bulk cancel; [`TestHandler_ResourceScoping`](ds:block?id=admin-testhandler-resourcescoping-5yy3zqwg)) and look the run up for single-run actions. With `Job.Tenant` set on every run, [`admin.WithTenantScope`](ds:block?id=admin-withtenantscope-cbtmkxnu) does all of it ([Per-tenant dashboard](tenants.md)).
- **Force actions are writes like any other.** `runs.force_complete` and `runs.force_fail` are not read-only ([`Action.ReadOnly`](ds:block?id=admin-action-readonly-fgxr6xzq)), so a role that may only read is refused them; an `Authorize` that wants a reason for every forced failure can refuse an empty `Resource.Reason`.
- **Run responses carry each failed attempt's error, not its stack**, unless the handler is built with [`admin.WithStacks()`](ds:block?id=admin-withstacks-p2zf9mau).
- **Mount it behind TLS and your usual rate limits.** The handler adds none of its own (its only limit is the body size, [`DefaultMaxBody`](ds:block?id=admin-defaultmaxbody-wnrmhxza)).
- **Bulk actions are writes, previews are not free.** A dry run lists up to the bound of runs ([`admin.DefaultMaxBulk`](ds:block?id=admin-defaultmaxbulk-dtm2uzhk)); a role that may preview but not act is an `Authorize` that allows `runs.retry_all` and `runs.delete_all` only with `Resource.DryRun`.
- **`/health` is not a probe.** It needs the admin login and answers 200 whatever the verdict ([`health`](ds:block?id=admin-handler-health-czcfdgkm)); give your orchestrator its own route on `workflow.CheckHealth` ([Nodes and health](../operations/nodes.md#health-and-readiness-probes)).
- **A delayed run's ID is a `ScheduledID` until it starts.** The routes on one run take it with `?scheduled=1`; without it the ID is read as a run ID and the answer is 404.
- **The edit body's payload is JSON.** [`POST /runs/{id}/edit`](ds:block?id=admin-handler-editrun-bzau6dfy) sets the payload to the JSON value given, so a payload that is not JSON cannot be set over HTTP; use `Admin.EditRun` from Go.
- **`/queues` reads pages of runs** ([`Admin.QueueStats`](ds:block?id=admin-admin-queuestats-vxjxr6h3)). Poll it from a dashboard at most every few seconds; see [Admin operations › Pitfalls](operations.md#pitfalls).

## Related

- [Admin operations](operations.md): the same operations from Go.
- [Queue alerts](alerts.md).
- [Per-tenant dashboard](tenants.md): `WithTenantScope`, one tenant per caller.
- Examples: [examples/admin](../../examples/admin/), [examples/admin-routes](../../examples/admin-routes/) (every route and error code).
