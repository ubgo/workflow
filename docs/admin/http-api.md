# Admin JSON API

`admin.NewHandler` serves the [admin operations](operations.md) as a JSON API, the base for a dashboard or a support tool. It is closed by default: it answers nothing until the app says who the caller is and what they may do, and every way that decision can fail ends in a refusal.

## Before you start

- An `*admin.Admin` over your client ([Admin operations](operations.md)).
- Your app's own login: a session cookie, a bearer token or an API key. The handler never authenticates anyone itself.
- The runnable program for this page is [examples/admin](../../examples/admin/).

## Step 1: say who may do what

`admin.Auth[P]` has two functions, and `P` is your own principal type (a user struct, a claims struct, an ID):

- `Identify(r) (P, error)` returns the caller from the request. An error means anonymous.
- `Authorize(ctx, admin.Request[P]) error` allows a request by returning nil. `Request` carries `Who` (what `Identify` returned), `Action` (an `admin.Action` such as `runs.retry`) and `Resource` (the run, the parsed run filter or the pause target the request is about), plus the `*http.Request` for headers.

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

`Action.ReadOnly()` is true for the reads, which makes a read-only role one line. Because `Resource.Query` is the parsed filter, an `Authorize` can confine a caller to its own runs, for example by refusing any list whose `Labels` lack the caller's tenant label.

## Step 2: mount the handler

<!-- snippet: examples/admin/main.go#mount -->
```go
adm := admin.New(c)
mux := http.NewServeMux()
mux.Handle("/admin/", http.StripPrefix("/admin", admin.NewHandler(adm, auth)))
```

What happens to each request, in order:

1. With `Auth.Identify` or `Auth.Authorize` nil, every request on every path is refused with 403. The zero `Auth` is closed.
2. `Identify` runs before routing. An error or a panic is a 401, so an anonymous caller learns nothing about the routes, not even which exist.
3. A write (`POST`, `DELETE`) without `Content-Type: application/json` is a 415. A browser cannot send that header cross-site without a CORS preflight the handler never answers, so a forged form post from another site is refused.
4. The request is parsed; a malformed filter or body is a 400.
5. `Authorize` runs with the action and resource. An error or a panic is a 403: an authorizer that fails, fails closed.
6. The operation runs.

## Routes

| Method and path | Action | What it does |
|---|---|---|
| `GET /capabilities` | `capabilities.read` | Every operation, whether the client supports it, and the capabilities it lacks |
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
| `POST /pause` | `pauses.create` | Body `{"workflow": "..."}`, `{"queue": "..."}` or `{"all": true}` |
| `POST /resume` | `pauses.delete` | Same body |
| `GET /pauses` | `pauses.list` | The active pauses |
| `GET /schedules` | `schedules.list` | Schedules with next fire times; `?limit=` (default 100, at most 1,000) and `?cursor=` |
| `GET /queues` | `queues.list` | Depth and oldest wait per queue, and whether the scan was truncated; `workers` and `running` per queue on a client with `CapWorkerScale` |
| `POST /schedules/{id}/pause` | `schedules.pause` | Pause one schedule; needs `CapScheduleList` and `CapSchedulePause` |
| `POST /schedules/{id}/resume` | `schedules.resume` | Resume one schedule |
| `GET /schedules/{id}` | `schedules.show` | One schedule: cron, zone, overlap policy, next fire and its last fire with that run's ref and status |
| `POST /schedules/{id}/trigger` | `schedules.trigger` | Run the schedule now with its input and labels; returns `{"ref": ...}`; answers 409 when its overlap policy skips; needs `CapScheduleList` and `CapScheduleTrigger` |
| `POST /runs/{id}/complete` | `runs.force_complete` | Mark an unfinished or failed run succeeded; needs `CapForceRun` |
| `POST /runs/{id}/fail` | `runs.force_fail` | Body `{"reason": "..."}` (optional, at most 1,024 bytes): mark an unfinished run failed without a retry; the reason reaches `Authorize` as `Resource.Reason` |
| `GET /runs/{id}/output` | `runs.output` | Captured output of each attempt, when the handler's `Admin` was built with `admin.WithOutputs`; 501 without it |
| `GET /metrics` | `metrics.read` | Run ends per step by status for the charts, when the handler's `Admin` was built with `admin.WithMetrics`; 501 without it |
| `GET /nodes` | `nodes.list` | The online nodes: `id`, `mode`, `queues`, `workers`, `running`, `started_at`, `last_seen`; needs a client for which `workflow.Supports(c, workflow.OpNodes)` holds |
| `GET /health` | `health.read` | `{"live", "ready", "checks": [{"name", "ok", "detail"}]}`, always status 200: a screen's read, not a probe |
| `GET /`, `GET /ui`, `GET /ui/{file}` | `ui.read` | The [web UI](ui.md): the first two redirect to `ui/` |

Run filters in the query string: `workflow`, `queue`, `tenant` (`CapTenant`), `status` (repeat it or separate with commas), `label=key=value` (repeat it; every pair must match), `label=key=prefix*` for a value that starts with a prefix and `label=key=*` for "has the key" (a trailing `*` only; it needs `CapLabelPrefix`, else 501), `from` and `to` (RFC 3339 enqueue times), `due_from` and `due_to` (RFC 3339 due times of delayed runs, `[due_from, due_to)`, `CapDueQuery`), `limit` (at most 1,000), `cursor` (the previous page's `next`), and `count=1` on `/runs`, `/failures` and `/discarded`, which adds `total`, `truncated` (the count stopped at the max scan) and `cursors` (the cursor that starts each page at this limit, for numbered pages; the request's own `cursor` must be empty). The `POST /runs/cancel`, `/runs/retry` and `/runs/delete` bodies take the same filters as JSON (`workflow`, `queue`, `tenant`, `statuses`, `labels`, `label_prefixes`, `from`, `to`) and refuse unknown fields, so a typo is an error rather than a wider cancel. They also take `"dry_run": true`, and the retry and delete bodies `"max": n` to act on fewer runs than the handler's bound (`admin.WithMaxBulk`, 1,000 by default). A preview or a bulk retry or delete answers:

```json
{"matched": 5, "done": 0, "skipped": 0, "truncated": false, "dry_run": true, "sample": [{"run_id": "run-14"}]}
```

`matched` is how many runs the filter matched, up to the bound; `done` how many were retried or deleted (zero on a dry run); `skipped` how many changed between the listing and the action; `truncated` that more match, so the same call should be repeated; `sample` the first 20 refs. `Resource.DryRun` tells `Authorize` that a call only previews, so a role may be allowed to preview a bulk action it may not run ([Admin operations › Act on many runs at once](operations.md#act-on-many-runs-at-once)).

`GET /metrics` reads `window` (a duration such as `24h`, default one hour, ending now) or `from` and `to` (RFC 3339), `step` (a duration; default the span split into 60 steps, at least one second) and `workflow`; `from` together with `window` is a 400, and so is a query of more than 1,440 points. It answers `{"from", "to", "step_seconds", "since", "points": [{"at", "succeeded", "failed", "cancelled", "discarded"}]}` ([Admin operations › Run-end history for charts](operations.md#run-end-history-for-charts)).

A run in a response also carries, where the driver fills them: `tenant`, `run_at` (when a delayed run is or was due), `progress` (`percent`, `message`, `step`, `at`), `attempted_by` (the nodes that ran its attempts), `has_output` (it has a result) and, on each failed attempt in `errors`, `node`. The `GET /capabilities` answer lists `reason` for an operation that is unsupported only because an interface is missing, and `metrics`, true when charts can be read; `nodes`, `reschedule` and `cancel_step` are reported by `workflow.Supports`, so behind a wrapper (`workflow.Forward`, `failover`) they are supported only when the client underneath does them. A schedule in `/schedules` and `/schedules/{id}` carries `tenant` when it has one (omitted when empty).

Errors are `{"error": {"code": "...", "message": "..."}}`:

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

## What is left out

Payloads are not in run responses unless the handler is built with `admin.WithPayloads()`, and neither are a run's result (`output`) and its progress `data`, which are app data in the same way: they can carry personal data, and an admin screen is seen by more people than the code that wrote them. Labels and error texts are included. Responses carry `Cache-Control: no-store`.

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

## Pitfalls

- **Identify is your login, not a password check of its own.** Compare tokens with `crypto/subtle` or look them up by hash; the example's map lookup stands in for a session store.
- **Scope by resource when tenants share a deployment.** An `Authorize` that checks only the role lets a tenant admin list another tenant's runs; check `Resource.Query.Labels` (lists, bulk cancel) and look the run up for single-run actions.
- **Force actions are writes like any other.** `runs.force_complete` and `runs.force_fail` are not read-only, so a role that may only read is refused them; an `Authorize` that wants a reason for every forced failure can refuse an empty `Resource.Reason`.
- **Run responses carry each failed attempt's error, not its stack**, unless the handler is built with `admin.WithStacks()`.
- **Mount it behind TLS and your usual rate limits.** The handler adds none of its own.
- **Bulk actions are writes, previews are not free.** A dry run lists up to the bound of runs; a role that may preview but not act is an `Authorize` that allows `runs.retry_all` and `runs.delete_all` only with `Resource.DryRun`.
- **`/health` is not a probe.** It needs the admin login and answers 200 whatever the verdict; give your orchestrator its own route on `workflow.CheckHealth` ([Nodes and health](../operations/nodes.md#health-and-readiness-probes)).
- **`/queues` reads pages of runs.** Poll it from a dashboard at most every few seconds; see [Admin operations › Pitfalls](operations.md#pitfalls).

## Related

- [Admin operations](operations.md): the same operations from Go.
- [Queue alerts](alerts.md).
- Example: [examples/admin](../../examples/admin/)
