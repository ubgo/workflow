# Per-tenant dashboard

`admin.WithTenantScope` confines each caller of the [admin JSON API](http-api.md), and of the [web UI](ui.md) on top of it, to one tenant (`workflow.Job.Tenant`), so a SaaS app can show its customers their own runs and schedules with the same handler its operators use. The scope is enforced by the handler on every route, not left to each `Authorize` to remember.

## Before you start

- A driver that declares `workflow.CapTenant` and jobs enqueued with `Job.Tenant` set ([Tenants](../jobs/tenants.md)). Without the capability a scoped handler refuses every request with 501.
- An `admin.Auth[P]` whose principal knows its tenant ([Admin JSON API](http-api.md)).
- The runnable program for this page is [examples/admin-tenants](../../examples/admin-tenants/).

## Step 1: say which tenant each caller sees

A `TenantScope[P]` is `func(r *http.Request, who P) (tenant string, err error)`, with `P` the handler's principal type. It runs after `Identify`, on every request, and returns the caller's tenant, or `admin.AllTenants` for an operator who sees every tenant:

<!-- snippet: examples/admin-tenants/main.go#scope -->
```go
// user is the app's own principal type: a customer's user carries its
// tenant, an operator none.
type user struct {
	Name   string
	Tenant string
}

// tokens stands in for the app's session or API-token store.
var tokens = map[string]user{
	"t-acme":   {"ann", "acme"},
	"t-globex": {"gil", "globex"},
	"t-ops":    {"oz", ""},
}

var auth = admin.Auth[user]{
	Identify: func(r *http.Request) (user, error) {
		u, ok := tokens[strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")]
		if !ok {
			return user{}, errors.New("no session")
		}
		return u, nil
	},
	Authorize: func(context.Context, admin.Request[user]) error { return nil },
}

// scope confines a customer to its tenant and lets an operator see all.
var scope = admin.WithTenantScope(func(_ *http.Request, u user) (string, error) {
	if u.Tenant == "" {
		return admin.AllTenants, nil
	}
	return u.Tenant, nil
})
```

The scope fails closed: an error, a panic, an empty string or a value that fails `workflow.ValidateTenant` refuses the request with 403. `AllTenants` is `"*"`, which is not a valid tenant ID, so no tenant can be mistaken for it.

## Step 2: mount the handler with the scope

<!-- snippet: examples/admin-tenants/main.go#mount -->
```go
mux := http.NewServeMux()
mux.Handle("/admin/", http.StripPrefix("/admin", admin.NewHandler(admin.New(c), auth, scope)))
```

## What a scoped caller sees

| Routes | For a caller scoped to tenant T |
|---|---|
| `GET /runs`, `/failures`, `/discarded` (with `count=1`) | Only T's runs. The `tenant` filter is set to T before the request is parsed and authorized, so `Resource.Query.Tenant` is T when `Authorize` runs. A request naming another tenant is refused with 403. |
| `POST /runs/cancel`, `/runs/retry`, `/runs/delete` | Act on T's runs only, dry runs included. The body's `tenant` is set to T (whatever its spelling, since JSON field names match without regard to case); a body naming another tenant is 403, one that is not a single JSON object is 400. |
| Every route on one run: `GET /runs/{id}`, retry, cancel, delete, complete, fail, output, reschedule, cancel a step | The run is read first. A run of another tenant, or of none, answers 404 with the very body a run that does not exist gets, so a caller cannot tell the two apart. |
| `GET /schedules`, and every route on one schedule | Only the schedules whose `Schedule.Tenant` is T; any other answers 404. A declared `Cron` has no tenant, so it is hidden. |
| `POST /pause`, `POST /resume`, `GET /pauses`, `GET /queues`, `GET /metrics`, `GET /nodes`, `GET /health` | 403. A queue, a pause, a chart or a node is shared by every tenant, so none of it can be shown to one. |
| `GET /capabilities` and the UI's files | Served. The capabilities answer names the tenant and reports queues, pauses, nodes and health as unsupported and metrics as absent, so the UI hides those pages and opens on the runs list. |
| Any other path | 403, so a route added to the handler later stays closed to scoped callers until it has a rule. |

`Authorize` still runs on every request the scope lets through, with the same actions as before; `admin.TenantFrom(req.HTTP.Context())` gives it the caller's tenant. A caller given `AllTenants` is served exactly as without the scope.

The conservative choice is deliberate: the routes that cannot be confined to one tenant are refused for every scoped caller, whatever `Authorize` says. An operator who needs them gets `AllTenants` from the scope, which is one place to audit.

## Try it

```sh
cd examples
go run ./admin-tenants
```

```text
-- A customer sees its own tenant
acme GET /runs: 200, tenants acme,acme
acme GET globex's run: 404
acme GET /runs?tenant=globex: 403
acme GET /queues: 403
acme GET /capabilities: tenant acme
-- A bulk action stays in the tenant
acme POST /runs/cancel: 200 {"cancelled":2}
globex's run: waiting
-- An operator sees every tenant
ops GET /runs: 200, tenants globex,acme,acme
ops GET /queues: 200
```

## Pitfalls

- The scope's `P` must be the handler's: `WithTenantScope` built for another principal type closes the handler, and every request answers 403.
- A run enqueued without `Job.Tenant` belongs to no tenant and is visible only to `AllTenants` callers.
- With `admin.WithRunQuerier`, the querier's capabilities must include `workflow.CapTenant` too, since lists and run reads go through it.
- Captured output (`WithOutputs`) is read by run ID; the scope checks the run's tenant before the output route runs, so an output store shared by every tenant is safe to wire.
- `POST /runs/delete` needs a filter besides the tenant: "forget every finished run of this tenant" still has to list the statuses, as it does without a scope.

## Related

- [Admin JSON API](http-api.md): `Auth`, the routes and their actions.
- [Admin web UI](ui.md): the screens a scoped caller sees through the same handler.
- [Tenants](../jobs/tenants.md): `Job.Tenant` and `RunQuery.Tenant`.
- Example: [examples/admin-tenants](../../examples/admin-tenants/).
