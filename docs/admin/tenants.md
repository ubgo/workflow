# Per-tenant dashboard

[`admin.WithTenantScope`](ds:block?id=admin-withtenantscope-cbtmkxnu) confines each caller of the [admin JSON API](http-api.md), and of the [web UI](ui.md) on top of it, to one tenant (`workflow.Job.Tenant`), so a SaaS app can show its customers their own runs and schedules with the same handler its operators use. The scope is enforced by the handler on every route ([`route`](ds:block?id=admin-handler-route-xh5wtfdb)), not left to each `Authorize` to remember.

## Before you start

- A driver that declares `workflow.CapTenant` and jobs enqueued with `Job.Tenant` set ([Tenants](../jobs/tenants.md)). Without the capability a scoped handler refuses every request with 501 ([`TestTenantScope_RefusedWithoutCapTenant`](ds:block?id=admin-testtenantscope-refusedwithoutcaptenant-p5864kjk)).
- An [`admin.Auth[P]`](ds:block?id=admin-auth-sq9pywcz) whose principal knows its tenant ([Admin JSON API](http-api.md)).
- The runnable program for this page is [examples/admin-tenants](../../examples/admin-tenants/).

## Step 1: say which tenant each caller sees

A [`TenantScope[P]`](ds:block?id=admin-tenantscope-44wgkctt) is `func(r *http.Request, who P) (tenant string, err error)`, with `P` the handler's principal type. It runs after `Identify`, on every request, and returns the caller's tenant, or [`admin.AllTenants`](ds:block?id=admin-alltenants-kd8276ff) for an operator who sees every tenant:

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

// auth identifies a request's user by its bearer token and checks each
// action with authorize.
var auth = admin.Auth[user]{
	Identify: func(r *http.Request) (user, error) {
		u, ok := tokens[strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")]
		if !ok {
			return user{}, errors.New("no session")
		}
		return u, nil
	},
	Authorize: authorize,
}

// scope confines a customer to its tenant and lets an operator see all.
var scope = admin.WithTenantScope(func(_ *http.Request, u user) (string, error) {
	if u.Tenant == "" {
		return admin.AllTenants, nil
	}
	return u.Tenant, nil
})
```

The scope fails closed: an error, a panic, an empty string or a value that fails `workflow.ValidateTenant` refuses the request with 403 ([`callScope`](ds:block?id=admin-handler-callscope-5pkzhbay), [`TestTenantScope_FailsClosed`](ds:block?id=admin-testtenantscope-failsclosed-2558p8wz)). [`AllTenants`](ds:block?id=admin-alltenants-kd8276ff) is `"*"`, which is not a valid tenant ID, so no tenant can be mistaken for it.

## Step 2: mount the handler with the scope

The scope is one more option of [`admin.NewHandler`](ds:block?id=admin-newhandler-np68g63j): pass [`admin.WithTenantScope(fn)`](ds:block?id=admin-withtenantscope-cbtmkxnu) beside the `Auth`, and mount the handler as usual.

<!-- snippet: examples/admin-tenants/main.go#mount -->
```go
mux := http.NewServeMux()
mux.Handle("/admin/", http.StripPrefix("/admin", admin.NewHandler(admin.New(c), auth, scope)))
```

## What a scoped caller sees

Each route's rule is in [`tenantRoutes`](ds:block?id=admin-tenantroutes-h2f4ptcs), applied by [`scoped`](ds:block?id=admin-handler-scoped-66mv5cdw); [`TestTenantRoutes_CoverEveryRoute`](ds:block?id=admin-testtenantroutes-covereveryroute-fg2ywua7) fails when a route has none.

| Routes | For a caller scoped to tenant T |
|---|---|
| `GET /runs`, `/failures`, `/discarded` (with `count=1`) | Only T's runs. The `tenant` filter is set to T before the request is parsed and authorized, so `Resource.Query.Tenant` is T when `Authorize` runs. A request naming another tenant is refused with 403 ([`TestTenantScope_ListsPinned`](ds:block?id=admin-testtenantscope-listspinned-umru9eeq)). |
| `POST /runs/cancel`, `/runs/retry`, `/runs/delete` | Act on T's runs only, dry runs included. The body's `tenant` is set to T (whatever its spelling, since JSON field names match without regard to case); a body naming another tenant is 403, one that is not a single JSON object is 400 ([`pinBodyTenant`](ds:block?id=admin-pinbodytenant-vfk7mnu4), [`TestTenantScope_BulkPinned`](ds:block?id=admin-testtenantscope-bulkpinned-vurwbhtu)). |
| Every route on one run: `GET /runs/{id}`, retry, cancel, delete, complete, fail, output, reschedule, edit, cancel a step, reset, fork | The run is read first. A run of another tenant, or of none, answers 404 with the very body a run that does not exist gets, so a caller cannot tell the two apart ([`TestTenantScope_OneRun`](ds:block?id=admin-testtenantscope-onerun-74avd33p)). |
| `GET /schedules`, and every route on one schedule | Only the schedules whose `Schedule.Tenant` is T; any other answers 404. A declared `Cron` has no tenant, so it is hidden ([`scopeSchedules`](ds:block?id=admin-scopeschedules-eyb6wmhq), [`TestTenantScope_Schedules`](ds:block?id=admin-testtenantscope-schedules-rqpxmf4k)). |
| `POST /pause`, `POST /resume`, `GET /pauses`, `GET /queues`, `GET /metrics`, `GET /nodes`, `GET /health`, `POST /signals/waiting` | 403. A queue, a pause, a chart or a node is shared by every tenant, so none of it can be shown to one, and an event has no tenant: it would resume every tenant's waiting runs ([`TestTenantScope_SystemRoutesRefused`](ds:block?id=admin-testtenantscope-systemroutesrefused-xs5gfv48)). |
| `GET /capabilities` and the UI's files | Served. The capabilities answer names the tenant and reports queues, pauses, nodes, health and signal-waiting as unsupported ([`refusedOps`](ds:block?id=admin-refusedops-2e7gq2bt), [`scopeSupport`](ds:block?id=admin-scopesupport-gjcgsdh6)) and metrics as absent, so the UI hides those pages and opens on the runs list. |
| Any other path | 403, so a route added to the handler later stays closed to scoped callers until it has a rule. |

`Authorize` still runs on every request the scope lets through, with the same actions as before; [`admin.TenantFrom(req.HTTP.Context())`](ds:block?id=admin-tenantfrom-q4yqeu8a) gives it the caller's tenant, and false for a caller given `AllTenants`, who is served exactly as without the scope. The scope decides what a customer can see; `Authorize` still decides what it may do. The example lets a customer's users read and cancel their runs and nothing else:

<!-- snippet: examples/admin-tenants/main.go#authorize -->
```go
// authorize lets a customer's users read their tenant's runs and cancel
// them, and nothing else; an operator may do anything. The scope has
// already confined the request to the tenant: TenantFrom says which one,
// and false for an operator given AllTenants.
func authorize(_ context.Context, req admin.Request[user]) error {
	tenant, scoped := admin.TenantFrom(req.HTTP.Context())
	if !scoped || req.Action.ReadOnly() {
		return nil
	}
	switch req.Action {
	case admin.ActionCancelRun, admin.ActionCancelRuns:
		return nil
	}
	return fmt.Errorf("%s users may not %s", tenant, req.Action)
}
```

The handler's `Auth.Authorize` is this function, so a customer's `DELETE` of its own run answers 403 while its bulk cancel goes through, and the operator, unscoped, may do both.

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
acme DELETE its own run: 403 {"error":{"code":"forbidden","message":"not allowed: runs.delete"}}
-- A bulk action stays in the tenant
acme POST /runs/cancel: 200 {"cancelled":2}
globex's run: waiting
-- An operator sees every tenant
ops GET /runs: 200, tenants globex,acme,acme
ops GET /queues: 200
```

## Pitfalls

- The scope's `P` must be the handler's: [`WithTenantScope`](ds:block?id=admin-withtenantscope-cbtmkxnu) built for another principal type closes the handler, and every request answers 403 ([`TestTenantScope_PrincipalTypeMismatch`](ds:block?id=admin-testtenantscope-principaltypemismatch-m2uw22tb)).
- A run enqueued without `Job.Tenant` belongs to no tenant and is visible only to [`AllTenants`](ds:block?id=admin-alltenants-kd8276ff) callers ([`TestTenantScope_OneRun`](ds:block?id=admin-testtenantscope-onerun-74avd33p)).
- With [`admin.WithRunQuerier`](ds:block?id=admin-withrunquerier-g356xuy3), the querier's capabilities must include `workflow.CapTenant` too, since lists and run reads go through it ([`newTenantGate`](ds:block?id=admin-handler-newtenantgate-2r52a7yf)).
- Captured output (`WithOutputs`) is read by run ID; the scope checks the run's tenant before the output route runs ([`scoped`](ds:block?id=admin-handler-scoped-66mv5cdw)), so an output store shared by every tenant is safe to wire.
- A fork keeps the original's tenant, so `POST /runs/{id}/fork` needs only the run-in-scope check. An idempotency key is not per tenant, though: when the fork's key is held by another tenant's unfinished run, the driver returns that run, and the handler answers 409 without naming it ([`WithTenantScope`](ds:block?id=admin-withtenantscope-cbtmkxnu)). The caller learns only that the key is in use; give forks keys that include the tenant.
- `POST /runs/delete` needs a filter besides the tenant ([`Admin.DeleteAll`](ds:block?id=admin-admin-deleteall-hfykz854)): "forget every finished run of this tenant" still has to list the statuses, as it does without a scope.

## Related

- [Admin JSON API](http-api.md): `Auth`, the routes and their actions.
- [Admin web UI](ui.md): the screens a scoped caller sees through the same handler.
- [Tenants](../jobs/tenants.md): `Job.Tenant` and `RunQuery.Tenant`.
- Example: [examples/admin-tenants](../../examples/admin-tenants/).
