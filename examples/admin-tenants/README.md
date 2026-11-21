# admin-tenants

The admin JSON API served to an app's customers, each confined to its own tenant with `admin.WithTenantScope`. Two customers' users and an operator call it through HTTP: a customer's run list holds only its tenant's runs, another tenant's run answers 404 as if it did not exist, a filter naming another tenant and the system-wide `GET /queues` answer 403, `GET /capabilities` names the caller's tenant, an `Authorize` that reads `admin.TenantFrom` lets a customer cancel its runs but not delete them, a bulk cancel touches only the caller's runs, and the operator, given `admin.AllTenants`, sees every tenant and the queues. The client is the memory driver, which declares `workflow.CapTenant`.

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

The guide [Per-tenant dashboard](../../docs/admin/tenants.md) explains each step. `main_test.go` runs the same program and checks every line above.
