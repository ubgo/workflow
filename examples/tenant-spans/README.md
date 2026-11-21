# tenant-spans

`otel.WithTenant` on the in-memory driver: one job for tenant `acme` runs through the OpenTelemetry step and enqueue middleware twice. Built without the option (the default), no span carries the tenant; built with it, the enqueue span and the step span both carry `workflow.tenant`. Spans go to an in-memory exporter, so no collector is needed.

```sh
cd examples
go run ./tenant-spans
```

```text
default:
  workflow.enqueue send-invoice (producer) workflow.tenant: (not recorded)
  workflow.step send-invoice/send (consumer) workflow.tenant: (not recorded)
WithTenant:
  workflow.enqueue send-invoice (producer) workflow.tenant: acme
  workflow.step send-invoice/send (consumer) workflow.tenant: acme
```

The guide that copies this program is [tenants: read the tenant where the run is](../../docs/jobs/tenants.md#read-the-tenant-where-the-run-is). The whole OpenTelemetry setup is in [OpenTelemetry](../../docs/observability/opentelemetry.md).
