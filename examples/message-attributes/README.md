# message-attributes

A job's metadata and trace context crossing a hop that is not the library's own store. A producer traced with the otel module encodes a job into HTTP headers with `workflow.Attributes` and posts it; the consumer decodes the headers, enqueues the job on its own client with the decoded labels, tenant and trace labels, and the step's span lands in the producer's trace. The same job as SQS message attributes keeps each label as its own attribute until a label key SQS cannot name forces them into one `ubgo-labels` attribute, and Pub/Sub refuses a 2000-byte value instead of cutting it.

```sh
cd examples
go run ./message-attributes
```

```text
http hop:
  step result: "tenant acme, source shop"
  step span in the producer's trace: true
sqs: traceparent ubgo-l-source ubgo-tenant ubgo-workflow
sqs, a label SQS cannot name: traceparent ubgo-labels ubgo-tenant ubgo-workflow
pub/sub, a 2000-byte key refused: true
```

The guide is [OpenTelemetry › Across a broker or HTTP hop](../../docs/observability/opentelemetry.md#across-a-broker-or-http-hop).
