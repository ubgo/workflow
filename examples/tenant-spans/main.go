// Command tenant-spans shows otel.WithTenant: the OpenTelemetry
// middleware records Job.Tenant as the workflow.tenant attribute on the
// enqueue span and on every step span, but only when built with the
// option, because a tenant ID can identify a customer. It runs one job for
// tenant "acme" on the in-memory driver twice, without and with the
// option, and prints the attribute each span carries. Spans go to an
// in-memory exporter, so no collector is needed.
//
// It is the program behind the "Read the tenant where the run is" section
// of docs/jobs/tenants.md: the guide copies its regions (marked
// "snippet:") and task docs:snippets fails when they drift.
package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"sort"
	"time"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/memory"
	"github.com/ubgo/workflow/middleware"
	wfotel "github.com/ubgo/workflow/otel"
)

// tenant is the customer the demo job runs for.
const tenant = "acme"

// awaitPoll is how often the example polls for the run's end; any short
// interval works on the in-memory driver.
const awaitPoll = 2 * time.Millisecond

// absent is what the program prints for a span without the attribute.
const absent = "(not recorded)"

// main runs the example and exits non-zero on error.
func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "tenant-spans:", err)
		os.Exit(1)
	}
}

// run is main without the process exit, so the test drives the same code
// and reads what it wrote to w.
func run(ctx context.Context, w io.Writer) error {
	if err := traced(ctx, w, "default"); err != nil {
		return err
	}
	// snippet:with-tenant
	opts := []wfotel.Option{
		wfotel.WithTenant(), // Job.Tenant becomes the workflow.tenant span attribute
	}
	// snippet:end
	return traced(ctx, w, "WithTenant", opts...)
}

// traced runs one job for the tenant through the OpenTelemetry middleware
// built with opts, then prints each span's name and tenant attribute.
func traced(ctx context.Context, w io.Writer, title string, opts ...wfotel.Option) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	spans := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(spans))

	// snippet:setup
	inst, err := wfotel.New(append([]wfotel.Option{wfotel.WithTracerProvider(tp)}, opts...)...)
	if err != nil {
		return err
	}
	c := memory.New(memory.WithMiddleware(inst.Step())) // step spans
	client := middleware.WrapClient(c, inst.Enqueue())  // enqueue spans
	// snippet:end
	if err := c.Register(workflow.Workflow{Name: "send-invoice", Steps: []workflow.Step{{Name: "send", Run: func(context.Context, []byte) ([]byte, error) {
		return nil, nil
	}}}}); err != nil {
		return err
	}
	if err := client.Start(ctx); err != nil {
		return err
	}
	defer c.Stop()
	ref, err := client.Enqueue(ctx, workflow.Job{Workflow: "send-invoice", Tenant: tenant})
	if err != nil {
		return err
	}
	if _, err := workflow.Await(ctx, client, ref, workflow.AwaitOptions{Poll: awaitPoll}); err != nil {
		return err
	}

	fmt.Fprintf(w, "%s:\n", title)
	stubs := spans.GetSpans()
	sort.Slice(stubs, func(i, j int) bool { return stubs[i].SpanKind < stubs[j].SpanKind }) // producer (enqueue) before consumer (step)
	for _, s := range stubs {
		got := absent
		for _, kv := range s.Attributes {
			if kv.Key == wfotel.AttrTenant {
				got = kv.Value.AsString()
			}
		}
		fmt.Fprintf(w, "  %s (%s) %s: %s\n", s.Name, s.SpanKind, wfotel.AttrTenant, got)
	}
	return nil
}
