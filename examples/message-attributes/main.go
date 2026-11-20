// Command message-attributes shows a job's metadata and trace context
// crossing a hop that is not the library's own store (MQ-16): the
// producer encodes a job into HTTP headers with workflow.Attributes, a
// consumer on the other side decodes them and enqueues the job on its own
// client, and the step's span continues the producer's trace. It also
// prints what the same job looks like as SQS message attributes (labels
// packed into one attribute when SQS cannot name them) and a job Pub/Sub
// refuses rather than cut.
//
// It is the program behind the "Across a broker or HTTP hop" section of
// docs/observability/opentelemetry.md: the guide copies its regions
// (marked "snippet:") and task docs:snippets fails when they drift.
package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"strings"
	"time"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/memory"
	wfotel "github.com/ubgo/workflow/otel"
)

func main() {
	if err := run(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, "message-attributes:", err)
		os.Exit(1)
	}
}

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	spans := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(spans))
	inst, err := wfotel.New(wfotel.WithTracerProvider(tp))
	if err != nil {
		return err
	}

	// The consumer: its own client, steps traced.
	consumer := memory.New(memory.WithMiddleware(inst.Step()))
	if err := consumer.Register(workflow.Workflow{Name: "invoice", Steps: []workflow.Step{{Name: "send", Run: func(ctx context.Context, in []byte) ([]byte, error) {
		info, _ := workflow.StepInfoFrom(ctx)
		return []byte(fmt.Sprintf("%q", "tenant "+info.Tenant+", source "+info.Labels["source"])), nil
	}}}}); err != nil {
		return err
	}
	if err := consumer.Start(ctx); err != nil {
		return err
	}
	done := make(chan workflow.RunRef, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		// snippet:consume
		codec := workflow.Attributes{Limits: workflow.HTTPHeaders}
		d, err := codec.Decode(workflow.HeaderAttributes(r.Header))
		if err != nil {
			http.Error(w, "bad attributes", http.StatusBadRequest)
			return
		}
		job := d.Job
		job.Payload = body
		job.Labels = map[string]string{}
		maps.Copy(job.Labels, d.Job.Labels)
		maps.Copy(job.Labels, codec.TraceLabels(d)) // the steps continue the sender's trace
		ref, err := consumer.Enqueue(workflow.AllowReservedPrefix(r.Context(), workflow.LabelPrefixOTel), job)
		// snippet:end
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		done <- ref
	}))
	defer srv.Close()

	// The producer: the otel enqueue middleware writes the trace context
	// into the job's labels; the hop is an HTTP POST.
	// snippet:produce
	codec := workflow.Attributes{Limits: workflow.HTTPHeaders}
	send := func(ctx context.Context, job workflow.Job) (workflow.RunRef, error) {
		attrs, err := codec.Encode(job) // refuses what the transport cannot carry
		if err != nil {
			return workflow.RunRef{}, err
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL, bytes.NewReader(job.Payload))
		if err != nil {
			return workflow.RunRef{}, err
		}
		workflow.SetHeader(req.Header, attrs)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return workflow.RunRef{}, err
		}
		resp.Body.Close()
		return workflow.RunRef{}, nil
	}
	producer := inst.Enqueue()(send)
	// snippet:end
	pctx, parent := tp.Tracer("shop").Start(ctx, "checkout")
	job := workflow.Job{Workflow: "invoice", Tenant: "acme", Payload: []byte(`{"order":42}`), Labels: map[string]string{"source": "shop"}}
	if _, err := producer(pctx, job); err != nil {
		return err
	}
	parent.End()
	ref := <-done
	out, err := workflow.AwaitResult(ctx, consumer, ref, workflow.AwaitOptions{Poll: 2 * time.Millisecond})
	if err != nil {
		return err
	}
	fmt.Println("http hop:")
	fmt.Println("  step result:", string(out))
	same := false
	for _, s := range spans.GetSpans() {
		if strings.HasPrefix(s.Name, "workflow.step") {
			same = same || s.SpanContext.TraceID() == parent.SpanContext().TraceID()
		}
	}
	fmt.Println("  step span in the producer's trace:", same)

	// The same job as SQS message attributes.
	traced := workflow.Job{Workflow: "invoice", Tenant: "acme", Labels: map[string]string{"source": "shop"}}
	if traced, err = captureLabels(inst, pctx, traced); err != nil {
		return err
	}
	// snippet:sqs
	attrs, err := workflow.Attributes{Limits: workflow.SQSAttributes}.Encode(traced)
	// snippet:end
	if err != nil {
		return err
	}
	fmt.Println("sqs:", strings.Join(slices.Sorted(maps.Keys(attrs)), " "))
	traced.Labels["ubgo.workflow/batch"] = "b-7" // '/' cannot appear in an SQS name
	attrs, _ = workflow.Attributes{Limits: workflow.SQSAttributes}.Encode(traced)
	fmt.Println("sqs, a label SQS cannot name:", strings.Join(slices.Sorted(maps.Keys(attrs)), " "))

	_, err = workflow.Attributes{Limits: workflow.PubSubAttributes}.Encode(workflow.Job{Workflow: "invoice", IdempotencyKey: strings.Repeat("k", 2000)})
	fmt.Println("pub/sub, a 2000-byte key refused:", errors.Is(err, workflow.ErrInvalid))
	return nil
}

// captureLabels runs job through the otel enqueue middleware and returns
// it as the middleware passed it on, trace labels included.
func captureLabels(inst *wfotel.Instrumentation, ctx context.Context, job workflow.Job) (workflow.Job, error) {
	var out workflow.Job
	_, err := inst.Enqueue()(func(_ context.Context, j workflow.Job) (workflow.RunRef, error) {
		out = j
		return workflow.RunRef{}, nil
	})(ctx, job)
	return out, err
}
