package middleware_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/middleware"
)

// sealWith runs plain through a DecodeStep handler that outputs it, which
// is the only exported way to get a sealed value: what a parent step
// stores.
func sealWith(t *testing.T, c middleware.Codec, plain []byte) []byte {
	t.Helper()
	h := middleware.DecodeStep(c)(func(context.Context, []byte) ([]byte, error) { return plain, nil })
	out, err := h(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// TestDecodeStep_SeveralParents builds a child's input exactly as a driver
// does (workflow.StepInput over the parents' sealed outputs) and checks the
// child sees the object it would have seen with no codec at all.
func TestDecodeStep_SeveralParents(t *testing.T) {
	codec, _ := middleware.Encrypt(testKey(1))
	plain := map[string][]byte{
		"json":  []byte(`{"total":42}`),
		"text":  []byte("not json \xff bytes"),
		"empty": nil,
	}
	parents := []string{"json", "text", "empty", "missing"}
	sealed := map[string][]byte{}
	for k, v := range plain {
		sealed[k] = sealWith(t, codec, v)
	}
	in := workflow.StepInput(nil, parents, sealed)
	if bytes.Contains(in, []byte("total")) {
		t.Fatal("merged input carries plaintext")
	}
	var seen []byte
	child := middleware.DecodeStep(codec)(func(_ context.Context, b []byte) ([]byte, error) { seen = b; return nil, nil })
	if _, err := child(context.Background(), in); err != nil {
		t.Fatal(err)
	}
	if want := workflow.StepInput(nil, parents, plain); !bytes.Equal(seen, want) {
		t.Fatalf("child saw\n%s\nwant\n%s", seen, want)
	}
	if !json.Valid(seen) {
		t.Fatal("opened input is not JSON")
	}

	// One unsealed parent value is refused, not passed through.
	mixed := workflow.StepInput(nil, []string{"a", "b"}, map[string][]byte{"a": sealed["json"], "b": []byte(`{"leak":1}`)})
	if _, err := child(context.Background(), mixed); !errors.Is(err, middleware.ErrDecode) || !workflow.IsPermanent(err) {
		t.Fatalf("unsealed parent accepted: %v", err)
	}
	// A parent sealed under another key is refused too.
	other, _ := middleware.Encrypt(testKey(2))
	foreign := workflow.StepInput(nil, []string{"a", "b"}, map[string][]byte{"a": sealed["json"], "b": sealWith(t, other, []byte("x"))})
	if _, err := child(context.Background(), foreign); !errors.Is(err, middleware.ErrDecode) {
		t.Fatalf("foreign parent accepted: %v", err)
	}
}

func TestDecodeStep_RefusesPlaintext(t *testing.T) {
	codec, _ := middleware.Encrypt(testKey(1))
	ran := false
	h := middleware.DecodeStep(codec)(func(context.Context, []byte) ([]byte, error) { ran = true; return nil, nil })
	for _, in := range []string{`{"card":"4111"}`, `"plain string"`, `"wfenc1:!!notbase64"`, "raw bytes", `[1,2]`} {
		if _, err := h(context.Background(), []byte(in)); !errors.Is(err, middleware.ErrDecode) || !workflow.IsPermanent(err) {
			t.Errorf("input %q: err = %v", in, err)
		}
	}
	if ran {
		t.Fatal("handler ran on unsealed input")
	}
	if _, err := h(context.Background(), nil); err != nil || !ran {
		t.Fatalf("empty input refused: %v", err)
	}
}

// TestWithCodec_SealsSignalAndSchedule covers the bytes that never pass
// through Enqueue: a Signal payload becomes a wait step's output, which
// its child opens, and a Schedule's Input becomes each fired run's payload.
func TestWithCodec_SealsSignalAndSchedule(t *testing.T) {
	codec, _ := middleware.Encrypt(testKey(1))
	stub := &stubClient{}
	c := middleware.WithCodec(stub, codec)
	ctx := context.Background()

	if err := c.Signal(ctx, workflow.Event{Name: "paid", Key: "o1", Payload: []byte(`{"amount":5}`)}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Schedule(ctx, workflow.Schedule{Workflow: "w", Cron: "* * * * *", Location: time.UTC, Input: []byte("nightly")}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Enqueue(ctx, workflow.Job{Workflow: "w", Payload: []byte("job-payload")}); err != nil {
		t.Fatal(err)
	}
	ev, sc, job := stub.events[0], stub.schedules[0], stub.jobs[0]
	if ev.Name != "paid" || ev.Key != "o1" || bytes.Contains(ev.Payload, []byte("amount")) {
		t.Fatalf("event not sealed or routing fields changed: %+v", ev)
	}
	if bytes.Contains(sc.Input, []byte("nightly")) || bytes.Contains(job.Payload, []byte("job-payload")) {
		t.Fatal("schedule input or job payload not sealed")
	}

	var seen []byte
	child := middleware.DecodeStep(codec)(func(_ context.Context, b []byte) ([]byte, error) { seen = b; return nil, nil })
	for want, in := range map[string][]byte{`{"amount":5}`: ev.Payload, "nightly": sc.Input, "job-payload": job.Payload} {
		if _, err := child(ctx, in); err != nil || string(seen) != want {
			t.Fatalf("opened %q, want %q (err %v)", seen, want, err)
		}
	}
	// A wait step joined with a run step: both sealed, both opened.
	runOut := sealWith(t, codec, []byte(`"charged"`))
	in := workflow.StepInput(nil, []string{"wait", "charge"}, map[string][]byte{"wait": ev.Payload, "charge": runOut})
	if _, err := child(ctx, in); err != nil || string(seen) != `{"wait":{"amount":5},"charge":"charged"}` {
		t.Fatalf("joined input %s, err %v", seen, err)
	}
	if c.Capabilities() != stub.Capabilities() {
		t.Fatal("WithCodec changed capabilities")
	}
}
