package push_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/events"
	"github.com/ubgo/workflow/memory"
	"github.com/ubgo/workflow/push"
)

var t0 = time.Date(2026, 11, 20, 9, 0, 0, 0, time.UTC)

var secret = "whsec_" + base64.StdEncoding.EncodeToString([]byte("a test secret, 32 bytes long....."))

func scheme(t *testing.T, clk clock.Clock) *events.StandardWebhooks {
	t.Helper()
	s, err := events.NewStandardWebhooks([]string{secret}, time.Minute, clk)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// stepCtx is a step's ctx, as a driver builds it.
func stepCtx() context.Context {
	return workflow.WithStepInfo(context.Background(), workflow.StepInfo{Workflow: "dispatch", Step: "send", RunID: "run-1", Attempt: 2})
}

// TestDeliver_Answers maps every kind of endpoint answer to the step's
// result, and checks what the endpoint received.
func TestDeliver_Answers(t *testing.T) {
	clk := clock.NewFake(t0)
	signer := scheme(t, clk)
	cases := []struct {
		name      string
		status    int
		header    map[string]string
		body      string
		opts      []push.Option
		wantOut   string
		permanent bool
		delay     time.Duration // > 0: a RetryAfter of this length
		wantErr   error
	}{
		{name: "2xx body is the output", status: 200, body: `{"sent":true}`, wantOut: `{"sent":true}`},
		{name: "204 is success with no output", status: 204},
		{name: "429 with Retry-After seconds", status: 429, header: map[string]string{"Retry-After": "7"}, delay: 7 * time.Second},
		{name: "503 with Retry-After date", status: 503, header: map[string]string{"Retry-After": t0.Add(90 * time.Second).Format(http.TimeFormat)}, delay: 90 * time.Second},
		{name: "503 without Retry-After is retryable", status: 503},
		{name: "500 is retryable", status: 500, body: "boom"},
		{name: "404 is permanent", status: 404, permanent: true},
		{name: "422 is permanent", status: 422, permanent: true},
		{name: "a body over the limit is permanent", status: 200, body: "0123456789", opts: []push.Option{push.WithMaxResponse(4)}, permanent: true, wantErr: push.ErrResponseTooLarge},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var got *http.Request
			var gotBody []byte
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				got = r
				gotBody, _ = readAll(r)
				for k, v := range tc.header {
					w.Header().Set(k, v)
				}
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer srv.Close()
			opts := append([]push.Option{push.WithClient(srv.Client()), push.WithClock(clk), push.WithHeader("X-Api-Key", "k")}, tc.opts...)
			out, err := push.Deliver(srv.URL, signer, opts...)(stepCtx(), []byte(`{"to":"bo"}`))
			if string(out) != tc.wantOut {
				t.Errorf("output %q, want %q", out, tc.wantOut)
			}
			ok := tc.status >= 200 && tc.status < 300 && tc.wantErr == nil
			if ok != (err == nil) {
				t.Fatalf("err %v", err)
			}
			if workflow.IsPermanent(err) != tc.permanent {
				t.Errorf("permanent %v, want %v (%v)", workflow.IsPermanent(err), tc.permanent, err)
			}
			d, hasDelay := workflow.RetryAfterDelay(err, 1)
			if hasDelay != (tc.delay > 0) || d != tc.delay {
				t.Errorf("delay %v %v, want %v", d, hasDelay, tc.delay)
			}
			if tc.wantErr != nil && !errors.Is(err, tc.wantErr) {
				t.Errorf("err %v, want %v", err, tc.wantErr)
			}
			var se *push.StatusError
			if !ok && tc.wantErr == nil && (!errors.As(err, &se) || se.Code != tc.status) {
				t.Errorf("err %v is not a StatusError %d", err, tc.status)
			}
			// What the endpoint received: a signed envelope.
			if id, err := signer.Verify(got, gotBody); err != nil || id != "run-1.send" {
				t.Errorf("signature: id %q err %v", id, err)
			}
			if got.Header.Get("X-Api-Key") != "k" || got.Header.Get("Content-Type") != "application/json" {
				t.Errorf("headers %v", got.Header)
			}
			var env push.Envelope
			if err := json.Unmarshal(gotBody, &env); err != nil {
				t.Fatal(err)
			}
			if env.Workflow != "dispatch" || env.Run != "run-1" || env.Step != "send" || env.Attempt != 2 || string(env.Payload) != `{"to":"bo"}` {
				t.Errorf("envelope %+v", env)
			}
		})
	}
}

func readAll(r *http.Request) ([]byte, error) {
	var b bytes.Buffer
	_, err := b.ReadFrom(r.Body)
	return b.Bytes(), err
}

// TestDeliver_Refusals: a network error is retryable; a misuse fails for
// good with ErrInvalid.
func TestDeliver_Refusals(t *testing.T) {
	signer := scheme(t, nil)
	srv := httptest.NewServer(http.NotFoundHandler())
	url := srv.URL
	srv.Close() // nothing listens any more
	if _, err := push.Deliver(url, signer, push.WithTimeout(time.Second))(stepCtx(), nil); err == nil || workflow.IsPermanent(err) {
		t.Errorf("network error: %v, want retryable", err)
	}
	for name, call := range map[string]func() error{
		"no step":   func() error { _, err := push.Deliver(url, signer)(context.Background(), nil); return err },
		"no url":    func() error { _, err := push.Deliver("", signer)(stepCtx(), nil); return err },
		"no signer": func() error { _, err := push.Deliver(url, nil)(stepCtx(), nil); return err },
	} {
		if err := call(); !errors.Is(err, workflow.ErrInvalid) || !workflow.IsPermanent(err) {
			t.Errorf("%s: %v, want permanent ErrInvalid", name, err)
		}
	}
}

// receiver is a workflow on the serverless side whose step does what the
// payload says.
func receiver() workflow.Workflow {
	return workflow.Workflow{Name: "send-email", Steps: []workflow.Step{{Name: "send", Run: func(ctx context.Context, in []byte) ([]byte, error) {
		switch string(in) {
		case `"permanent"`:
			return nil, workflow.Permanent(errors.New("address 10.0.0.7 rejected"))
		case `"later"`:
			return nil, workflow.RetryAfter(errors.New("rate limited"), 1500*time.Millisecond)
		case `"discard"`:
			return nil, workflow.Discard(errors.New("unsubscribed"))
		case `"fail"`:
			return nil, errors.New("smtp down")
		}
		return []byte(`{"sent":` + string(in) + `}`), nil
	}}}}
}

// TestHandler_Answers covers every status the receiving side answers.
func TestHandler_Answers(t *testing.T) {
	clk := clock.NewFake(t0)
	sw := scheme(t, clk)
	h := push.Handler(receiver(), sw, push.WithMaxBody(256))
	envelope := func(payload string) []byte {
		b, _ := json.Marshal(push.Envelope{Workflow: "dispatch", Run: "r", Step: "s", Attempt: 1, Payload: []byte(payload)})
		return b
	}
	signed := func(body []byte) *http.Request {
		r := httptest.NewRequest("POST", "/", bytes.NewReader(body))
		for k, v := range sw.Headers("r.s", body) {
			r.Header[k] = v
		}
		return r
	}
	cases := []struct {
		name  string
		req   func() *http.Request
		code  int
		body  string
		retry string
	}{
		{name: "success", req: func() *http.Request { return signed(envelope(`"bo"`)) }, code: 200, body: `{"sent":"bo"}`},
		{name: "GET", req: func() *http.Request { return httptest.NewRequest("GET", "/", nil) }, code: 405},
		{name: "unsigned", req: func() *http.Request { return httptest.NewRequest("POST", "/", bytes.NewReader(envelope(`"bo"`))) }, code: 401},
		{name: "tampered", req: func() *http.Request {
			r := signed(envelope(`"bo"`))
			r.Body = httptest.NewRequest("POST", "/", strings.NewReader(string(envelope(`"eve"`)))).Body
			return r
		}, code: 401},
		{name: "replayed after the tolerance", req: func() *http.Request {
			r := signed(envelope(`"bo"`))
			clk.Advance(2 * time.Minute)
			return r
		}, code: 401},
		{name: "not an envelope", req: func() *http.Request { return signed([]byte(`{"nope":1}`)) }, code: 400},
		{name: "too large", req: func() *http.Request { return signed(envelope(strings.Repeat("x", 300))) }, code: 413},
		{name: "step failed for good", req: func() *http.Request { return signed(envelope(`"permanent"`)) }, code: 422},
		{name: "step discarded", req: func() *http.Request { return signed(envelope(`"discard"`)) }, code: 422},
		{name: "step asked for later", req: func() *http.Request { return signed(envelope(`"later"`)) }, code: 503, retry: "2"},
		{name: "step failed", req: func() *http.Request { return signed(envelope(`"fail"`)) }, code: 500},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			h.ServeHTTP(w, tc.req())
			if w.Code != tc.code {
				t.Fatalf("code %d %s, want %d", w.Code, w.Body, tc.code)
			}
			if tc.body != "" && w.Body.String() != tc.body {
				t.Errorf("body %q, want %q", w.Body, tc.body)
			}
			if got := w.Header().Get("Retry-After"); got != tc.retry {
				t.Errorf("Retry-After %q, want %q", got, tc.retry)
			}
			if strings.Contains(w.Body.String(), "10.0.0.7") {
				t.Error("a step's error text reached the sender")
			}
		})
	}
	w := httptest.NewRecorder()
	push.Handler(receiver(), nil).ServeHTTP(w, signed(envelope(`"bo"`)))
	if w.Code != 401 {
		t.Errorf("nil verifier: %d, want 401", w.Code)
	}
}

// TestSenderAndReceiver: a run on the memory driver delivers to the
// receiving handler; the receiver's output becomes the run's result, a
// receiver failure is retried, and a permanent one fails the run after
// one attempt.
func TestSenderAndReceiver(t *testing.T) {
	sw := scheme(t, nil)
	var flaky atomic.Int32
	rx := receiver()
	inner := rx.Steps[0].Run
	rx.Steps[0].Run = func(ctx context.Context, in []byte) ([]byte, error) {
		if string(in) == `"flaky"` && flaky.Add(1) == 1 {
			return nil, errors.New("cold start")
		}
		return inner(ctx, in)
	}
	srv := httptest.NewServer(push.Handler(rx, sw))
	defer srv.Close()

	c := memory.New()
	dispatch := workflow.Workflow{Name: "dispatch", Retry: workflow.RetryPolicy{Max: 3, DelayFor: workflow.RetryAfterDelay},
		Steps: []workflow.Step{{Name: "send", Run: push.Deliver(srv.URL, sw, push.WithClient(srv.Client()))}}}
	if err := c.Register(dispatch); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	run := func(payload string) ([]byte, workflow.RunInfo, error) {
		ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "dispatch", Payload: []byte(payload)})
		if err != nil {
			t.Fatal(err)
		}
		res, err := workflow.AwaitResult(ctx, c, ref, workflow.AwaitOptions{Poll: time.Millisecond})
		page, qerr := c.Runs(ctx, workflow.RunQuery{Ref: ref, Limit: 1})
		if qerr != nil || len(page.Runs) != 1 {
			t.Fatal(qerr)
		}
		return res, page.Runs[0], err
	}
	res, info, err := run(`"bo"`)
	if err != nil || string(res) != `{"sent":"bo"}` || info.Attempts != 1 {
		t.Fatalf("delivered: %s, %d attempts, %v", res, info.Attempts, err)
	}
	res, info, err = run(`"flaky"`)
	if err != nil || string(res) != `{"sent":"flaky"}` || info.Attempts != 2 {
		t.Fatalf("flaky: %s, %d attempts, %v", res, info.Attempts, err)
	}
	_, info, err = run(`"permanent"`)
	if err == nil || info.Status != workflow.StatusFailed || info.Attempts != 1 {
		t.Fatalf("permanent: %v, status %s, %d attempts", err, info.Status, info.Attempts)
	}
}
