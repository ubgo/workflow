// Package push delivers a job to an HTTP endpoint instead of running it in
// the worker: Deliver is a step that POSTs the job, signed, to a URL (a
// serverless function, another service), and Handler is the receiving end
// that checks the signature and runs a workflow inline on the payload.
// The queue keeps what it is good at (retries, backoff, delays,
// concurrency, the admin view); the work runs wherever the endpoint is.
//
// Every delivery is signed with the Standard Webhooks scheme
// (events.StandardWebhooks, or any Signer), so the endpoint can refuse a
// forged or replayed request; Handler verifies it with any
// events.Verifier.
//
// The status codes carry the step's outcome both ways:
//
//	2xx                       success; the response body is the step's output
//	429 or 503 + Retry-After  retried after that wait (workflow.RetryAfter)
//	other 4xx                 failed for good (workflow.Permanent)
//	5xx, network error        retried under the step's retry policy
//
// and Handler answers with them: 200 with the workflow's output, 401 for a
// bad signature, 400 for a bad body, 422 when a step failed for good or
// discarded the run, 503 with Retry-After when a step asked to be retried
// later, 500 for any other step error.
//
// Deliver is an ordinary step, so it runs on every driver; Handler runs
// the workflow with workflow.RunInline, so it needs no driver at all. It
// needs nothing beyond the standard library and the core module.
package push

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/events"
)

// Envelope is the JSON body of a delivery.
//
//ds:def id=push-envelope-53rwh3es owner=@khanakia stability=stable
type Envelope struct {
	// Workflow, Run and Step name the delivering step (StepInfo).
	Workflow string `json:"workflow"`
	Run      string `json:"run"`
	Step     string `json:"step"`
	// Attempt is the delivering step's attempt, from 1, so the endpoint
	// can tell a retry from a first delivery.
	Attempt int `json:"attempt"`
	// Payload is the step's input (base64 in the JSON).
	Payload []byte `json:"payload"`
}

// Signer signs an outgoing body: it returns the headers to send.
// *events.StandardWebhooks is one. id is the delivery ID; it stays the
// same across the retries of one step, so the endpoint can deduplicate.
//
//ds:def id=push-signer-46q9qw54 owner=@khanakia stability=stable
type Signer interface {
	Headers(id string, body []byte) http.Header
}

// The defaults.
const (
	//ds:def id=push-defaulttimeout-85ebt6k7 owner=@khanakia stability=stable
	DefaultTimeout = 30 * time.Second
	//ds:def id=push-defaultmaxresponse-5fbhh25p owner=@khanakia stability=stable
	DefaultMaxResponse = 1 << 20
	//ds:def id=push-defaultmaxbody-rk3rgbcy owner=@khanakia stability=stable
	DefaultMaxBody = 1 << 20
)

// ErrResponseTooLarge means the endpoint's 2xx body was larger than the
// limit (WithMaxResponse). It is permanent: delivering again would get
// the same answer.
//
//ds:def id=push-errresponsetoolarge-j8fkf2m6 owner=@khanakia stability=stable
var ErrResponseTooLarge = errors.New("push: response body too large")

// StatusError is a non-2xx answer from the endpoint.
type StatusError struct {
	// Code is the HTTP status.
	Code int
	// Body is the start of the response body (at most 512 bytes), for the
	// run's error message.
	Body string
}

// Error names the status and the start of the body.
func (e *StatusError) Error() string {
	msg := "push: endpoint answered " + strconv.Itoa(e.Code)
	if e.Body != "" {
		msg += ": " + e.Body
	}
	return msg
}

// Option configures Deliver.
type Option func(*deliverConfig)

type deliverConfig struct {
	client  *http.Client
	timeout time.Duration
	max     int64
	clk     clock.Clock
	id      func(workflow.StepInfo) string
	header  http.Header
}

// WithClient sets the HTTP client (transport, proxy, TLS). Nil means
// http.DefaultClient. Deliver sets its own timeout per request through
// the ctx; a client's Timeout applies too.
//
//ds:def id=push-withclient-3d86bh2a owner=@khanakia stability=stable
func WithClient(c *http.Client) Option { return func(d *deliverConfig) { d.client = c } }

// WithTimeout bounds one delivery (DefaultTimeout); a timeout is a network
// error, so it is retried.
//
//ds:def id=push-withtimeout-7r4g988d owner=@khanakia stability=stable
func WithTimeout(t time.Duration) Option { return func(d *deliverConfig) { d.timeout = t } }

// WithMaxResponse bounds the 2xx body read as the step's output
// (DefaultMaxResponse, 1 MiB). A larger body fails the step for good
// (ErrResponseTooLarge) rather than being cut short silently.
//
//ds:def id=push-withmaxresponse-az87qpmk owner=@khanakia stability=stable
func WithMaxResponse(n int64) Option { return func(d *deliverConfig) { d.max = n } }

// WithClock is read for a Retry-After given as an HTTP date. Nil means
// clock.Real.
//
//ds:def id=push-withclock-rbhnyx5d owner=@khanakia stability=stable
func WithClock(c clock.Clock) Option { return func(d *deliverConfig) { d.clk = c } }

// WithDeliveryID replaces how the delivery ID is made from the step. The
// default is "<run>.<step>", the same for every attempt.
//
//ds:def id=push-withdeliveryid-u9vheqyc owner=@khanakia stability=stable
func WithDeliveryID(fn func(workflow.StepInfo) string) Option {
	return func(d *deliverConfig) { d.id = fn }
}

// WithHeader adds a header to every delivery (an API key the endpoint's
// gateway wants). The signature headers win over one of the same name.
//
//ds:def id=push-withheader-w7tfu4ss owner=@khanakia stability=stable
func WithHeader(key, value string) Option {
	return func(d *deliverConfig) { d.header.Add(key, value) }
}

// Deliver returns a step handler that POSTs the step's input to url as an
// Envelope, signed by signer, and maps the answer as the package doc
// says. It must run as a step (it reads StepInfo for the envelope and the
// delivery ID); outside one it fails for good with an error matching
// workflow.ErrInvalid, as it does with an empty url or a nil signer, so a
// misconfiguration is never retried. A Retry-After wait is honoured only
// when the step's retry policy reads it (RetryPolicy.DelayFor set to
// workflow.RetryAfterDelay, which needs CapRetryDelay); under any other
// policy the delivery retries on the normal backoff.
//
//ds:def id=push-deliver-n3r3thp8 owner=@khanakia stability=stable
func Deliver(url string, signer Signer, opts ...Option) workflow.Handler {
	cfg := deliverConfig{timeout: DefaultTimeout, max: DefaultMaxResponse, header: http.Header{}}
	for _, o := range opts {
		if o != nil {
			o(&cfg)
		}
	}
	if cfg.client == nil {
		cfg.client = http.DefaultClient
	}
	if cfg.clk == nil {
		cfg.clk = clock.Real{}
	}
	if cfg.max <= 0 {
		cfg.max = DefaultMaxResponse
	}
	if cfg.id == nil {
		cfg.id = func(i workflow.StepInfo) string { return i.RunID.String() + "." + i.Step }
	}
	return func(ctx context.Context, in []byte) ([]byte, error) {
		if url == "" || signer == nil {
			return nil, workflow.Permanent(fmt.Errorf("%w: push: Deliver needs a url and a signer", workflow.ErrInvalid))
		}
		info, ok := workflow.StepInfoFrom(ctx)
		if !ok {
			return nil, workflow.Permanent(fmt.Errorf("%w: push: Deliver runs as a step (no StepInfo on ctx)", workflow.ErrInvalid))
		}
		body, err := json.Marshal(Envelope{Workflow: info.Workflow, Run: info.RunID.String(), Step: info.Step, Attempt: info.Attempt, Payload: in})
		if err != nil {
			return nil, workflow.Permanent(err)
		}
		if cfg.timeout > 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, cfg.timeout)
			defer cancel()
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
		if err != nil {
			return nil, workflow.Permanent(fmt.Errorf("%w: push: %w", workflow.ErrInvalid, err))
		}
		for k, vs := range cfg.header {
			req.Header[k] = append([]string(nil), vs...)
		}
		for k, vs := range signer.Headers(cfg.id(info), body) {
			req.Header[k] = vs
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := cfg.client.Do(req)
		if err != nil {
			return nil, fmt.Errorf("push: deliver to endpoint: %w", err) // retryable
		}
		defer resp.Body.Close()
		return cfg.answer(resp)
	}
}

// answer maps the endpoint's response to the step's result.
//
//ds:def id=push-deliverconfig-answer-t9wu9nmj owner=@khanakia stability=stable
func (cfg *deliverConfig) answer(resp *http.Response) ([]byte, error) {
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		out, err := io.ReadAll(io.LimitReader(resp.Body, cfg.max+1))
		if err != nil {
			return nil, fmt.Errorf("push: read response: %w", err)
		}
		if int64(len(out)) > cfg.max {
			return nil, workflow.Permanent(fmt.Errorf("%w: more than %d bytes", ErrResponseTooLarge, cfg.max))
		}
		return out, nil
	}
	snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
	serr := &StatusError{Code: resp.StatusCode, Body: strings.TrimSpace(string(snippet))}
	switch {
	case resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode == http.StatusServiceUnavailable:
		if d, ok := retryAfter(resp.Header.Get("Retry-After"), cfg.clk.Now()); ok {
			return nil, workflow.RetryAfter(serr, d)
		}
		return nil, serr
	case resp.StatusCode >= 500:
		return nil, serr
	default: // other 4xx, and a 1xx or 3xx the client did not follow
		return nil, workflow.Permanent(serr)
	}
}

// retryAfter parses a Retry-After value: seconds, or an HTTP date read
// against now.
//
//ds:def id=push-retryafter-pnmntf8x owner=@khanakia stability=stable
func retryAfter(v string, now time.Time) (time.Duration, bool) {
	v = strings.TrimSpace(v)
	if v == "" {
		return 0, false
	}
	if n, err := strconv.Atoi(v); err == nil {
		if n < 0 {
			return 0, false
		}
		return time.Duration(n) * time.Second, true
	}
	if t, err := http.ParseTime(v); err == nil {
		return max(t.Sub(now), 0), true
	}
	return 0, false
}

// HandlerOption configures Handler.
type HandlerOption func(*handlerConfig)

type handlerConfig struct {
	max    int64
	inline []workflow.InlineOption
}

// WithMaxBody bounds the delivery body Handler reads (DefaultMaxBody,
// 1 MiB); a larger one answers 413.
//
//ds:def id=push-withmaxbody-v56e8b2s owner=@khanakia stability=stable
func WithMaxBody(n int64) HandlerOption { return func(c *handlerConfig) { c.max = n } }

// WithInline passes options to workflow.RunInline (middleware, a sleeper
// for retries inside the endpoint, a clock).
func WithInline(opts ...workflow.InlineOption) HandlerOption {
	return func(c *handlerConfig) { c.inline = append(c.inline, opts...) }
}

// Handler is the receiving end: it reads a POSTed Envelope, verifies its
// signature with verifier (the replay window is the verifier's: with
// events.StandardWebhooks a delivery older or newer than its tolerance is
// refused), runs wf with workflow.RunInline on the envelope's payload, and
// answers 200 with wf's output (workflow.RunOutput). It answers 405 for a
// method other than POST, 413 for a body over the limit, 401 for a bad
// signature (without saying why), 400 for a body that is not an Envelope
// or a job wf refuses, 422 when a step failed for good or discarded the
// run (the sender then fails the step for good), 503 with Retry-After when
// a step returned workflow.RetryAfter, and 500 for any other step error
// (the sender retries). Error bodies are a short text; a step's own error
// message is never sent back, since it can name internal hosts or data.
//
// Delivery is at least once: a retry after a lost response runs wf again,
// so its steps must be idempotent (key them on the webhook-id header, the
// same for every attempt of one step). A nil verifier refuses every
// request with 401, so the zero configuration is closed.
//
//ds:def id=push-handler-g994qhc5 owner=@khanakia stability=stable
func Handler(wf workflow.Workflow, verifier events.Verifier, opts ...HandlerOption) http.Handler {
	cfg := handlerConfig{max: DefaultMaxBody}
	for _, o := range opts {
		if o != nil {
			o(&cfg)
		}
	}
	if cfg.max <= 0 {
		cfg.max = DefaultMaxBody
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, cfg.max))
		if err != nil {
			var tooBig *http.MaxBytesError
			if errors.As(err, &tooBig) {
				http.Error(w, "body too large", http.StatusRequestEntityTooLarge)
				return
			}
			http.Error(w, "could not read the body", http.StatusBadRequest)
			return
		}
		if verifier == nil {
			http.Error(w, "signature does not verify", http.StatusUnauthorized)
			return
		}
		if _, err := verifier.Verify(r, body); err != nil {
			http.Error(w, "signature does not verify", http.StatusUnauthorized)
			return
		}
		var env Envelope
		dec := json.NewDecoder(bytes.NewReader(body))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&env); err != nil {
			http.Error(w, "body is not a delivery envelope", http.StatusBadRequest)
			return
		}
		res, err := workflow.RunInline(r.Context(), wf, workflow.Job{Workflow: wf.Name, Payload: env.Payload}, cfg.inline...)
		if err != nil {
			answerError(w, err)
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(workflow.RunOutput(wf, res.Outputs))
	})
}

// answerError maps a RunInline error to the status the sender reads.
//
//ds:def id=push-answererror-qvwz2cde owner=@khanakia stability=stable
func answerError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, workflow.ErrInvalid):
		http.Error(w, "the job is invalid", http.StatusBadRequest)
	case workflow.IsPermanent(err), workflow.IsDiscarded(err):
		http.Error(w, "the job failed for good", http.StatusUnprocessableEntity)
	default:
		if d, ok := workflow.RetryAfterDelay(err, 0); ok {
			w.Header().Set("Retry-After", strconv.Itoa(int((d+time.Second-1)/time.Second)))
			http.Error(w, "retry later", http.StatusServiceUnavailable)
			return
		}
		http.Error(w, "the job failed", http.StatusInternalServerError)
	}
}
