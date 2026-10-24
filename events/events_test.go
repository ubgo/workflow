package events_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/events"
	"github.com/ubgo/workflow/memory"
)

// recorder is a workflow that records the payloads and labels it ran.
type recorder struct {
	mu   sync.Mutex
	runs []workflow.StepInfo
	ins  []string
}

func (r *recorder) workflow(name string) workflow.Workflow {
	return workflow.Workflow{Name: name, Steps: []workflow.Step{{Name: "s", Run: func(ctx context.Context, in []byte) ([]byte, error) {
		info, _ := workflow.StepInfoFrom(ctx)
		r.mu.Lock()
		r.runs = append(r.runs, info)
		r.ins = append(r.ins, string(in))
		r.mu.Unlock()
		return nil, nil
	}}}}
}

func (r *recorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.runs)
}

func started(t *testing.T, wfs ...workflow.Workflow) *memory.Client {
	t.Helper()
	c := memory.New()
	for _, wf := range wfs {
		if err := c.Register(wf); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	return c
}

func waitAll(t *testing.T, c workflow.Client, p events.Published) {
	t.Helper()
	for _, d := range p.Deliveries {
		for i := 0; ; i++ {
			st, err := c.Status(context.Background(), d.Ref)
			if err != nil {
				t.Fatal(err)
			}
			if st.IsTerminal() {
				break
			}
			if i > 2000 {
				t.Fatalf("run %v did not finish", d.Ref)
			}
			time.Sleep(2 * time.Millisecond)
		}
	}
}

func TestPublishStartsSubscribers(t *testing.T) {
	mail, audit := &recorder{}, &recorder{}
	c := started(t, mail.workflow("mail"), audit.workflow("audit"))
	bus, err := events.New(c)
	if err != nil {
		t.Fatal(err)
	}
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(bus.Subscribe(events.Subscription{Name: "send-receipt", Event: "order.paid", Workflow: "mail",
		Filter: func(m events.Message) bool { return m.Key != "test" }}))
	must(bus.Subscribe(events.Subscription{Name: "audit-all", Event: "order.paid",
		Job: func(m events.Message) (workflow.Job, error) {
			return workflow.Job{Workflow: "audit", Payload: append([]byte("audit:"), m.Payload...)}, nil
		}}))
	if err := bus.Subscribe(events.Subscription{Name: "send-receipt", Event: "x", Workflow: "mail"}); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("duplicate subscription = %v", err)
	}

	p, err := bus.Publish(context.Background(), events.Message{ID: "evt-1", Name: "order.paid", Payload: []byte(`{"order":1}`),
		Labels: map[string]string{"tenant": "acme"}})
	if err != nil || len(p.Deliveries) != 2 {
		t.Fatalf("publish = %+v, %v", p, err)
	}
	waitAll(t, c, p)
	if mail.count() != 1 || audit.count() != 1 || audit.ins[0] != `audit:{"order":1}` {
		t.Fatalf("mail %d, audit %d %v", mail.count(), audit.count(), audit.ins)
	}
	l := mail.runs[0].Labels
	if l[events.LabelEvent] != "order.paid" || l[events.LabelEventID] != "evt-1" || l[events.LabelSubscription] != "send-receipt" || l["tenant"] != "acme" {
		t.Fatalf("labels = %v", l)
	}

	// The filter drops a test event; an unmatched name starts nothing.
	p, err = bus.Publish(context.Background(), events.Message{Name: "order.paid", Key: "test"})
	if err != nil || len(p.Deliveries) != 1 || p.ID == "" {
		t.Fatalf("filtered publish = %+v, %v", p, err)
	}
	if p, err := bus.Publish(context.Background(), events.Message{Name: "nobody"}); err != nil || len(p.Deliveries) != 0 {
		t.Fatalf("unmatched = %+v, %v", p, err)
	}
	if _, err := bus.Publish(context.Background(), events.Message{Name: "x", Labels: map[string]string{events.LabelEvent: "forged"}}); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("reserved label = %v", err)
	}
}

func TestPublishSameIDDedups(t *testing.T) {
	gate := make(chan struct{})
	var runs sync.WaitGroup
	slow := workflow.Workflow{Name: "slow", Steps: []workflow.Step{{Name: "s", Run: func(context.Context, []byte) ([]byte, error) {
		runs.Done()
		<-gate
		return nil, nil
	}}}}
	c := started(t, slow)
	bus, _ := events.New(c)
	_ = bus.Subscribe(events.Subscription{Name: "s", Event: "e", Workflow: "slow"})
	runs.Add(1)
	p1, err := bus.Publish(context.Background(), events.Message{ID: "same", Name: "e"})
	if err != nil {
		t.Fatal(err)
	}
	p2, err := bus.Publish(context.Background(), events.Message{ID: "same", Name: "e"})
	if err != nil {
		t.Fatal(err)
	}
	if p1.Deliveries[0].Ref != p2.Deliveries[0].Ref {
		t.Fatalf("republish started a second run: %v vs %v", p1.Deliveries[0].Ref, p2.Deliveries[0].Ref)
	}
	runs.Wait()
	close(gate)
}

// noKeys hides CapIdempotencyKey.
type noKeys struct{ workflow.Client }

func (n noKeys) Capabilities() workflow.Caps {
	return n.Client.Capabilities() &^ workflow.CapIdempotencyKey
}

func TestNewRefusesClientWithoutKeys(t *testing.T) {
	c := memory.New()
	if _, err := events.New(noKeys{c}); !errors.Is(err, workflow.ErrUnsupported) {
		t.Fatalf("New(no keys) = %v, want ErrUnsupported", err)
	}
	if _, err := events.New(noKeys{c}, events.AllowDuplicates()); err != nil {
		t.Fatalf("AllowDuplicates: %v", err)
	}
	if _, err := events.New(nil); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("New(nil) = %v", err)
	}
}

func TestPublishSignalsWaitingRuns(t *testing.T) {
	var got sync.Map
	wf := workflow.Workflow{Name: "await", Steps: []workflow.Step{
		{Name: "w", Wait: &workflow.WaitSpec{Event: "approved", Key: "doc-1"}},
		{Name: "s", After: []string{"w"}, Run: func(_ context.Context, in []byte) ([]byte, error) {
			got.Store("in", string(in))
			return nil, nil
		}},
	}}
	c := started(t, wf)
	ref, err := c.Enqueue(context.Background(), workflow.Job{Workflow: "await"})
	if err != nil {
		t.Fatal(err)
	}
	bus, _ := events.New(c, events.WithSignal())
	p, err := bus.Publish(context.Background(), events.Message{Name: "approved", Key: "doc-1", Payload: []byte("yes")})
	if err != nil || !p.Signalled {
		t.Fatalf("publish = %+v, %v", p, err)
	}
	waitAll(t, c, events.Published{Deliveries: []events.Delivery{{Ref: ref}}})
	if v, _ := got.Load("in"); v != "yes" {
		t.Fatalf("waiting run got %v", v)
	}
}

func TestPublishReportsEachFailure(t *testing.T) {
	ok := &recorder{}
	c := started(t, ok.workflow("ok"))
	bus, _ := events.New(c)
	_ = bus.Subscribe(events.Subscription{Name: "missing", Event: "e", Workflow: "not-registered"})
	_ = bus.Subscribe(events.Subscription{Name: "fine", Event: "e", Workflow: "ok"})
	p, err := bus.Publish(context.Background(), events.Message{Name: "e"})
	if err == nil || len(p.Deliveries) != 2 || p.Deliveries[0].Err == nil || p.Deliveries[1].Err != nil {
		t.Fatalf("publish = %+v, %v; want the first failed and the second started", p, err)
	}
}

func TestHMACSignVerify(t *testing.T) {
	h := events.HMAC{Header: "X-Sig", Prefix: "sha256=", Secrets: [][]byte{[]byte("new"), []byte("old")}, IDHeader: "X-Id"}
	body := []byte(`{"a":1}`)
	sig, err := h.Sign(body)
	if err != nil {
		t.Fatal(err)
	}
	req := func(sig string, body []byte) *http.Request {
		r := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
		r.Header.Set("X-Sig", sig)
		r.Header.Set("X-Id", "d-1")
		return r
	}
	if id, err := h.Verify(req(sig, body), body); err != nil || id != "d-1" {
		t.Fatalf("verify = %q, %v", id, err)
	}
	oldSig, _ := events.HMAC{Header: "X-Sig", Prefix: "sha256=", Secrets: [][]byte{[]byte("old")}}.Sign(body)
	if _, err := h.Verify(req(oldSig, body), body); err != nil {
		t.Fatalf("rotated secret: %v", err)
	}
	for _, bad := range []string{"", "sha256=00", sig[:len(sig)-1] + "0", "md5=" + sig[7:]} {
		if _, err := h.Verify(req(bad, body), body); !errors.Is(err, events.ErrSignature) {
			t.Errorf("sig %q = %v, want ErrSignature", bad, err)
		}
	}
	if _, err := h.Verify(req(sig, []byte(`{"a":2}`)), []byte(`{"a":2}`)); !errors.Is(err, events.ErrSignature) {
		t.Fatalf("tampered body = %v", err)
	}
	b64 := events.HMAC{Header: "X-Shopify-Hmac-Sha256", Encoding: events.EncodingBase64, Secrets: [][]byte{[]byte("k")}}
	s, _ := b64.Sign(body)
	r := httptest.NewRequest(http.MethodPost, "/", nil)
	r.Header.Set("X-Shopify-Hmac-Sha256", s)
	if _, err := b64.Verify(r, body); err != nil {
		t.Fatalf("base64: %v", err)
	}
}

func TestStandardWebhooks(t *testing.T) {
	secret := "whsec_" + base64.StdEncoding.EncodeToString([]byte("0123456789abcdef"))
	clk := clock.NewFake(time.Unix(1_700_000_000, 0))
	sw, err := events.NewStandardWebhooks([]string{secret}, 0, clk)
	if err != nil {
		t.Fatal(err)
	}
	body := []byte(`{"type":"invoice.paid"}`)
	h := sw.Headers("msg_1", body)
	r := httptest.NewRequest(http.MethodPost, "/", nil)
	r.Header = h
	if id, err := sw.Verify(r, body); err != nil || id != "msg_1" {
		t.Fatalf("verify = %q, %v", id, err)
	}
	r2 := httptest.NewRequest(http.MethodPost, "/", nil)
	r2.Header = h.Clone()
	r2.Header.Set(events.HeaderWebhookID, "msg_2")
	if _, err := sw.Verify(r2, body); !errors.Is(err, events.ErrSignature) {
		t.Fatalf("id swapped = %v", err)
	}
	// Several signatures, one valid, as during a key rotation.
	r3 := httptest.NewRequest(http.MethodPost, "/", nil)
	r3.Header = h.Clone()
	r3.Header.Set(events.HeaderWebhookSignature, "v1,AAAA "+h.Get(events.HeaderWebhookSignature))
	if _, err := sw.Verify(r3, body); err != nil {
		t.Fatalf("multi signature: %v", err)
	}
	clk.Advance(6 * time.Minute)
	if _, err := sw.Verify(r, body); !errors.Is(err, events.ErrSignature) {
		t.Fatalf("stale = %v, want ErrSignature", err)
	}
	if got := h.Get(events.HeaderWebhookTimestamp); got != strconv.Itoa(1_700_000_000) {
		t.Fatalf("timestamp = %q", got)
	}
	if _, err := events.NewStandardWebhooks([]string{"whsec_!!"}, 0, nil); err == nil {
		t.Fatal("bad secret accepted")
	}
}

// failing is a client whose Enqueue fails with err.
type failing struct {
	workflow.Client
	err error
}

func (f failing) Enqueue(context.Context, workflow.Job) (workflow.RunRef, error) {
	return workflow.RunRef{}, f.err
}

func TestWebhookHandler(t *testing.T) {
	rec := &recorder{}
	c := started(t, rec.workflow("ingest"))
	v := events.HMAC{Header: "X-Sig", Secrets: [][]byte{[]byte("k")}}
	hook, err := events.NewWebhook(events.WebhookConfig{Verifier: v, Client: c, Workflow: "ingest", MaxBody: 64,
		Labels: func(r *http.Request) map[string]string { return map[string]string{"src": r.URL.Query().Get("src")} }})
	if err != nil {
		t.Fatal(err)
	}
	post := func(h http.Handler, body []byte, sig string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(http.MethodPost, "/hook?src=shop", bytes.NewReader(body))
		if sig != "" {
			r.Header.Set("X-Sig", sig)
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w
	}
	body := []byte(`{"order":7}`)
	sig, _ := v.Sign(body)
	if w := post(hook, body, sig); w.Code != http.StatusOK || !bytes.Contains(w.Body.Bytes(), []byte(`"id"`)) {
		t.Fatalf("signed = %d %s", w.Code, w.Body)
	}
	if w := post(hook, body, "bad"); w.Code != http.StatusUnauthorized {
		t.Fatalf("bad sig = %d", w.Code)
	}
	if w := post(hook, body, ""); w.Code != http.StatusUnauthorized {
		t.Fatalf("no sig = %d", w.Code)
	}
	big := bytes.Repeat([]byte("x"), 65)
	bigSig, _ := v.Sign(big)
	if w := post(hook, big, bigSig); w.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("big = %d", w.Code)
	}
	w := httptest.NewRecorder()
	hook.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/hook", nil))
	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("GET = %d", w.Code)
	}
	for range 200 {
		if rec.count() > 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if rec.count() != 1 || rec.ins[0] != `{"order":7}` || rec.runs[0].Labels["src"] != "shop" {
		t.Fatalf("runs = %d %v", rec.count(), rec.ins)
	}

	for want, err := range map[int]error{
		http.StatusServiceUnavailable:  workflow.Unavailable(errors.New("db down")),
		http.StatusInternalServerError: errors.New("bug"),
	} {
		h, _ := events.NewWebhook(events.WebhookConfig{Verifier: v, Client: failing{c, err}, Workflow: "ingest"})
		if w := post(h, body, sig); w.Code != want || bytes.Contains(w.Body.Bytes(), []byte("db down")) {
			t.Errorf("enqueue %v = %d %s, want %d without detail", err, w.Code, w.Body, want)
		}
	}
}

func TestWebhookConfigIsClosedByDefault(t *testing.T) {
	c := memory.New()
	bus, _ := events.New(c)
	v := events.HMAC{Header: "X", Secrets: [][]byte{[]byte("k")}}
	for name, cfg := range map[string]events.WebhookConfig{
		"no verifier": {Client: c, Workflow: "w"},
		"no target":   {Verifier: v},
		"two targets": {Verifier: v, Client: c, Workflow: "w", Bus: bus, Event: "e"},
		"half bus":    {Verifier: v, Bus: bus},
		"half client": {Verifier: v, Workflow: "w"},
	} {
		if _, err := events.NewWebhook(cfg); !errors.Is(err, workflow.ErrInvalid) {
			t.Errorf("%s = %v, want ErrInvalid", name, err)
		}
	}
	if _, err := events.NewWebhook(events.WebhookConfig{Unsigned: true, Bus: bus, Event: "e"}); err != nil {
		t.Fatalf("explicit Unsigned: %v", err)
	}
}

func TestWebhookPublishesToBusWithDeliveryID(t *testing.T) {
	gate := make(chan struct{})
	t.Cleanup(func() { close(gate) })
	slow := workflow.Workflow{Name: "slow", Steps: []workflow.Step{{Name: "s", Run: func(context.Context, []byte) ([]byte, error) {
		<-gate
		return nil, nil
	}}}}
	c := started(t, slow)
	bus, _ := events.New(c)
	_ = bus.Subscribe(events.Subscription{Name: "sub", Event: "shop.order", Workflow: "slow"})
	v := events.HMAC{Header: "X-Sig", Secrets: [][]byte{[]byte("k")}, IDHeader: "X-Delivery"}
	hook, err := events.NewWebhook(events.WebhookConfig{Verifier: v, Bus: bus, Event: "shop.order"})
	if err != nil {
		t.Fatal(err)
	}
	body := []byte(`{}`)
	sig, _ := v.Sign(body)
	for range 2 { // a sender's retry of one delivery
		r := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
		r.Header.Set("X-Sig", sig)
		r.Header.Set("X-Delivery", "dl-9")
		w := httptest.NewRecorder()
		hook.ServeHTTP(w, r)
		if w.Code != http.StatusOK || !bytes.Contains(w.Body.Bytes(), []byte("dl-9")) {
			t.Fatalf("delivery = %d %s", w.Code, w.Body)
		}
	}
	page, err := c.Runs(context.Background(), workflow.RunQuery{Workflow: "slow"})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Runs) != 1 || page.Runs[0].Labels[events.LabelEventID] != "dl-9" {
		t.Fatalf("runs = %+v, want one run for delivery dl-9", page.Runs)
	}
}

// TestStandardWebhooksKnownVector checks interop with other
// implementations: the vector published with the reference libraries.
func TestStandardWebhooksKnownVector(t *testing.T) {
	clk := clock.NewFake(time.Unix(1614265330, 0))
	sw, err := events.NewStandardWebhooks([]string{"whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw"}, 0, clk)
	if err != nil {
		t.Fatal(err)
	}
	body := []byte(`{"test": 2432232314}`)
	h := sw.Headers("msg_p5jXN8AQM9LWM0D4loKWxJek", body)
	if got := h.Get(events.HeaderWebhookSignature); got != "v1,g0hM9SsE+OTPJTGt/tmIKtSyZlE3uFJELVlNIOLJ1OE=" {
		t.Fatalf("signature = %q", got)
	}
}

func TestReservedLabelsRefused(t *testing.T) {
	rec := &recorder{}
	c := started(t, rec.workflow("w"))
	bus, _ := events.New(c)
	_ = bus.Subscribe(events.Subscription{Name: "forge", Event: "e", Job: func(events.Message) (workflow.Job, error) {
		return workflow.Job{Workflow: "w", Labels: map[string]string{events.LabelEventID: "forged"}}, nil
	}})
	p, err := bus.Publish(context.Background(), events.Message{Name: "e"})
	if !errors.Is(err, workflow.ErrInvalid) || len(p.Deliveries) != 1 || p.Deliveries[0].Err == nil {
		t.Fatalf("Job with reserved label = %+v, %v; want ErrInvalid", p, err)
	}
	var logs bytes.Buffer
	hook, _ := events.NewWebhook(events.WebhookConfig{Unsigned: true, Client: c, Workflow: "w",
		Logger: slog.New(slog.NewTextHandler(&logs, nil)),
		Labels: func(*http.Request) map[string]string { return map[string]string{events.LabelEvent: "x"} }})
	w := httptest.NewRecorder()
	hook.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/", bytes.NewReader([]byte("{}"))))
	if w.Code != http.StatusInternalServerError || !bytes.Contains(logs.Bytes(), []byte("configuration error")) || bytes.Contains(logs.Bytes(), []byte("enqueue failed")) {
		t.Fatalf("reserved webhook label = %d, log %q", w.Code, logs.String())
	}
}
