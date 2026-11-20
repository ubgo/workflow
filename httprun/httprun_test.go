package httprun_test

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/httprun"
	"github.com/ubgo/workflow/memory"
)

// user is the test's principal: the X-User header.
type user string

func identify(r *http.Request) (user, error) {
	u := r.Header.Get("X-User")
	if u == "" {
		return "", errors.New("anonymous")
	}
	return user(u), nil
}

// ownerOnly lets everyone start a run, and only the user named in the
// run's "owner" label read it.
func ownerOnly(_ context.Context, req httprun.Request[user]) error {
	if req.Action == httprun.ActionStart {
		return nil
	}
	if req.Resource.Info == nil || req.Resource.Info.Labels["owner"] != string(req.Who) {
		return errors.New("not yours")
	}
	return nil
}

// stampOwner puts the caller in the job's owner label.
func stampOwner(r *http.Request, job *workflow.Job) error {
	who, _ := httprun.PrincipalFrom[user](r.Context())
	job.Labels = map[string]string{"owner": string(who)}
	return nil
}

func echoWorkflow(name string) workflow.Workflow {
	return workflow.Workflow{Name: name, Steps: []workflow.Step{{Name: "echo", Run: func(ctx context.Context, in []byte) ([]byte, error) {
		return in, nil
	}}}}
}

// startClient registers wfs on a memory client, builds the handler with
// cfg (before Start, so lifecycle events are live), and starts it.
func startClient(t *testing.T, c workflow.Client, cfg httprun.Config[user], wfs ...workflow.Workflow) http.Handler {
	t.Helper()
	for _, wf := range wfs {
		if err := c.Register(wf); err != nil {
			t.Fatal(err)
		}
	}
	h := httprun.New(c, cfg)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	return h
}

func do(h http.Handler, method, path, who, ctype, body string, hdr ...string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	if who != "" {
		r.Header.Set("X-User", who)
	}
	if ctype != "" {
		r.Header.Set("Content-Type", ctype)
	}
	for i := 0; i+1 < len(hdr); i += 2 {
		r.Header.Set(hdr[i], hdr[i+1])
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func errCode(t *testing.T, w *httptest.ResponseRecorder) string {
	t.Helper()
	var e struct {
		Error struct{ Code string } `json:"error"`
	}
	_ = json.Unmarshal(w.Body.Bytes(), &e)
	return e.Error.Code
}

// TestClosedByDefault: without both Auth functions every path answers
// 403, and a known route is no different from an unknown one.
func TestClosedByDefault(t *testing.T) {
	for name, auth := range map[string]httprun.Auth[user]{
		"zero":         {},
		"no Authorize": {Identify: identify},
		"no Identify":  {Authorize: func(context.Context, httprun.Request[user]) error { return nil }},
	} {
		h := httprun.New(memory.New(), httprun.Config[user]{Auth: auth, Workflows: []string{"echo"}})
		for _, path := range []string{"/echo", "/runs/x", "/runs/x/events", "/nope"} {
			method := http.MethodGet
			if path == "/echo" {
				method = http.MethodPost
			}
			w := do(h, method, path, "ann", "application/json", `{}`)
			if w.Code != http.StatusForbidden || errCode(t, w) != httprun.CodeForbidden {
				t.Errorf("%s %s %s: %d %s, want 403", name, method, path, w.Code, w.Body)
			}
		}
	}
}

// TestGate: anonymous is 401 before routing, Authorize refusing or
// panicking is 403, and the start route's own refusals.
func TestGate(t *testing.T) {
	var asked []httprun.Request[user]
	var mu sync.Mutex
	auth := httprun.Auth[user]{Identify: identify, Authorize: func(_ context.Context, req httprun.Request[user]) error {
		mu.Lock()
		asked = append(asked, req)
		mu.Unlock()
		switch req.Who {
		case "mallory":
			return errors.New("no")
		case "panic":
			panic("boom")
		}
		return nil
	}}
	h := startClient(t, memory.New(), httprun.Config[user]{Auth: auth, Workflows: []string{"echo"}, MaxBody: 16, MediaTypes: []string{"application/json", "text/plain"}}, echoWorkflow("echo"), echoWorkflow("secret"))
	cases := []struct {
		name, method, path, who, ctype, body string
		code                                 int
		errc                                 string
	}{
		{"anonymous unknown route", "GET", "/nope", "", "", "", 401, httprun.CodeUnauthorized},
		{"anonymous start", "POST", "/echo", "", "application/json", `{}`, 401, httprun.CodeUnauthorized},
		{"refused", "POST", "/echo", "mallory", "application/json", `{}`, 403, httprun.CodeForbidden},
		{"authorize panics", "POST", "/echo", "panic", "application/json", `{}`, 403, httprun.CodeForbidden},
		{"workflow not listed", "POST", "/secret", "ann", "application/json", `{}`, 404, httprun.CodeNotFound},
		{"no content type", "POST", "/echo", "ann", "", `{}`, 415, httprun.CodeMediaType},
		{"form post", "POST", "/echo", "ann", "application/x-www-form-urlencoded", `a=b`, 415, httprun.CodeMediaType},
		{"text/plain cannot be allowed", "POST", "/echo", "ann", "text/plain", `hi`, 415, httprun.CodeMediaType},
		{"bad JSON", "POST", "/echo", "ann", "application/json", `{`, 400, httprun.CodeInvalid},
		{"too large", "POST", "/echo", "ann", "application/json", `{"a":"0123456789abcdef"}`, 413, httprun.CodeTooLarge},
		{"unknown route", "GET", "/nope", "ann", "", "", 404, httprun.CodeNotFound},
	}
	for _, tc := range cases {
		w := do(h, tc.method, tc.path, tc.who, tc.ctype, tc.body)
		if w.Code != tc.code || errCode(t, w) != tc.errc {
			t.Errorf("%s: %d %s, want %d %s", tc.name, w.Code, w.Body, tc.code, tc.errc)
		}
	}
	w := do(h, "POST", "/echo", "ann", "application/json; charset=utf-8", `{"n":1}`)
	if w.Code != http.StatusAccepted {
		t.Fatalf("start: %d %s", w.Code, w.Body)
	}
	mu.Lock()
	last := asked[len(asked)-1]
	mu.Unlock()
	if last.Action != httprun.ActionStart || last.Resource.Workflow != "echo" || last.Resource.Job == nil || string(last.Resource.Job.Payload) != `{"n":1}` {
		t.Fatalf("Authorize saw %+v", last)
	}
}

// TestStartAndStatus: 202 with a Location, the status route with the
// result once it succeeded, payloads and errors hidden by default, and
// an owner check through Resource.Info.
func TestStartAndStatus(t *testing.T) {
	failing := workflow.Workflow{Name: "fail", Steps: []workflow.Step{{Name: "s", Run: func(context.Context, []byte) ([]byte, error) {
		return nil, workflow.Permanent(errors.New("db at 10.0.0.7 refused"))
	}}}}
	c := memory.New()
	h := startClient(t, c, httprun.Config[user]{Auth: httprun.Auth[user]{Identify: identify, Authorize: ownerOnly}, Workflows: []string{"echo", "fail"}, Prepare: stampOwner}, echoWorkflow("echo"), failing)

	w := do(h, "POST", "/echo", "ann", "application/json", `{"to":"bo@example.com"}`)
	if w.Code != http.StatusAccepted {
		t.Fatalf("start: %d %s", w.Code, w.Body)
	}
	var started struct {
		Ref struct {
			RunID string `json:"run_id"`
		} `json:"ref"`
		StatusURL string `json:"status_url"`
		EventsURL string `json:"events_url"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &started); err != nil {
		t.Fatal(err)
	}
	id := started.Ref.RunID
	if loc := w.Header().Get("Location"); loc != "runs/"+id || started.StatusURL != loc || started.EventsURL != loc+"/events" {
		t.Fatalf("Location %q, body %+v", loc, started)
	}
	ref := workflow.RunRef{RunID: workflow.RunID(id)}
	if _, err := workflow.Await(context.Background(), c, ref, workflow.AwaitOptions{Poll: time.Millisecond}); err != nil {
		t.Fatal(err)
	}
	w = do(h, "GET", "/runs/"+id, "ann", "", "")
	if w.Code != 200 {
		t.Fatalf("status: %d %s", w.Code, w.Body)
	}
	var st map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &st)
	if st["status"] != "succeeded" || st["done"] != true || st["workflow"] != "echo" {
		t.Fatalf("status body %s", w.Body)
	}
	if res, _ := json.Marshal(st["result"]); string(res) != `{"to":"bo@example.com"}` {
		t.Fatalf("result %s", res)
	}
	if _, ok := st["payload"]; ok {
		t.Fatal("payload shown without Config.Payloads")
	}
	if w := do(h, "GET", "/runs/"+id, "bo", "", ""); w.Code != http.StatusForbidden {
		t.Fatalf("another user read the run: %d", w.Code)
	}
	if w := do(h, "GET", "/runs/no-such-run", "ann", "", ""); w.Code != http.StatusForbidden {
		t.Fatalf("unknown run under an owner check: %d, want 403 (Authorize sees Info nil)", w.Code)
	}

	w = do(h, "POST", "/fail", "ann", "application/json", `{}`)
	_ = json.Unmarshal(w.Body.Bytes(), &started)
	fref := workflow.RunRef{RunID: workflow.RunID(started.Ref.RunID)}
	if _, err := workflow.Await(context.Background(), c, fref, workflow.AwaitOptions{Poll: time.Millisecond}); err != nil {
		t.Fatal(err)
	}
	w = do(h, "GET", "/runs/"+started.Ref.RunID, "ann", "", "")
	if !strings.Contains(w.Body.String(), `"status":"failed"`) || strings.Contains(w.Body.String(), "10.0.0.7") {
		t.Fatalf("failed run: %s (the error must stay hidden without Config.Errors)", w.Body)
	}
}

// TestStatusOptions: Payloads and Errors show what they name, Mount makes
// the Location absolute, an unknown run is 404 for an allowed caller,
// and the Idempotency-Key header returns the same run.
func TestStatusOptions(t *testing.T) {
	failing := workflow.Workflow{Name: "fail", Steps: []workflow.Step{{Name: "s", Run: func(context.Context, []byte) ([]byte, error) {
		return nil, workflow.Permanent(errors.New("card declined"))
	}}}}
	release := make(chan struct{})
	hold := workflow.Workflow{Name: "hold", Steps: []workflow.Step{{Name: "s", Run: func(ctx context.Context, in []byte) ([]byte, error) {
		<-release
		return in, nil
	}}}}
	defer close(release)
	c := memory.New()
	allow := httprun.Auth[user]{Identify: identify, Authorize: func(context.Context, httprun.Request[user]) error { return nil }}
	h := startClient(t, c, httprun.Config[user]{Auth: allow, Workflows: []string{"hold", "fail"}, Payloads: true, Errors: true, Mount: "/api/jobs/"}, hold, failing)
	held := do(h, "POST", "/hold", "ann", "application/json", `{}`, "Idempotency-Key", "k1").Header().Get("Location")
	if !strings.HasPrefix(held, "/api/jobs/runs/") {
		t.Fatalf("Location %q", held)
	}
	if again := do(h, "POST", "/hold", "ann", "application/json", `{}`, "Idempotency-Key", "k1").Header().Get("Location"); again != held {
		t.Fatalf("same key gave %q, want %q", again, held)
	}
	w := do(h, "POST", "/fail", "ann", "application/json", `"p"`)
	loc := w.Header().Get("Location")
	id := strings.TrimPrefix(loc, "/api/jobs/runs/")
	if _, err := workflow.Await(context.Background(), c, workflow.RunRef{RunID: workflow.RunID(id)}, workflow.AwaitOptions{Poll: time.Millisecond}); err != nil {
		t.Fatal(err)
	}
	w = do(h, "GET", "/runs/"+id, "ann", "", "")
	if body := w.Body.String(); !strings.Contains(body, `"payload":"p"`) || !strings.Contains(body, "card declined") {
		t.Fatalf("status %s", body)
	}
	if w := do(h, "GET", "/runs/nope", "ann", "", ""); w.Code != http.StatusNotFound {
		t.Fatalf("unknown run: %d", w.Code)
	}
}

// sseEvent is one parsed Server-Sent Event.
type sseEvent struct{ id, event, data string }

func readSSE(t *testing.T, body io.Reader) []sseEvent {
	t.Helper()
	var out []sseEvent
	var cur sseEvent
	sc := bufio.NewScanner(body)
	for sc.Scan() {
		line := sc.Text()
		switch {
		case line == "":
			if cur.event != "" {
				out = append(out, cur)
			}
			cur = sseEvent{}
		case strings.HasPrefix(line, "id: "):
			cur.id = line[4:]
		case strings.HasPrefix(line, "event: "):
			cur.event = line[7:]
		case strings.HasPrefix(line, "data: "):
			cur.data = line[6:]
		}
	}
	return out
}

// gatedStream is a workflow that emits two chunks, waits for release,
// emits a third and returns.
func gatedStream(release <-chan struct{}) workflow.Workflow {
	return workflow.Workflow{Name: "answer", Steps: []workflow.Step{{Name: "llm", Run: func(ctx context.Context, in []byte) ([]byte, error) {
		for _, tok := range []string{"Hel", "lo"} {
			if err := workflow.Emit(ctx, []byte(tok)); err != nil {
				return nil, err
			}
		}
		select {
		case <-release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		if err := workflow.Emit(ctx, []byte("!")); err != nil {
			return nil, err
		}
		return []byte(`"Hello!"`), nil
	}}}}
}

// TestEvents_Live: on the memory driver with lifecycle events, the route
// sends the backlog, the live chunks and status changes, then the end
// with the result, and closes; Last-Event-ID resumes after a chunk.
func TestEvents_Live(t *testing.T) {
	ms := workflow.NewMemoryStream(workflow.MemoryStreamOptions{})
	c, err := workflow.Use(memory.New(), workflow.Stream(ms))
	if err != nil {
		t.Fatal(err)
	}
	release := make(chan struct{})
	allow := httprun.Auth[user]{Identify: identify, Authorize: func(context.Context, httprun.Request[user]) error { return nil }}
	h := startClient(t, c, httprun.Config[user]{Auth: allow, Workflows: []string{"answer"}, Stream: ms, Poll: time.Hour}, gatedStream(release))
	srv := httptest.NewServer(h)
	defer srv.Close()

	w := do(h, "POST", "/answer", "ann", "application/json", `{}`)
	id := strings.TrimPrefix(w.Header().Get("Location"), "runs/")
	// Wait for the first two chunks, so the route starts with a backlog.
	for i := 0; i < 2000; i++ {
		if cs, _ := ms.Read(context.Background(), workflow.RunID(id), 0); len(cs) == 2 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	req, _ := http.NewRequest("GET", srv.URL+"/runs/"+id+"/events", nil)
	req.Header.Set("X-User", "ann")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("Content-Type %q", ct)
	}
	close(release)
	evs := readSSE(t, resp.Body) // Poll is an hour: only lifecycle events can end it
	var chunks []string
	for _, e := range evs {
		if e.event == httprun.EventChunk {
			var c struct{ Text string }
			_ = json.Unmarshal([]byte(e.data), &c)
			chunks = append(chunks, e.id+"="+c.Text)
		}
	}
	if got := strings.Join(chunks, ","); got != "1=Hel,2=lo,3=!" {
		t.Fatalf("chunks %s; events %+v", got, evs)
	}
	last := evs[len(evs)-1]
	if last.event != httprun.EventEnd || !strings.Contains(last.data, `"status":"succeeded"`) || !strings.Contains(last.data, `"result":"Hello!"`) {
		t.Fatalf("last event %+v", last)
	}
	if evs[0].event != httprun.EventChunk && evs[0].event != httprun.EventStatus {
		t.Fatalf("first event %+v", evs[0])
	}

	// A finished run: status, the chunks after Last-Event-ID, end.
	req.Header.Set("Last-Event-ID", "2")
	resp2, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp2.Body.Close()
	evs = readSSE(t, resp2.Body)
	var kinds []string
	for _, e := range evs {
		kinds = append(kinds, e.event+e.id)
	}
	if got := strings.Join(kinds, ","); got != "chunk3,status,end" {
		t.Fatalf("resumed stream %s", got)
	}
}

// statusOnly hides every optional interface of the client (lifecycle,
// results, run queries), so the route polls Status and sends no result.
type statusOnly struct{ workflow.Client }

// TestEvents_PollingOnFakeClock: without lifecycle events the route polls
// Status on the injected clock and ends once the run is terminal; with no
// CapResult the end carries no result.
func TestEvents_PollingOnFakeClock(t *testing.T) {
	clk := clock.NewFake(time.Date(2026, 11, 20, 9, 0, 0, 0, time.UTC))
	mc := memory.New()
	release := make(chan struct{})
	gate := workflow.Workflow{Name: "slow", Steps: []workflow.Step{{Name: "s", Run: func(ctx context.Context, in []byte) ([]byte, error) {
		<-release
		return []byte(`"done"`), nil
	}}}}
	allow := httprun.Auth[user]{Identify: identify, Authorize: func(context.Context, httprun.Request[user]) error { return nil }}
	h := startClient(t, statusOnly{mc}, httprun.Config[user]{Auth: allow, Workflows: []string{"slow"}, Clock: clk, Poll: time.Second, Heartbeat: time.Hour}, gate)
	srv := httptest.NewServer(h)
	defer srv.Close()
	ref, err := mc.Enqueue(context.Background(), workflow.Job{Workflow: "slow"})
	if err != nil {
		t.Fatal(err)
	}
	req, _ := http.NewRequest("GET", srv.URL+"/runs/"+ref.RunID.String()+"/events", nil)
	req.Header.Set("X-User", "ann")
	done := make(chan []sseEvent, 1)
	go func() {
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			done <- nil
			return
		}
		defer resp.Body.Close()
		done <- readSSE(t, resp.Body)
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := clk.BlockUntil(ctx, 2); err != nil { // the poll and heartbeat timers
		t.Fatal(err)
	}
	close(release)
	if _, err := workflow.Await(ctx, mc, ref, workflow.AwaitOptions{Poll: time.Millisecond}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
		t.Fatal("the stream ended before the clock reached the poll")
	case <-time.After(20 * time.Millisecond):
	}
	clk.Advance(time.Second)
	var evs []sseEvent
	select {
	case evs = <-done:
	case <-ctx.Done():
		t.Fatal("the stream did not end after the poll")
	}
	var kinds []string
	for _, e := range evs {
		kinds = append(kinds, e.event)
	}
	if got := strings.Join(kinds, ","); got != "status,status,end" {
		t.Fatalf("events %s", got)
	}
	if !strings.Contains(evs[1].data, `"status":"succeeded"`) || strings.Contains(evs[2].data, "result") {
		t.Fatalf("events %+v", evs)
	}
}

// echoStream is a StreamReader whose Read and Subscribe overlap: Read
// always returns chunks 1 and 2, and a subscription starts with 2 and 3
// buffered, then closes. The route must send each Pos once.
type echoStream struct{}

func (echoStream) Read(context.Context, workflow.RunID, int64) ([]workflow.StreamChunk, error) {
	return []workflow.StreamChunk{{Pos: 1, Data: []byte("a")}, {Pos: 2, Data: []byte("b")}}, nil
}

func (echoStream) Subscribe(context.Context, workflow.RunID) (<-chan workflow.StreamChunk, func()) {
	ch := make(chan workflow.StreamChunk, 2)
	ch <- workflow.StreamChunk{Pos: 2, Data: []byte("b")}
	ch <- workflow.StreamChunk{Pos: 3, Data: []byte("c")}
	close(ch)
	return ch, func() {}
}

// TestEvents_SendsEachChunkOnce: chunks seen by both Read and Subscribe
// go out once, in Pos order.
func TestEvents_SendsEachChunkOnce(t *testing.T) {
	release := make(chan struct{})
	hold := workflow.Workflow{Name: "hold", Steps: []workflow.Step{{Name: "s", Run: func(ctx context.Context, in []byte) ([]byte, error) {
		<-release
		return nil, nil
	}}}}
	mc := memory.New()
	allow := httprun.Auth[user]{Identify: identify, Authorize: func(context.Context, httprun.Request[user]) error { return nil }}
	h := startClient(t, statusOnly{mc}, httprun.Config[user]{Auth: allow, Stream: echoStream{}, Poll: 5 * time.Millisecond}, hold)
	ref, err := mc.Enqueue(context.Background(), workflow.Job{Workflow: "hold"})
	if err != nil {
		t.Fatal(err)
	}
	time.AfterFunc(50*time.Millisecond, func() { close(release) })
	w := do(h, "GET", "/runs/"+ref.RunID.String()+"/events", "ann", "", "")
	var ids []string
	for _, e := range readSSE(t, w.Body) {
		if e.event == httprun.EventChunk {
			ids = append(ids, e.id)
		}
	}
	if got := strings.Join(ids, ","); got != "1,2,3" {
		t.Fatalf("chunk ids %s, want 1,2,3", got)
	}
}
