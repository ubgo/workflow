// Command http-status puts a workflow behind the three routes of the
// httprun package on the memory driver: a frontend starts a run with
// POST and gets 202 Accepted with a Location, reads the status and the
// result from that Location, and follows the run live with Server-Sent
// Events, including the output a step streams with workflow.Emit while it
// is still running. The handler is closed until the app says who may do
// what.
//
// It is the program behind docs/jobs/http-start.md: the guide copies its
// regions (marked "snippet:") and task docs:snippets fails when they
// drift.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/httprun"
	"github.com/ubgo/workflow/memory"
)

func main() {
	if err := run(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, "http-status:", err)
		os.Exit(1)
	}
}

// snippet:auth
// user is the app's own principal type; httprun.Config is generic over it.
type user string

// sessions stands in for the app's session store.
var sessions = map[string]user{"s-ann": "ann", "s-bo": "bo"}

var auth = httprun.Auth[user]{
	Identify: func(r *http.Request) (user, error) {
		u, ok := sessions[strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")]
		if !ok {
			return "", errors.New("no session")
		}
		return u, nil
	},
	// Anyone signed in may start a run; only its owner may read it.
	Authorize: func(_ context.Context, req httprun.Request[user]) error {
		if req.Action == httprun.ActionStart {
			return nil
		}
		if req.Resource.Info == nil || req.Resource.Info.Labels["owner"] != string(req.Who) {
			return errors.New("not your run")
		}
		return nil
	},
}

// stampOwner records who started the run, for Authorize above.
func stampOwner(r *http.Request, job *workflow.Job) error {
	who, _ := httprun.PrincipalFrom[user](r.Context())
	job.Labels = map[string]string{"owner": string(who)}
	return nil
}

// snippet:end

// summarizeWorkflow streams its answer word by word, the way a model
// streams tokens, and returns the whole answer as the run's result.
// release holds the step until the program has opened the event stream,
// so the output is the same on every run.
func summarizeWorkflow(release <-chan struct{}) workflow.Workflow {
	// snippet:workflow
	return workflow.Workflow{Name: "summarize", Steps: []workflow.Step{{Name: "answer", Run: func(ctx context.Context, in []byte) ([]byte, error) {
		<-release // the example waits here until it is listening
		words := []string{"Three ", "orders ", "shipped."}
		for _, w := range words {
			if err := workflow.Emit(ctx, []byte(w)); err != nil {
				return nil, err
			}
		}
		return json.Marshal(map[string]string{"summary": strings.Join(words, "")})
	}}}}
	// snippet:end
}

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	release := make(chan struct{})
	summarize := summarizeWorkflow(release)

	// snippet:handler
	stream := workflow.NewMemoryStream(workflow.MemoryStreamOptions{})
	c, err := workflow.Use(memory.New(), workflow.Stream(stream, workflow.StreamCloseOnFinish()))
	if err != nil {
		return err
	}
	if err := c.Register(summarize); err != nil {
		return err
	}
	// Build the handler before Start, so it can follow lifecycle events.
	h := httprun.New(c, httprun.Config[user]{
		Auth:      auth,
		Workflows: []string{"summarize"}, // nothing else can be started here
		Prepare:   stampOwner,
		Stream:    stream,
		Mount:     "/api/jobs",
	})
	mux := http.NewServeMux()
	mux.Handle("/api/jobs/", http.StripPrefix("/api/jobs", h))
	if err := c.Start(ctx); err != nil {
		return err
	}
	// snippet:end
	srv := httptest.NewServer(mux)
	defer srv.Close()

	fmt.Println("closed by default:")
	resp, err := call(ctx, srv.URL+"/api/jobs/summarize", "", `{}`)
	if err != nil {
		return err
	}
	fmt.Printf("  no session: %d\n", resp.StatusCode)
	resp, err = call(ctx, srv.URL+"/api/jobs/delete-everything", "s-ann", `{}`)
	if err != nil {
		return err
	}
	fmt.Printf("  a workflow not listed: %d\n", resp.StatusCode)

	// snippet:start
	resp, err = call(ctx, srv.URL+"/api/jobs/summarize", "s-ann", `{"day":"2026-11-20"}`)
	if err != nil {
		return err
	}
	location := resp.Header.Get("Location") // "/api/jobs/runs/<id>"
	// snippet:end
	fmt.Println("start:")
	fmt.Printf("  %d, Location %s\n", resp.StatusCode, strings.Replace(location, runIDIn(location), "<id>", 1))

	ref := workflow.RunRef{RunID: workflow.RunID(runIDIn(location))}
	if err := waitFor(ctx, c, ref, workflow.StatusRunning); err != nil {
		return err
	}
	if err := showStatus(ctx, srv.URL+location, "s-ann", "status while running:"); err != nil {
		return err
	}
	if err := showStatus(ctx, srv.URL+location, "s-bo", "another user:"); err != nil {
		return err
	}

	// snippet:events
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+location+"/events", nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer s-ann")
	events, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer events.Body.Close()
	// snippet:end
	fmt.Println("events:")
	first := true
	err = readEvents(events.Body, func(name, id, data string) {
		if first {
			first = false
			close(release) // the stream is open: let the step answer
		}
		fmt.Printf("  %s\n", describe(name, id, data))
	})
	if err != nil {
		return err
	}
	return showStatus(ctx, srv.URL+location, "s-ann", "status when done:")
}

// call POSTs body as JSON with the session's token.
func call(ctx context.Context, url, session, body string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, strings.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if session != "" {
		req.Header.Set("Authorization", "Bearer "+session)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp, resp.Body.Close()
}

// showStatus GETs a status URL and prints the fields a frontend reads.
func showStatus(ctx context.Context, url, session, title string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+session)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	var st struct {
		Status string          `json:"status"`
		Done   bool            `json:"done"`
		Result json.RawMessage `json:"result"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&st); err != nil {
		return err
	}
	fmt.Println(title)
	if resp.StatusCode != http.StatusOK {
		fmt.Printf("  %d\n", resp.StatusCode)
		return nil
	}
	fmt.Printf("  %d, status %s, done %v", resp.StatusCode, st.Status, st.Done)
	if st.Result != nil {
		fmt.Printf(", result %s", st.Result)
	}
	fmt.Println()
	return nil
}

// readEvents calls fn for every Server-Sent Event until the stream ends.
func readEvents(body io.Reader, fn func(name, id, data string)) error {
	var name, id, data string
	sc := bufio.NewScanner(body)
	for sc.Scan() {
		line := sc.Text()
		switch {
		case line == "":
			if name != "" {
				fn(name, id, data)
			}
			name, id, data = "", "", ""
		case strings.HasPrefix(line, "event: "):
			name = line[len("event: "):]
		case strings.HasPrefix(line, "id: "):
			id = line[len("id: "):]
		case strings.HasPrefix(line, "data: "):
			data = line[len("data: "):]
		}
	}
	return sc.Err()
}

// describe prints an event without its timestamps, so the output is the
// same on every run.
func describe(name, id, data string) string {
	var ev struct {
		Status string          `json:"status"`
		Kind   string          `json:"kind"`
		Text   string          `json:"text"`
		Result json.RawMessage `json:"result"`
	}
	_ = json.Unmarshal([]byte(data), &ev)
	switch name {
	case httprun.EventChunk:
		return fmt.Sprintf("chunk id=%s %q", id, ev.Text)
	case httprun.EventEnd:
		return fmt.Sprintf("end %s %s", ev.Status, ev.Result)
	}
	if ev.Kind != "" {
		return fmt.Sprintf("%s %s (%s)", name, ev.Status, ev.Kind)
	}
	return fmt.Sprintf("%s %s", name, ev.Status)
}

func runIDIn(location string) string {
	return location[strings.LastIndex(location, "/")+1:]
}

// waitFor polls until the run reaches st.
func waitFor(ctx context.Context, c workflow.Client, ref workflow.RunRef, st workflow.Status) error {
	for {
		got, err := c.Status(ctx, ref)
		if err != nil {
			return err
		}
		if got == st {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Millisecond):
		}
	}
}
