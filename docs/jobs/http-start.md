# Start a run over HTTP and follow it live

A browser asks for a summary of today's orders. Generating it takes twenty seconds, longer than a request should hang, and the page wants to show the answer as it is written, word by word. The `httprun` package puts a workflow behind three routes for exactly this: `POST /summarize` answers `202 Accepted` at once with a `Location`, `GET` on that Location answers the status and, once the run succeeded, its result, and `GET <Location>/events` is a Server-Sent Events stream of status changes and of the output the step sends with `workflow.Emit` while it runs.

## Before you start

- Starting a run and reading its status work on every driver. The result needs `workflow.CapResult` (memory, SQL, Redis, Hatchet).
- Live status events come from `workflow.LifecycleNotifier` when the client has it and the handler is built before `Start`; otherwise the events route polls `Status` every `Config.Poll` (one second by default). It always polls as well, since lifecycle events are local to the process that ran the step.
- Streamed output needs the `workflow.Stream` plugin on the client and a `workflow.StreamReader` in `Config.Stream`. `workflow.MemoryStream` keeps chunks in this process: it serves one process that both runs the steps and answers the routes. A fleet needs a shared store (a table, a Redis stream) implementing `StreamSink` and `StreamReader`.
- `httprun` needs only the standard library and the core module.
- The example is [`examples/http-status`](../../examples/http-status). It runs on the memory driver. Run it with `go run ./http-status` from `examples/`.

## Step 1: say who may do what

The handler is closed by default, exactly like the [admin API](../admin/http-api.md): with a nil `Identify` or `Authorize` every request, on every path, answers 403. `Identify` names the caller (401 when it fails, before any routing); `Authorize` gets the typed action (`httprun.ActionStart`, `ActionStatus`, `ActionEvents`) and the resource. For a status or events request the resource carries the run's `RunInfo` when the client can list runs (`CapRunQuery`), so an app can let each user read only the runs they started. `Config.Prepare` is where the app stamps the owner on the job:

<!-- snippet: examples/http-status/main.go#auth -->
```go
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
```

`Authorize` is called even for a run that does not exist (with `Info` nil), so a caller who may not read it gets 403 whether or not it exists.

## Step 2: mount the handler

`Config.Workflows` names the workflows the start route may start; any other name answers 404, so registering a new workflow never exposes it by accident. Build the handler before `Start` to get live lifecycle events. `Config.Mount` makes the `Location` absolute; without it the Location is relative (`runs/<id>`), which resolves under the mount as long as the start path sits directly below it.

<!-- snippet: examples/http-status/main.go#handler -->
```go
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
```

## Step 3: stream output from a step

`workflow.Emit(ctx, chunk)` hands a chunk to the sink the plugin bound to the step. The step's return value is still its output and the run's result; the chunks are a side channel for a live reader. Without the plugin, or outside a step, `Emit` returns an error matching `workflow.ErrUnsupported` rather than dropping the chunk.

<!-- snippet: examples/http-status/main.go#workflow -->
```go
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
```

## Routes

| Route | What it does |
|---|---|
| `POST /{workflow}` | Enqueues the body as the payload and answers 202 with `Location: runs/{id}` and `{"ref", "status_url", "events_url"}`. The body must be `Content-Type: application/json` and valid JSON (or a type in `Config.MediaTypes`), at most `Config.MaxBody` (1 MiB, else 413). An `Idempotency-Key` header becomes the job's idempotency key (needs `CapIdempotencyKey`, else 501). |
| `GET /runs/{id}` | `{"ref", "workflow", "status", "done", "progress", "enqueued_at", "started_at", "finished_at"}` and, once the run succeeded, `result` (or `result_base64` when it is not JSON). `payload` only with `Config.Payloads`, `error` only with `Config.Errors`. |
| `GET /runs/{id}/events` | Server-Sent Events: `status` at the start and on every change, `chunk` for every streamed chunk (its SSE id is the chunk's position), `end` with the final status and the result, then the stream closes. `error` when it cannot go on (the run was deleted). A comment every `Config.Heartbeat` keeps proxies from closing it. |

A run whose enqueue returned only a `ScheduledID` (a delayed start) is addressed with `?scheduled=1`, which the Location carries.

The start returns at once:

<!-- snippet: examples/http-status/main.go#start -->
```go
resp, err = call(ctx, srv.URL+"/api/jobs/summarize", "s-ann", `{"day":"2026-11-20"}`)
if err != nil {
	return err
}
location := resp.Header.Get("Location") // "/api/jobs/runs/<id>"
```

and the page opens the event stream (in a browser, `new EventSource(location + "/events")`, which reconnects by itself and sends `Last-Event-ID`, so it gets only the chunks it missed):

<!-- snippet: examples/http-status/main.go#events -->
```go
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
```

## Try it

```text
closed by default:
  no session: 401
  a workflow not listed: 404
start:
  202, Location /api/jobs/runs/<id>
status while running:
  200, status running, done false
another user:
  403
events:
  status running
  chunk id=1 "Three "
  chunk id=2 "orders "
  chunk id=3 "shipped."
  status succeeded (finished)
  end succeeded {"summary":"Three orders shipped."}
status when done:
  200, status succeeded, done true, result {"summary":"Three orders shipped."}
```

## Pitfalls

- **The result is shown; the payload and the error are not.** Returning the result is what the status route is for, so `Authorize` must decide who reads a run. Payloads and error messages can carry personal data or internal hosts: turn them on with `Config.Payloads` and `Config.Errors` only for callers who may see them.
- **`MemoryStream` is per process.** A reader connected to a process that did not run the step sees no chunks. Use a shared store for a fleet, or route the events request to the worker.
- **Chunks are bounded.** `MemoryStream` keeps at most 1 MiB and 4,096 chunks per run, 64 KiB per chunk and 1,000 runs (`MemoryStreamOptions`); past a run's bound the oldest chunks are dropped, so a late reader sees a gap in the positions. Chunks of a closed run stay readable for a minute, and any run is dropped an hour after its last chunk.
- **A retried step streams again.** Chunks carry their step attempt (`attempt` in the event) and restart at `seq` 1, so a page showing one answer starts over when the attempt changes.
- **Build the handler before `Start`.** After `Start` the client refuses new lifecycle observers; the handler logs it and the events route falls back to polling.
- **Writes need a JSON content type.** It is the CSRF guard: a browser cannot send it cross-site without a CORS preflight, which the handler never answers. The three types a form can send are ignored even if listed in `Config.MediaTypes`.

## Related

- [Results and awaiting a run](results.md): what a run's result is.
- [Progress and heartbeats](progress.md): the progress a status carries.
- [Lifecycle events](../observability/lifecycle.md): the feed the events route follows.
- [Admin JSON API](../admin/http-api.md): the same gate for operators.
- [Push delivery](push-delivery.md): the other HTTP direction, a job delivered to an endpoint.
- Example: [examples/http-status](../../examples/http-status/).
