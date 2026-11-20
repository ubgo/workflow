# http-status

A workflow behind the three routes of the `httprun` package, on the in-memory driver: `POST /api/jobs/summarize` answers 202 with a `Location`, the status route answers the run's status and, once it succeeded, its result, and the events route is a Server-Sent Events stream of status changes and of the words the step sends with `workflow.Emit` while it runs. The handler is closed until `Identify` and `Authorize` are set; here only the user who started a run may read it.

```sh
cd examples
go run ./http-status
```

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

The guide that copies this program is [start a run over HTTP and follow it live](../../docs/jobs/http-start.md). `go test ./http-status` checks every line.
