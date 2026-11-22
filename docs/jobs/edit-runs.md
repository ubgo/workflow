# Edit a scheduled run

A reminder is scheduled for tomorrow with the wrong address. Cancelling it and enqueuing a new one loses its RunRef, its place in the admin and its idempotency key; editing it keeps them. [`workflow.RunEditor`](ds:block?id=runeditor-ss73q99q) changes a delayed run's payload, priority and start before it runs, and the admin module offers the same edit to operators in Go and over HTTP, with an `Authorizer` that can judge each field.

Use it when a run is right in kind but wrong in detail: a typo in an address, a priority that should jump the queue, a start that should move. When the run must change workflow, labels, queue or idempotency key ([`JobEdit`](ds:block?id=jobedit-aut4bg45) has no field for them), cancel it and enqueue a new job instead.

## Before you start

- [`RunEditor`](ds:block?id=runeditor-ss73q99q) is an optional interface with no capability bit, like `RunRescheduler`. The [memory](ds:block?id=memory-client-editrun-3afh5vbv), [SQL](ds:block?id=sql-client-editrun-fwr9u4yr) and [Redis](ds:block?id=redis-client-editrun-fwewy3st) drivers have it; Hatchet does not (its scheduled runs cannot be edited). Every wrapper in the library (`workflow.Use` and `hybrid` through [`workflow.Forward`](ds:block?id=forward-editrun-9nwm59f3), [`failover`](ds:block?id=failover-client-editrun-ybkr9r89), [`regional`](ds:block?id=regional-client-editrun-d4b8fp5g), [`cutover`](ds:block?id=cutover-client-editrun-ydkzez9d)) has the method and forwards it, so a type assertion succeeds on any wrapper; [`workflow.Supports(c, workflow.OpEditRun)`](ds:block?id=supports-4xyjjwvz) asks the client underneath, and a wrapper over a client without it returns an `UnsupportedError` from the call. See [Hatchet limitations](../drivers/hatchet.md#limitations).
- Only a run that has not started can be edited: one enqueued with a future `Job.RunAt` (needs [`workflow.CapDelayedStart`](ds:block?id=capdelayedstart-qbxsx6hb)), in `StatusScheduled` ([memory `EditRun`](ds:block?id=memory-client-editrun-3afh5vbv)).
- The example is [`examples/edit-run`](../../examples/edit-run), on the in-memory driver. Run it with `go run ./edit-run` from `examples/`. The admin part needs the `github.com/ubgo/workflow/admin` module.

## Step 1 — Edit the run in Go

Assert the interface on the client you hold, and pass a [`workflow.JobEdit`](ds:block?id=jobedit-aut4bg45) with only the fields to change. Here a reminder due tomorrow gets a new address, a higher priority and a start of now:

<!-- snippet: examples/edit-run/main.go#edit -->
```go
editor, ok := workflow.Client(c).(workflow.RunEditor)
if !ok {
	return errors.New("this driver cannot edit runs")
}
payload := []byte(`{"to":"new@example.com"}`)
priority := 10 // above the default 0: it runs ahead of waiting work
now := time.Now()
ref, err = editor.EditRun(ctx, ref, workflow.JobEdit{Payload: &payload, Priority: &priority, RunAt: &now})
if err != nil {
	return err // ErrWrongState once the run has started
}
```

A [`JobEdit`](ds:block?id=jobedit-aut4bg45) field left nil is not changed: `Payload` replaces the payload (the driver's payload limit applies, as at `Enqueue`), `Priority` replaces `Job.Priority`, `RunAt` moves the start as `Reschedule` does (a past time means now). The run keeps its RunRef, labels, idempotency key and queue. A concurrency or rate key computed from the payload is computed again from the new one ([memory](ds:block?id=memory-client-editrun-3afh5vbv), [SQL](ds:block?id=sql-client-editrun-fwr9u4yr), [Redis](ds:block?id=redis-client-editrun-fwewy3st)). `EditRun` returns the run's ref after the edit.

A `RunAt` of now or in the past admits the run before `EditRun` returns, so the returned ref carries its `RunID` as well as its `ScheduledID` on every driver ([memory](ds:block?id=memory-client-editrun-3afh5vbv), [SQL](ds:block?id=sql-client-editrun-fwr9u4yr), Redis through [`Reschedule`](ds:block?id=redis-client-reschedule-j5u3fw5f)). Keep the returned ref, not the one you passed.

[`workflow.CheckJobEdit`](ds:block?id=checkjobedit-xnynunz4) refuses an edit that changes nothing with `ErrInvalid`; every driver calls it first, and so can a form that builds an edit from operator input.

<a id="from-the-admin"></a>

## Step 2 — Edit from the admin in Go

`admin.Admin` wraps a client for operator tools. [`Admin.EditRun`](ds:block?id=admin-admin-editrun-yxk3t4zu) is the same operation ([`admin.OpEditRun`](ds:block?id=admin-opeditrun-rccmgvd7) in its list of operations): it checks the client can edit ([`Admin.check`](ds:block?id=admin-admin-check-re2vjymn)), refuses an empty ref or an empty edit with `ErrInvalid`, then calls the driver:

<!-- snippet: examples/edit-run/main.go#admin-go -->
```go
ops := admin.New(c)
urgent := 20 // above the first reminder's 10
second, err = ops.EditRun(ctx, second, workflow.JobEdit{Priority: &urgent})
if err != nil {
	return err // UnsupportedError on a client without RunEditor
}
```

## Step 3 — Edit over HTTP, and decide who may change what

The admin handler serves `POST /runs/{id}/edit` with a body of `{"payload": <JSON>, "priority": n, "run_at": "RFC 3339"}`, each field optional, behind the action [`admin.ActionEditRun`](ds:block?id=admin-actioneditrun-rzvgr253). The handler ([`editRun`](ds:block?id=admin-handler-editrun-bzau6dfy)) hands the parsed edit to your `Authorizer` in [`Resource.Edit`](ds:block?id=admin-resource-6fktgbm8) before it runs, so the decision can depend on the fields. This one lets any named operator move a run or change its priority, and only a lead change its payload:

<!-- snippet: examples/edit-run/main.go#admin-auth -->
```go
// auth lets every named operator edit a scheduled run's time and priority,
// and only a lead change its payload: the Authorizer sees the edit itself
// in Resource.Edit, so it can judge each field.
var auth = admin.Auth[string]{
	Identify: func(r *http.Request) (string, error) {
		if who := r.Header.Get(operatorHeader); who != "" {
			return who, nil
		}
		return "", errors.New("no operator")
	},
	Authorize: func(_ context.Context, req admin.Request[string]) error {
		if req.Action == admin.ActionEditRun && req.Resource.Edit.Payload != nil && req.Who != roleLead {
			return fmt.Errorf("%s may move this run but not change its payload", req.Who)
		}
		return nil
	},
}
```

A delayed run that has not started has only a `ScheduledID`, so its route needs `?scheduled=1` (as for every run route); once it has a `RunID`, use that. The calls below move the run (200), try to change its payload as support (403, the `Authorizer` refused), change it as a lead (200) and try to edit the first reminder, which already ran (409):

<!-- snippet: examples/edit-run/main.go#admin-http -->
```go
// edit posts one edit to a run as operator who and returns the status.
edit := func(who string, ref workflow.RunRef, body string) (int, error) {
	path := "/runs/" + string(ref.RunID) + "/edit"
	if ref.RunID.IsZero() { // a delayed run not yet started has only a ScheduledID
		path = "/runs/" + string(ref.ScheduledID) + "/edit?scheduled=1"
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL+path, strings.NewReader(body))
	if err != nil {
		return 0, err
	}
	req.Header.Set(operatorHeader, who)
	req.Header.Set("Content-Type", "application/json")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer res.Body.Close()
	_, err = io.Copy(io.Discard, res.Body)
	return res.StatusCode, err
}
in48h := tomorrow.Add(24 * time.Hour).UTC().Format(time.RFC3339) // one more day out
moved, err := edit("support", second, `{"run_at":"`+in48h+`"}`)
if err != nil {
	return err
}
readdressed, err := edit("support", second, `{"payload":{"to":"bob@example.com"}}`)
if err != nil {
	return err
}
byLead, err := edit(roleLead, second, `{"payload":{"to":"bob@example.com"}}`)
if err != nil {
	return err
}
finished, err := edit(roleLead, ref, `{"priority":1}`) // the first reminder already ran
if err != nil {
	return err
}
```

The handler maps the errors the way every admin route does ([`fail`](ds:block?id=admin-handler-fail-nf9d8sqp)): `ErrInvalid` (an empty edit, a bad body) is 400, `ErrNotFound` 404, `ErrWrongState` 409, an `UnsupportedError` 501. The payload travels as JSON in the body ([`editRun`](ds:block?id=admin-handler-editrun-bzau6dfy)), so a payload that is not JSON cannot be set over HTTP; use `Admin.EditRun` for that.

## Try it

```sh
cd examples
go run ./edit-run
```

```text
sent to {"to":"new@example.com"}
editing it after it ran: true
admin edit: priority 20 status scheduled
POST /runs/{id}/edit: support moves it 200 support readdresses it 403 lead readdresses it 200 a finished run 409
after the HTTP edits: due in 24h0m0s more, priority 20 same ref true
```

The first line is the reminder that ran with its edited address; the second, the refusal of an edit after it ran. The rest is the second reminder: its priority raised by `Admin.EditRun`, then the four HTTP calls with their status codes, and finally its state, due a day later than first planned, still at priority 20, with the same ref.

## Pitfalls

- **Only a run in `StatusScheduled` can be edited.** A run that was admitted may already be running on another node, so any other status returns `ErrWrongState`, and so does an edit that races the run's due time and loses ([SQL](ds:block?id=sql-client-editrun-fwr9u4yr), [Redis](ds:block?id=redis-client-editrun-fwewy3st)). An unknown ref returns `ErrNotFound`, an edit that changes nothing `ErrInvalid` ([`CheckJobEdit`](ds:block?id=checkjobedit-xnynunz4)).
- **A type assertion is not proof.** Wrappers have `EditRun` whether or not their inner client does ([`Forward.EditRun`](ds:block?id=forward-editrun-9nwm59f3)). Ask [`workflow.Supports`](ds:block?id=supports-4xyjjwvz) with [`workflow.OpEditRun`](ds:block?id=opeditrun-9u2hhcey) before offering an edit button, as the admin does.
- **Moving the start to now admits the run.** It cannot be edited again. The ref `EditRun` returns carries the `RunID` ([memory](ds:block?id=memory-client-editrun-3afh5vbv), [SQL](ds:block?id=sql-client-editrun-fwr9u4yr), [Redis](ds:block?id=redis-client-editrun-fwewy3st)), so later admin calls address the run by it, without `?scheduled=1`; the ref you passed, with only the `ScheduledID`, still finds the run too.
- **Labels, queue and idempotency key cannot be edited** ([`JobEdit`](ds:block?id=jobedit-aut4bg45)). Cancel and enqueue again for that.
- **The `Authorizer` decides on the edit, not on the result.** It sees `Resource.Edit` before the driver runs ([`editRun`](ds:block?id=admin-handler-editrun-bzau6dfy)); an edit it allows can still fail with `ErrWrongState` if the run started meanwhile.

## Related

- [Manage runs](manage-runs.md#reschedule-a-delayed-run): `Reschedule`, which moves only the start.
- [Run a job later](delays.md): `Job.RunAt` and delayed runs.
- [Admin operations](../admin/operations.md) and [HTTP API](../admin/http-api.md): the other run routes and their actions.
- [Failover](../reliability/failover.md): editing through a failover client.
