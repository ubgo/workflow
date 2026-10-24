# flow-children

Child runs started from inside a step, on the in-memory driver. `Spawn` starts two notification runs and moves on; `AwaitChildren` starts one resize run per image, puts the parent step back while they run, and returns their results in job order. A child that fails fails the parent.

```sh
cd examples
go run ./flow-children
```

```text
spawn:
  parent finished, children started: 2
  child of welcome, parent run matches: true
  child of welcome, parent run matches: true
  notifications sent: 2
await children:
  results in job order: ["a@2x.png","b@2x.png","c@2x.png"]
  parent step attempts spent: 1
  a failed child fails the parent: failed
  parent error names the child: true
```

## The code

Start children and do not wait for them:

<!-- snippet: examples/flow-children/main.go#spawn -->
```go
refs, err := workflow.Spawn(ctx, c, "notify", []workflow.Job{
	{Workflow: "notify", Payload: []byte(`"email"`)},
	{Workflow: "notify", Payload: []byte(`"sms"`)},
})
if err != nil {
	return nil, err
}
```

Every child carries its parent's run ID and step name as labels:

<!-- snippet: examples/flow-children/main.go#children -->
```go
page, err := c.(workflow.RunQuerier).Runs(ctx, workflow.RunQuery{Workflow: "notify"})
if err != nil {
	return err
}
for _, r := range page.Runs {
	fmt.Fprintf(out, "  child of %s, parent run matches: %v\n",
		r.Labels[workflow.LabelParentStep], r.Labels[workflow.LabelParentRun] == ref.RunID.String())
}
```

Start children and join their results:

<!-- snippet: examples/flow-children/main.go#await -->
```go
jobs := make([]workflow.Job, len(images))
for i, img := range images {
	jobs[i] = workflow.Job{Workflow: "resize", Payload: []byte(`"` + img + `"`)}
}
outs, err := workflow.AwaitChildren(ctx, c, "resize", jobs, workflow.ChildOptions{Poll: 5 * time.Millisecond})
if err != nil {
	return nil, err // a Snooze while children run; a failed child fails this step
}
names := make([]string, len(outs))
for i, o := range outs {
	if err := json.Unmarshal(o, &names[i]); err != nil {
		return nil, workflow.Permanent(err)
	}
}
return json.Marshal(names)
```

A child that fails ends the parent with a `*workflow.RunFailedError`:

<!-- snippet: examples/flow-children/main.go#failed -->
```go
ref, err = c.Enqueue(ctx, workflow.Job{Workflow: "album", Payload: []byte(`["a.png","bad.png"]`)})
if err != nil {
	return err
}
_, err = workflow.AwaitResult(ctx, c, ref, await)
var failed *workflow.RunFailedError
if errors.As(err, &failed) {
	fmt.Fprintf(out, "  a failed child fails the parent: %s\n", failed.Status)
	fmt.Fprintf(out, "  parent error names the child: %v\n", strings.Contains(failed.Err, `await child "resize" #1`))
}
```

The parent spends one attempt however long the children take, because each look at an unfinished child is a snooze, not a failure.

The guide that copies this program is [child runs](../../docs/workflows/children.md). `go test ./flow-children` checks every line.
