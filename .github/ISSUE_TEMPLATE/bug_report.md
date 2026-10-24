---
name: Bug report
about: A job, schedule or workflow did the wrong thing on a driver
title: "[bug] "
labels: bug
---

**What happened**

A clear description of the bug, and what you expected instead.

**The workflow**

The Go that registers and enqueues it, reduced as far as it still reproduces:

```go
c := memory.New() // or sqlstore.New(db, dialect), hatchet.New(...)
err := c.Register(workflow.Workflow{
	Name:  "...",
	Steps: []workflow.Step{...},
})
ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "...", Payload: ...})
```

**Driver and backend**

- Driver: memory, sqlstore (SQLite, Postgres or MySQL, with version), or Hatchet (engine version)
- Commit of `github.com/ubgo/workflow` and of any module used (`hatchet`, `otel`, `sqlstore`)
- Go version, from `go version`
- OS and architecture

**Does it reproduce on the memory driver?**

If it happens on one driver only, say which; that usually locates the problem faster than the description does.

**Does `task check` pass on your clone?**

If it fails, paste the failing output.
