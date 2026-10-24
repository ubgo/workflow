# functions

A durable closure: a Go function registered under a name when the process starts and enqueued by that name with a JSON argument. The program retries one on the in-memory driver, runs its catch function when a run fails for good, shows the refusal of a name nobody registered, then enqueues a delayed run on the SQL driver over a SQLite file, stops that client, and lets a second client on the same file (standing in for a restarted process) register the same name and run it.

```sh
cd examples
go run ./functions
```

```text
send-welcome: succeeded after 2 attempts, step "func"
bad address: failed, catch ran for "eve@example.invalid"
unregistered name: ErrNotFound true
first process stopped with the run waiting
second process ran send-welcome: succeeded
```

`workflow.NewFunc` builds a `workflow.Func`, a one-step workflow underneath, so every driver runs it with no extra capability. `workflow.RegisterFunc` registers it, `Func.Enqueue` and `Func.Job` enqueue it with a typed argument, and `workflow.FuncJob` builds the job from the name alone. `FuncRetry`, `FuncTimeout`, `FuncQueue` and `FuncCatch` set the definition's retry policy, attempt timeout, queue and failure handler.

The guide that copies this program is [functions](../../docs/jobs/functions.md). `go test ./functions` checks every line.
