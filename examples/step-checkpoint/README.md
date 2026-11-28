# step-checkpoint

A step that pages through an upstream API saves the next page in a typed checkpoint (`DefineCheckpoint`) after every page; the upstream fails once on page 3, and the retry resumes at page 3 instead of fetching pages 1 and 2 again. The client caps checkpoints at 1 KiB with `memory.WithMaxCheckpoint`.

```sh
cd examples
go run ./step-checkpoint
```

```text
attempt 1 starts at page 1
fetched page 1
fetched page 2
attempt 2 starts at page 3
fetched page 3
fetched page 4
the run: succeeded
```

The guide is [Checkpoints](../../docs/jobs/checkpoints.md).
