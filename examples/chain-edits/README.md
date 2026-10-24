# chain-edits

A chain of jobs on the in-memory driver (`Job.Chain`: each job runs after the one before it succeeds), and a running step changing what comes after it with `workflow.PrependToChain` and `workflow.AppendToChain`. It also shows that a retried step adds its jobs once, that a failed run ends the chain, and that the Hatchet driver refuses a chain.

```sh
cd examples
go run ./chain-edits
```

```text
chain: import-file(a.csv) -> notify(imported)
edited while running (scan tried 2 times): import-files(inbox) -> import-files(inbox) -> import-file(b.csv) -> notify(imported) -> reindex(all)
a failed run ends the chain: charge(o-1) (failed)
AppendToChain outside a step: true, missing Chain
on hatchet: ErrUnsupported true (workflow: Job.Chain unsupported by this driver (missing Chain): job for workflow import-file)
```

The `scan` step prepends `import-file(b.csv)` and appends `reindex(all)` on both of its attempts. The first attempt fails, so its edits are dropped; the second succeeds and its edits apply, once. The prepended job runs before the chain's own `notify`, the appended one after it. The `charge` run fails, so its chained `notify` never runs. Hatchet fixes a run's shape when the workflow registers, so it does not declare `CapChain`: a job with a chain is refused, and a step there starts child runs instead.

The guide that copies this program is [chains](../../docs/workflows/chains.md). `go test ./chain-edits` checks every line.
