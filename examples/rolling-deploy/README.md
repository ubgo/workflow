# rolling-deploy

Workflow versions during a rolling deploy. Old and new processes share one SQLite file while the new code adds a fraud check to a checkout workflow (`Version: 2`). The old worker finishes the run its web node started and leaves the run the new web node started queued, because it would run it with the old steps. When the old worker is replaced, the new worker runs the version 2 run and drains the version 1 run the old web node enqueued meanwhile, branching on `StepInfo.Version` where the two versions differ.

```sh
cd examples
go run ./rolling-deploy
```

```text
old worker up; old web node enqueues one run
new web node enqueues one run; the old worker leaves it alone
  the v2 run on a fleet of v1 workers: queued
new worker up: runs the v2 run and drains the v1 run
run versions: [1 1 2]
journal (sorted; the two runs interleave):
  new: charge (run v1)
  new: charge (run v2)
  new: email with the fraud score (run v2)
  new: fraud check (run v2)
  new: no fraud check for a run the old code started (run v1)
  new: plain email for a run the old code started (run v1)
  old: charge (run v1)
  old: email (run v1)
```

The guide is [Deploy › Deploy a new version while old workers run](../../docs/operations/deploy.md#deploy-a-new-version-while-old-workers-run).
