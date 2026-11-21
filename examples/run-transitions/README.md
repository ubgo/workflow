# run-transitions

The run state machine as data: `workflow.StatusRetrying.TransitionsFrom()` lists the moves out of one status with what causes each and the capabilities it needs, `Status.CanTransitionTo` asks whether a move exists at all, `workflow.Transitions()` filtered by a driver's capabilities counts the moves that driver can report, and a small guard checks a status change the way a driver of your own would before storing it.

```sh
cd examples
go run ./run-transitions
```

```text
retrying -> running: the backoff elapsed and a worker claimed the step (needs RunStates)
retrying -> queued: the backoff elapsed (needs RunStates)
retrying -> waiting: parallel steps changed which status describes the run (needs RunStates)
retrying -> cancelled: Cancel or CancelAll (needs Cancel)
retrying -> failed: the run went past Workflow.RunTimeout (needs RunTimeout)
retrying -> failed: ForceFail (needs ForceRun)
retrying -> succeeded: ForceComplete (needs ForceRun)
failed -> queued allowed: true
discarded -> queued allowed: false
scheduled -> succeeded allowed: true
succeeded -> failed allowed: false
the machine has 40 moves; memory reports 40, without CapRunStates 32
memory, running -> retrying: <nil>
no CapRunStates, running -> retrying: illegal status change: running to retrying needs capabilities this driver lacks
memory, cancelled -> queued: illegal status change: no move from cancelled to queued
```

The guide that copies this program is [run states](../../docs/jobs/run-states.md#the-state-machine).
