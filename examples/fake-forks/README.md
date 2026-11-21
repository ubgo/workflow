# fake-forks

Test the app code behind a support console with `workflowtest.NewFake()` and no worker. The app forks a finished order run from a later step, resets a failed one in place, wakes only the approval steps waiting now (`SignalWaiting`), settles refund promises, calls a virtual object and dispatches a batch. The Fake records each call and refuses what a driver refuses; the program reads the records back with `Resets`, `Forks`, `SignalsWaiting`, `Promises`, `ObjectCalls` and `Batches`, and `main_test.go` checks the same calls with `AssertReset`, `AssertForked`, `AssertSignalledWaiting`, `AssertPromiseResolved`, `AssertPromiseRejected`, `AssertPromisePending`, `AssertObjectCalled`, `AssertObjectNotCalled` and `AssertBatched`.

```sh
cd examples
go run ./fake-forks
```

```text
reset fake-run-1 from pack (succeeded run: false)
fork fake-run-2 of fake-run-1 from ship by=support redo=true
signal-waiting manager-online/refunds (Signal calls: 0)
promise refund-7 settled=true value="approved" err=<nil>
promise refund-8 settled=true value= err=workflow: promise "refund-8" rejected: used item
promise refund-9 settled=false value= err=<nil>
object cart key=ada method=clear
batch of 2 reship jobs, first order-7
```

The reset kept the run's ID. The fork is a new run with the app's `by` label and `workflow.LabelRedo`, because the run it came from had succeeded. `SignalWaiting` events are kept apart from `Signal` ones. A settled promise also sent its `workflow.PromiseEvent` signal through the Fake, which `AssertPromiseResolved` checks.

The guide that copies this program is [Test app code with the Fake](../../docs/testing/fake.md#forks-and-batches). `go test ./fake-forks` checks every line and runs the assertions.
