# driver-conformance

The tests a driver author copies, run here on the memory driver: a share of the conformance suite (`workflowtest.Run` with `Harness.Groups`) and every core-helper case (`RunChildFlows`, `RunResetFlows`, `RunRedoFlows`, `RunForkFlows`, `RunSignalFlows`, `RunPromiseStore`, `RunReplayFlows`, `RunObjectFlows`, `RunMachineFlows`, `RunSlotFlows`) with the harness each one needs. Replace `memory.New()` with your client and the same tests hold your driver to the contract.

The program itself prints the plan that splits the suite into shares, after checking it against `workflowtest.GroupNames()`: a share that names a group the suite does not have, or a group named twice, is refused. The last share runs every group the others do not name, so a group added to the suite later still runs.

```sh
cd examples
go run ./driver-conformance
```

```text
share 1: lifecycle run retry timeout onfailure
share 2: cron misfire schedule overlap trigger
share 3: every other group (57 of 67)
```

```sh
go test ./driver-conformance -run 'TestConformance|TestCoreHelperFlows' -v | grep -E '^(--- |    --- |ok)'
```

```text
--- PASS: TestConformance (0.43s)
    --- PASS: TestConformance/lifecycle (0.00s)
    --- PASS: TestConformance/run (0.11s)
    --- PASS: TestConformance/retry (0.21s)
    --- PASS: TestConformance/timeout (0.01s)
    --- PASS: TestConformance/onfailure (0.11s)
--- PASS: TestCoreHelperFlows (2.81s)
    --- PASS: TestCoreHelperFlows/child (0.07s)
    --- PASS: TestCoreHelperFlows/reset (0.01s)
    --- PASS: TestCoreHelperFlows/redo (0.17s)
    --- PASS: TestCoreHelperFlows/fork (0.07s)
    --- PASS: TestCoreHelperFlows/signal (0.13s)
    --- PASS: TestCoreHelperFlows/promisestore (0.00s)
    --- PASS: TestCoreHelperFlows/replay (0.62s)
    --- PASS: TestCoreHelperFlows/object (0.01s)
    --- PASS: TestCoreHelperFlows/machine (1.60s)
    --- PASS: TestCoreHelperFlows/slot (0.13s)
ok  	github.com/ubgo/workflow/examples/driver-conformance	3.249s
```

The `grep` keeps the top two levels; `-v` alone prints every case. Only the first share runs here, to keep the examples' test run short; a driver runs every share, each from a small test package of its own. The counts in the last line of the plan follow the suite.

The guide that copies this program is [Write a driver](../../docs/extending/writing-a-driver.md#run-the-suite-in-shares). `go test ./driver-conformance` checks the plan and runs the tests.
