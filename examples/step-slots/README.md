# step-slots

Heavy steps that hold several worker slots, on the in-memory driver with a pool of four (`memory.WithWorkers(4)`): a render step with `Slots: 3` runs beside at most one ordinary webhook attempt, a step that needs more slots than the whole pool is refused at `Register` with `ErrInvalid`, and `workflow.SlotPool`, the rule every driver with `CapSlotCost` shares, is driven by hand.

```sh
cd examples
go run ./step-slots
```

```text
memory declares CapSlotCost: true
webhooks that ran beside the render: 1 of 3
most slots held at once: 4 of 4
register a 5-slot step on a 4-slot pool: true
  workflow: invalid spec: workflow "transcode-8k" step "transcode" needs 5 worker slots and this client has 4
pool: 3 of 4 in use after one render
pool: a second render waits until its context ends: true
pool: 0 in use after Release
pool: asking for more than the pool: true
```

The handlers report the slots they hold to a small meter, since the driver's pool is not exported; the render holds its slots until one webhook has started beside it, so the counts are exact on every run.

The guide that copies this program is [heavy steps and worker slots](../../docs/jobs/slots.md).
