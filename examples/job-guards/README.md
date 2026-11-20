# job-guards

The guards that keep a job from running when it should not, on the in-memory driver: an expiry for a one-time code that must not be mailed late, a sweep that cancels runs no worker took, a bounded queue and a per-tenant quota, quiet hours in the recipient's zone, payload validation at enqueue, an explicit retry ladder, an invariant and a precondition that fail a step at once, a pool of two GPU slots, a critical section inside one run, and a webhook endpoint that switches itself off after three failures.

```sh
cd examples
go run ./job-guards
```

```text
expiry:
  late run: discarded, code sent 0 time(s)
sweep:
  checked 2 waiting run(s), cancelled 2
bounds:
  acme: enqueued
  acme: enqueued
  acme: refused, quota exceeded
  globex: enqueued
  globex: refused, queue full
quiet hours:
  Asia/Tokyo: starts Tue 08:00 JST
  Europe/Berlin: starts now
validation:
  invalid: true workflow: invalid spec: payload of workflow "charge": order o-7: amount 0 is not positive
  valid order enqueued
ladder:
  4 retries after 5s, 5m0s, 30m0s, 2h0m0s
conditions:
  qty 4: stock left 6
  qty 0: failed after 1 attempt(s), constraint violated: true
  qty 12: failed after 1 attempt(s), constraint violated: true
pool:
  6 renders succeeded, at most 2 at once
critical section:
  transfer succeeded, at most 1 step(s) in the section at once
auto-disable:
  delivery 1: failed
  delivery 2: failed
  disabled https://hooks.example.com/a - tell its owner
  delivery 3: failed
  delivery 4: discarded
  delivery 5: discarded
  endpoint called 3 time(s)
  disabled after Enable: false
```

The guides that copy this program are [job guards](../../docs/jobs/guards.md) and [pools and critical sections](../../docs/reliability/pools.md). `go test ./job-guards` checks every line.
