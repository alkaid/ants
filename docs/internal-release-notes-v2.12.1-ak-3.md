# v2.12.1-ak-3 Internal Release Notes

This version is for the `github.com/alkaid/ants/v2` internal mirror. Delivery
uses a local lightweight tag only. The tag is not pushed, and no GitHub release
or public fork release identity is created.

## Breaking Changes

1. `Options` now includes the PoolWithID fields `RunningTaskTimeout`,
   `MaxEscapedWorkers`, and `MaxEscapedWorkersPerID` in addition to the earlier
   fork fields. Upstream unkeyed `Options` literals are source-incompatible;
   use option functions or keyed literals.
2. A zero `TaskBuffer` now selects `DefaultTaskBuffer=100`, with a physical
   per-ID channel capacity of 200. The previous default was 10 with capacity
   20.
3. Running-task recovery no longer reuses `ExpiryDuration`. A zero
   `RunningTaskTimeout` selects 5 minutes, while a zero PoolWithID
   `ExpiryDuration` selects 30 seconds and applies only to idle owners.
4. `MaxBlockingTasks` and `Waiting()` now cover owner-capacity waits,
   same-ID allocation followers, and existing-ID queue-space waits. Existing-ID
   submissions can now return `ErrPoolOverload` when the shared blocking quota
   is full.

## New Features

1. Running-task escape has global and per-ID hard limits through
   `MaxEscapedWorkers` and `MaxEscapedWorkersPerID`. Capacity-derived defaults
   remain bounded, and explicit positive limits stay fixed across `Tune`.
2. Escape observability now includes generation-aware worker, exit, and budget
   exhaustion events; authoritative snapshots; O(1) worker totals and budget
   status; and a dropped-event counter. Escape state remains continuous across
   `Release` and `Reboot`.
3. `WithDisablePurgeRunning(true)` disables only running-task escape.
   `WithDisablePurge(true)` keeps the fork's combined behavior and disables
   both idle purge and running-task escape.

## Fixes & Improvements

1. `Tune` now updates capacity and wakes waiters under the same lock. Capacity
   reductions retire excess idle owners and converge after in-flight submits
   finish.
2. New-ID queues are allocated outside the pool lock through generation-bound
   reservations. Release and reboot wait for reservations to settle, and stale
   allocators cannot publish into a new generation.
3. `TaskBuffer` is validated before background work starts. Values above
   `MaxTaskBuffer=64*1024` or below zero return
   `ErrInvalidPoolWithIDTaskBuffer`.
4. `ReleaseContext` and `ReleaseTimeout` wait for the current generation's
   admission work, accepted queues, managed owners, and background loop. They
   do not wait for escaped tasks, and a completed managed close prevents new
   escape starts for that generation.
5. PoolWithID no longer starts an unused per-instance ticker. With purge
   disabled it skips expiry-index maintenance, and enabled purge work runs in
   bounded batches that release locks between batches.
6. PoolWithID throughput and lock-diagnostic benchmarks are separated and no
   longer add per-operation timing or shared sampling contention to the hot
   path.

## Performance Validation

The comparison uses base `199d705ce7866cc55e46f387d9ab0919cf32ec5d` and
Phase 5 commit `77519b654a47b8ee9cc666a0e188fd627c87f41b`. Each cell
below has 10 samples with the same comparison benchmark on Linux/amd64, Go
1.26.5, and an AMD Ryzen 5 5600X host with 7 online vCPUs. Values are median
base to candidate `ns/op`; lower is better.

| Active IDs | GOMAXPROCS=1 | GOMAXPROCS=4 | GOMAXPROCS=7 |
|---|---:|---:|---:|
| 1 | 308.5 to 253.9 (-17.70%) | 409.2 to 348.6 (-14.83%) | 435.1 to 368.5 (-15.32%) |
| 64 | 359.2 to 269.9 (-24.89%) | 285.2 to 188.5 (-33.88%) | 292.2 to 191.7 (-34.39%) |
| 1024 | 351.0 to 297.3 (-15.30%) | 254.2 to 212.0 (-16.58%) | 258.5 to 217.2 (-15.96%) |

All nine comparisons are statistically significant (`p=0.000`, `n=10`). The
geomean improves from 322.7 to 253.7 ns/op (-21.37%), while every case remains
at 0 B/op and 0 allocs/op. Registry and expiry sharding remain deferred: the
unsharded candidate already improves every available CPU/ID combination, and
this host cannot provide valid 8-CPU evidence for a larger concurrency
restructure.

## Deprecations

1. `MinTaskBuffer=10` remains exported for source compatibility but no longer
   represents the default. Use `DefaultTaskBuffer` or an explicit value.

## Compatibility And Verification Boundary

The module keeps the `/v2` path and supports Go 1.19 and later. The optional
registry and expiry sharding work remains deferred. Benchmark comparison for
this release uses 1, 4, and 7 CPUs because the validation host has 7 online
vCPUs; this note makes no 8-CPU benchmark or acceptance claim.

See [PoolWithID migration guide](pool-with-id-migration.md) for configuration,
memory sizing, observability, shutdown, and rollback guidance.
