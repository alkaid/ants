# v2.12.1-ak-4 Internal Release Notes

This version of `github.com/alkaid/ants/v2` fixes concurrency issues in PoolWithID
and ordinary pools and is distributed as Git tag `v2.12.1-ak-4`.

## Fixes

1. Overlapping submissions can no longer leave a retired entry in the expiry
   index, which could keep purge busy and prevent later idle cleanup or
   running-task escape after purge, `Tune`, or `Release`.
2. Escaped workers continue to count toward escape budgets and worker totals
   until their panic handler or logger finishes, preventing blocked callbacks
   from bypassing the configured limits.
3. `Pool`, `PoolWithFunc`, and `PoolWithFuncGeneric` synchronize worker exit,
   release, and reboot so that restarting a pool cannot race with its previous
   workers. Submissions waiting to acquire the worker lock are rejected if the
   pool has closed, and release waits stay attached to the generation they closed.

The public API, default configuration, module path, and minimum Go version
(1.19) are unchanged from `v2.12.1-ak-3`. See the
[PoolWithID migration guide](pool-with-id-migration.md) when upgrading from
earlier fork versions or upstream ants.
