package ants

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const poolWithIDP2StructuralEntries = 100_000

func newPoolWithIDForP2StructureTest(t *testing.T) *PoolWithID {
	t.Helper()
	p, err := NewPoolWithID(
		1,
		WithExpiryDuration(time.Hour),
		WithLogger(poolWithIDDiscardLogger{}),
	)
	if err != nil {
		t.Fatalf("NewPoolWithID: %v", err)
	}
	t.Cleanup(func() {
		p.lock.Lock()
		purgeCancel := p.purgeCancel
		tickCancel := p.tickCancel
		purgeFinished := p.purgeFinished
		tickFinished := p.tickFinished
		p.purgeCancel = nil
		p.tickCancel = nil
		p.lock.Unlock()
		if purgeCancel != nil {
			purgeCancel()
		}
		if tickCancel != nil {
			tickCancel()
		}
		select {
		case <-purgeFinished:
		case <-time.After(poolWithIDTestTimeout):
			t.Error("timed out stopping purge loop")
		}
		select {
		case <-tickFinished:
		case <-time.After(poolWithIDTestTimeout):
			t.Error("timed out stopping clock loop")
		}
	})
	return p
}

func poolWithIDP2AddSyntheticEntry(
	p *PoolWithID,
	id int,
	kind workerIDEntryListKind,
	timestamp time.Time,
) *workerIDEntry {
	entry := newWorkerIDEntry(p.registry, id, 0, p.generation.Load(), timestamp)
	entry.owner = newWorkerWithID(p, entry)
	if kind == workerIDEntryListRunning {
		entry.taskStartedAt = timestamp
	}
	p.registry.items[id] = entry
	if kind == workerIDEntryListIdle {
		p.registry.appendIdle(entry)
	} else {
		p.registry.appendRunning(entry)
	}
	return entry
}

type poolWithIDP2CountingLock struct {
	sync.Locker
	locks atomic.Int64
}

func (l *poolWithIDP2CountingLock) Lock() {
	l.Locker.Lock()
	l.locks.Add(1)
}

func poolWithIDP2StopPurge(t *testing.T, p *PoolWithID) {
	t.Helper()
	p.lock.Lock()
	cancel := p.purgeCancel
	finished := p.purgeFinished
	p.purgeCancel = nil
	p.lock.Unlock()
	if cancel != nil {
		cancel()
	}
	poolWithIDReceive(t, finished)
}

func TestPoolWithIDEntryRegistryRemainsStableAcrossReboot(t *testing.T) {
	for _, test := range []struct {
		name   string
		goexit bool
	}{
		{name: "return"},
		{name: "goexit", goexit: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			const id = 71
			p := poolWithIDObserveNewPool(t, 1)
			release := make(chan struct{})
			started := make(chan struct{})
			returned := make(chan struct{})
			replacementFinished := make(chan struct{})
			t.Cleanup(func() { poolWithIDObserveClose(release) })

			if err := p.Submit(id, func() {
				defer close(returned)
				close(started)
				<-release
				if test.goexit {
					runtime.Goexit()
				}
			}); err != nil {
				t.Fatalf("submit escaped task: %v", err)
			}
			poolWithIDReceive(t, started)
			if err := p.Submit(id, func() { close(replacementFinished) }); err != nil {
				t.Fatalf("submit replacement task: %v", err)
			}
			entry, _, startedAt := poolWithIDObserveEntryState(t, p, id)
			oldRegistry := entry.registry
			p.purgeExpired(startedAt.Add(p.options.RunningTaskTimeout))
			poolWithIDReceive(t, replacementFinished)
			poolWithIDReceive(t, p.EscapeEvents())

			poolWithIDObserveRelease(t, p)
			p.Reboot()
			poolWithIDP2StopPurge(t, p)
			newRegistry := p.registry
			if oldRegistry == newRegistry {
				t.Fatal("Reboot reused the previous registry")
			}

			oldLock := &poolWithIDP2CountingLock{Locker: oldRegistry.expiryMu}
			newLock := &poolWithIDP2CountingLock{Locker: newRegistry.expiryMu}
			oldRegistry.expiryMu = oldLock
			newRegistry.expiryMu = newLock

			close(release)
			poolWithIDReceive(t, returned)
			poolWithIDReceive(t, p.EscapeEvents())
			if got := oldLock.locks.Load(); got == 0 {
				t.Fatal("late escaped owner did not lock its original registry")
			}
			if got := newLock.locks.Load(); got != 0 {
				t.Fatalf("late escaped owner locked the Reboot registry %d times", got)
			}
		})
	}
}

func poolWithIDP2AssertList(
	t *testing.T,
	list workerIDEntryList,
	kind workerIDEntryListKind,
	want int,
) {
	t.Helper()
	count := 0
	var previous *workerIDEntry
	for entry := list.head; entry != nil; entry = entry.expiryNext {
		if entry.expiryList != kind || entry.expiryPrev != previous {
			t.Fatalf("invalid list link for ID %d", entry.id)
		}
		previous = entry
		count++
		if count > want {
			t.Fatal("expiry list contains a cycle")
		}
	}
	if count != want {
		t.Fatalf("expiry list length = %d, want %d", count, want)
	}
	if previous != list.tail {
		t.Fatal("expiry list tail does not match its final entry")
	}
}

func TestWorkerIDRegistryExpiryListTransitions(t *testing.T) {
	registry := newWorkerIDRegistry()
	entries := []*workerIDEntry{
		newWorkerIDEntry(registry, 1, 0, 1, time.Unix(0, 1)),
		newWorkerIDEntry(registry, 2, 0, 1, time.Unix(0, 2)),
		newWorkerIDEntry(registry, 3, 0, 1, time.Unix(0, 3)),
	}

	registry.expiryMu.Lock()
	for _, entry := range entries {
		entry.mu.Lock()
		registry.appendIdle(entry)
		entry.mu.Unlock()
	}
	poolWithIDP2AssertList(t, registry.idle, workerIDEntryListIdle, 3)

	entries[1].mu.Lock()
	registry.appendRunning(entries[1])
	entries[1].mu.Unlock()
	poolWithIDP2AssertList(t, registry.idle, workerIDEntryListIdle, 2)
	poolWithIDP2AssertList(t, registry.running, workerIDEntryListRunning, 1)

	registry.removeExpiry(entries[0])
	registry.removeExpiry(entries[1])
	registry.removeExpiry(entries[2])
	poolWithIDP2AssertList(t, registry.idle, workerIDEntryListIdle, 0)
	poolWithIDP2AssertList(t, registry.running, workerIDEntryListRunning, 0)
	registry.expiryMu.Unlock()
}

func TestWorkerIDRegistryPurgeVisitsOnlyIdleExpiryPrefix(t *testing.T) {
	p := newPoolWithIDForP2StructureTest(t)
	const expired = 37
	now := time.Now()
	expiry := p.options.ExpiryDuration

	p.lock.Lock()
	p.registry.expiryMu.Lock()
	for id := 0; id < poolWithIDP2StructuralEntries; id++ {
		idleAt := now.Add(-expiry + time.Nanosecond + time.Duration(id)*time.Nanosecond)
		if id < expired {
			idleAt = now.Add(-expiry - time.Duration(expired-id)*time.Nanosecond)
		}
		poolWithIDP2AddSyntheticEntry(p, id, workerIDEntryListIdle, idleAt)
	}
	p.registry.expiryMu.Unlock()
	p.lock.Unlock()

	var visited atomic.Int64
	p.testHooks.afterPurgeEntryVisited = func() { visited.Add(1) }
	p.purgeExpired(now)

	if got, want := visited.Load(), int64(expired+1); got != want {
		t.Fatalf("purge visited %d idle entries, want %d", got, want)
	}
	p.lock.Lock()
	p.registry.expiryMu.Lock()
	if got, want := len(p.registry.items), poolWithIDP2StructuralEntries-expired; got != want {
		t.Fatalf("registry size = %d, want %d", got, want)
	}
	if p.registry.idle.head == nil || p.registry.idle.head.id != expired {
		t.Fatalf("oldest remaining idle ID = %v, want %d", p.registry.idle.head, expired)
	}
	poolWithIDP2AssertList(
		t,
		p.registry.idle,
		workerIDEntryListIdle,
		poolWithIDP2StructuralEntries-expired,
	)
	p.registry.expiryMu.Unlock()
	p.lock.Unlock()
}

func TestWorkerIDRegistryPurgeVisitsOneUnexpiredRunningOwner(t *testing.T) {
	p := newPoolWithIDForP2StructureTest(t)
	now := time.Now()
	timeout := p.options.RunningTaskTimeout

	p.lock.Lock()
	p.registry.expiryMu.Lock()
	for id := 0; id < poolWithIDP2StructuralEntries; id++ {
		startedAt := now.Add(-timeout + time.Nanosecond + time.Duration(id)*time.Nanosecond)
		poolWithIDP2AddSyntheticEntry(p, id, workerIDEntryListRunning, startedAt)
	}
	p.registry.expiryMu.Unlock()
	p.lock.Unlock()

	var visited atomic.Int64
	p.testHooks.afterPurgeEntryVisited = func() { visited.Add(1) }
	p.purgeExpired(now)

	if got := visited.Load(); got != 1 {
		t.Fatalf("purge visited %d running entries, want 1", got)
	}
	p.lock.Lock()
	p.registry.expiryMu.Lock()
	if got := len(p.registry.items); got != poolWithIDP2StructuralEntries {
		t.Fatalf("registry size = %d, want %d", got, poolWithIDP2StructuralEntries)
	}
	poolWithIDP2AssertList(
		t,
		p.registry.running,
		workerIDEntryListRunning,
		poolWithIDP2StructuralEntries,
	)
	p.registry.expiryMu.Unlock()
	p.lock.Unlock()
}

func TestWorkerIDRegistryPurgeVisitsRunningExpiryPrefix(t *testing.T) {
	const (
		expired = 8
		total   = expired + 1
	)
	p := newPoolWithIDForTest(
		t,
		total,
		WithExpiryDuration(time.Hour),
		WithMaxEscapedWorkers(expired),
		WithTaskBuffer(1),
	)
	releaseTasks := make(chan struct{})
	closeReleaseTasks := poolWithIDCloseOnCleanup(t, releaseTasks)
	startedAt := make([]time.Time, total)
	returned := make(chan struct{}, total)

	for id := 0; id < total; id++ {
		started := make(chan struct{})
		if err := p.Submit(id, func() {
			close(started)
			<-releaseTasks
			returned <- struct{}{}
		}); err != nil {
			t.Fatalf("Submit ID %d: %v", id, err)
		}
		poolWithIDReceive(t, started)
		_, _, startedAt[id] = poolWithIDObserveEntryState(t, p, id)
	}
	if !startedAt[expired].After(startedAt[expired-1]) {
		t.Fatalf("running timestamps are not ordered: %v then %v",
			startedAt[expired-1], startedAt[expired])
	}

	var visited atomic.Int64
	p.testHooks.afterPurgeEntryVisited = func() { visited.Add(1) }
	p.purgeExpired(startedAt[expired-1].Add(p.options.RunningTaskTimeout))
	if got, want := visited.Load(), int64(expired+1); got != want {
		t.Fatalf("purge visited %d running entries, want %d", got, want)
	}
	if snapshot := p.EscapeSnapshot(); snapshot.Total != expired {
		t.Fatalf("escaped workers = %d, want %d", snapshot.Total, expired)
	}

	closeReleaseTasks()
	for range total {
		poolWithIDReceive(t, returned)
	}
	poolWithIDObserveRelease(t, p)
}
