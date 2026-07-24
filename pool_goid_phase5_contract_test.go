package ants

import (
	"errors"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

func poolWithIDPhase5AssertExpiryIndexEmpty(t *testing.T, p *PoolWithID) {
	t.Helper()
	p.lock.Lock()
	registry := p.registry
	registry.expiryMu.Lock()
	defer registry.expiryMu.Unlock()
	defer p.lock.Unlock()

	if registry.idle.head != nil || registry.idle.tail != nil ||
		registry.running.head != nil || registry.running.tail != nil ||
		registry.deferred.head != nil || registry.deferred.tail != nil ||
		registry.deferredCount != 0 {
		t.Fatalf("disabled expiry index is not empty: idle=%p/%p running=%p/%p deferred=%p/%p (%d)",
			registry.idle.head, registry.idle.tail,
			registry.running.head, registry.running.tail,
			registry.deferred.head, registry.deferred.tail, registry.deferredCount)
	}
	for id, entry := range registry.items {
		entry.mu.Lock()
		indexed := entry.expiryList != workerIDEntryListNone ||
			entry.expiryPrev != nil || entry.expiryNext != nil || entry.expiryPending
		entry.mu.Unlock()
		if indexed {
			t.Fatalf("entry %d retained disabled expiry-index state", id)
		}
	}
}

func poolWithIDPhase5WaitForEntryDrained(t *testing.T, p *PoolWithID, id int) {
	t.Helper()
	waitForReservationCondition(t, func() bool {
		p.lock.Lock()
		entry := p.registry.items[id]
		if entry == nil {
			p.lock.Unlock()
			return false
		}
		entry.mu.Lock()
		drained := entry.drained()
		entry.mu.Unlock()
		p.lock.Unlock()
		return drained
	})
}

func TestPoolWithIDDisablePurgeBypassesExpiryIndexAcrossLifecycle(t *testing.T) {
	const id = 801
	p := newPoolWithIDForTest(t, 2, WithDisablePurge(true), WithTaskBuffer(2))

	releaseFirst := make(chan struct{})
	closeReleaseFirst := poolWithIDCloseOnCleanup(t, releaseFirst)
	firstStarted := make(chan struct{})
	secondFinished := make(chan struct{})
	if err := p.Submit(id, func() {
		close(firstStarted)
		<-releaseFirst
	}); err != nil {
		t.Fatalf("Submit(first task): %v", err)
	}
	poolWithIDReceive(t, firstStarted)
	if err := p.Submit(id, func() { close(secondFinished) }); err != nil {
		t.Fatalf("Submit(second task): %v", err)
	}
	poolWithIDPhase5AssertExpiryIndexEmpty(t, p)

	closeReleaseFirst()
	poolWithIDReceive(t, secondFinished)
	poolWithIDPhase5WaitForEntryDrained(t, p, id)
	poolWithIDPhase5AssertExpiryIndexEmpty(t, p)

	const goexitID = 802
	goexitStarted := make(chan struct{})
	replacementFinished := make(chan struct{})
	if err := p.Submit(goexitID, func() {
		close(goexitStarted)
		runtime.Goexit()
	}); err != nil {
		t.Fatalf("Submit(Goexit task): %v", err)
	}
	poolWithIDReceive(t, goexitStarted)
	if err := p.Submit(goexitID, func() { close(replacementFinished) }); err != nil {
		t.Fatalf("Submit(replacement task): %v", err)
	}
	poolWithIDReceive(t, replacementFinished)
	poolWithIDPhase5WaitForEntryDrained(t, p, goexitID)
	poolWithIDPhase5AssertExpiryIndexEmpty(t, p)

	if err := p.ReleaseTimeout(poolWithIDTestTimeout); err != nil {
		t.Fatalf("ReleaseTimeout: %v", err)
	}
	p.Reboot()
	rebootFinished := make(chan struct{})
	if err := p.Submit(803, func() { close(rebootFinished) }); err != nil {
		t.Fatalf("Submit(after Reboot): %v", err)
	}
	poolWithIDReceive(t, rebootFinished)
	poolWithIDPhase5WaitForEntryDrained(t, p, 803)
	poolWithIDPhase5AssertExpiryIndexEmpty(t, p)
}

func TestPoolWithIDDisablePurgeTuneDownScansRegistry(t *testing.T) {
	const owners = 4
	p := newPoolWithIDForTest(t, owners, WithDisablePurge(true))

	for id := 0; id < owners; id++ {
		finished := make(chan struct{})
		if err := p.Submit(id, func() { close(finished) }); err != nil {
			t.Fatalf("Submit(ID %d): %v", id, err)
		}
		poolWithIDReceive(t, finished)
		poolWithIDPhase5WaitForEntryDrained(t, p, id)
	}
	poolWithIDPhase5AssertExpiryIndexEmpty(t, p)

	p.Tune(1)
	waitForTuneCondition(t, func() bool { return p.Running() == 1 })
	p.lock.Lock()
	remaining := len(p.registry.items)
	p.lock.Unlock()
	if remaining != 1 {
		t.Fatalf("registry owners after Tune(1) = %d, want 1", remaining)
	}
	poolWithIDPhase5AssertExpiryIndexEmpty(t, p)

	if err := p.ReleaseTimeout(poolWithIDTestTimeout); err != nil &&
		!errors.Is(err, ErrPoolClosed) {
		t.Fatalf("ReleaseTimeout: %v", err)
	}
}

func TestPoolWithIDPurgeBatchesReleaseLocksAndDrainImmediately(t *testing.T) {
	const (
		entries   = 10
		batchSize = 3
	)
	p := newPoolWithIDForP2StructureTest(t)
	now := time.Now()

	p.lock.Lock()
	p.registry.expiryMu.Lock()
	for id := 0; id < entries; id++ {
		poolWithIDP2AddSyntheticEntry(
			p,
			id,
			workerIDEntryListIdle,
			now.Add(-p.options.ExpiryDuration-time.Duration(entries-id)*time.Nanosecond),
		)
	}
	p.registry.expiryMu.Unlock()
	p.lock.Unlock()

	var batches, registrySizes []int
	p.testHooks.afterPurgeBatch = func(visited int, _ bool) {
		if visited > batchSize {
			t.Fatalf("purge batch visited %d entries, limit %d", visited, batchSize)
		}
		p.lock.Lock()
		p.registry.expiryMu.Lock()
		registrySizes = append(registrySizes, len(p.registry.items))
		p.registry.expiryMu.Unlock()
		p.lock.Unlock()
		_ = p.EscapeSnapshot()
		batches = append(batches, visited)
	}
	p.purgeExpiredBatched(now, now.Sub, batchSize)
	p.testHooks.afterPurgeBatch = nil

	if got, want := len(batches), 4; got != want {
		t.Fatalf("purge batches = %d (%v), want %d", got, batches, want)
	}
	wantSizes := []int{7, 4, 1, 0}
	for i := range wantSizes {
		if registrySizes[i] != wantSizes[i] {
			t.Fatalf("registry sizes after each batch = %v, want %v", registrySizes, wantSizes)
		}
	}
	p.lock.Lock()
	remaining := len(p.registry.items)
	p.lock.Unlock()
	if remaining != 0 {
		t.Fatalf("registry entries after batched purge = %d, want 0", remaining)
	}
}

func TestPoolWithIDPurgeRefreshesTimestampAfterEachBatch(t *testing.T) {
	const (
		firstRunningID  = 811
		secondRunningID = 812
		betweenID       = 813
	)
	clock := newPoolWithIDPhase3FakeClock()
	p := poolWithIDPhase3NewPool(t, 3, clock,
		WithExpiryDuration(time.Hour),
		WithRunningTaskTimeout(time.Hour),
		WithMaxEscapedWorkers(2),
		WithMaxEscapedWorkersPerID(1),
	)

	releaseRunning := make(chan struct{})
	closeReleaseRunning := poolWithIDCloseOnCleanup(t, releaseRunning)
	for _, id := range []int{firstRunningID, secondRunningID} {
		started := make(chan struct{})
		if err := p.Submit(id, func() {
			close(started)
			<-releaseRunning
		}); err != nil {
			t.Fatalf("Submit running ID %d: %v", id, err)
		}
		poolWithIDReceive(t, started)
	}
	clock.Advance(time.Hour)

	var batches atomic.Int32
	p.testHooks.afterPurgeBatch = func(_ int, _ bool) {
		if batches.Add(1) != 1 {
			return
		}
		clock.Advance(time.Second)
		finished := make(chan struct{})
		if err := p.Submit(betweenID, func() { close(finished) }); err != nil {
			t.Fatalf("Submit between batches: %v", err)
		}
		poolWithIDReceive(t, finished)
		poolWithIDPhase5WaitForEntryDrained(t, p, betweenID)
		clock.Advance(time.Second)
	}
	p.purgeExpiredBatchedWithClock(clock.Now, clock.Since, 1)
	p.testHooks.afterPurgeBatch = nil

	p.lock.Lock()
	p.registry.expiryMu.Lock()
	var ids []int
	var previous time.Time
	for entry := p.registry.idle.head; entry != nil; entry = entry.expiryNext {
		entry.mu.Lock()
		idleAt := entry.lastIdleAt
		entry.mu.Unlock()
		if !previous.IsZero() && idleAt.Before(previous) {
			p.registry.expiryMu.Unlock()
			p.lock.Unlock()
			t.Fatalf("idle timestamps out of order at ID %d: %v before %v", entry.id, idleAt, previous)
		}
		previous = idleAt
		ids = append(ids, entry.id)
	}
	p.registry.expiryMu.Unlock()
	p.lock.Unlock()

	want := []int{firstRunningID, betweenID, secondRunningID}
	if len(ids) != len(want) {
		t.Fatalf("idle IDs = %v, want %v", ids, want)
	}
	for i := range want {
		if ids[i] != want[i] {
			t.Fatalf("idle IDs = %v, want %v", ids, want)
		}
	}
	closeReleaseRunning()
}

func TestPoolWithIDPurgeClampsDeferredWorkAfterEntriesExit(t *testing.T) {
	const entries = 5
	p := newPoolWithIDForP2StructureTest(t)
	now := time.Now()

	p.lock.Lock()
	p.registry.expiryMu.Lock()
	for id := 1; id <= entries; id++ {
		entry := poolWithIDP2AddSyntheticEntry(
			p,
			id,
			workerIDEntryListRunning,
			now,
		)
		p.registry.appendDeferred(entry)
	}
	p.registry.expiryMu.Unlock()
	p.lock.Unlock()

	var batches []int
	p.testHooks.afterPurgeBatch = func(visited int, _ bool) {
		batches = append(batches, visited)
		if len(batches) != 1 {
			return
		}
		p.lock.Lock()
		p.registry.expiryMu.Lock()
		for id := 2; id <= entries; id++ {
			entry := p.registry.items[id]
			p.registry.removeExpiry(entry)
			delete(p.registry.items, id)
		}
		p.registry.expiryMu.Unlock()
		p.lock.Unlock()
	}
	p.purgeExpiredBatched(now, now.Sub, 1)
	p.testHooks.afterPurgeBatch = nil

	want := []int{1, 1, 0}
	if len(batches) != len(want) {
		t.Fatalf("purge batches = %v, want %v", batches, want)
	}
	for i := range want {
		if batches[i] != want[i] {
			t.Fatalf("purge batches = %v, want %v", batches, want)
		}
	}
}
