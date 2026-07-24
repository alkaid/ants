package ants

import (
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func waitForReservationCondition(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(poolWithIDTestTimeout)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for reservation condition")
		}
		runtime.Gosched()
	}
}

func TestPoolWithIDReservationAllocatesOutsideLockAndCoalescesID(t *testing.T) {
	const (
		id        = 101
		submitter = 8
	)
	p := newPoolWithIDForTest(t, submitter, WithDisablePurge(true))

	allocationStarted := make(chan struct{})
	allowAllocation := make(chan struct{})
	closeAllowAllocation := poolWithIDCloseOnCleanup(t, allowAllocation)
	var allocationCalls atomic.Int32
	p.testHooks.beforeReservationAllocate = func(gotID int) {
		if gotID != id {
			t.Errorf("reservation ID = %d, want %d", gotID, id)
		}
		if allocationCalls.Add(1) == 1 {
			close(allocationStarted)
			<-allowAllocation
		}
	}
	t.Cleanup(func() { p.testHooks.beforeReservationAllocate = nil })

	var tasks sync.WaitGroup
	tasks.Add(submitter)
	results := make(chan error, submitter)
	go func() { results <- p.Submit(id, tasks.Done) }()
	poolWithIDReceive(t, allocationStarted)

	lockAcquired := make(chan struct{})
	go func() {
		p.lock.Lock()
		close(lockAcquired)
		p.lock.Unlock()
	}()
	poolWithIDReceive(t, lockAcquired)

	for i := 1; i < submitter; i++ {
		go func() { results <- p.Submit(id, tasks.Done) }()
	}
	waitForReservationCondition(t, func() bool { return p.Waiting() == submitter-1 })

	p.lock.Lock()
	reservation := p.reservations[id]
	reserved := p.reservedOwners.Load()
	registryEntries := len(p.registry.items)
	p.lock.Unlock()
	if reservation == nil || reservation.state != workerIDReservationPending {
		t.Fatalf("pending reservation = %#v", reservation)
	}
	if reserved != 1 || registryEntries != 0 || p.Running() != 0 {
		t.Fatalf("allocation state: reserved=%d registry=%d running=%d, want 1/0/0",
			reserved, registryEntries, p.Running())
	}

	closeAllowAllocation()
	for i := 0; i < submitter; i++ {
		if err := poolWithIDReceive(t, results); err != nil {
			t.Fatalf("Submit result %d: %v", i, err)
		}
	}
	tasksDone := make(chan struct{})
	go func() {
		tasks.Wait()
		close(tasksDone)
	}()
	poolWithIDReceive(t, tasksDone)
	waitForReservationCondition(t, func() bool {
		return p.Waiting() == 0 && p.reservedOwners.Load() == 0
	})

	p.lock.Lock()
	registryEntries = len(p.registry.items)
	reservationEntries := len(p.reservations)
	p.lock.Unlock()
	if registryEntries != 1 || reservationEntries != 0 || p.Running() != 1 {
		t.Fatalf("committed state: registry=%d reservations=%d running=%d, want 1/0/1",
			registryEntries, reservationEntries, p.Running())
	}
	if got := allocationCalls.Load(); got != 1 {
		t.Fatalf("allocator calls = %d, want 1", got)
	}
}

func TestPoolWithIDNonblockingReservationFollowerReturnsOverload(t *testing.T) {
	const id = 102
	p := newPoolWithIDForTest(t, 2,
		WithDisablePurge(true),
		WithNonblocking(true),
	)

	allocationStarted := make(chan struct{})
	allowAllocation := make(chan struct{})
	closeAllowAllocation := poolWithIDCloseOnCleanup(t, allowAllocation)
	p.testHooks.beforeReservationAllocate = func(int) {
		close(allocationStarted)
		<-allowAllocation
	}
	t.Cleanup(func() { p.testHooks.beforeReservationAllocate = nil })

	firstTaskDone := make(chan struct{})
	firstResult := make(chan error, 1)
	go func() {
		firstResult <- p.Submit(id, func() { close(firstTaskDone) })
	}()
	poolWithIDReceive(t, allocationStarted)

	if err := p.Submit(id, func() {}); !errors.Is(err, ErrPoolOverload) {
		t.Fatalf("nonblocking follower error = %v, want %v", err, ErrPoolOverload)
	}
	if got := p.Waiting(); got != 0 {
		t.Fatalf("Waiting after nonblocking follower = %d, want 0", got)
	}

	closeAllowAllocation()
	if err := poolWithIDReceive(t, firstResult); err != nil {
		t.Fatalf("allocator Submit: %v", err)
	}
	poolWithIDReceive(t, firstTaskDone)
}

func TestPoolWithIDReleaseWaitsForReservationAllocatorAbort(t *testing.T) {
	const id = 103
	p := newPoolWithIDForTest(t, 2, WithDisablePurge(true))

	allocationStarted := make(chan struct{})
	allowAllocation := make(chan struct{})
	closeAllowAllocation := poolWithIDCloseOnCleanup(t, allowAllocation)
	p.testHooks.beforeReservationAllocate = func(int) {
		close(allocationStarted)
		<-allowAllocation
	}
	t.Cleanup(func() { p.testHooks.beforeReservationAllocate = nil })

	allocatorResult := make(chan error, 1)
	go func() { allocatorResult <- p.Submit(id, func() {}) }()
	poolWithIDReceive(t, allocationStarted)

	p.lock.Lock()
	reservation := p.reservations[id]
	p.lock.Unlock()
	if reservation == nil {
		t.Fatal("allocator did not publish a reservation")
	}

	followerResult := make(chan error, 1)
	go func() { followerResult <- p.Submit(id, func() {}) }()
	waitForReservationCondition(t, func() bool { return p.Waiting() == 1 })

	p.Release()
	select {
	case <-reservation.done:
		t.Fatal("Release closed allocator-owned reservation completion")
	default:
	}
	select {
	case <-p.managedDone:
		t.Fatal("managedDone closed while an allocator was still reserved")
	default:
	}
	if err := poolWithIDReceive(t, followerResult); !errors.Is(err, ErrPoolClosed) {
		t.Fatalf("follower after Release error = %v, want %v", err, ErrPoolClosed)
	}
	waitForReservationCondition(t, func() bool { return p.Waiting() == 0 })
	select {
	case <-p.managedDone:
		t.Fatal("managedDone closed before allocator abort")
	default:
	}

	closeAllowAllocation()
	if err := poolWithIDReceive(t, allocatorResult); !errors.Is(err, ErrPoolClosed) {
		t.Fatalf("allocator after Release error = %v, want %v", err, ErrPoolClosed)
	}
	poolWithIDReceive(t, p.closedDone)

	p.lock.Lock()
	reservationEntries := len(p.reservations)
	registryEntries := len(p.registry.items)
	p.lock.Unlock()
	if reservation.state != workerIDReservationAborted || reservationEntries != 0 ||
		registryEntries != 0 || p.reservedOwners.Load() != 0 || p.Running() != 0 || p.Waiting() != 0 {
		t.Fatalf("aborted state: state=%d reservations=%d registry=%d reserved=%d running=%d waiting=%d",
			reservation.state, reservationEntries, registryEntries, p.reservedOwners.Load(),
			p.Running(), p.Waiting())
	}
}

func TestPoolWithIDReservationCannotCommitAcrossReboot(t *testing.T) {
	const id = 104
	p := newPoolWithIDForTest(t, 1, WithDisablePurge(true))
	generation := p.generation.Load()

	allocationStarted := make(chan struct{})
	allowAllocation := make(chan struct{})
	closeAllowAllocation := poolWithIDCloseOnCleanup(t, allowAllocation)
	p.testHooks.beforeReservationAllocate = func(int) {
		close(allocationStarted)
		<-allowAllocation
	}
	t.Cleanup(func() { p.testHooks.beforeReservationAllocate = nil })

	allocatorResult := make(chan error, 1)
	go func() { allocatorResult <- p.Submit(id, func() {}) }()
	poolWithIDReceive(t, allocationStarted)
	p.Release()

	rebootDone := make(chan struct{})
	go func() {
		p.Reboot()
		close(rebootDone)
	}()
	select {
	case <-rebootDone:
		t.Fatal("Reboot completed before the reservation allocator converged")
	default:
	}

	closeAllowAllocation()
	if err := poolWithIDReceive(t, allocatorResult); !errors.Is(err, ErrPoolClosed) {
		t.Fatalf("stale allocator error = %v, want %v", err, ErrPoolClosed)
	}
	poolWithIDReceive(t, rebootDone)
	p.testHooks.beforeReservationAllocate = nil

	if got := p.generation.Load(); got != generation+1 {
		t.Fatalf("generation after Reboot = %d, want %d", got, generation+1)
	}
	p.lock.Lock()
	reservationEntries := len(p.reservations)
	registryEntries := len(p.registry.items)
	p.lock.Unlock()
	if reservationEntries != 0 || registryEntries != 0 || p.reservedOwners.Load() != 0 || p.Running() != 0 {
		t.Fatalf("new generation polluted: reservations=%d registry=%d reserved=%d running=%d",
			reservationEntries, registryEntries, p.reservedOwners.Load(), p.Running())
	}

	finished := make(chan struct{})
	if err := p.Submit(id, func() { close(finished) }); err != nil {
		t.Fatalf("Submit in rebooted generation: %v", err)
	}
	poolWithIDReceive(t, finished)
}

func TestPoolWithIDReservationCommitsAfterTuneDownThenConverges(t *testing.T) {
	p := newPoolWithIDForTest(t, 2, WithDisablePurge(true))

	releaseFirst := make(chan struct{})
	closeReleaseFirst := poolWithIDCloseOnCleanup(t, releaseFirst)
	firstStarted := make(chan struct{})
	if err := p.Submit(1, func() {
		close(firstStarted)
		<-releaseFirst
	}); err != nil {
		t.Fatalf("submit first ID: %v", err)
	}
	poolWithIDReceive(t, firstStarted)

	allocationStarted := make(chan struct{})
	allowAllocation := make(chan struct{})
	closeAllowAllocation := poolWithIDCloseOnCleanup(t, allowAllocation)
	p.testHooks.beforeReservationAllocate = func(id int) {
		if id == 2 {
			close(allocationStarted)
			<-allowAllocation
		}
	}
	t.Cleanup(func() { p.testHooks.beforeReservationAllocate = nil })

	secondTaskDone := make(chan struct{})
	secondResult := make(chan error, 1)
	go func() {
		secondResult <- p.Submit(2, func() { close(secondTaskDone) })
	}()
	poolWithIDReceive(t, allocationStarted)
	if p.reservedOwners.Load() != 1 || p.Running() != 1 {
		t.Fatalf("before Tune: reserved=%d running=%d, want 1/1", p.reservedOwners.Load(), p.Running())
	}

	p.Tune(1)
	if got := p.Cap(); got != 1 {
		t.Fatalf("capacity after Tune = %d, want 1", got)
	}
	closeAllowAllocation()
	if err := poolWithIDReceive(t, secondResult); err != nil {
		t.Fatalf("reserved Submit after Tune: %v", err)
	}
	poolWithIDReceive(t, secondTaskDone)
	waitForReservationCondition(t, func() bool { return p.Running() == 1 })

	p.lock.Lock()
	_, firstPresent := p.registry.items[1]
	_, secondPresent := p.registry.items[2]
	p.lock.Unlock()
	if !firstPresent || secondPresent || p.reservedOwners.Load() != 0 {
		t.Fatalf("post-Tune registry: first=%v second=%v reserved=%d, want true/false/0",
			firstPresent, secondPresent, p.reservedOwners.Load())
	}
	closeReleaseFirst()
}

func TestPoolWithIDReservationCapacityConversionCannotUndercount(t *testing.T) {
	p := newPoolWithIDForTest(t, 2, WithDisablePurge(true))

	releaseFirst := make(chan struct{})
	closeReleaseFirst := poolWithIDCloseOnCleanup(t, releaseFirst)
	firstStarted := make(chan struct{})
	if err := p.Submit(1, func() {
		close(firstStarted)
		<-releaseFirst
	}); err != nil {
		t.Fatalf("submit first ID: %v", err)
	}
	poolWithIDReceive(t, firstStarted)

	allocationStarted := make(chan struct{})
	allowAllocation := make(chan struct{})
	closeAllowAllocation := poolWithIDCloseOnCleanup(t, allowAllocation)
	p.testHooks.beforeReservationAllocate = func(id int) {
		if id == 2 {
			close(allocationStarted)
			<-allowAllocation
		}
	}
	t.Cleanup(func() { p.testHooks.beforeReservationAllocate = nil })

	releaseSecond := make(chan struct{})
	closeReleaseSecond := poolWithIDCloseOnCleanup(t, releaseSecond)
	secondStarted := make(chan struct{})
	secondResult := make(chan error, 1)
	go func() {
		secondResult <- p.Submit(2, func() {
			close(secondStarted)
			<-releaseSecond
		})
	}()
	poolWithIDReceive(t, allocationStarted)
	p.Tune(1)

	lock := &tuneAttemptLock{Locker: p.lock, attempted: make(chan struct{})}
	p.lock = lock
	p.cond.L = lock
	conversionReached := make(chan struct{})
	allowConversion := make(chan struct{})
	closeAllowConversion := poolWithIDCloseOnCleanup(t, allowConversion)
	p.testHooks.duringReservationCapacityConvert = func(int) {
		close(conversionReached)
		<-allowConversion
	}
	t.Cleanup(func() { p.testHooks.duringReservationCapacityConvert = nil })

	closeAllowAllocation()
	poolWithIDReceive(t, conversionReached)
	if running, reserved := p.Running(), p.reservedOwners.Load(); running != 2 || reserved != 1 {
		t.Fatalf("conversion state = running:%d reserved:%d, want 2/1", running, reserved)
	}

	lock.armed.Store(true)
	closeReleaseFirst()
	poolWithIDReceive(t, lock.attempted)
	closeAllowConversion()
	if err := poolWithIDReceive(t, secondResult); err != nil {
		t.Fatalf("reserved Submit after conversion: %v", err)
	}
	poolWithIDReceive(t, secondStarted)
	waitForReservationCondition(t, func() bool { return p.Running() == 1 })

	p.lock.Lock()
	_, firstPresent := p.registry.items[1]
	_, secondPresent := p.registry.items[2]
	p.lock.Unlock()
	if firstPresent || !secondPresent || p.reservedOwners.Load() != 0 {
		t.Fatalf("conversion registry: first=%v second=%v reserved=%d, want false/true/0",
			firstPresent, secondPresent, p.reservedOwners.Load())
	}
	closeReleaseSecond()
}

func TestPoolWithIDReservationPanicAbortsAndFollowerRetries(t *testing.T) {
	const id = 105
	p := newPoolWithIDForTest(t, 2, WithDisablePurge(true))

	allocationStarted := make(chan struct{})
	allowPanic := make(chan struct{})
	closeAllowPanic := poolWithIDCloseOnCleanup(t, allowPanic)
	var startedOnce sync.Once
	p.testHooks.beforeReservationAllocate = func(int) {
		startedOnce.Do(func() { close(allocationStarted) })
		<-allowPanic
	}
	panicValue := &struct{ message string }{message: "reservation allocation panic"}
	var allocatedCalls atomic.Int32
	p.testHooks.afterReservationAllocated = func(int) {
		if allocatedCalls.Add(1) == 1 {
			panic(panicValue)
		}
	}
	t.Cleanup(func() {
		p.testHooks.beforeReservationAllocate = nil
		p.testHooks.afterReservationAllocated = nil
	})

	allocatorPanic := make(chan any, 1)
	go func() {
		var recovered any
		func() {
			defer func() { recovered = recover() }()
			_ = p.Submit(id, func() {})
		}()
		allocatorPanic <- recovered
	}()
	poolWithIDReceive(t, allocationStarted)

	p.lock.Lock()
	firstReservation := p.reservations[id]
	p.lock.Unlock()
	followerTaskDone := make(chan struct{})
	followerResult := make(chan error, 1)
	go func() {
		followerResult <- p.Submit(id, func() { close(followerTaskDone) })
	}()
	waitForReservationCondition(t, func() bool { return p.Waiting() == 1 })

	closeAllowPanic()
	if recovered := poolWithIDReceive(t, allocatorPanic); recovered != panicValue {
		t.Fatalf("allocator panic = %#v, want %#v", recovered, panicValue)
	}
	if err := poolWithIDReceive(t, followerResult); err != nil {
		t.Fatalf("follower after allocator panic: %v", err)
	}
	poolWithIDReceive(t, followerTaskDone)
	waitForReservationCondition(t, func() bool {
		return p.Waiting() == 0 && p.reservedOwners.Load() == 0
	})

	p.lock.Lock()
	reservationEntries := len(p.reservations)
	registryEntries := len(p.registry.items)
	p.lock.Unlock()
	if firstReservation == nil || firstReservation.state != workerIDReservationAborted ||
		reservationEntries != 0 || registryEntries != 1 || p.Running() != 1 {
		t.Fatalf("panic recovery state: first=%#v reservations=%d registry=%d running=%d",
			firstReservation, reservationEntries, registryEntries, p.Running())
	}
	if got := allocatedCalls.Load(); got != 2 {
		t.Fatalf("allocation attempts = %d, want 2", got)
	}
}

func TestPoolWithIDWaiterTokenTransfersFromCapacityToReservation(t *testing.T) {
	p := newPoolWithIDForTest(t, 1,
		WithDisablePurge(true),
		WithMaxBlockingTasks(1),
	)

	releaseFirst := make(chan struct{})
	closeReleaseFirst := poolWithIDCloseOnCleanup(t, releaseFirst)
	firstStarted := make(chan struct{})
	if err := p.Submit(1, func() {
		close(firstStarted)
		<-releaseFirst
	}); err != nil {
		t.Fatalf("submit first ID: %v", err)
	}
	poolWithIDReceive(t, firstStarted)

	capacityWaitRegistered := make(chan struct{})
	var waitOnce sync.Once
	p.testHooks.afterCapacityWaitRegistered = func() {
		waitOnce.Do(func() { close(capacityWaitRegistered) })
	}
	t.Cleanup(func() { p.testHooks.afterCapacityWaitRegistered = nil })

	secondTaskDone := make(chan struct{})
	secondResult := make(chan error, 1)
	go func() {
		secondResult <- p.Submit(2, func() { close(secondTaskDone) })
	}()
	poolWithIDReceive(t, capacityWaitRegistered)

	allocationStarted := make(chan struct{})
	allowAllocation := make(chan struct{})
	closeAllowAllocation := poolWithIDCloseOnCleanup(t, allowAllocation)
	p.testHooks.beforeReservationAllocate = func(int) {
		close(allocationStarted)
		<-allowAllocation
	}
	t.Cleanup(func() { p.testHooks.beforeReservationAllocate = nil })

	p.lock.Lock()
	atomic.StoreInt32(&p.capacity, 2)
	reservation := p.reserveOwnerLocked(2, p.generation.Load())
	p.cond.Broadcast()
	p.lock.Unlock()

	allocatorDone := make(chan error, 1)
	go func() {
		entry, _, err := p.allocateReservedOwner(reservation, reservation.allocator)
		if err == nil {
			p.finishSubmit(entry, false)
		}
		allocatorDone <- err
	}()
	poolWithIDReceive(t, allocationStarted)
	waitForReservationCondition(t, func() bool { return p.Waiting() == 1 })
	select {
	case err := <-secondResult:
		t.Fatalf("capacity waiter returned while reservation was pending: %v", err)
	default:
	}

	closeAllowAllocation()
	if err := poolWithIDReceive(t, allocatorDone); err != nil {
		t.Fatalf("synthetic allocator: %v", err)
	}
	if err := poolWithIDReceive(t, secondResult); err != nil {
		t.Fatalf("transferred waiter Submit: %v", err)
	}
	poolWithIDReceive(t, secondTaskDone)
	waitForReservationCondition(t, func() bool { return p.Waiting() == 0 })
	closeReleaseFirst()
}
