package ants

import (
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type tuneAttemptLock struct {
	sync.Locker
	armed     atomic.Bool
	attempted chan struct{}
}

func (l *tuneAttemptLock) Lock() {
	if l.armed.CompareAndSwap(true, false) {
		close(l.attempted)
	}
	l.Locker.Lock()
}

func waitForTuneCondition(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(poolWithIDTestTimeout)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for Tune condition")
		}
		runtime.Gosched()
	}
}

func waitForTuneLockAttempt(t *testing.T, attempted, tuneDone <-chan struct{}) {
	t.Helper()
	select {
	case <-attempted:
	case <-tuneDone:
		t.Fatal("Tune returned without synchronizing with the capacity waiter lock")
	case <-time.After(poolWithIDTestTimeout):
		t.Fatal("timed out waiting for Tune to acquire the capacity waiter lock")
	}
}

func TestPoolWithIDTuneUpDoesNotLoseCapacityWakeup(t *testing.T) {
	p := newPoolWithIDForTest(t, 1, WithDisablePurge(true))

	releaseFirst := make(chan struct{})
	closeReleaseFirst := poolWithIDCloseOnCleanup(t, releaseFirst)
	firstStarted := make(chan struct{})
	if err := p.Submit(1, func() {
		close(firstStarted)
		<-releaseFirst
	}); err != nil {
		t.Fatalf("submit first task: %v", err)
	}
	poolWithIDReceive(t, firstStarted)

	lock := &tuneAttemptLock{Locker: p.lock, attempted: make(chan struct{})}
	p.lock = lock
	p.cond.L = lock
	waitRegistered := make(chan struct{})
	allowWait := make(chan struct{})
	closeAllowWait := poolWithIDCloseOnCleanup(t, allowWait)
	p.testHooks.afterCapacityWaitRegistered = func() {
		close(waitRegistered)
		<-allowWait
	}
	t.Cleanup(func() { p.testHooks.afterCapacityWaitRegistered = nil })

	secondStarted := make(chan struct{})
	secondResult := make(chan error, 1)
	go func() {
		secondResult <- p.Submit(2, func() { close(secondStarted) })
	}()
	poolWithIDReceive(t, waitRegistered)

	lock.armed.Store(true)
	tuneDone := make(chan struct{})
	go func() {
		p.Tune(2)
		close(tuneDone)
	}()
	waitForTuneLockAttempt(t, lock.attempted, tuneDone)

	closeAllowWait()
	poolWithIDReceive(t, tuneDone)
	if err := poolWithIDReceive(t, secondResult); err != nil {
		t.Fatalf("submit after Tune: %v", err)
	}
	poolWithIDReceive(t, secondStarted)
	closeReleaseFirst()
}

func TestPoolTuneUpDoesNotLoseCapacityWakeup(t *testing.T) {
	p, err := NewPool(1, WithDisablePurge(true))
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	t.Cleanup(func() { p.Release() })

	releaseFirst := make(chan struct{})
	closeReleaseFirst := poolWithIDCloseOnCleanup(t, releaseFirst)
	firstStarted := make(chan struct{})
	if err := p.Submit(func() {
		close(firstStarted)
		<-releaseFirst
	}); err != nil {
		t.Fatalf("submit first task: %v", err)
	}
	poolWithIDReceive(t, firstStarted)

	lock := &tuneAttemptLock{Locker: p.lock, attempted: make(chan struct{})}
	p.lock = lock
	p.cond.L = lock
	waitRegistered := make(chan struct{})
	allowWait := make(chan struct{})
	closeAllowWait := poolWithIDCloseOnCleanup(t, allowWait)
	p.testHooks.afterCapacityWaitRegistered = func() {
		close(waitRegistered)
		<-allowWait
	}
	t.Cleanup(func() { p.testHooks.afterCapacityWaitRegistered = nil })

	secondStarted := make(chan struct{})
	secondResult := make(chan error, 1)
	go func() {
		secondResult <- p.Submit(func() { close(secondStarted) })
	}()
	poolWithIDReceive(t, waitRegistered)

	lock.armed.Store(true)
	tuneDone := make(chan struct{})
	go func() {
		p.Tune(2)
		close(tuneDone)
	}()
	waitForTuneLockAttempt(t, lock.attempted, tuneDone)

	closeAllowWait()
	poolWithIDReceive(t, tuneDone)
	if err := poolWithIDReceive(t, secondResult); err != nil {
		t.Fatalf("submit after Tune: %v", err)
	}
	poolWithIDReceive(t, secondStarted)
	closeReleaseFirst()
}

func TestPoolWithIDTuneDownRetiresIdleOwnersWithoutPurge(t *testing.T) {
	for _, nonblocking := range []bool{false, true} {
		name := "blocking"
		if nonblocking {
			name = "nonblocking"
		}
		t.Run(name, func(t *testing.T) {
			p := newPoolWithIDForTest(t, 2,
				WithDisablePurge(true),
				WithNonblocking(nonblocking),
			)

			idle := make(chan struct{})
			var finished atomic.Int32
			p.testHooks.afterTaskFinished = func() {
				if finished.Add(1) == 2 {
					close(idle)
				}
			}
			for id := 1; id <= 2; id++ {
				if err := p.Submit(id, func() {}); err != nil {
					t.Fatalf("submit ID %d: %v", id, err)
				}
			}
			poolWithIDReceive(t, idle)
			p.testHooks.afterTaskFinished = nil

			p.Tune(1)
			waitForTuneCondition(t, func() bool { return p.Running() == 1 })
			p.lock.Lock()
			remaining := len(p.registry.items)
			p.lock.Unlock()
			if remaining != 1 {
				t.Fatalf("registry has %d owners after Tune down, want 1", remaining)
			}

			p.Tune(2)
			thirdFinished := make(chan struct{})
			if err := p.Submit(3, func() { close(thirdFinished) }); err != nil {
				t.Fatalf("submit new ID after Tune convergence: %v", err)
			}
			poolWithIDReceive(t, thirdFinished)
		})
	}
}

func TestPoolWithIDTuneDownRetiresAfterFinishSubmit(t *testing.T) {
	p := newPoolWithIDForTest(t, 2, WithDisablePurge(true))

	releaseSecond := make(chan struct{})
	closeReleaseSecond := poolWithIDCloseOnCleanup(t, releaseSecond)
	secondStarted := make(chan struct{})
	if err := p.Submit(2, func() {
		close(secondStarted)
		<-releaseSecond
	}); err != nil {
		t.Fatalf("submit ID 2: %v", err)
	}
	poolWithIDReceive(t, secondStarted)

	finishReached := make(chan struct{})
	allowFinish := make(chan struct{})
	closeAllowFinish := poolWithIDCloseOnCleanup(t, allowFinish)
	var hookOnce sync.Once
	p.testHooks.beforeSubmitFinished = func() {
		hookOnce.Do(func() { close(finishReached) })
		<-allowFinish
	}
	t.Cleanup(func() { p.testHooks.beforeSubmitFinished = nil })

	firstTaskFinished := make(chan struct{})
	firstResult := make(chan error, 1)
	go func() {
		firstResult <- p.Submit(1, func() { close(firstTaskFinished) })
	}()
	poolWithIDReceive(t, firstTaskFinished)
	poolWithIDReceive(t, finishReached)

	p.Tune(1)
	if got := p.Running(); got != 2 {
		t.Fatalf("Running after Tune with no drained owner = %d, want 2", got)
	}
	closeAllowFinish()
	if err := poolWithIDReceive(t, firstResult); err != nil {
		t.Fatalf("submit ID 1: %v", err)
	}
	p.testHooks.beforeSubmitFinished = nil
	waitForTuneCondition(t, func() bool { return p.Running() == 1 })

	p.lock.Lock()
	_, firstPresent := p.registry.items[1]
	_, secondPresent := p.registry.items[2]
	p.lock.Unlock()
	if firstPresent || !secondPresent {
		t.Fatalf("registry after finishSubmit retirement: ID1=%v ID2=%v, want false/true", firstPresent, secondPresent)
	}
	closeReleaseSecond()
}

func TestPoolWithIDTuneDownDoesNotDoubleRetirePendingOwnerExit(t *testing.T) {
	p := newPoolWithIDForTest(t, 2, WithDisablePurge(true))

	idle := make(chan struct{})
	var finished atomic.Int32
	p.testHooks.afterTaskFinished = func() {
		if finished.Add(1) == 2 {
			close(idle)
		}
	}
	for id := 1; id <= 2; id++ {
		if err := p.Submit(id, func() {}); err != nil {
			t.Fatalf("submit ID %d: %v", id, err)
		}
	}
	poolWithIDReceive(t, idle)
	p.testHooks.afterTaskFinished = nil

	ownerExitReached := make(chan struct{})
	allowOwnerExit := make(chan struct{})
	closeAllowOwnerExit := poolWithIDCloseOnCleanup(t, allowOwnerExit)
	p.testHooks.beforeOwnerExited = func(*goWorkerWithID) {
		close(ownerExitReached)
		<-allowOwnerExit
	}
	t.Cleanup(func() { p.testHooks.beforeOwnerExited = nil })

	p.Tune(1)
	poolWithIDReceive(t, ownerExitReached)
	if got := p.Running(); got != 2 {
		t.Fatalf("Running while detached owner exit is paused = %d, want 2", got)
	}

	p.lock.Lock()
	var remainingID int
	var remainingEntry *workerIDEntry
	for id, entry := range p.registry.items {
		remainingID = id
		remainingEntry = entry
	}
	p.lock.Unlock()
	if remainingEntry == nil {
		t.Fatal("Tune removed every registry owner")
	}

	submitFinishReached := make(chan struct{})
	allowSubmitFinish := make(chan struct{})
	closeAllowSubmitFinish := poolWithIDCloseOnCleanup(t, allowSubmitFinish)
	p.testHooks.beforeSubmitFinished = func() {
		close(submitFinishReached)
		<-allowSubmitFinish
	}
	t.Cleanup(func() { p.testHooks.beforeSubmitFinished = nil })
	taskFinished := make(chan struct{})
	p.testHooks.afterTaskFinished = func() { close(taskFinished) }
	t.Cleanup(func() { p.testHooks.afterTaskFinished = nil })

	submitResult := make(chan error, 1)
	go func() {
		submitResult <- p.Submit(remainingID, func() {})
	}()
	poolWithIDReceive(t, submitFinishReached)
	poolWithIDReceive(t, taskFinished)
	closeAllowSubmitFinish()
	if err := poolWithIDReceive(t, submitResult); err != nil {
		t.Fatalf("submit to remaining ID: %v", err)
	}
	p.testHooks.beforeSubmitFinished = nil
	p.testHooks.afterTaskFinished = nil

	p.lock.Lock()
	gotEntry := p.registry.items[remainingID]
	p.lock.Unlock()
	if gotEntry != remainingEntry {
		t.Fatal("finishSubmit retired the last attached owner while another owner exit was pending")
	}

	p.testHooks.beforeOwnerExited = nil
	closeAllowOwnerExit()
	waitForTuneCondition(t, func() bool { return p.Running() == 1 })
}

func TestPoolWithIDTuneDownRetiresDrainedTimeoutReplacement(t *testing.T) {
	p := newPoolWithIDForTest(t, 2, WithExpiryDuration(time.Hour))

	releaseTasks := []chan struct{}{make(chan struct{}), make(chan struct{})}
	closeReleaseTasks := make([]func(), 0, len(releaseTasks))
	for _, releaseTask := range releaseTasks {
		closeReleaseTasks = append(closeReleaseTasks, poolWithIDCloseOnCleanup(t, releaseTask))
	}
	started := []chan struct{}{make(chan struct{}), make(chan struct{})}
	for i := range started {
		id := i + 1
		releaseTask := releaseTasks[i]
		startedTask := started[i]
		if err := p.Submit(id, func() {
			close(startedTask)
			<-releaseTask
		}); err != nil {
			t.Fatalf("submit ID %d: %v", id, err)
		}
	}
	for _, startedTask := range started {
		poolWithIDReceive(t, startedTask)
	}

	_, _, firstStartedAt := poolWithIDObserveEntryState(t, p, 1)
	_, _, secondStartedAt := poolWithIDObserveEntryState(t, p, 2)
	latestStartedAt := firstStartedAt
	if secondStartedAt > latestStartedAt {
		latestStartedAt = secondStartedAt
	}

	p.Tune(1)
	if got := p.Running(); got != 2 {
		t.Fatalf("Running after Tune with running owners = %d, want 2", got)
	}
	p.purgeExpired(latestStartedAt + int64(p.options.ExpiryDuration))
	waitForTuneCondition(t, func() bool { return p.Running() == 1 })

	p.lock.Lock()
	remaining := len(p.registry.items)
	p.lock.Unlock()
	if remaining != 1 {
		t.Fatalf("registry has %d owners after timeout replacements, want 1", remaining)
	}

	for _, closeReleaseTask := range closeReleaseTasks {
		closeReleaseTask()
	}
}

func TestPoolWithIDTuneLifecycleStates(t *testing.T) {
	p := newPoolWithIDForTest(t, 2, WithDisablePurge(true), WithNonblocking(true))
	p.Tune(3)
	if got := p.Cap(); got != 3 {
		t.Fatalf("OPENED Tune capacity = %d, want 3", got)
	}

	releaseTask := make(chan struct{})
	closeReleaseTask := poolWithIDCloseOnCleanup(t, releaseTask)
	started := make(chan struct{})
	if err := p.Submit(1, func() {
		close(started)
		<-releaseTask
	}); err != nil {
		t.Fatalf("submit closing task: %v", err)
	}
	poolWithIDReceive(t, started)
	p.Release()
	if state := atomic.LoadInt32(&p.state); state != CLOSING {
		t.Fatalf("state after Release = %d, want CLOSING", state)
	}
	p.Tune(1)
	if got := p.Cap(); got != 1 {
		t.Fatalf("CLOSING Tune capacity = %d, want 1", got)
	}
	closeReleaseTask()
	poolWithIDReceive(t, p.closedDone)

	p.Tune(4)
	if got := p.Cap(); got != 4 {
		t.Fatalf("CLOSED Tune capacity = %d, want 4", got)
	}
	p.Reboot()
	if got := p.Cap(); got != 4 {
		t.Fatalf("Reboot capacity = %d, want 4", got)
	}
	nextRelease := make(chan struct{})
	closeNextRelease := poolWithIDCloseOnCleanup(t, nextRelease)
	nextStarted := make([]chan struct{}, 4)
	for i := range nextStarted {
		nextStarted[i] = make(chan struct{})
		startedTask := nextStarted[i]
		if err := p.Submit(10+i, func() {
			close(startedTask)
			<-nextRelease
		}); err != nil {
			t.Fatalf("submit next-generation ID %d: %v", 10+i, err)
		}
	}
	for _, startedTask := range nextStarted {
		poolWithIDReceive(t, startedTask)
	}
	if got := p.Running(); got != 4 {
		t.Fatalf("next-generation Running = %d, want 4", got)
	}
	closeNextRelease()

	infinite := newPoolWithIDForTest(t, -1, WithDisablePurge(true))
	infinite.Tune(2)
	if got := infinite.Cap(); got != -1 {
		t.Fatalf("infinite PoolWithID Tune capacity = %d, want -1", got)
	}
}

func TestPoolWithIDTuneRejectsInvalidSizes(t *testing.T) {
	p := newPoolWithIDForTest(t, 2, WithDisablePurge(true))
	for _, size := range []int{0, -1, 2} {
		p.Tune(size)
		if got := p.Cap(); got != 2 {
			t.Fatalf("Tune(%d) capacity = %d, want 2", size, got)
		}
	}

	if err := p.Submit(1, func() {}); err != nil && !errors.Is(err, ErrPoolClosed) {
		t.Fatalf("pool unusable after invalid Tune sizes: %v", err)
	}
}
