package ants

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type poolWithIDClosingObserverLock struct {
	sync.Locker
	state    *int32
	armed    atomic.Bool
	once     sync.Once
	observed chan int32
}

type poolWithIDUnlockBarrierLock struct {
	sync.Locker
	armed    atomic.Bool
	unlocked chan struct{}
	proceed  <-chan struct{}
}

func (l *poolWithIDUnlockBarrierLock) Unlock() {
	l.Locker.Unlock()
	if l.armed.CompareAndSwap(true, false) {
		close(l.unlocked)
		<-l.proceed
	}
}

func (l *poolWithIDClosingObserverLock) Unlock() {
	if l.armed.Load() {
		l.once.Do(func() {
			l.observed <- atomic.LoadInt32(l.state)
		})
	}
	l.Locker.Unlock()
}

func TestPoolWithIDPendingSubmitSurvivesSyntheticPurge(t *testing.T) {
	const id = 61
	p := newPoolWithIDForTest(t, 1,
		WithTaskBuffer(1),
		WithExpiryDuration(time.Hour),
	)

	idleReady := make(chan struct{})
	p.testHooks.afterTaskFinished = func() { close(idleReady) }
	if err := p.Submit(id, func() {}); err != nil {
		t.Fatalf("submit warm-up task: %v", err)
	}
	poolWithIDReceive(t, idleReady)
	p.testHooks.afterTaskFinished = nil

	entry, owner, _ := poolWithIDObserveEntryState(t, p, id)
	entry.mu.Lock()
	lastIdleAt := entry.lastIdleAt
	if !entry.drained() {
		entry.mu.Unlock()
		t.Fatal("warm-up entry is not idle")
	}
	entry.mu.Unlock()

	registered := make(chan struct{})
	allowSend := make(chan struct{})
	closeAllowSend := poolWithIDCloseOnCleanup(t, allowSend)
	p.testHooks.afterSubmitRegistered = func() {
		close(registered)
		<-allowSend
	}

	taskFinished := make(chan struct{})
	submitResult := make(chan error, 1)
	go func() {
		submitResult <- p.Submit(id, func() { close(taskFinished) })
	}()
	poolWithIDReceive(t, registered)
	p.testHooks.afterSubmitRegistered = nil

	entry.mu.Lock()
	if entry.pendingSubmits != 1 || entry.outstanding != 1 {
		pending, outstanding := entry.pendingSubmits, entry.outstanding
		entry.mu.Unlock()
		t.Fatalf("registered submit state = pending:%d outstanding:%d, want 1/1", pending, outstanding)
	}
	entry.mu.Unlock()

	p.purgeExpired(lastIdleAt + int64(p.options.ExpiryDuration))
	currentEntry, currentOwner, _ := poolWithIDObserveEntryState(t, p, id)
	if currentEntry != entry || currentOwner != owner {
		t.Fatal("purge retired an entry with a registered submit")
	}

	closeAllowSend()
	if err := poolWithIDReceive(t, submitResult); err != nil {
		t.Fatalf("registered Submit after purge: %v", err)
	}
	poolWithIDReceive(t, taskFinished)
}

func TestPoolWithIDCapacityReservedBeforeRegistryUnlock(t *testing.T) {
	p := newPoolWithIDForTest(t, 1,
		WithTaskBuffer(1),
		WithNonblocking(true),
		WithDisablePurge(true),
	)

	proceed := make(chan struct{})
	closeProceed := poolWithIDCloseOnCleanup(t, proceed)
	barrier := &poolWithIDUnlockBarrierLock{
		Locker:   p.lock,
		unlocked: make(chan struct{}),
		proceed:  proceed,
	}
	barrier.armed.Store(true)
	p.lock = barrier

	firstFinished := make(chan struct{})
	firstResult := make(chan error, 1)
	go func() {
		firstResult <- p.Submit(71, func() { close(firstFinished) })
	}()
	poolWithIDReceive(t, barrier.unlocked)

	if got := p.Running(); got != 1 {
		t.Fatalf("Running() at registry unlock = %d, want reserved capacity 1", got)
	}
	secondResult := make(chan error, 1)
	go func() {
		secondResult <- p.Submit(72, func() {})
	}()
	if err := poolWithIDReceive(t, secondResult); !errors.Is(err, ErrPoolOverload) {
		t.Fatalf("second new-ID Submit() during unlock barrier error = %v, want %v", err, ErrPoolOverload)
	}

	closeProceed()
	if err := poolWithIDReceive(t, firstResult); err != nil {
		t.Fatalf("first new-ID Submit() error = %v", err)
	}
	poolWithIDReceive(t, firstFinished)
	if got := p.Running(); got > p.Cap() {
		t.Fatalf("Running() after competing submissions = %d, capacity = %d", got, p.Cap())
	}
}

func TestPoolWithIDRebootJoinsClosingGeneration(t *testing.T) {
	const id = 62
	p := newPoolWithIDForTest(t, 1,
		WithTaskBuffer(1),
		WithDisablePurge(true),
	)

	taskStarted := make(chan struct{})
	releaseTask := make(chan struct{})
	closeReleaseTask := poolWithIDCloseOnCleanup(t, releaseTask)
	if err := p.Submit(id, func() {
		close(taskStarted)
		<-releaseTask
	}); err != nil {
		t.Fatalf("submit closing-generation task: %v", err)
	}
	poolWithIDReceive(t, taskStarted)

	generation := p.generation.Load()
	p.Release()
	if state := atomic.LoadInt32(&p.state); state != CLOSING {
		t.Fatalf("state after Release = %d, want CLOSING", state)
	}

	observer := &poolWithIDClosingObserverLock{
		Locker:   p.lock,
		state:    &p.state,
		observed: make(chan int32, 1),
	}
	p.lock = observer
	observer.armed.Store(true)

	rebootDone := make(chan struct{})
	go func() {
		p.Reboot()
		close(rebootDone)
	}()
	if state := poolWithIDReceive(t, observer.observed); state != CLOSING {
		t.Fatalf("state observed by Reboot unlock = %d, want CLOSING", state)
	}

	closeReleaseTask()
	poolWithIDReceive(t, rebootDone)
	if state := atomic.LoadInt32(&p.state); state != OPENED {
		t.Fatalf("state after Reboot joined closing generation = %d, want OPENED", state)
	}
	if got := p.generation.Load(); got != generation+1 {
		t.Fatalf("generation after Reboot = %d, want %d", got, generation+1)
	}

	postRebootFinished := make(chan struct{})
	if err := p.Submit(id, func() { close(postRebootFinished) }); err != nil {
		t.Fatalf("submit after Reboot: %v", err)
	}
	poolWithIDReceive(t, postRebootFinished)
}

func TestPoolWithIDRunningPlusEscapedMatchesLiveWorkers(t *testing.T) {
	const id = 63
	p := poolWithIDObserveNewPool(t, 1)

	releaseA := make(chan struct{})
	releaseB := make(chan struct{})
	closeReleaseA := poolWithIDCloseOnCleanup(t, releaseA)
	closeReleaseB := poolWithIDCloseOnCleanup(t, releaseB)
	aStarted := make(chan struct{})
	aFinished := make(chan struct{})
	bStarted := make(chan struct{})
	bFinished := make(chan struct{})
	var live atomic.Int32

	if err := p.Submit(id, func() {
		live.Add(1)
		defer live.Add(-1)
		close(aStarted)
		<-releaseA
		close(aFinished)
	}); err != nil {
		t.Fatalf("submit task A: %v", err)
	}
	poolWithIDReceive(t, aStarted)
	if err := p.Submit(id, func() {
		live.Add(1)
		defer live.Add(-1)
		close(bStarted)
		<-releaseB
		close(bFinished)
	}); err != nil {
		t.Fatalf("submit task B: %v", err)
	}

	_, _, startedAt := poolWithIDObserveEntryState(t, p, id)
	p.purgeExpired(startedAt + int64(p.options.ExpiryDuration))
	poolWithIDObserveReceive(t, p.EscapeEvents(), "task A escape")
	poolWithIDObserveReceive(t, bStarted, "task B start")

	snapshot := p.EscapeSnapshot()
	managedAndEscaped := p.Running() + snapshot.Total
	if got := int(live.Load()); got != managedAndEscaped || got != 2 {
		t.Fatalf("live workers = %d, Running()+escaped = %d+%d=%d, want 2",
			got, p.Running(), snapshot.Total, managedAndEscaped)
	}
	if got := p.Free(); got != 0 {
		t.Fatalf("Free() with one managed owner = %d, want 0", got)
	}

	closeReleaseA()
	poolWithIDReceive(t, aFinished)
	poolWithIDObserveReceive(t, p.EscapeEvents(), "task A escaped-worker exit")
	if snapshot = p.EscapeSnapshot(); snapshot.Total != 0 || len(snapshot.ByID) != 0 {
		t.Fatalf("escaped task A left stale state: %+v", snapshot)
	}
	if got, want := int(live.Load()), p.Running()+snapshot.Total; got != want || got != 1 {
		t.Fatalf("live workers after escaped exit = %d, Running()+escaped = %d, want 1", got, want)
	}

	closeReleaseB()
	poolWithIDReceive(t, bFinished)
}
