package ants

import (
	"errors"
	"runtime"
	"strings"
	"sync"
	"testing"
)

func poolWithIDPhase4AssertQueueFull(t *testing.T, p *PoolWithID, id int) *workerIDEntry {
	t.Helper()
	entry := poolWithIDEntryForTest(t, p, id)
	if got, want := len(entry.tasks), cap(entry.tasks); got != want {
		t.Fatalf("physical queue length = %d, want full capacity %d", got, want)
	}
	return entry
}

func poolWithIDPhase4WaitForWaiting(t *testing.T, p *PoolWithID, want int) {
	t.Helper()
	waitForReservationCondition(t, func() bool { return p.Waiting() == want })
}

func poolWithIDPhase4IDClockGoroutines() int {
	records := make([]runtime.StackRecord, runtime.NumGoroutine()+32)
	for {
		n, ok := runtime.GoroutineProfile(records)
		if !ok {
			records = make([]runtime.StackRecord, n+32)
			continue
		}

		count := 0
		for _, record := range records[:n] {
			frames := runtime.CallersFrames(record.Stack())
			for {
				frame, more := frames.Next()
				if strings.HasSuffix(frame.Function, ".tickIDClock") {
					count++
					break
				}
				if !more {
					break
				}
			}
		}
		return count
	}
}

func TestPoolWithIDWaitingExistingQueueLimitAndRelease(t *testing.T) {
	const id = 701
	p := newPoolWithIDForTest(t, 1,
		WithDisablePurge(true),
		WithTaskBuffer(1),
		WithMaxBlockingTasks(1),
	)

	releaseRunning := make(chan struct{})
	closeReleaseRunning := poolWithIDCloseOnCleanup(t, releaseRunning)
	runningStarted := make(chan struct{})
	if err := p.Submit(id, func() {
		close(runningStarted)
		<-releaseRunning
	}); err != nil {
		t.Fatalf("Submit(running task): %v", err)
	}
	poolWithIDReceive(t, runningStarted)
	for i := 0; i < 2; i++ {
		if err := p.Submit(id, func() {}); err != nil {
			t.Fatalf("Submit(queue filler %d): %v", i, err)
		}
	}
	poolWithIDPhase4AssertQueueFull(t, p, id)

	waitRegistered := make(chan struct{})
	var waitOnce sync.Once
	p.testHooks.afterQueueWaitRegistered = func() {
		waitOnce.Do(func() { close(waitRegistered) })
	}

	firstResult := make(chan error, 1)
	go func() { firstResult <- p.Submit(id, func() {}) }()
	poolWithIDReceive(t, waitRegistered)
	if got := p.Waiting(); got != 1 {
		t.Fatalf("Waiting() with one physical-queue blocker = %d, want 1", got)
	}

	if err := p.Submit(id, func() {}); !errors.Is(err, ErrPoolOverload) {
		t.Fatalf("second blocked Submit error = %v, want %v", err, ErrPoolOverload)
	}
	if got := p.Waiting(); got != 1 {
		t.Fatalf("Waiting() after overload = %d, want 1", got)
	}

	p.Release()
	if err := poolWithIDReceive(t, firstResult); !errors.Is(err, ErrPoolClosed) {
		t.Fatalf("queue waiter after Release error = %v, want %v", err, ErrPoolClosed)
	}
	poolWithIDPhase4WaitForWaiting(t, p, 0)
	closeReleaseRunning()
	if err := p.ReleaseTimeout(poolWithIDTestTimeout); err != nil &&
		!errors.Is(err, ErrPoolClosed) {
		t.Fatalf("ReleaseTimeout: %v", err)
	}
}

func TestPoolWithIDBlockingSecondSendConsumesOpenedQueueSlot(t *testing.T) {
	const (
		id      = 702
		otherID = 703
	)
	p := newPoolWithIDForTest(t, 1,
		WithDisablePurge(true),
		WithTaskBuffer(1),
		WithMaxBlockingTasks(1),
	)

	releaseFirst := make(chan struct{})
	closeReleaseFirst := poolWithIDCloseOnCleanup(t, releaseFirst)
	firstStarted := make(chan struct{})
	if err := p.Submit(id, func() {
		close(firstStarted)
		<-releaseFirst
	}); err != nil {
		t.Fatalf("Submit(first running task): %v", err)
	}
	poolWithIDReceive(t, firstStarted)

	releaseSecond := make(chan struct{})
	closeReleaseSecond := poolWithIDCloseOnCleanup(t, releaseSecond)
	secondStarted := make(chan struct{})
	if err := p.Submit(id, func() {
		close(secondStarted)
		<-releaseSecond
	}); err != nil {
		t.Fatalf("Submit(second task): %v", err)
	}
	if err := p.Submit(id, func() {}); err != nil {
		t.Fatalf("Submit(queue filler): %v", err)
	}
	poolWithIDPhase4AssertQueueFull(t, p, id)

	capacityWaitRegistered := make(chan struct{})
	var capacityOnce sync.Once
	p.testHooks.afterCapacityWaitRegistered = func() {
		capacityOnce.Do(func() { close(capacityWaitRegistered) })
	}
	fastSendMissed := make(chan struct{})
	allowSecondSend := make(chan struct{})
	closeAllowSecondSend := poolWithIDCloseOnCleanup(t, allowSecondSend)
	var fastMissOnce sync.Once
	p.testHooks.afterQueueFastSendMiss = func() {
		fastMissOnce.Do(func() { close(fastSendMissed) })
		<-allowSecondSend
	}
	queueWaitRegistered := make(chan struct{}, 1)
	p.testHooks.afterQueueWaitRegistered = func() {
		select {
		case queueWaitRegistered <- struct{}{}:
		default:
		}
	}

	capacityResult := make(chan error, 1)
	go func() { capacityResult <- p.Submit(otherID, func() {}) }()
	poolWithIDReceive(t, capacityWaitRegistered)
	if got := p.Waiting(); got != 1 {
		t.Fatalf("Waiting() with capacity token occupied = %d, want 1", got)
	}

	queuedTaskDone := make(chan struct{})
	queueResult := make(chan error, 1)
	go func() {
		queueResult <- p.Submit(id, func() { close(queuedTaskDone) })
	}()
	poolWithIDReceive(t, fastSendMissed)

	closeReleaseFirst()
	poolWithIDReceive(t, secondStarted)
	closeAllowSecondSend()
	if err := poolWithIDReceive(t, queueResult); err != nil {
		t.Fatalf("Submit after queue slot opened: %v", err)
	}
	select {
	case <-queueWaitRegistered:
		t.Fatal("second nonblocking send acquired a queue waiter token")
	default:
	}
	if got := p.Waiting(); got != 1 {
		t.Fatalf("Waiting() after second send = %d, want existing capacity waiter only", got)
	}

	p.Release()
	if err := poolWithIDReceive(t, capacityResult); !errors.Is(err, ErrPoolClosed) {
		t.Fatalf("capacity waiter after Release error = %v, want %v", err, ErrPoolClosed)
	}
	poolWithIDPhase4WaitForWaiting(t, p, 0)
	closeReleaseSecond()
	poolWithIDReceive(t, queuedTaskDone)
	if err := p.ReleaseTimeout(poolWithIDTestTimeout); err != nil &&
		!errors.Is(err, ErrPoolClosed) {
		t.Fatalf("ReleaseTimeout: %v", err)
	}
}

func TestPoolWithIDWaitingTransferredTokenIsSingleCount(t *testing.T) {
	const id = 704
	p := newPoolWithIDForTest(t, 1,
		WithDisablePurge(true),
		WithTaskBuffer(1),
		WithMaxBlockingTasks(1),
	)

	releaseRunning := make(chan struct{})
	closeReleaseRunning := poolWithIDCloseOnCleanup(t, releaseRunning)
	runningStarted := make(chan struct{})
	if err := p.Submit(id, func() {
		close(runningStarted)
		<-releaseRunning
	}); err != nil {
		t.Fatalf("Submit(running task): %v", err)
	}
	poolWithIDReceive(t, runningStarted)
	for i := 0; i < 2; i++ {
		if err := p.Submit(id, func() {}); err != nil {
			t.Fatalf("Submit(queue filler %d): %v", i, err)
		}
	}
	entry := poolWithIDPhase4AssertQueueFull(t, p, id)

	// Capacity and reservation waits intentionally share this stage-neutral
	// token. Entering an adjacent queue wait must retain one count, not acquire
	// a second slot or reject itself at MaxBlockingTasks=1.
	waiter := poolWithIDWaiter{}
	p.lock.Lock()
	if !p.acquireWaiterLocked(&waiter) {
		p.lock.Unlock()
		t.Fatal("failed to seed transferred waiter token")
	}
	stop := p.submitStop
	generation := p.generation.Load()
	p.lock.Unlock()

	waitRegistered := make(chan struct{})
	var waitOnce sync.Once
	p.testHooks.afterQueueWaitRegistered = func() {
		waitOnce.Do(func() { close(waitRegistered) })
	}
	result := make(chan error, 1)
	go func() {
		result <- p.submitBlocking(entry, stop, generation, func() {}, &waiter)
	}()
	poolWithIDReceive(t, waitRegistered)
	if got := p.Waiting(); got != 1 {
		t.Fatalf("Waiting() after transferred-token queue admission = %d, want 1", got)
	}
	select {
	case err := <-result:
		t.Fatalf("transferred-token queue wait returned early: %v", err)
	default:
	}

	p.Release()
	if err := poolWithIDReceive(t, result); !errors.Is(err, ErrPoolClosed) {
		t.Fatalf("transferred queue waiter after Release error = %v, want %v", err, ErrPoolClosed)
	}
	poolWithIDPhase4WaitForWaiting(t, p, 0)
	closeReleaseRunning()
	if err := p.ReleaseTimeout(poolWithIDTestTimeout); err != nil &&
		!errors.Is(err, ErrPoolClosed) {
		t.Fatalf("ReleaseTimeout: %v", err)
	}
}

func TestPoolWithIDBackgroundDisablePurgeHasNoPerPoolIDClock(t *testing.T) {
	const poolCount = 64
	before := poolWithIDPhase4IDClockGoroutines()
	pools := make([]*PoolWithID, 0, poolCount)
	t.Cleanup(func() {
		for _, p := range pools {
			if err := p.ReleaseTimeout(poolWithIDTestTimeout); err != nil &&
				!errors.Is(err, ErrPoolClosed) {
				t.Errorf("ReleaseTimeout() during cleanup: %v", err)
			}
		}
	})

	for i := 0; i < poolCount; i++ {
		p, err := NewPoolWithID(1,
			WithDisablePurge(true),
			WithLogger(poolWithIDDiscardLogger{}),
		)
		if err != nil {
			t.Fatalf("NewPoolWithID(%d): %v", i, err)
		}
		pools = append(pools, p)
	}
	runtime.Gosched()
	after := poolWithIDPhase4IDClockGoroutines()
	if after > before {
		t.Fatalf("PoolWithID clock goroutines grew from %d to %d after creating %d DisablePurge pools",
			before, after, poolCount)
	}
}
