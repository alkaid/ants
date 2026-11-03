package ants

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// poolTestLockBarrier pauses one acquisition before taking the underlying lock.
type poolTestLockBarrier struct {
	sync.Locker
	armed   atomic.Bool
	reached chan struct{}
	resume  chan struct{}
}

func (b *poolTestLockBarrier) Lock() {
	if b.armed.CompareAndSwap(true, false) {
		close(b.reached)
		<-b.resume
	}
	b.Locker.Lock()
}

func TestPoolWithIDOverlappingSubmitFinalizersKeepEntryRegistered(t *testing.T) {
	for _, retirement := range []string{"purge", "tune", "release"} {
		t.Run(retirement, func(t *testing.T) {
			p := newPoolWithIDForTest(t, 2,
				WithExpiryDuration(time.Hour), WithDisablePurgeRunning(true))
			poolWithIDP2StopPurge(t, p)

			// Keep a second owner busy so Tune(1) must retire the first ID.
			keeperStarted := make(chan struct{})
			keeperDone := make(chan struct{})
			poolWithIDCloseOnCleanup(t, keeperDone)
			if err := p.Submit(2, func() {
				close(keeperStarted)
				<-keeperDone
			}); err != nil {
				t.Fatal(err)
			}
			poolWithIDReceive(t, keeperStarted)

			barrier := &poolTestLockBarrier{
				Locker:  p.registry.expiryMu,
				reached: make(chan struct{}),
				resume:  make(chan struct{}),
			}
			p.registry.expiryMu = barrier
			resume := poolWithIDCloseOnCleanup(t, barrier.resume)
			finished := []chan struct{}{make(chan struct{}), make(chan struct{})}
			var taskFinishes, submitFinishes atomic.Int32
			p.testHooks.afterTaskFinished = func() {
				if index := int(taskFinishes.Add(1)) - 1; index < len(finished) {
					close(finished[index])
				}
			}
			p.testHooks.beforeSubmitFinished = func() {
				index := int(submitFinishes.Add(1)) - 1
				if index < len(finished) {
					<-finished[index]
				}
				if index == 0 {
					barrier.armed.Store(true)
				}
			}

			firstDone := make(chan error, 1)
			go func() { firstDone <- p.Submit(1, func() {}) }()
			poolWithIDReceive(t, barrier.reached)
			entry := poolWithIDEntryForTest(t, p, 1)
			if err := p.Submit(1, func() {}); err != nil {
				t.Fatal(err)
			}

			switch retirement {
			case "purge":
				entry.mu.Lock()
				now := entry.lastIdleAt.Add(p.options.ExpiryDuration)
				entry.mu.Unlock()
				p.purgeExpired(now)
			case "tune":
				p.Tune(1)
			case "release":
				p.Release()
			}
			p.lock.Lock()
			registered := p.registry.items[1] == entry
			p.lock.Unlock()
			if !registered {
				t.Error("entry retired while a Submit was still updating its expiry index")
			}

			resume()
			if err := poolWithIDReceive(t, firstDone); err != nil {
				t.Fatal(err)
			}
			p.lock.Lock()
			p.registry.expiryMu.Lock()
			stale := p.registry.items[1] != entry && entry.expiryList != workerIDEntryListNone
			p.registry.expiryMu.Unlock()
			p.lock.Unlock()
			if stale {
				t.Error("completed Submit reinserted a retired entry into the expiry index")
			}

			// Exercise a bounded production batch so a stale head fails the test
			// without hanging in the outer purge loop.
			entry.mu.Lock()
			now := entry.lastIdleAt.Add(p.options.ExpiryDuration)
			entry.mu.Unlock()
			scan := poolWithIDPurgeScan{}
			more, visited := p.purgeExpiredBatch(func() time.Time { return now }, now.Sub, 4, &scan)
			if more {
				t.Errorf("purge did not finish scanning one idle entry: visited=%d", visited)
			}
		})
	}
}

func TestPoolWithIDEscapedPanicCallbackRetainsBudget(t *testing.T) {
	for _, callback := range []string{"handler", "logger", "panicking handler"} {
		t.Run(callback, func(t *testing.T) {
			clock := newPoolWithIDPhase3FakeClock()
			logger := newPoolWithIDPhase3BlockingLogger()
			options := []Option{
				WithExpiryDuration(24 * time.Hour),
				WithRunningTaskTimeout(time.Hour),
				WithMaxEscapedWorkers(1),
				WithMaxEscapedWorkersPerID(1),
				WithLogger(logger),
			}
			switch callback {
			case "handler":
				options = append(options, WithPanicHandler(func(any) {
					close(logger.entered)
					<-logger.unblock
				}))
			case "panicking handler":
				options = append(options, WithPanicHandler(func(any) {
					panic("handler failed")
				}))
			}
			p := poolWithIDPhase3NewPool(t, 1, clock, options...)
			poolWithIDP2StopPurge(t, p)
			unblockCallback := poolWithIDCloseOnCleanup(t, logger.unblock)
			panicNow := make(chan struct{})
			triggerPanic := poolWithIDCloseOnCleanup(t, panicNow)
			started := make(chan struct{})
			if err := p.Submit(1, func() {
				close(started)
				<-panicNow
				panic("escaped task failed")
			}); err != nil {
				t.Fatal(err)
			}
			poolWithIDReceive(t, started)
			clock.Advance(time.Hour)
			p.purgeExpiredNow()
			escape := poolWithIDReceive(t, p.EscapeEvents())
			poolWithIDPhase3AssertEvent(t, escape, PoolWithIDWorkerEscaped, 1, 1, 1, 1, 0, 1, 1)
			triggerPanic()
			poolWithIDReceive(t, logger.entered)
			if p.Escaped() != 1 || p.TotalWorkers() != 2 {
				t.Fatalf("blocked callback lost its worker budget: escaped=%d total=%d, want 1/2",
					p.Escaped(), p.TotalWorkers())
			}
			poolWithIDPhase3AssertNoSignal(t, p.EscapeEvents(), "premature worker exit")

			replacementStarted := make(chan struct{})
			replacementDone := make(chan struct{})
			finishReplacement := poolWithIDCloseOnCleanup(t, replacementDone)
			if err := p.Submit(1, func() {
				close(replacementStarted)
				<-replacementDone
			}); err != nil {
				t.Fatal(err)
			}
			poolWithIDReceive(t, replacementStarted)
			clock.Advance(time.Hour)
			p.purgeExpiredNow()
			exhausted := poolWithIDReceive(t, p.EscapeEvents())
			poolWithIDPhase3AssertEvent(t, exhausted, PoolWithIDEscapeBudgetExhausted, 1, 1, 1, 1,
				PoolWithIDEscapeGlobalBudgetExhausted|PoolWithIDEscapePerIDBudgetExhausted, 1, 1)

			unblockCallback()
			exit := poolWithIDReceive(t, p.EscapeEvents())
			poolWithIDPhase3AssertEvent(t, exit, PoolWithIDEscapedWorkerExited, 1, 1, 0, 0, 0, 1, 1)
			p.purgeExpiredNow()
			escape = poolWithIDReceive(t, p.EscapeEvents())
			poolWithIDPhase3AssertEvent(t, escape, PoolWithIDWorkerEscaped, 1, 1, 1, 1, 0, 1, 1)
			finishReplacement()
			exit = poolWithIDReceive(t, p.EscapeEvents())
			poolWithIDPhase3AssertEvent(t, exit, PoolWithIDEscapedWorkerExited, 1, 1, 0, 0, 0, 1, 1)
		})
	}
}
