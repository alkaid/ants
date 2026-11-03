package ants

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"testing"
	"time"
)

type poolReleaseContextBarrier struct {
	context.Context
	once    sync.Once
	reached chan struct{}
	resume  chan struct{}
}

func (ctx *poolReleaseContextBarrier) Done() <-chan struct{} {
	ctx.once.Do(func() {
		close(ctx.reached)
		<-ctx.resume
	})
	return ctx.Context.Done()
}

func newPoolForLifecycleTest(t *testing.T, kind string) (*poolCommon, func(func()) error) {
	t.Helper()
	var (
		common *poolCommon
		submit func(func()) error
		err    error
	)
	switch kind {
	case "pool":
		var p *Pool
		p, err = NewPool(1, WithDisablePurge(true))
		if err == nil {
			common, submit = p.poolCommon, p.Submit
		}
	case "func":
		var p *PoolWithFunc
		p, err = NewPoolWithFunc(1, func(task any) { task.(func())() }, WithDisablePurge(true))
		if err == nil {
			common, submit = p.poolCommon, func(task func()) error { return p.Invoke(task) }
		}
	case "generic":
		var p *PoolWithFuncGeneric[func()]
		p, err = NewPoolWithFuncGeneric(1, func(task func()) { task() }, WithDisablePurge(true))
		if err == nil {
			common, submit = p.poolCommon, p.Invoke
		}
	}
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := common.ReleaseTimeout(poolWithIDTestTimeout); err != nil && !errors.Is(err, ErrPoolClosed) {
			t.Errorf("ReleaseTimeout during cleanup: %v", err)
		}
	})
	return common, submit
}

func TestPoolReleaseRejectsPendingWorkerRetrieval(t *testing.T) {
	for _, kind := range []string{"pool", "func", "generic"} {
		t.Run(kind, func(t *testing.T) {
			p, submit := newPoolForLifecycleTest(t, kind)
			barrier := &poolTestLockBarrier{
				Locker:  p.lock,
				reached: make(chan struct{}),
				resume:  make(chan struct{}),
			}
			p.lock = barrier
			p.cond.L = barrier
			resume := poolWithIDCloseOnCleanup(t, barrier.resume)
			barrier.armed.Store(true)
			result := make(chan error, 1)
			go func() { result <- submit(func() {}) }()
			poolWithIDReceive(t, barrier.reached)

			// The public Submit/Invoke check saw OPENED, but Release completes
			// before retrieveWorker can acquire the admission lock.
			if err := p.ReleaseTimeout(poolWithIDTestTimeout); err != nil {
				t.Fatal(err)
			}
			resume()
			if err := poolWithIDReceive(t, result); !errors.Is(err, ErrPoolClosed) {
				t.Fatalf("submission after release = %v, want %v", err, ErrPoolClosed)
			}

			p.Reboot()
			completed := make(chan struct{})
			if err := submit(func() { close(completed) }); err != nil {
				t.Fatalf("submission after reboot: %v", err)
			}
			poolWithIDReceive(t, completed)
			if err := p.ReleaseTimeout(poolWithIDTestTimeout); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestPoolReleaseContextDoesNotWaitForRebootedGeneration(t *testing.T) {
	for _, kind := range []string{"pool", "func", "generic"} {
		t.Run(kind, func(t *testing.T) {
			p, submit := newPoolForLifecycleTest(t, kind)
			baseCtx, cancel := context.WithTimeout(context.Background(), poolWithIDTestTimeout)
			defer cancel()
			ctx := &poolReleaseContextBarrier{
				Context: baseCtx,
				reached: make(chan struct{}),
				resume:  make(chan struct{}),
			}
			resume := poolWithIDCloseOnCleanup(t, ctx.resume)
			released := make(chan error, 1)
			go func() { released <- p.ReleaseContext(ctx) }()
			poolWithIDReceive(t, ctx.reached)

			// Pause the release waiter after it closes the pool, then keep a
			// worker busy in the next generation before letting it resume.
			p.Reboot()
			started := make(chan struct{})
			finish := make(chan struct{})
			finishTask := poolWithIDCloseOnCleanup(t, finish)
			if err := submit(func() {
				close(started)
				<-finish
			}); err != nil {
				t.Fatal(err)
			}
			poolWithIDReceive(t, started)
			resume()
			if err := poolWithIDReceive(t, released); err != nil {
				t.Fatalf("old generation's ReleaseContext: %v", err)
			}
			if p.IsClosed() || p.Running() != 1 {
				t.Fatalf("new generation interrupted: closed=%v, running=%d", p.IsClosed(), p.Running())
			}
			finishTask()
		})
	}
}

func TestPoolConcurrentRebootJoinsSameGeneration(t *testing.T) {
	for _, kind := range []string{"pool", "func", "generic"} {
		t.Run(kind, func(t *testing.T) {
			p, submit := newPoolForLifecycleTest(t, kind)
			started := make(chan struct{})
			finish := make(chan struct{})
			finishTask := poolWithIDCloseOnCleanup(t, finish)
			if err := submit(func() {
				close(started)
				<-finish
			}); err != nil {
				t.Fatal(err)
			}
			poolWithIDReceive(t, started)
			p.Release()
			counter := &poolWithIDP2CountingLock{Locker: p.lock}
			p.lock = counter
			p.cond.L = counter
			rebooted := make(chan struct{}, 2)
			for i := 0; i < 2; i++ {
				go func() {
					p.Reboot()
					rebooted <- struct{}{}
				}()
			}
			// With purge disabled and the old worker still blocked, only the
			// two Reboot calls can acquire the lock to capture this generation.
			deadline := time.NewTimer(poolWithIDTestTimeout)
			defer deadline.Stop()
			for counter.locks.Load() < 2 {
				select {
				case <-deadline.C:
					t.Fatal("Reboot calls did not both capture the closing generation")
				default:
					runtime.Gosched()
				}
			}
			finishTask()
			poolWithIDReceive(t, rebooted)
			poolWithIDReceive(t, rebooted)

			completed := make(chan struct{})
			if err := submit(func() { close(completed) }); err != nil {
				t.Fatalf("submission after concurrent reboot: %v", err)
			}
			poolWithIDReceive(t, completed)
		})
	}
}
