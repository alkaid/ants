// MIT License

// Copyright (c) 2018 Andy Pan

// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:

// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.

// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package ants

import (
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const poolWithIDTestTimeout = 3 * time.Second

type poolWithIDDiscardLogger struct{}

func (poolWithIDDiscardLogger) Printf(string, ...any) {}

func newPoolWithIDForTest(t *testing.T, size int, options ...Option) *PoolWithID {
	t.Helper()
	options = append(options, WithLogger(poolWithIDDiscardLogger{}))
	p, err := NewPoolWithID(size, options...)
	if err != nil {
		t.Fatalf("NewPoolWithID() error = %v", err)
	}
	t.Cleanup(func() {
		err := p.ReleaseTimeout(poolWithIDTestTimeout)
		if err != nil && !errors.Is(err, ErrPoolClosed) {
			t.Errorf("ReleaseTimeout() during cleanup error = %v", err)
		}
	})
	return p
}

func poolWithIDCloseOnCleanup(t *testing.T, ch chan struct{}) func() {
	t.Helper()
	var once sync.Once
	closeChannel := func() {
		once.Do(func() { close(ch) })
	}
	t.Cleanup(closeChannel)
	return closeChannel
}

func poolWithIDReceive[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(poolWithIDTestTimeout):
		t.Fatal("timed out waiting for test synchronization")
		var zero T
		return zero
	}
}

func poolWithIDEntryForTest(t *testing.T, p *PoolWithID, id int) *workerIDEntry {
	t.Helper()
	p.lock.Lock()
	entry := p.registry.items[id]
	p.lock.Unlock()
	if entry == nil {
		t.Fatalf("entry for ID %d was not registered", id)
	}
	return entry
}

func TestPoolWithIDTaskBufferConfiguration(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		p := newPoolWithIDForTest(t, 1, WithDisablePurge(true))
		if p.admissionLimit != DefaultTaskBuffer {
			t.Fatalf("admission limit = %d, want %d", p.admissionLimit, DefaultTaskBuffer)
		}

		started := make(chan struct{})
		unblock := make(chan struct{})
		closeUnblock := poolWithIDCloseOnCleanup(t, unblock)
		if err := p.Submit(1, func() {
			close(started)
			<-unblock
		}); err != nil {
			t.Fatalf("Submit() error = %v", err)
		}
		poolWithIDReceive(t, started)
		if got, want := cap(poolWithIDEntryForTest(t, p, 1).tasks), 2*DefaultTaskBuffer; got != want {
			t.Fatalf("physical task capacity = %d, want %d", got, want)
		}
		closeUnblock()
	})

	t.Run("one hundred", func(t *testing.T) {
		p := newPoolWithIDForTest(t, 1, WithTaskBuffer(100), WithDisablePurge(true))
		if p.admissionLimit != 100 {
			t.Fatalf("admission limit = %d, want 100", p.admissionLimit)
		}

		started := make(chan struct{})
		unblock := make(chan struct{})
		closeUnblock := poolWithIDCloseOnCleanup(t, unblock)
		if err := p.Submit(1, func() {
			close(started)
			<-unblock
		}); err != nil {
			t.Fatalf("Submit() error = %v", err)
		}
		poolWithIDReceive(t, started)
		if got := cap(poolWithIDEntryForTest(t, p, 1).tasks); got != 200 {
			t.Fatalf("physical task capacity = %d, want 200", got)
		}
		closeUnblock()
	})

	t.Run("invalid final value", func(t *testing.T) {
		maxInt := int(^uint(0) >> 1)
		for _, taskBuffer := range []int{-1, MaxTaskBuffer + 1, maxInt / 2} {
			p, err := NewPoolWithID(1, WithTaskBuffer(taskBuffer))
			if !errors.Is(err, ErrInvalidPoolWithIDTaskBuffer) {
				t.Fatalf("NewPoolWithID(TaskBuffer=%d) error = %v, want %v", taskBuffer, err, ErrInvalidPoolWithIDTaskBuffer)
			}
			if p != nil {
				t.Fatalf("NewPoolWithID(TaskBuffer=%d) returned non-nil pool", taskBuffer)
			}
		}
	})

	t.Run("maximum", func(t *testing.T) {
		p := newPoolWithIDForTest(t, 1, WithTaskBuffer(MaxTaskBuffer), WithDisablePurge(true))
		finished := make(chan struct{})
		if err := p.Submit(1, func() { close(finished) }); err != nil {
			t.Fatalf("Submit() at MaxTaskBuffer: %v", err)
		}
		poolWithIDReceive(t, finished)
		if got, want := cap(poolWithIDEntryForTest(t, p, 1).tasks), 2*MaxTaskBuffer; got != want {
			t.Fatalf("physical task capacity = %d, want %d", got, want)
		}
	})

	t.Run("options apply in call order", func(t *testing.T) {
		direct := func(opts *Options) { opts.TaskBuffer = 3 }
		cases := []struct {
			name    string
			options []Option
			want    int
		}{
			{name: "direct option", options: []Option{direct}, want: 3},
			{name: "WithOptions last", options: []Option{WithTaskBuffer(-1), WithOptions(Options{TaskBuffer: 4, DisablePurge: true})}, want: 4},
			{name: "WithTaskBuffer last", options: []Option{WithOptions(Options{TaskBuffer: 4, DisablePurge: true}), WithTaskBuffer(5)}, want: 5},
			{name: "later valid overrides invalid", options: []Option{WithTaskBuffer(-1), WithTaskBuffer(6)}, want: 6},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				p := newPoolWithIDForTest(t, 1, tc.options...)
				if p.admissionLimit != tc.want {
					t.Fatalf("admission limit = %d, want %d", p.admissionLimit, tc.want)
				}
			})
		}

		p, err := NewPoolWithID(1, WithTaskBuffer(6), WithTaskBuffer(-1))
		if !errors.Is(err, ErrInvalidPoolWithIDTaskBuffer) || p != nil {
			t.Fatalf("later invalid option returned pool=%v, error=%v", p, err)
		}
	})

	t.Run("PreAlloc is accepted without a worker queue", func(t *testing.T) {
		p := newPoolWithIDForTest(t, -1, WithPreAlloc(false), WithPreAlloc(true), WithDisablePurge(true))
		if !p.options.PreAlloc {
			t.Fatal("later WithPreAlloc(true) was not retained")
		}
		if p.workers != nil {
			t.Fatalf("PoolWithID allocated ordinary worker queue %T", p.workers)
		}

		p2 := newPoolWithIDForTest(t, 1, WithPreAlloc(true), WithPreAlloc(false), WithDisablePurge(true))
		if p2.options.PreAlloc {
			t.Fatal("later WithPreAlloc(false) was not retained")
		}
		if p2.workers != nil {
			t.Fatalf("PoolWithID allocated ordinary worker queue %T", p2.workers)
		}

		p3 := newPoolWithIDForTest(t, 1, WithPreAlloc(true), WithDisablePurge(true))
		p3.Tune(2)
		if got := p3.Cap(); got != 2 {
			t.Fatalf("Tune() capacity with no-op PreAlloc = %d, want 2", got)
		}
	})
}

func TestPoolWithIDInvalidTaskBufferStartsNoBackground(t *testing.T) {
	var starts atomic.Int32
	poolWithIDBackgroundStartHook = func() { starts.Add(1) }
	t.Cleanup(func() { poolWithIDBackgroundStartHook = nil })

	maxInt := int(^uint(0) >> 1)
	for _, taskBuffer := range []int{-1, MaxTaskBuffer + 1, maxInt / 2} {
		pool, err := NewPoolWithID(1, WithTaskBuffer(taskBuffer))
		if pool != nil || !errors.Is(err, ErrInvalidPoolWithIDTaskBuffer) {
			t.Fatalf("NewPoolWithID(TaskBuffer=%d) = (%v, %v), want (nil, %v)",
				taskBuffer, pool, err, ErrInvalidPoolWithIDTaskBuffer)
		}
	}
	if got := starts.Load(); got != 0 {
		t.Fatalf("invalid constructors started %d background generations", got)
	}

	pool := newPoolWithIDForTest(t, 1, WithDisablePurge(true))
	if pool == nil {
		t.Fatal("valid constructor returned a nil pool")
	}
	if got := starts.Load(); got != 1 {
		t.Fatalf("valid constructor started %d background generations, want 1", got)
	}
	poolWithIDBackgroundStartHook = nil
}

func TestPoolWithIDTinyExpiryConstruction(t *testing.T) {
	const iterations = 8
	for i := 0; i < iterations; i++ {
		pool, err := NewPoolWithID(
			1,
			WithExpiryDuration(time.Nanosecond),
			WithDisablePurgeRunning(true),
			WithLogger(poolWithIDDiscardLogger{}),
		)
		if err != nil {
			t.Fatalf("iteration %d: NewPoolWithID() error = %v", i, err)
		}

		finished := make(chan struct{})
		if err := pool.Submit(i, func() { close(finished) }); err != nil {
			t.Fatalf("iteration %d: Submit() error = %v", i, err)
		}
		poolWithIDReceive(t, finished)
		if err := pool.ReleaseTimeout(poolWithIDTestTimeout); err != nil {
			t.Fatalf("iteration %d: ReleaseTimeout() error = %v", i, err)
		}
	}
}

func TestPoolWithIDGoexitDoesNotStrandEntry(t *testing.T) {
	tests := []struct {
		name    string
		options []Option
		task    func(started chan<- struct{})
	}{
		{
			name: "task",
			task: func(started chan<- struct{}) {
				close(started)
				runtime.Goexit()
			},
		},
		{
			name: "panic handler",
			options: []Option{WithPanicHandler(func(any) {
				runtime.Goexit()
			})},
			task: func(started chan<- struct{}) {
				close(started)
				panic("handled by a Goexit panic handler")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			options := append([]Option{WithDisablePurge(true)}, tt.options...)
			p := newPoolWithIDForTest(t, 1, options...)

			started := make(chan struct{})
			if err := p.Submit(1, func() { tt.task(started) }); err != nil {
				t.Fatalf("submit Goexit task: %v", err)
			}
			poolWithIDReceive(t, started)

			followupFinished := make(chan struct{})
			if err := p.Submit(1, func() { close(followupFinished) }); err != nil {
				t.Fatalf("submit follow-up task: %v", err)
			}
			poolWithIDReceive(t, followupFinished)

			if got := p.Running(); got != 1 {
				t.Fatalf("Running() after Goexit recovery = %d, want 1", got)
			}
			if snapshot := p.EscapeSnapshot(); snapshot.Total != 0 || len(snapshot.ByID) != 0 {
				t.Fatalf("Goexit was incorrectly recorded as an escape: %+v", snapshot)
			}
		})
	}
}

func TestPoolWithIDNonblockingAdmissionWatermark(t *testing.T) {
	p := newPoolWithIDForTest(t, 1,
		WithTaskBuffer(100),
		WithNonblocking(true),
		WithDisablePurge(true),
	)

	started := make(chan struct{})
	unblock := make(chan struct{})
	closeUnblock := poolWithIDCloseOnCleanup(t, unblock)
	if err := p.Submit(1, func() {
		close(started)
		<-unblock
	}); err != nil {
		t.Fatalf("Submit(running task) error = %v", err)
	}
	poolWithIDReceive(t, started)

	done := make(chan struct{}, 100)
	for i := 0; i < 100; i++ {
		if err := p.Submit(1, func() { done <- struct{}{} }); err != nil {
			t.Fatalf("Submit(queue index %d) error = %v", i, err)
		}
	}
	entry := poolWithIDEntryForTest(t, p, 1)
	if got := len(entry.tasks); got != 100 {
		t.Fatalf("queued tasks = %d, want 100", got)
	}
	if got := cap(entry.tasks); got != 200 {
		t.Fatalf("physical task capacity = %d, want 200", got)
	}
	if err := p.Submit(1, func() {}); !errors.Is(err, ErrPoolOverload) {
		t.Fatalf("Submit() above admission watermark error = %v, want %v", err, ErrPoolOverload)
	}

	closeUnblock()
	for i := 0; i < 100; i++ {
		poolWithIDReceive(t, done)
	}
}

func TestPoolWithIDConcurrentAdmissionChecksUseReserve(t *testing.T) {
	const contenders = 3
	p := newPoolWithIDForTest(t, 1,
		WithTaskBuffer(2),
		WithNonblocking(true),
		WithDisablePurge(true),
	)

	started := make(chan struct{})
	unblock := make(chan struct{})
	closeUnblock := poolWithIDCloseOnCleanup(t, unblock)
	if err := p.Submit(1, func() {
		close(started)
		<-unblock
	}); err != nil {
		t.Fatalf("Submit(running task) error = %v", err)
	}
	poolWithIDReceive(t, started)

	done := make(chan struct{}, contenders+1)
	if err := p.Submit(1, func() { done <- struct{}{} }); err != nil {
		t.Fatalf("Submit(prefill task) error = %v", err)
	}
	entry := poolWithIDEntryForTest(t, p, 1)
	if got := len(entry.tasks); got != p.admissionLimit-1 {
		t.Fatalf("prefill queue length = %d, want %d", got, p.admissionLimit-1)
	}

	checked := make(chan struct{}, contenders)
	send := make(chan struct{})
	closeSend := poolWithIDCloseOnCleanup(t, send)
	p.testHooks.afterAdmissionCheck = func() {
		checked <- struct{}{}
		<-send
	}
	results := make(chan error, contenders)
	startSubmit := make(chan struct{})
	for i := 0; i < contenders; i++ {
		go func() {
			<-startSubmit
			results <- p.Submit(1, func() { done <- struct{}{} })
		}()
	}
	close(startSubmit)
	for i := 0; i < contenders; i++ {
		poolWithIDReceive(t, checked)
	}
	if got := len(entry.tasks); got != p.admissionLimit-1 {
		t.Fatalf("queue changed before admission-check barrier was released: %d", got)
	}

	// Each contender observed the queue below the admission watermark. Their
	// sends may therefore consume the reserved half of the physical channel.
	p.testHooks.afterAdmissionCheck = nil
	closeSend()
	for i := 0; i < contenders; i++ {
		if err := poolWithIDReceive(t, results); err != nil {
			t.Fatalf("concurrent Submit() error = %v", err)
		}
	}
	if got, want := len(entry.tasks), cap(entry.tasks); got != want {
		t.Fatalf("queue length after concurrent sends = %d, want physical capacity %d", got, want)
	}

	closeUnblock()
	for i := 0; i < contenders+1; i++ {
		poolWithIDReceive(t, done)
	}
}

func TestPoolWithIDNonblockingFinalSendRejectsFullPhysicalQueue(t *testing.T) {
	const contenders = 5
	p := newPoolWithIDForTest(t, 1,
		WithTaskBuffer(2),
		WithNonblocking(true),
		WithDisablePurge(true),
	)

	started := make(chan struct{})
	unblock := make(chan struct{})
	closeUnblock := poolWithIDCloseOnCleanup(t, unblock)
	if err := p.Submit(1, func() {
		close(started)
		<-unblock
	}); err != nil {
		t.Fatalf("Submit(running task) error = %v", err)
	}
	poolWithIDReceive(t, started)

	entry := poolWithIDEntryForTest(t, p, 1)
	done := make(chan struct{}, cap(entry.tasks))
	checked := make(chan struct{}, contenders)
	send := make(chan struct{})
	closeSend := poolWithIDCloseOnCleanup(t, send)
	p.testHooks.afterAdmissionCheck = func() {
		checked <- struct{}{}
		<-send
	}
	results := make(chan error, contenders)
	startSubmit := make(chan struct{})
	for i := 0; i < contenders; i++ {
		go func() {
			<-startSubmit
			results <- p.Submit(1, func() { done <- struct{}{} })
		}()
	}
	close(startSubmit)
	for i := 0; i < contenders; i++ {
		poolWithIDReceive(t, checked)
	}
	p.testHooks.afterAdmissionCheck = nil
	closeSend()

	succeeded := 0
	rejected := 0
	for i := 0; i < contenders; i++ {
		switch err := poolWithIDReceive(t, results); {
		case err == nil:
			succeeded++
		case errors.Is(err, ErrPoolOverload):
			rejected++
		default:
			t.Fatalf("concurrent Submit() error = %v", err)
		}
	}
	if succeeded != cap(entry.tasks) || rejected != contenders-cap(entry.tasks) {
		t.Fatalf("concurrent sends succeeded=%d rejected=%d, want succeeded=%d rejected=%d",
			succeeded, rejected, cap(entry.tasks), contenders-cap(entry.tasks))
	}
	if got := len(entry.tasks); got != cap(entry.tasks) {
		t.Fatalf("queue length = %d, want full physical capacity %d", got, cap(entry.tasks))
	}

	closeUnblock()
	for i := 0; i < succeeded; i++ {
		poolWithIDReceive(t, done)
	}
}

func TestPoolWithIDNewAndExistingIDCapacity(t *testing.T) {
	p := newPoolWithIDForTest(t, 1,
		WithTaskBuffer(2),
		WithNonblocking(true),
		WithDisablePurge(true),
	)

	started := make(chan struct{})
	unblock := make(chan struct{})
	closeUnblock := poolWithIDCloseOnCleanup(t, unblock)
	if err := p.Submit(1, func() {
		close(started)
		<-unblock
	}); err != nil {
		t.Fatalf("Submit(ID 1) error = %v", err)
	}
	poolWithIDReceive(t, started)

	if err := p.Submit(2, func() {}); !errors.Is(err, ErrPoolOverload) {
		t.Fatalf("Submit(new ID at owner capacity) error = %v, want %v", err, ErrPoolOverload)
	}
	existingDone := make(chan struct{})
	if err := p.Submit(1, func() { close(existingDone) }); err != nil {
		t.Fatalf("Submit(existing ID below its watermark) error = %v", err)
	}

	closeUnblock()
	poolWithIDReceive(t, existingDone)
}

func TestPoolWithIDBlockingQueueWaitsForSpaceAndClose(t *testing.T) {
	t.Run("space becomes available", func(t *testing.T) {
		p := newPoolWithIDForTest(t, 1, WithTaskBuffer(1), WithDisablePurge(true))
		runningStarted := make(chan struct{})
		runningGate := make(chan struct{})
		closeRunningGate := poolWithIDCloseOnCleanup(t, runningGate)
		if err := p.Submit(1, func() {
			close(runningStarted)
			<-runningGate
		}); err != nil {
			t.Fatalf("Submit(running task) error = %v", err)
		}
		poolWithIDReceive(t, runningStarted)

		queuedGate := make(chan struct{})
		closeQueuedGate := poolWithIDCloseOnCleanup(t, queuedGate)
		for i := 0; i < 2; i++ {
			if err := p.Submit(1, func() { <-queuedGate }); err != nil {
				t.Fatalf("Submit(queue index %d) error = %v", i, err)
			}
		}

		thirdRan := make(chan struct{})
		result := make(chan error, 1)
		registered := make(chan struct{})
		p.testHooks.afterSubmitRegistered = func() { close(registered) }
		go func() {
			result <- p.Submit(1, func() { close(thirdRan) })
		}()
		poolWithIDReceive(t, registered)
		p.testHooks.afterSubmitRegistered = nil
		select {
		case err := <-result:
			t.Fatalf("Submit() returned while the physical queue was full: %v", err)
		default:
		}

		closeRunningGate()
		if err := poolWithIDReceive(t, result); err != nil {
			t.Fatalf("Submit() after queue space became available error = %v", err)
		}
		closeQueuedGate()
		poolWithIDReceive(t, thirdRan)
	})

	t.Run("pool closes", func(t *testing.T) {
		p := newPoolWithIDForTest(t, 1, WithTaskBuffer(1), WithDisablePurge(true))
		runningStarted := make(chan struct{})
		runningGate := make(chan struct{})
		closeRunningGate := poolWithIDCloseOnCleanup(t, runningGate)
		if err := p.Submit(1, func() {
			close(runningStarted)
			<-runningGate
		}); err != nil {
			t.Fatalf("Submit(running task) error = %v", err)
		}
		poolWithIDReceive(t, runningStarted)

		queuedDone := make(chan struct{}, 2)
		for i := 0; i < 2; i++ {
			if err := p.Submit(1, func() { queuedDone <- struct{}{} }); err != nil {
				t.Fatalf("Submit(queue index %d) error = %v", i, err)
			}
		}

		result := make(chan error, 1)
		registered := make(chan struct{})
		p.testHooks.afterSubmitRegistered = func() { close(registered) }
		go func() { result <- p.Submit(1, func() {}) }()
		poolWithIDReceive(t, registered)
		p.testHooks.afterSubmitRegistered = nil
		p.Release()
		if err := poolWithIDReceive(t, result); !errors.Is(err, ErrPoolClosed) {
			t.Fatalf("waiting Submit() after Release error = %v, want %v", err, ErrPoolClosed)
		}

		closeRunningGate()
		poolWithIDReceive(t, queuedDone)
		poolWithIDReceive(t, queuedDone)
		if err := p.ReleaseTimeout(poolWithIDTestTimeout); err != nil && !errors.Is(err, ErrPoolClosed) {
			t.Fatalf("ReleaseTimeout() after drain error = %v", err)
		}
	})
}

func TestPoolWithIDMaxBlockingTasksOnlyLimitsNewIDs(t *testing.T) {
	p := newPoolWithIDForTest(t, 1,
		WithTaskBuffer(1),
		WithMaxBlockingTasks(1),
		WithDisablePurge(true),
	)

	runningStarted := make(chan struct{})
	runningGate := make(chan struct{})
	closeRunningGate := poolWithIDCloseOnCleanup(t, runningGate)
	if err := p.Submit(1, func() {
		close(runningStarted)
		<-runningGate
	}); err != nil {
		t.Fatalf("Submit(ID 1) error = %v", err)
	}
	poolWithIDReceive(t, runningStarted)

	capacityWaitRegistered := make(chan struct{}, 1)
	p.testHooks.afterCapacityWaitRegistered = func() {
		select {
		case capacityWaitRegistered <- struct{}{}:
		default:
		}
	}
	newIDResult := make(chan error, 1)
	go func() { newIDResult <- p.Submit(2, func() {}) }()
	poolWithIDReceive(t, capacityWaitRegistered)
	p.testHooks.afterCapacityWaitRegistered = nil
	if got := p.Waiting(); got != 1 {
		t.Fatalf("capacity waiters = %d, want 1", got)
	}
	if err := p.Submit(3, func() {}); !errors.Is(err, ErrPoolOverload) {
		t.Fatalf("second new-ID waiter error = %v, want %v", err, ErrPoolOverload)
	}

	queuedGate := make(chan struct{})
	closeQueuedGate := poolWithIDCloseOnCleanup(t, queuedGate)
	for i := 0; i < 2; i++ {
		if err := p.Submit(1, func() { <-queuedGate }); err != nil {
			t.Fatalf("Submit(existing ID queue index %d) error = %v", i, err)
		}
	}
	existingResult := make(chan error, 1)
	existingRan := make(chan struct{})
	existingRegistered := make(chan struct{})
	p.testHooks.afterSubmitRegistered = func() { close(existingRegistered) }
	go func() {
		existingResult <- p.Submit(1, func() { close(existingRan) })
	}()
	poolWithIDReceive(t, existingRegistered)
	p.testHooks.afterSubmitRegistered = nil
	if got := p.Waiting(); got != 1 {
		t.Fatalf("capacity waiters changed to %d for an existing-ID queue wait, want 1", got)
	}

	closeRunningGate()
	if err := poolWithIDReceive(t, existingResult); err != nil {
		t.Fatalf("existing-ID Submit() after space became available error = %v", err)
	}
	closeQueuedGate()
	poolWithIDReceive(t, existingRan)

	p.Release()
	if err := poolWithIDReceive(t, newIDResult); !errors.Is(err, ErrPoolClosed) {
		t.Fatalf("new-ID capacity waiter after Release error = %v, want %v", err, ErrPoolClosed)
	}
}

func TestPoolWithIDRecursiveSubmit(t *testing.T) {
	t.Run("below admission watermark", func(t *testing.T) {
		p := newPoolWithIDForTest(t, 1,
			WithTaskBuffer(2),
			WithNonblocking(true),
			WithDisablePurge(true),
		)
		result := make(chan error, 1)
		innerRan := make(chan struct{})
		if err := p.Submit(1, func() {
			result <- p.Submit(1, func() { close(innerRan) })
		}); err != nil {
			t.Fatalf("Submit(outer task) error = %v", err)
		}
		if err := poolWithIDReceive(t, result); err != nil {
			t.Fatalf("recursive Submit() error = %v", err)
		}
		poolWithIDReceive(t, innerRan)
	})

	t.Run("at admission watermark", func(t *testing.T) {
		p := newPoolWithIDForTest(t, 1,
			WithTaskBuffer(2),
			WithNonblocking(true),
			WithDisablePurge(true),
		)
		outerStarted := make(chan struct{})
		recurse := make(chan struct{})
		closeRecurse := poolWithIDCloseOnCleanup(t, recurse)
		result := make(chan error, 1)
		if err := p.Submit(1, func() {
			close(outerStarted)
			<-recurse
			result <- p.Submit(1, func() {})
		}); err != nil {
			t.Fatalf("Submit(outer task) error = %v", err)
		}
		poolWithIDReceive(t, outerStarted)

		queuedDone := make(chan struct{}, 2)
		for i := 0; i < 2; i++ {
			if err := p.Submit(1, func() { queuedDone <- struct{}{} }); err != nil {
				t.Fatalf("Submit(queue index %d) error = %v", i, err)
			}
		}
		closeRecurse()
		if err := poolWithIDReceive(t, result); !errors.Is(err, ErrPoolOverload) {
			t.Fatalf("recursive Submit() at watermark error = %v, want %v", err, ErrPoolOverload)
		}
		poolWithIDReceive(t, queuedDone)
		poolWithIDReceive(t, queuedDone)
	})
}

func TestPoolWithIDDoesNotUseDefaultPool(t *testing.T) {
	Release()
	t.Cleanup(Reboot)

	p := newPoolWithIDForTest(t, 1,
		WithTaskBuffer(1),
		WithNonblocking(true),
		WithDisablePurge(true),
	)
	done := make(chan struct{})
	if err := p.Submit(1, func() { close(done) }); err != nil {
		t.Fatalf("independent PoolWithID Submit() error with closed default pool = %v", err)
	}
	poolWithIDReceive(t, done)
}

func TestPoolWithIDSuccessfulSubmitsStartFIFO(t *testing.T) {
	const taskCount = 32
	p := newPoolWithIDForTest(t, 1, WithTaskBuffer(taskCount), WithDisablePurge(true))

	firstStarted := make(chan struct{})
	firstGate := make(chan struct{})
	closeFirstGate := poolWithIDCloseOnCleanup(t, firstGate)
	if err := p.Submit(1, func() {
		close(firstStarted)
		<-firstGate
	}); err != nil {
		t.Fatalf("Submit(first task) error = %v", err)
	}
	poolWithIDReceive(t, firstStarted)

	started := make(chan int, taskCount)
	for i := 0; i < taskCount; i++ {
		index := i
		if err := p.Submit(1, func() { started <- index }); err != nil {
			t.Fatalf("Submit(task %d) error = %v", i, err)
		}
	}
	closeFirstGate()
	for want := 0; want < taskCount; want++ {
		if got := poolWithIDReceive(t, started); got != want {
			t.Fatalf("task start order[%d] = %d, want %d", want, got, want)
		}
	}
}

func TestPoolWithIDRunningPeakNeverExceedsCapacity(t *testing.T) {
	const (
		capacity  = 4
		submitter = 64
	)
	p := newPoolWithIDForTest(t, capacity,
		WithTaskBuffer(1),
		WithNonblocking(true),
		WithDisablePurge(true),
	)

	startSubmit := make(chan struct{})
	releaseTasks := make(chan struct{})
	closeReleaseTasks := poolWithIDCloseOnCleanup(t, releaseTasks)
	results := make(chan error, submitter)
	started := make(chan struct{}, capacity)
	finished := make(chan struct{}, capacity)
	var live atomic.Int32
	var peak atomic.Int32

	for id := 0; id < submitter; id++ {
		id := id
		go func() {
			<-startSubmit
			results <- p.Submit(id, func() {
				current := live.Add(1)
				for {
					oldPeak := peak.Load()
					if current <= oldPeak || peak.CompareAndSwap(oldPeak, current) {
						break
					}
				}
				started <- struct{}{}
				<-releaseTasks
				live.Add(-1)
				finished <- struct{}{}
			})
		}()
	}
	close(startSubmit)

	succeeded := 0
	for i := 0; i < submitter; i++ {
		err := poolWithIDReceive(t, results)
		switch {
		case err == nil:
			succeeded++
		case errors.Is(err, ErrPoolOverload):
		default:
			t.Fatalf("concurrent Submit() error = %v", err)
		}
	}
	if succeeded != capacity {
		t.Fatalf("successful new-ID submissions = %d, want %d", succeeded, capacity)
	}
	for i := 0; i < succeeded; i++ {
		poolWithIDReceive(t, started)
	}
	if got := p.Running(); got > capacity {
		t.Fatalf("Running() = %d, capacity = %d", got, capacity)
	}
	if got := peak.Load(); got > capacity {
		t.Fatalf("live worker peak = %d, capacity = %d", got, capacity)
	}

	closeReleaseTasks()
	for i := 0; i < succeeded; i++ {
		poolWithIDReceive(t, finished)
	}
}

func TestPoolWithIDReleaseDrainsAcceptedTasks(t *testing.T) {
	t.Run("normal return", func(t *testing.T) {
		p := newPoolWithIDForTest(t, 1, WithTaskBuffer(1), WithDisablePurge(true))
		firstStarted := make(chan struct{})
		firstGate := make(chan struct{})
		closeFirstGate := poolWithIDCloseOnCleanup(t, firstGate)
		if err := p.Submit(1, func() {
			close(firstStarted)
			<-firstGate
		}); err != nil {
			t.Fatalf("Submit(first task) error = %v", err)
		}
		poolWithIDReceive(t, firstStarted)

		secondDone := make(chan struct{})
		if err := p.Submit(1, func() { close(secondDone) }); err != nil {
			t.Fatalf("Submit(second task) error = %v", err)
		}
		p.Release()
		if !p.IsClosed() {
			t.Fatal("IsClosed() = false after Release")
		}
		closeFirstGate()
		poolWithIDReceive(t, secondDone)
		if err := p.ReleaseTimeout(poolWithIDTestTimeout); err != nil && !errors.Is(err, ErrPoolClosed) {
			t.Fatalf("ReleaseTimeout() after accepted tasks drained error = %v", err)
		}
	})

	t.Run("panic", func(t *testing.T) {
		panicSeen := make(chan any, 1)
		p := newPoolWithIDForTest(t, 1,
			WithTaskBuffer(1),
			WithDisablePurge(true),
			WithPanicHandler(func(value any) { panicSeen <- value }),
		)
		firstStarted := make(chan struct{})
		firstGate := make(chan struct{})
		closeFirstGate := poolWithIDCloseOnCleanup(t, firstGate)
		if err := p.Submit(1, func() {
			close(firstStarted)
			<-firstGate
		}); err != nil {
			t.Fatalf("Submit(first task) error = %v", err)
		}
		poolWithIDReceive(t, firstStarted)
		if err := p.Submit(1, func() { panic("accepted panic") }); err != nil {
			t.Fatalf("Submit(panicking task) error = %v", err)
		}

		p.Release()
		closeFirstGate()
		if got := poolWithIDReceive(t, panicSeen); got != "accepted panic" {
			t.Fatalf("panic handler value = %v, want %q", got, "accepted panic")
		}
		if err := p.ReleaseTimeout(poolWithIDTestTimeout); err != nil && !errors.Is(err, ErrPoolClosed) {
			t.Fatalf("ReleaseTimeout() after accepted panic error = %v", err)
		}
	})
}

func TestPoolWithIDReleaseTimeoutWithFullQueue(t *testing.T) {
	p := newPoolWithIDForTest(t, 1, WithTaskBuffer(1), WithDisablePurge(true))
	runningStarted := make(chan struct{})
	runningGate := make(chan struct{})
	closeRunningGate := poolWithIDCloseOnCleanup(t, runningGate)
	if err := p.Submit(1, func() {
		close(runningStarted)
		<-runningGate
	}); err != nil {
		t.Fatalf("Submit(running task) error = %v", err)
	}
	poolWithIDReceive(t, runningStarted)

	queuedDone := make(chan struct{}, 2)
	for i := 0; i < 2; i++ {
		if err := p.Submit(1, func() { queuedDone <- struct{}{} }); err != nil {
			t.Fatalf("Submit(queue index %d) error = %v", i, err)
		}
	}

	result := make(chan error, 1)
	go func() { result <- p.ReleaseTimeout(20 * time.Millisecond) }()
	if err := poolWithIDReceive(t, result); !errors.Is(err, ErrTimeout) {
		t.Fatalf("ReleaseTimeout() with blocked task error = %v, want %v", err, ErrTimeout)
	}

	closeRunningGate()
	poolWithIDReceive(t, queuedDone)
	poolWithIDReceive(t, queuedDone)
	if err := p.ReleaseTimeout(poolWithIDTestTimeout); err != nil && !errors.Is(err, ErrPoolClosed) {
		t.Fatalf("ReleaseTimeout() after queue drained error = %v", err)
	}
}

func TestPoolWithIDSubmitReleaseLinearization(t *testing.T) {
	t.Run("Submit linearizes first", func(t *testing.T) {
		p := newPoolWithIDForTest(t, 1,
			WithTaskBuffer(1),
			WithNonblocking(true),
			WithDisablePurge(true),
		)
		submitChecked := make(chan struct{})
		allowSubmit := make(chan struct{})
		closeAllowSubmit := poolWithIDCloseOnCleanup(t, allowSubmit)
		releaseBeforeLock := make(chan struct{})
		allowRelease := make(chan struct{})
		closeAllowRelease := poolWithIDCloseOnCleanup(t, allowRelease)
		p.testHooks.afterAdmissionCheck = func() {
			close(submitChecked)
			<-allowSubmit
		}
		p.testHooks.beforeReleaseLock = func() {
			close(releaseBeforeLock)
			<-allowRelease
		}

		submitResult := make(chan error, 1)
		taskDone := make(chan struct{})
		go func() {
			submitResult <- p.Submit(1, func() { close(taskDone) })
		}()
		poolWithIDReceive(t, submitChecked)

		releaseReturned := make(chan struct{})
		go func() {
			p.Release()
			close(releaseReturned)
		}()
		poolWithIDReceive(t, releaseBeforeLock)

		p.testHooks.afterAdmissionCheck = nil
		closeAllowSubmit()
		if err := poolWithIDReceive(t, submitResult); err != nil {
			t.Fatalf("Submit() that linearized first error = %v", err)
		}
		poolWithIDReceive(t, taskDone)

		p.testHooks.beforeReleaseLock = nil
		closeAllowRelease()
		poolWithIDReceive(t, releaseReturned)
	})

	t.Run("Release linearizes first", func(t *testing.T) {
		p := newPoolWithIDForTest(t, 1,
			WithTaskBuffer(1),
			WithNonblocking(true),
			WithDisablePurge(true),
		)
		releaseHasLock := make(chan struct{})
		allowRelease := make(chan struct{})
		closeAllowRelease := poolWithIDCloseOnCleanup(t, allowRelease)
		p.testHooks.afterReleaseLock = func() {
			close(releaseHasLock)
			<-allowRelease
		}

		releaseReturned := make(chan struct{})
		go func() {
			p.Release()
			close(releaseReturned)
		}()
		poolWithIDReceive(t, releaseHasLock)

		submitStarted := make(chan struct{})
		submitResult := make(chan error, 1)
		go func() {
			close(submitStarted)
			submitResult <- p.Submit(1, func() {})
		}()
		poolWithIDReceive(t, submitStarted)
		select {
		case err := <-submitResult:
			t.Fatalf("Submit() returned before Release relinquished the registry lock: %v", err)
		default:
		}

		p.testHooks.afterReleaseLock = nil
		closeAllowRelease()
		poolWithIDReceive(t, releaseReturned)
		if err := poolWithIDReceive(t, submitResult); !errors.Is(err, ErrPoolClosed) {
			t.Fatalf("Submit() after Release linearized error = %v, want %v", err, ErrPoolClosed)
		}
	})
}

func TestPoolWithIDRebootDoesNotCarryQueuedTasksAcrossIDs(t *testing.T) {
	p := newPoolWithIDForTest(
		t,
		1,
		WithTaskBuffer(1),
		WithExpiryDuration(time.Hour),
	)

	aStarted := make(chan struct{})
	unblockA := make(chan struct{})
	closeUnblockA := poolWithIDCloseOnCleanup(t, unblockA)
	if err := p.Submit(1, func() {
		close(aStarted)
		<-unblockA
	}); err != nil {
		t.Fatalf("submit old task A: %v", err)
	}
	poolWithIDReceive(t, aStarted)

	var oldQueuedRuns atomic.Int32
	oldQueuedFinished := make(chan struct{})
	if err := p.Submit(1, func() {
		oldQueuedRuns.Add(1)
		close(oldQueuedFinished)
	}); err != nil {
		t.Fatalf("submit old queued task: %v", err)
	}

	p.Release()
	closeUnblockA()
	poolWithIDReceive(t, oldQueuedFinished)
	if err := p.ReleaseTimeout(poolWithIDTestTimeout); err != nil && !errors.Is(err, ErrPoolClosed) {
		t.Fatalf("finish old release: %v", err)
	}

	p.Reboot()
	newTaskFinished := make(chan struct{})
	if err := p.Submit(2, func() { close(newTaskFinished) }); err != nil {
		t.Fatalf("submit post-Reboot task: %v", err)
	}
	poolWithIDReceive(t, newTaskFinished)
	if err := p.ReleaseTimeout(poolWithIDTestTimeout); err != nil {
		t.Fatalf("finish post-Reboot release: %v", err)
	}
	if got := oldQueuedRuns.Load(); got != 1 {
		t.Fatalf("old queued task executed %d times across Reboot, want 1", got)
	}
}
