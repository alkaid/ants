// MIT License

// Copyright (c) 2018 Andy Pan

// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package ants

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	syncx "github.com/panjf2000/ants/v2/internal/sync"
)

const MinTaskBuffer = 10

// PoolWithID accepts the tasks from client, it limits the total of goroutines to a given number by recycling goroutines.
//
// 池子里的线程都带id, PoolWithID.Submit 到同一id线程的任务会在同一线程内保序执行,
// 但业务方须注意任务的执行时间不能超过超时时间 WithExpiryDuration 否则可能产生脏线程及线程泄漏.
// 若任务是耗时长的常驻循环任务,不希望所在线程被回收,请单独使用不回收的线程池,指定 WithDisablePurge(true)
type PoolWithID struct {
	// capacity of the pool, a negative value means that the capacity of pool is limitless, an infinite pool is used to
	// avoid potential issue of endless blocking caused by nested usage of a pool: submitting a task to pool
	// which submits a new task to the same pool.
	capacity int32

	// running is the number of the currently running goroutines.
	running int32

	// lock for protecting the worker queue.
	lock sync.Locker

	// workers is a slice that store the available workers.
	workers workerQueue

	// state is used to notice the pool to closed itself.
	state int32

	// cond for waiting to get an idle worker.
	cond *sync.Cond

	// workerCache speeds up the obtainment of a usable worker in function:retrieveWorker.
	workerCache sync.Pool

	// waiting is the number of goroutines already been blocked on pool.Submit(), protected by pool.lock
	waiting int32

	purgeDone int32
	stopPurge context.CancelFunc

	ticktockDone int32
	stopTicktock context.CancelFunc

	now atomic.Value

	options *Options
}

// purgeStaleWorkers clears stale workers periodically, it runs in an individual goroutine, as a scavenger.
func (p *PoolWithID) purgeStaleWorkers(ctx context.Context) {
	ticker := time.NewTicker(p.options.ExpiryDuration)

	defer func() {
		ticker.Stop()
		atomic.StoreInt32(&p.purgeDone, 1)
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		if p.IsClosed() {
			break
		}

		var isDormant bool
		p.lock.Lock()
		staleWorkers := p.workers.refresh(p.options.ExpiryDuration)
		n := p.Running()
		isDormant = n == 0 || n == len(staleWorkers)
		p.lock.Unlock()

		// Notify obsolete workers to stop.
		// This notification must be outside the p.lock, since w.task
		// may be blocking and may consume a lot of time if many workers
		// are located on non-local CPUs.
		for i := range staleWorkers {
			staleWorkers[i].finish()
			staleWorkers[i] = nil
		}

		// There might be a situation where all workers have been cleaned up(no worker is running),
		// while some invokers still are stuck in "p.cond.Wait()", then we need to awake those invokers.
		if isDormant && p.Waiting() > 0 {
			p.cond.Broadcast()
		}
	}
}

// ticktock is a goroutine that updates the current time in the pool regularly.
func (p *PoolWithID) ticktock(ctx context.Context) {
	ticker := time.NewTicker(nowTimeUpdateInterval)
	defer func() {
		ticker.Stop()
		atomic.StoreInt32(&p.ticktockDone, 1)
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		if p.IsClosed() {
			break
		}

		p.now.Store(time.Now())
	}
}

func (p *PoolWithID) goPurge() {
	if p.options.DisablePurge {
		return
	}

	// Start a goroutine to clean up expired workers periodically.
	var ctx context.Context
	ctx, p.stopPurge = context.WithCancel(context.Background())
	go p.purgeStaleWorkers(ctx)
}

func (p *PoolWithID) goTicktock() {
	p.now.Store(time.Now())
	var ctx context.Context
	ctx, p.stopTicktock = context.WithCancel(context.Background())
	go p.ticktock(ctx)
}

func (p *PoolWithID) nowTime() time.Time {
	return p.now.Load().(time.Time)
}

// NewPoolWithID generates an instance of PoolWithID.
func NewPoolWithID(size int, options ...Option) (*PoolWithID, error) {
	if size <= 0 {
		size = -1
	}

	opts := loadOptions(options...)

	if !opts.DisablePurge {
		if expiry := opts.ExpiryDuration; expiry < 0 {
			return nil, ErrInvalidPoolExpiry
		} else if expiry == 0 {
			opts.ExpiryDuration = DefaultCleanIntervalTime
		}
	}

	if opts.Logger == nil {
		opts.Logger = defaultLogger
	}

	p := &PoolWithID{
		capacity: int32(size),
		lock:     syncx.NewSpinLock(),
		options:  opts,
	}
	// 设置 task buffer
	taskBuffer := opts.TaskBuffer
	if taskBuffer < MinTaskBuffer {
		taskBuffer = MinTaskBuffer
	}
	p.workerCache.New = func() interface{} {
		return &goWorkerWithID{
			pool:      p,
			task:      make(chan func(), taskBuffer),
			keepAlive: opts.DisablePurgeRunning,
		}
	}
	p.workers = newWorkerMap(0)

	p.cond = sync.NewCond(p.lock)

	p.goPurge()
	p.goTicktock()

	return p, nil
}

// ---------------------------------------------------------------------------

// Submit submits a task to this pool.
//
// Submit 到同一id线程的任务会在同一线程内保序执行,
// 但业务方须注意任务的执行时间不能超过超时时间 WithExpiryDuration 否则可能产生脏线程及线程泄漏.
// 若任务是耗时长的常驻循环任务,不希望所在线程被回收,请单独使用不回收的线程池,指定 WithDisablePurge(true)
func (p *PoolWithID) Submit(id int, task func()) error {
	if p.IsClosed() {
		return ErrPoolClosed
	}
	if w := p.retrieveWorker(id); w != nil {
		w.inputFunc(task)
		return nil
	}
	return ErrPoolOverload
}

// Running returns the number of workers currently running.
func (p *PoolWithID) Running() int {
	return int(atomic.LoadInt32(&p.running))
}

// Free returns the number of available goroutines to work, -1 indicates this pool is unlimited.
func (p *PoolWithID) Free() int {
	c := p.Cap()
	if c < 0 {
		return -1
	}
	return c - p.Running()
}

// Waiting returns the number of tasks which are waiting be executed.
func (p *PoolWithID) Waiting() int {
	return int(atomic.LoadInt32(&p.waiting))
}

// Cap returns the capacity of this pool.
func (p *PoolWithID) Cap() int {
	return int(atomic.LoadInt32(&p.capacity))
}

// Tune changes the capacity of this pool, note that it is noneffective to the infinite or pre-allocation pool.
func (p *PoolWithID) Tune(size int) {
	capacity := p.Cap()
	if capacity == -1 || size <= 0 || size == capacity || p.options.PreAlloc {
		return
	}
	atomic.StoreInt32(&p.capacity, int32(size))
	if size > capacity {
		if size-capacity == 1 {
			p.cond.Signal()
			return
		}
		p.cond.Broadcast()
	}
}

// IsClosed indicates whether the pool is closed.
func (p *PoolWithID) IsClosed() bool {
	return atomic.LoadInt32(&p.state) == CLOSED
}

// Release closes this pool and releases the worker queue.
func (p *PoolWithID) Release() {
	if !atomic.CompareAndSwapInt32(&p.state, OPENED, CLOSED) {
		return
	}
	p.lock.Lock()
	p.workers.reset()
	p.lock.Unlock()
	// There might be some callers waiting in retrieveWorker(), so we need to wake them up to prevent
	// those callers blocking infinitely.
	p.cond.Broadcast()
}

// ReleaseTimeout is like Release but with a timeout, it waits all workers to exit before timing out.
func (p *PoolWithID) ReleaseTimeout(timeout time.Duration) error {
	if p.IsClosed() || (!p.options.DisablePurge && p.stopPurge == nil) || p.stopTicktock == nil {
		return ErrPoolClosed
	}

	if p.stopPurge != nil {
		p.stopPurge()
		p.stopPurge = nil
	}
	p.stopTicktock()
	p.stopTicktock = nil
	p.Release()

	endTime := time.Now().Add(timeout)
	for time.Now().Before(endTime) {
		if p.Running() == 0 &&
			(p.options.DisablePurge || atomic.LoadInt32(&p.purgeDone) == 1) &&
			atomic.LoadInt32(&p.ticktockDone) == 1 {
			return nil
		}
		time.Sleep(10 * time.Millisecond)
	}
	return ErrTimeout
}

// Reboot reboots a closed pool.
func (p *PoolWithID) Reboot() {
	if atomic.CompareAndSwapInt32(&p.state, CLOSED, OPENED) {
		atomic.StoreInt32(&p.purgeDone, 0)
		p.goPurge()
		atomic.StoreInt32(&p.ticktockDone, 0)
		p.goTicktock()
	}
}

// ---------------------------------------------------------------------------

func (p *PoolWithID) addRunning(delta int) {
	atomic.AddInt32(&p.running, int32(delta))
}

func (p *PoolWithID) addWaiting(delta int) {
	atomic.AddInt32(&p.waiting, int32(delta))
}

// retrieveWorker returns an available worker to run the tasks.
func (p *PoolWithID) retrieveWorker(id int) (w worker) {
	spawnWorker := func() {
		w = p.workerCache.Get().(*goWorkerWithID)
		w.(*goWorkerWithID).id = id
		w.(*goWorkerWithID).lastUsed = p.nowTime()
		err := p.workers.(*workerMap).insert(w)
		if err != nil {
			panic(err)
		}
	}

	p.lock.Lock()
	w = p.workers.(*workerMap).get(id, p.nowTime())
	if w != nil { // first try to fetch the worker from the queue
		p.lock.Unlock()
	} else if capacity := p.Cap(); capacity == -1 || capacity > p.Running() {
		// if the worker queue is empty and we don't run out of the pool capacity,
		// then just spawn a new worker goroutine.
		spawnWorker()
		p.lock.Unlock()
		w.run()
	} else { // otherwise, we'll have to keep them blocked and wait for at least one worker to be put back into pool.
		if p.options.Nonblocking {
			p.lock.Unlock()
			return
		}
	retry:
		if p.options.MaxBlockingTasks != 0 && p.Waiting() >= p.options.MaxBlockingTasks {
			p.lock.Unlock()
			return
		}

		p.addWaiting(1)
		p.cond.Wait() // block and wait for an available worker
		p.addWaiting(-1)

		if p.IsClosed() {
			p.lock.Unlock()
			return
		}

		if w = p.workers.(*workerMap).get(id, p.nowTime()); w == nil {
			if p.Free() > 0 {
				spawnWorker()
				p.lock.Unlock()
				w.run()
				return
			}
			goto retry
		}
		p.lock.Unlock()
	}
	return
}

// revertWorker puts a worker back into free pool, recycling the goroutines.
func (p *PoolWithID) revertWorker(w *goWorkerWithID) bool {
	if capacity := p.Cap(); (capacity > 0 && p.Running() > capacity) || p.IsClosed() {
		p.cond.Broadcast()
		return false
	}

	w.lastUsed = p.nowTime()
	// 只保活 不用回收，因为本来就还在map里
	// p.lock.Lock()
	// // To avoid memory leaks, add a double check in the lock scope.
	// // Issue: https://github.com/panjf2000/ants/issues/113
	// if p.IsClosed() {
	// 	p.lock.Unlock()
	// 	return false
	// }
	// if err := p.workers.insert(w); err != nil {
	// 	p.lock.Unlock()
	// 	return false
	// }
	// // Notify the invoker stuck in 'retrieveWorker()' of there is an available worker in the worker queue.
	// p.cond.Signal()
	// p.lock.Unlock()

	return true
}
func (p *PoolWithID) releaseWorker(w *goWorkerWithID) {
	p.lock.Lock()
	p.workers.(*workerMap).detachWithID(w.id)
	p.lock.Unlock()
}
