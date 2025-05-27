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

const MinTaskBuffer = 10

// PoolWithID accepts the tasks from client, it limits the total of goroutines to a given number by recycling goroutines.
//
// 池子里的线程都带id, PoolWithID.Submit 到同一id线程的任务会在同一线程内保序执行,
// 但业务方须注意任务的执行时间不能超过超时时间 WithExpiryDuration 否则可能产生脏线程及线程泄漏.
// 若任务是耗时长的常驻循环任务,不希望所在线程被回收,请单独使用不回收的线程池,指定 WithDisablePurge(true)
type PoolWithID struct {
	*poolCommon
}

// Submit submits a task to this pool.
//
// Submit 到同一id线程的任务会在同一线程内保序执行,
// 但业务方须注意任务的执行时间不能超过超时时间 WithExpiryDuration 否则可能产生脏线程及线程泄漏.
// 若任务是耗时长的常驻循环任务,不希望所在线程被回收,请单独使用不回收的线程池,指定 WithDisablePurge(true)
func (p *PoolWithID) Submit(id int, task func()) error {
	if p.IsClosed() {
		return ErrPoolClosed
	}
	w, err := p.retrieveWorker(id)
	if w != nil {
		w.inputFunc(task)
	}
	return err
}

// NewPoolWithID instantiates a PoolWithID with customized options.
func NewPoolWithID(size int, options ...Option) (*PoolWithID, error) {
	pc, err := newPool(size, options...)
	if err != nil {
		return nil, err
	}

	// 替换 workers 为带id的 workerMap
	pc.workers = newWorkerMap(0)

	pool := &PoolWithID{poolCommon: pc}

	// 设置 task buffer
	taskBuffer := pc.options.TaskBuffer
	if taskBuffer < MinTaskBuffer {
		taskBuffer = MinTaskBuffer
	}
	pool.workerCache.New = func() any {
		return &goWorkerWithID{
			pool: pool,
			task: make(chan func(), taskBuffer),
			// 设置keepAlive
			keepAlive: pc.options.DisablePurgeRunning,
		}
	}

	return pool, nil
}

// retrieveWorker returns an available worker to run the tasks.
// 从 ants.go poolCommon.retrieveWorker 方法复制过来, 并修改为支持 id
func (p *PoolWithID) retrieveWorker(id int) (w worker, err error) {
	p.lock.Lock()

retry:
	// First try to fetch the worker from the queue.
	if w = p.workers.(*workerMap).get(id, p.nowTime()); w != nil {
		p.lock.Unlock()
		return
	}

	// If the worker queue is empty, and we don't run out of the pool capacity,
	// then just spawn a new worker goroutine.
	if capacity := p.Cap(); capacity == -1 || capacity > p.Running() {
		p.lock.Unlock()
		w = p.workerCache.Get().(*goWorkerWithID)
		w.(*goWorkerWithID).id = id
		w.(*goWorkerWithID).lastUsed = p.nowTime()
		err = p.workers.(*workerMap).insert(w)
		if err != nil {
			panic(err)
		}
		w.run()
		return
	}

	// Bail out early if it's in nonblocking mode or the number of pending callers reaches the maximum limit value.
	if p.options.Nonblocking || (p.options.MaxBlockingTasks != 0 && p.Waiting() >= p.options.MaxBlockingTasks) {
		p.lock.Unlock()
		return nil, ErrPoolOverload
	}

	// Otherwise, we'll have to keep them blocked and wait for at least one worker to be put back into pool.
	p.addWaiting(1)
	p.cond.Wait() // block and wait for an available worker
	p.addWaiting(-1)

	if p.IsClosed() {
		p.lock.Unlock()
		return nil, ErrPoolClosed
	}

	goto retry
}

// revertWorker puts a worker back into free pool, recycling the goroutines.
// 从 ants.go poolCommon.revertWorker 方法复制过来, 并修改为支持 id
func (p *PoolWithID) revertWorker(worker worker) bool {
	if capacity := p.Cap(); (capacity > 0 && p.Running() > capacity) || p.IsClosed() {
		p.cond.Broadcast()
		return false
	}

	worker.setLastUsedTime(p.nowTime())

	p.lock.Lock()

	// 只保活 不用回收，因为本来就还在map里

	return true
}

// releaseWorker releases a worker back into free pool, recycling the goroutines.
// 新增
//
//	@receiver p
//	@param w
func (p *PoolWithID) releaseWorker(w *goWorkerWithID) {
	p.lock.Lock()
	p.workers.(*workerMap).detachWithID(w.id)
	p.lock.Unlock()
}
