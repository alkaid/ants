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
	"runtime/debug"
	"time"
)

// goWorkerWithID is the actual executor who runs the tasks,
// it starts a goroutine that accepts tasks and
// performs function calls.
type goWorkerWithID struct {
	// pool who owns this worker.
	pool *PoolWithID

	// task is a job should be done.
	task chan func()

	// lastUsed will be updated when putting a worker back into queue.
	lastUsed time.Time

	// pool 为stateful时有效
	id int

	running bool

	keepAlive bool
}

// run starts a goroutine to repeat the process
// that performs the function calls.
//
//	超时会导致线程退出回收,若任务时长超过超时时间,可能造成泄漏和脏线程
func (w *goWorkerWithID) run() {
	w.pool.addRunning(1)
	go func() {
		defer func() {
			w.running = false
			w.pool.releaseWorker(w)
			w.pool.addRunning(-1)
			w.pool.workerCache.Put(w)
			// Call Signal() here in case there are goroutines waiting for available workers.
			w.pool.cond.Signal()
		}()

		for f := range w.task {
			if f == nil {
				return
			}
			w.running = true
			// 安全执行,崩溃要恢复并执行下一个任务
			safeF := func() {
				defer func() {
					if p := recover(); p != nil {
						if ph := w.pool.options.PanicHandler; ph != nil {
							ph(p)
						} else {
							w.pool.options.Logger.Printf("id %d worker exits from panic: %v\n%s\n", w.id, p, debug.Stack())
						}
					}
				}()
				f()
			}
			safeF()
			if ok := w.pool.revertWorker(w); !ok {
				return
			}
			w.running = false
		}
	}()
}

func (w *goWorkerWithID) finish() {
	w.task <- nil
}

func (w *goWorkerWithID) lastUsedTime() time.Time {
	// 若保活且运行中则返回now,避免被回收
	if w.keepAlive && w.running {
		return w.pool.nowTime()
	}
	return w.lastUsed
}

func (w *goWorkerWithID) inputFunc(fn func()) {
	w.task <- fn
}

func (w *goWorkerWithID) inputParam(interface{}) {
	panic("unreachable")
}
