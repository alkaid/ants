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
)

// goWorkerWithID is the actual executor who runs the tasks,
// it starts a goroutine that accepts tasks and
// performs function calls.
type goWorkerWithID struct {
	pool       *PoolWithID
	entry      *workerIDEntry
	generation uint64
	// stop is closed either to retire this owner or after its escape transition
	// has been recorded. Those states are mutually exclusive for one owner.
	stop chan struct{}
}

func newWorkerWithID(pool *PoolWithID, entry *workerIDEntry) *goWorkerWithID {
	return &goWorkerWithID{
		pool:       pool,
		entry:      entry,
		generation: entry.generation,
		stop:       make(chan struct{}),
	}
}

// run starts this owner. Its managed-capacity slot is reserved by PoolWithID
// before run is called.
func (w *goWorkerWithID) run() {
	go w.loop()
}

func (w *goWorkerWithID) loop() {
	managedOwner := true
	defer func() {
		panicValue := recover()
		if managedOwner {
			w.pool.ownerExited(w)
		}
		if panicValue != nil {
			w.pool.logWorkerPanic(w.entry.id, panicValue, debug.Stack())
		}
	}()

	for {
		select {
		case task := <-w.entry.tasks:
			if !w.pool.startTask(w) {
				managedOwner = false
				<-w.stop
				w.pool.escapedWorkerExited(w)
				return
			}

			panicValue, stack := w.execute(task)
			panicHandled := false
			if panicValue != nil && w.pool.isManagedOwner(w) {
				w.pool.handleTaskPanic(w.entry.id, panicValue, stack)
				panicHandled = true
			}
			if w.pool.finishTask(w) {
				managedOwner = false
				<-w.stop
				w.pool.escapedWorkerExited(w)
				if panicValue != nil && !panicHandled {
					w.pool.handleTaskPanic(w.entry.id, panicValue, stack)
				}
				return
			}
			if panicValue != nil && !panicHandled {
				w.pool.handleTaskPanic(w.entry.id, panicValue, stack)
			}
			if hook := w.pool.testHooks.afterTaskFinished; hook != nil {
				hook()
			}
			if w.pool.retireOwnerIfDrained(w) {
				return
			}

		case <-w.stop:
			return
		}
	}
}

func (w *goWorkerWithID) execute(task func()) (panicValue any, stack []byte) {
	defer func() {
		if p := recover(); p != nil {
			panicValue = p
			stack = debug.Stack()
		}
	}()
	task()
	return nil, nil
}
