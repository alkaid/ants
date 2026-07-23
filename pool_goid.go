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
	"context"
	"errors"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"
)

// MinTaskBuffer is the default admission limit for a PoolWithID task queue.
// The physical queue capacity is twice this value.
const MinTaskBuffer = 10

// poolWithIDBackgroundStartHook is a signal-only test seam. Production code
// leaves it nil.
var poolWithIDBackgroundStartHook func()

// PoolWithID executes tasks for the same ID serially and in FIFO start order
// when each Submit has returned before the next begins. Concurrent Submit calls
// have no defined order, and the timeout recovery path does not guarantee task
// completion order.
//
// ExpiryDuration is measured from the start of task execution. When a task
// reaches that threshold, the current owner may escape and a replacement owner
// continues consuming the same ID queue. The escaped task cannot be forcibly
// stopped and may overlap later tasks, retain resources, or produce late side
// effects. WithDisablePurgeRunning(true) or WithDisablePurge(true) disables this
// recovery and allows a blocked task to block its ID indefinitely.
//
// Running and Free count only managed current owners. Escaped workers do not
// consume pool capacity; Running()+EscapeSnapshot().Total estimates the live
// worker goroutines associated with the pool.
type PoolWithID struct {
	*poolCommon

	registry       *workerIDRegistry
	admissionLimit int
	generation     atomic.Uint64

	submitStop  chan struct{}
	managedDone chan struct{}
	closedDone  chan struct{}
	managedOnce *sync.Once

	purgeCancel   context.CancelFunc
	purgeFinished chan struct{}
	tickCancel    context.CancelFunc
	tickFinished  chan struct{}

	escape *poolWithIDEscapeState

	testHooks poolWithIDTestHooks
}

type poolWithIDTestHooks struct {
	afterAdmissionCheck            func()
	afterSubmitRegistered          func()
	afterCapacityWaitRegistered    func()
	afterTaskFinished              func()
	afterPurgeEntryVisited         func()
	afterEscapeTransitionsRecorded func()
	beforeEscapeSnapshotLock       func()
	beforeReleaseLock              func()
	afterReleaseLock               func()
}

// Submit registers task for id. Successfully returned, non-concurrent submits
// for one ID start in FIFO order unless timeout recovery makes a replacement
// owner overlap an escaped task. Concurrent Submit calls have no defined order.
//
// In nonblocking mode, a new ID is rejected when owner capacity is unavailable.
// An existing ID is rejected when its observed queue length reaches TaskBuffer
// or when the final channel send cannot complete immediately. The admission
// check and send are not serialized, so concurrent submissions may enter the
// reserved half of the physical queue.
//
// In blocking mode, a new ID waits for owner capacity subject to
// MaxBlockingTasks. An existing ID may use the full physical queue and then
// waits for queue space or pool closure; MaxBlockingTasks does not limit that
// queue wait. A task that recursively submits to its own full queue in blocking
// mode is not guaranteed to make progress.
func (p *PoolWithID) Submit(id int, task func()) error {
	generation := p.generation.Load()
	entry, stop, err := p.registerSubmit(id, generation)
	if err != nil {
		return err
	}

	accepted := false
	defer func() {
		p.finishSubmit(entry, accepted)
	}()

	if p.options.Nonblocking {
		if len(entry.tasks) >= p.admissionLimit {
			return ErrPoolOverload
		}
		if hook := p.testHooks.afterAdmissionCheck; hook != nil {
			hook()
		}
		select {
		case entry.tasks <- task:
			accepted = true
			return nil
		case <-stop:
			return ErrPoolClosed
		default:
			return ErrPoolOverload
		}
	}

	select {
	case entry.tasks <- task:
		accepted = true
		return nil
	case <-stop:
		return ErrPoolClosed
	}
}

// NewPoolWithID instantiates a PoolWithID with the same public Option type used
// by the other pool constructors. It accepts direct Option values, expanded
// []Option slices, and WithOptions. WithPreAlloc(true) is accepted but does not
// preallocate or reuse ID workers.
//
// TaskBuffer is normalized to a default admission limit of 10 when it is zero;
// the physical per-ID queue has twice the configured admission capacity. A
// negative value or one that overflows when doubled returns
// ErrInvalidPoolWithIDTaskBuffer before background goroutines are started.
func NewPoolWithID(size int, options ...Option) (*PoolWithID, error) {
	pc, err := newPoolCommon(size, false, options...)
	if err != nil {
		return nil, err
	}

	limit := pc.options.TaskBuffer
	maxInt := int(^uint(0) >> 1)
	if limit == 0 {
		limit = MinTaskBuffer
	}
	if limit < 0 || limit > maxInt/2 {
		return nil, ErrInvalidPoolWithIDTaskBuffer
	}
	pc.options.TaskBuffer = limit

	p := &PoolWithID{
		poolCommon:     pc,
		registry:       newWorkerIDRegistry(),
		admissionLimit: limit,
		submitStop:     make(chan struct{}),
		managedDone:    make(chan struct{}),
		closedDone:     make(chan struct{}),
		managedOnce:    &sync.Once{},
		escape:         newPoolWithIDEscapeState(),
		purgeFinished:  make(chan struct{}),
		tickFinished:   make(chan struct{}),
	}
	p.generation.Store(1)
	p.startIDBackgroundLocked()
	return p, nil
}

func (p *PoolWithID) registerSubmit(id int, generation uint64) (*workerIDEntry, <-chan struct{}, error) {
	p.lock.Lock()
	for {
		if atomic.LoadInt32(&p.state) != OPENED ||
			p.generation.Load() != generation {
			p.maybeManagedDoneLocked()
			p.lock.Unlock()
			return nil, nil, ErrPoolClosed
		}

		if entry := p.registry.items[id]; entry != nil {
			entry.mu.Lock()
			entry.pendingSubmits++
			entry.outstanding++
			entry.mu.Unlock()
			stop := p.submitStop
			p.lock.Unlock()
			if hook := p.testHooks.afterSubmitRegistered; hook != nil {
				hook()
			}
			return entry, stop, nil
		}

		if capacity := p.Cap(); capacity == -1 || p.Running() < capacity {
			now := time.Now().UnixNano()
			registry := p.registry
			entry := newWorkerIDEntry(registry, id, p.admissionLimit*2, now)
			owner := newWorkerWithID(p, entry)
			entry.owner = owner
			entry.pendingSubmits = 1
			entry.outstanding = 1
			registry.items[id] = entry
			p.addRunning(1)
			owner.run()
			stop := p.submitStop
			p.lock.Unlock()
			if hook := p.testHooks.afterSubmitRegistered; hook != nil {
				hook()
			}
			return entry, stop, nil
		}

		if p.options.Nonblocking ||
			(p.options.MaxBlockingTasks != 0 && p.Waiting() >= p.options.MaxBlockingTasks) {
			p.lock.Unlock()
			return nil, nil, ErrPoolOverload
		}

		p.addWaiting(1)
		if hook := p.testHooks.afterCapacityWaitRegistered; hook != nil {
			hook()
		}
		p.cond.Wait()
		p.addWaiting(-1)
		if atomic.LoadInt32(&p.state) != OPENED ||
			p.generation.Load() != generation {
			p.maybeManagedDoneLocked()
			p.lock.Unlock()
			return nil, nil, ErrPoolClosed
		}
	}
}

func (p *PoolWithID) finishSubmit(entry *workerIDEntry, accepted bool) {
	entry.mu.Lock()
	entry.pendingSubmits--
	if !accepted {
		entry.outstanding--
	}
	idleCandidate := entry.pendingSubmits == 0 && entry.outstanding == 0 &&
		entry.taskStartedAt == 0
	if idleCandidate {
		entry.expiryPending = true
	}
	owner := entry.owner
	entry.mu.Unlock()

	if idleCandidate {
		registry := entry.registry
		registry.expiryMu.Lock()
		entry.mu.Lock()
		entry.expiryPending = false
		if entry.drained() {
			entry.lastIdleAt = time.Now().UnixNano()
			registry.appendIdle(entry)
		}
		owner = entry.owner
		entry.mu.Unlock()
		registry.expiryMu.Unlock()
	}

	if atomic.LoadInt32(&p.state) != OPENED {
		p.retireEntryIfDrained(entry, owner)
	}
}

func (p *PoolWithID) startTask(owner *goWorkerWithID) bool {
	entry := owner.entry
	registry := entry.registry
	registry.expiryMu.Lock()
	defer registry.expiryMu.Unlock()
	entry.mu.Lock()
	defer entry.mu.Unlock()
	if entry.owner != owner {
		return false
	}
	registry.removeExpiry(entry)
	if entry.outstanding > 0 {
		entry.outstanding--
	}
	entry.taskStartedAt = time.Now().UnixNano()
	registry.appendRunning(entry)
	return true
}

// finishTask returns true when owner was escaped while its task was running.
func (p *PoolWithID) finishTask(owner *goWorkerWithID) bool {
	entry := owner.entry
	registry := entry.registry
	registry.expiryMu.Lock()
	defer registry.expiryMu.Unlock()
	entry.mu.Lock()
	defer entry.mu.Unlock()
	if entry.owner != owner {
		return true
	}
	registry.removeExpiry(entry)
	entry.taskStartedAt = 0
	if entry.drained() {
		entry.lastIdleAt = time.Now().UnixNano()
		registry.appendIdle(entry)
	}
	return false
}

func (p *PoolWithID) retireOwnerIfDrained(owner *goWorkerWithID) bool {
	state := atomic.LoadInt32(&p.state)
	capacity := p.Cap()
	// Release transitions state and scans entries while holding p.lock, so the
	// normal opened path does not need to contend with Submit on that lock.
	if state == OPENED && (capacity <= 0 || p.Running() <= capacity) {
		return false
	}

	entry := owner.entry
	p.lock.Lock()
	state = atomic.LoadInt32(&p.state)
	capacity = p.Cap()
	overCapacity := capacity > 0 && p.Running() > capacity
	if state == OPENED && !overCapacity {
		p.maybeManagedDoneLocked()
		p.lock.Unlock()
		return false
	}
	registry := entry.registry
	registry.expiryMu.Lock()
	entry.mu.Lock()
	retired := p.retireEntryLocked(entry, owner)
	entry.mu.Unlock()
	registry.expiryMu.Unlock()
	if retired {
		p.cond.Broadcast()
	}
	p.maybeManagedDoneLocked()
	p.lock.Unlock()
	return retired
}

func (p *PoolWithID) retireEntryIfDrained(entry *workerIDEntry, owner *goWorkerWithID) bool {
	p.lock.Lock()
	registry := entry.registry
	registry.expiryMu.Lock()
	entry.mu.Lock()
	retired := p.retireEntryLocked(entry, owner)
	entry.mu.Unlock()
	registry.expiryMu.Unlock()
	if retired {
		p.cond.Broadcast()
	}
	p.maybeManagedDoneLocked()
	p.lock.Unlock()
	return retired
}

// detachEntryLocked requires the pool lock, expiryMu, and the entry lock. It
// removes scheduler ownership without waking the owner.
func (p *PoolWithID) detachEntryLocked(entry *workerIDEntry, owner *goWorkerWithID) bool {
	registry := entry.registry
	if p.registry != registry || registry.items[entry.id] != entry ||
		entry.owner != owner || !entry.drained() {
		return false
	}
	registry.removeExpiry(entry)
	delete(registry.items, entry.id)
	return true
}

// retireEntryLocked requires the pool lock, expiryMu, and the entry lock.
func (p *PoolWithID) retireEntryLocked(entry *workerIDEntry, owner *goWorkerWithID) bool {
	if !p.detachEntryLocked(entry, owner) {
		return false
	}
	close(owner.stop)
	return true
}

func (p *PoolWithID) ownerExited(owner *goWorkerWithID) {
	p.lock.Lock()
	entry := owner.entry
	registry := entry.registry
	registry.expiryMu.Lock()
	entry.mu.Lock()
	if entry.owner != owner {
		entry.mu.Unlock()
		registry.expiryMu.Unlock()
		p.lock.Unlock()
		<-owner.stop
		p.escapedWorkerExited(owner)
		return
	}
	if p.registry == registry && registry.items[entry.id] == entry && entry.owner == owner {
		registry.removeExpiry(entry)
		entry.taskStartedAt = 0
		entry.lastIdleAt = time.Now().UnixNano()
		state := atomic.LoadInt32(&p.state)
		overCapacity := p.Cap() > 0 && p.Running() > p.Cap()
		if !entry.drained() || (state == OPENED && !overCapacity) {
			replacement := newWorkerWithID(p, entry)
			entry.owner = replacement
			if entry.drained() {
				registry.appendIdle(entry)
			}
			replacement.run()
			entry.mu.Unlock()
			registry.expiryMu.Unlock()
			p.lock.Unlock()
			return
		}
		delete(registry.items, entry.id)
		close(owner.stop)
	}
	entry.mu.Unlock()
	registry.expiryMu.Unlock()
	p.addRunning(-1)
	p.cond.Broadcast()
	p.maybeManagedDoneLocked()
	p.lock.Unlock()
}

func (p *PoolWithID) maybeManagedDoneLocked() {
	if atomic.LoadInt32(&p.state) == CLOSING && len(p.registry.items) == 0 &&
		p.Running() == 0 && p.Waiting() == 0 {
		p.managedOnce.Do(func() { close(p.managedDone) })
	}
}

// IsClosed reports whether the pool is closing or fully closed.
func (p *PoolWithID) IsClosed() bool {
	return atomic.LoadInt32(&p.state) != OPENED
}

// Release stops accepting submissions and starts draining accepted work. It
// returns after the close transition and wakeups have been issued, without
// waiting for the managed drain. Use ReleaseTimeout or ReleaseContext to wait.
// Escaped workers are not part of the managed drain and may remain alive.
func (p *PoolWithID) Release() {
	_, _ = p.startRelease()
}

// ReleaseTimeout starts or joins the current release and waits for managed
// owners and accepted queues to drain until timeout. Tasks accepted before the
// release continue through normal completion, panic, or timeout escape.
// Escaped workers are not included in this wait; CLOSED therefore does not
// imply that every task goroutine has exited.
func (p *PoolWithID) ReleaseTimeout(timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	err := p.ReleaseContext(ctx)
	if errors.Is(err, context.DeadlineExceeded) {
		return ErrTimeout
	}
	return err
}

// ReleaseContext starts or joins the current release and waits until managed
// owners and accepted queues have drained. Escaped workers are not included in
// this wait. A nil context initiates release without waiting.
func (p *PoolWithID) ReleaseContext(ctx context.Context) error {
	done, closed := p.startRelease()
	if closed {
		return ErrPoolClosed
	}
	if ctx == nil {
		return nil
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *PoolWithID) startRelease() (<-chan struct{}, bool) {
	if hook := p.testHooks.beforeReleaseLock; hook != nil {
		hook()
	}
	p.lock.Lock()
	state := atomic.LoadInt32(&p.state)
	if state == CLOSED {
		done := p.closedDone
		p.lock.Unlock()
		return done, true
	}
	if state == CLOSING {
		done := p.closedDone
		p.lock.Unlock()
		return done, false
	}
	if hook := p.testHooks.afterReleaseLock; hook != nil {
		hook()
	}

	atomic.StoreInt32(&p.state, CLOSING)
	close(p.submitStop)

	p.registry.expiryMu.Lock()
	for _, entry := range p.registry.items {
		entry.mu.Lock()
		p.retireEntryLocked(entry, entry.owner)
		entry.mu.Unlock()
	}
	p.registry.expiryMu.Unlock()
	p.cond.Broadcast()
	p.maybeManagedDoneLocked()

	managedDone := p.managedDone
	purgeFinished := p.purgeFinished
	tickFinished := p.tickFinished
	closedDone := p.closedDone
	p.lock.Unlock()

	go p.awaitClosed(managedDone, purgeFinished, tickFinished, closedDone)
	return closedDone, false
}

func (p *PoolWithID) awaitClosed(managedDone, purgeFinished, tickFinished, closedDone chan struct{}) {
	<-managedDone

	p.lock.Lock()
	purgeCancel := p.purgeCancel
	p.purgeCancel = nil
	tickCancel := p.tickCancel
	p.tickCancel = nil
	p.lock.Unlock()
	if purgeCancel != nil {
		purgeCancel()
	}
	if tickCancel != nil {
		tickCancel()
	}

	<-purgeFinished
	<-tickFinished

	p.lock.Lock()
	if atomic.LoadInt32(&p.state) == CLOSING && p.closedDone == closedDone {
		atomic.StoreInt32(&p.state, CLOSED)
		close(closedDone)
	}
	p.lock.Unlock()
}

// Reboot waits for an in-progress managed drain, then opens a new empty ID
// registry. It does not wait for escaped workers. Such workers cannot modify
// the new scheduler state, but their tasks may overlap new work for the same ID
// and may still produce late side effects. Escape events, snapshot counts, and
// dropped-event totals remain continuous across the reboot.
func (p *PoolWithID) Reboot() {
	for {
		p.lock.Lock()
		state := atomic.LoadInt32(&p.state)
		if state == OPENED {
			p.lock.Unlock()
			return
		}
		if state == CLOSING {
			done := p.closedDone
			p.lock.Unlock()
			<-done
			continue
		}

		p.registry = newWorkerIDRegistry()
		p.submitStop = make(chan struct{})
		p.managedDone = make(chan struct{})
		p.closedDone = make(chan struct{})
		p.managedOnce = &sync.Once{}
		p.generation.Add(1)
		atomic.StoreInt32(&p.state, OPENED)
		p.startIDBackgroundLocked()
		p.lock.Unlock()
		return
	}
}

// Tune changes the owner capacity for PoolWithID. PreAlloc is intentionally a
// no-op for this pool and therefore does not disable tuning.
func (p *PoolWithID) Tune(size int) {
	capacity := p.Cap()
	if capacity == -1 || size <= 0 || size == capacity {
		return
	}
	atomic.StoreInt32(&p.capacity, int32(size))
	if size > capacity {
		p.cond.Broadcast()
	}
}

func (p *PoolWithID) startIDBackgroundLocked() {
	if hook := poolWithIDBackgroundStartHook; hook != nil {
		hook()
	}
	atomic.StoreInt64(&p.now, time.Now().UnixNano())

	tickCtx, tickCancel := context.WithCancel(context.Background())
	p.tickCancel = tickCancel
	p.tickFinished = make(chan struct{})
	go p.tickIDClock(tickCtx, p.tickFinished)

	p.purgeFinished = make(chan struct{})
	if p.options.DisablePurge {
		close(p.purgeFinished)
		p.purgeCancel = nil
		return
	}
	purgeCtx, purgeCancel := context.WithCancel(context.Background())
	p.purgeCancel = purgeCancel
	go p.purgeIDs(purgeCtx, p.purgeFinished)
}

func (p *PoolWithID) tickIDClock(ctx context.Context, finished chan struct{}) {
	ticker := time.NewTicker(nowTimeUpdateInterval)
	defer func() {
		ticker.Stop()
		close(finished)
	}()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			atomic.StoreInt64(&p.now, now.UnixNano())
		}
	}
}

func (p *PoolWithID) purgeIDs(ctx context.Context, finished chan struct{}) {
	ticker := time.NewTicker(p.options.ExpiryDuration)
	defer func() {
		ticker.Stop()
		close(finished)
	}()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			p.purgeExpired(now.UnixNano())
		}
	}
}

// purgeExpired is separated from the ticker so lifecycle races can be tested
// with a synthetic time and explicit synchronization.
func (p *PoolWithID) purgeExpired(now int64) {
	if p.options.DisablePurge {
		return
	}

	type transition struct {
		worker      *goWorkerWithID
		replacement *goWorkerWithID
		id          int
		event       PoolWithIDEscapeEvent
	}
	var transitions []transition
	var ownersToStop []*goWorkerWithID

	p.escape.transitionMu.Lock()
	p.lock.Lock()
	p.registry.expiryMu.Lock()
	state := atomic.LoadInt32(&p.state)
	if state == CLOSED {
		p.registry.expiryMu.Unlock()
		p.lock.Unlock()
		p.escape.transitionMu.Unlock()
		return
	}

	expiry := int64(p.options.ExpiryDuration)
	for entry := p.registry.idle.head; entry != nil; entry = p.registry.idle.head {
		if hook := p.testHooks.afterPurgeEntryVisited; hook != nil {
			hook()
		}
		entry.mu.Lock()
		if !entry.drained() {
			p.registry.removeExpiry(entry)
			entry.mu.Unlock()
			continue
		}
		if state != CLOSING && now-entry.lastIdleAt < expiry {
			entry.mu.Unlock()
			break
		}
		owner := entry.owner
		if p.detachEntryLocked(entry, owner) {
			ownersToStop = append(ownersToStop, owner)
		}
		entry.mu.Unlock()
	}

	if !p.options.DisablePurgeRunning {
		for entry := p.registry.running.head; entry != nil; entry = p.registry.running.head {
			if hook := p.testHooks.afterPurgeEntryVisited; hook != nil {
				hook()
			}
			entry.mu.Lock()
			if entry.taskStartedAt == 0 {
				p.registry.removeExpiry(entry)
				if entry.drained() {
					p.registry.appendIdle(entry)
				}
				entry.mu.Unlock()
				continue
			}
			if now-entry.taskStartedAt < expiry {
				entry.mu.Unlock()
				break
			}

			oldOwner := entry.owner
			newOwner := newWorkerWithID(p, entry)
			p.registry.removeExpiry(entry)
			entry.owner = newOwner
			entry.taskStartedAt = 0
			entry.lastIdleAt = now
			if entry.drained() {
				if state == CLOSING {
					if p.detachEntryLocked(entry, newOwner) {
						ownersToStop = append(ownersToStop, newOwner)
					}
				} else {
					p.registry.appendIdle(entry)
				}
			}
			transitions = append(transitions, transition{
				worker:      oldOwner,
				replacement: newOwner,
				id:          entry.id,
			})
			entry.mu.Unlock()
		}
	}
	p.registry.expiryMu.Unlock()
	p.lock.Unlock()

	for _, owner := range ownersToStop {
		close(owner.stop)
	}
	for _, item := range transitions {
		item.replacement.run()
	}
	for i := range transitions {
		transitions[i].event = p.recordWorkerEscaped(transitions[i].id)
	}
	if hook := p.testHooks.afterEscapeTransitionsRecorded; hook != nil {
		hook()
	}

	for _, item := range transitions {
		p.publishEscapeEvent(item.event)
	}
	for _, item := range transitions {
		close(item.worker.stop)
	}
	p.escape.transitionMu.Unlock()
	for _, item := range transitions {
		p.logEscapeEvent(item.event)
	}
}

func (p *PoolWithID) handleTaskPanic(id int, panicValue any, stack []byte) {
	if handler := p.options.PanicHandler; handler != nil {
		func() {
			defer func() {
				if nested := recover(); nested != nil {
					p.logWorkerPanic(id, nested, debug.Stack())
				}
			}()
			handler(panicValue)
		}()
		return
	}
	p.logWorkerPanic(id, panicValue, stack)
}

func (p *PoolWithID) logWorkerPanic(id int, panicValue any, stack []byte) {
	defer func() { _ = recover() }()
	p.options.Logger.Printf("id %d worker recovers from panic: %v\n%s\n", id, panicValue, stack)
}
