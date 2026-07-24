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

const (
	// MinTaskBuffer is retained for source compatibility.
	//
	// Deprecated: use DefaultTaskBuffer for the default admission limit.
	MinTaskBuffer = 10

	// DefaultTaskBuffer is the default PoolWithID admission limit per ID.
	DefaultTaskBuffer = 100

	// MaxTaskBuffer is the largest PoolWithID admission limit accepted per ID.
	MaxTaskBuffer = 64 * 1024
)

// poolWithIDBackgroundStartHook is a signal-only test seam. Production code
// leaves it nil.
var poolWithIDBackgroundStartHook func()

// PoolWithID executes tasks for the same ID serially and in FIFO start order
// when each Submit has returned before the next begins. Concurrent Submit calls
// have no defined order, and the timeout recovery path does not guarantee task
// completion order.
//
// RunningTaskTimeout is measured from the start of task execution. When a task
// reaches that threshold, the current owner may escape and a replacement owner
// continues consuming the same ID queue, subject to the configured escape
// budgets. The escaped task cannot be forcibly stopped and may overlap later
// tasks, retain resources, or produce late side effects.
//
// Running and Free count only managed current owners. Escaped workers do not
// consume pool capacity; Running()+EscapeSnapshot().Total estimates the live
// worker goroutines associated with the pool. TotalWorkers reports that sum.
type PoolWithID struct {
	*poolCommon

	registry          *workerIDRegistry
	reservations      map[int]*workerIDReservation
	reservedOwners    atomic.Int32
	nextAllocator     uint64
	admissionLimit    int
	generation        atomic.Uint64
	managedCloseFence atomic.Uint64
	clock             poolWithIDClock
	purgeWake         chan struct{}

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
	afterAdmissionCheck              func()
	afterSubmitRegistered            func()
	beforeSubmitFinished             func()
	afterCapacityWaitRegistered      func()
	afterTaskFinished                func()
	beforeOwnerExited                func(*goWorkerWithID)
	beforeReservationAllocate        func(int)
	afterReservationAllocated        func(int)
	duringReservationCapacityConvert func(int)
	afterPurgeEntryVisited           func()
	afterEscapeTransitionsRecorded   func()
	beforeEscapeSnapshotLock         func()
	afterManagedCloseFence           func(uint64)
	beforeReleaseLock                func()
	afterReleaseLock                 func()
}

type workerIDReservationState uint8

const (
	workerIDReservationPending workerIDReservationState = iota
	workerIDReservationCommitted
	workerIDReservationAborted
)

// workerIDReservation owns one capacity slot while its allocator creates the
// entry outside p.lock. Its fields and map membership are protected by p.lock;
// only the recorded allocator may commit or abort and close done.
type workerIDReservation struct {
	id         int
	registry   *workerIDRegistry
	generation uint64
	allocator  uint64
	state      workerIDReservationState
	done       chan struct{}
}

// poolWithIDWaiter transfers one Waiting slot across adjacent capacity and
// reservation waits without double-counting the Submit call.
type poolWithIDWaiter struct {
	held bool
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
	waiter := poolWithIDWaiter{}
	entry, stop, err := p.registerSubmit(id, generation, &waiter)
	if err != nil {
		return err
	}

	accepted := false
	defer func() {
		if hook := p.testHooks.beforeSubmitFinished; hook != nil {
			hook()
		}
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
// TaskBuffer is normalized to DefaultTaskBuffer when it is zero; the physical
// per-ID queue has twice the configured admission capacity. A negative value
// or one above MaxTaskBuffer returns
// ErrInvalidPoolWithIDTaskBuffer before background goroutines are started.
func NewPoolWithID(size int, options ...Option) (*PoolWithID, error) {
	pc, err := newPoolCommon(size, false, options...)
	if err != nil {
		return nil, err
	}
	if pc.options.ExpiryDuration < 0 {
		return nil, ErrInvalidPoolExpiry
	}
	if pc.options.ExpiryDuration == 0 {
		pc.options.ExpiryDuration = DefaultPoolWithIDExpiryDuration
	}
	if pc.options.RunningTaskTimeout < 0 {
		return nil, ErrInvalidPoolWithIDRunningTaskTimeout
	}
	if pc.options.RunningTaskTimeout == 0 {
		pc.options.RunningTaskTimeout = DefaultRunningTaskTimeout
	}
	if pc.options.MaxEscapedWorkers < 0 || pc.options.MaxEscapedWorkersPerID < 0 {
		return nil, ErrInvalidPoolWithIDEscapeBudget
	}

	limit := pc.options.TaskBuffer
	if limit == 0 {
		limit = DefaultTaskBuffer
	}
	if limit < 0 || limit > MaxTaskBuffer {
		return nil, ErrInvalidPoolWithIDTaskBuffer
	}
	pc.options.TaskBuffer = limit
	globalBudget, perIDBudget := resolvePoolWithIDEscapeBudgets(pc.Cap(), pc.options)

	p := &PoolWithID{
		poolCommon:     pc,
		registry:       newWorkerIDRegistry(),
		reservations:   make(map[int]*workerIDReservation),
		admissionLimit: limit,
		submitStop:     make(chan struct{}),
		managedDone:    make(chan struct{}),
		closedDone:     make(chan struct{}),
		managedOnce:    &sync.Once{},
		clock:          poolWithIDClockFactory(),
		purgeWake:      make(chan struct{}, 1),
		escape:         newPoolWithIDEscapeState(globalBudget, perIDBudget),
		purgeFinished:  make(chan struct{}),
		tickFinished:   make(chan struct{}),
	}
	p.generation.Store(1)
	p.startIDBackgroundLocked()
	return p, nil
}

func (p *PoolWithID) registerSubmit(
	id int,
	generation uint64,
	waiter *poolWithIDWaiter,
) (*workerIDEntry, <-chan struct{}, error) {
	p.lock.Lock()
	for {
		if atomic.LoadInt32(&p.state) != OPENED ||
			p.generation.Load() != generation {
			p.releaseWaiterLocked(waiter)
			p.lock.Unlock()
			return nil, nil, ErrPoolClosed
		}

		if entry := p.registry.items[id]; entry != nil {
			p.releaseWaiterLocked(waiter)
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

		if reservation := p.reservations[id]; reservation != nil {
			if p.options.Nonblocking || !p.acquireWaiterLocked(waiter) {
				p.releaseWaiterLocked(waiter)
				p.lock.Unlock()
				return nil, nil, ErrPoolOverload
			}
			done := reservation.done
			stop := p.submitStop
			p.lock.Unlock()
			select {
			case <-done:
			case <-stop:
			}
			p.lock.Lock()
			continue
		}

		capacity := p.Cap()
		reserved := int(p.reservedOwners.Load())
		if capacity == -1 || p.Running()+reserved < capacity {
			p.releaseWaiterLocked(waiter)
			reservation := p.reserveOwnerLocked(id, generation)
			allocator := reservation.allocator
			p.lock.Unlock()
			return p.allocateReservedOwner(reservation, allocator)
		}

		if p.options.Nonblocking || !p.acquireWaiterLocked(waiter) {
			p.releaseWaiterLocked(waiter)
			p.lock.Unlock()
			return nil, nil, ErrPoolOverload
		}

		if hook := p.testHooks.afterCapacityWaitRegistered; hook != nil {
			hook()
		}
		p.cond.Wait()
	}
}

func (p *PoolWithID) acquireWaiterLocked(waiter *poolWithIDWaiter) bool {
	if waiter.held {
		return true
	}
	if p.options.MaxBlockingTasks != 0 && p.Waiting() >= p.options.MaxBlockingTasks {
		return false
	}
	p.addWaiting(1)
	waiter.held = true
	return true
}

func (p *PoolWithID) releaseWaiterLocked(waiter *poolWithIDWaiter) {
	if !waiter.held {
		return
	}
	p.addWaiting(-1)
	waiter.held = false
	p.maybeManagedDoneLocked()
}

func (p *PoolWithID) reserveOwnerLocked(id int, generation uint64) *workerIDReservation {
	p.nextAllocator++
	reservation := &workerIDReservation{
		id:         id,
		registry:   p.registry,
		generation: generation,
		allocator:  p.nextAllocator,
		state:      workerIDReservationPending,
		done:       make(chan struct{}),
	}
	p.reservations[id] = reservation
	p.reservedOwners.Add(1)
	return reservation
}

func (p *PoolWithID) allocateReservedOwner(
	reservation *workerIDReservation,
	allocator uint64,
) (entry *workerIDEntry, stop <-chan struct{}, err error) {
	defer func() {
		if panicValue := recover(); panicValue != nil {
			p.abortReservation(reservation, allocator)
			panic(panicValue)
		}
	}()

	if hook := p.testHooks.beforeReservationAllocate; hook != nil {
		hook(reservation.id)
	}
	now := p.clock.Now()
	entry = newWorkerIDEntry(
		reservation.registry,
		reservation.id,
		p.admissionLimit*2,
		reservation.generation,
		now,
	)
	owner := newWorkerWithID(p, entry)
	entry.owner = owner
	if hook := p.testHooks.afterReservationAllocated; hook != nil {
		hook(reservation.id)
	}

	stop, err = p.commitReservation(reservation, allocator, entry, owner)
	if err != nil {
		return nil, nil, err
	}
	if hook := p.testHooks.afterSubmitRegistered; hook != nil {
		hook()
	}
	return entry, stop, nil
}

func (p *PoolWithID) commitReservation(
	reservation *workerIDReservation,
	allocator uint64,
	entry *workerIDEntry,
	owner *goWorkerWithID,
) (<-chan struct{}, error) {
	p.lock.Lock()
	defer p.lock.Unlock()

	if p.reservations[reservation.id] != reservation ||
		reservation.allocator != allocator ||
		reservation.state != workerIDReservationPending {
		return nil, ErrPoolClosed
	}
	if atomic.LoadInt32(&p.state) != OPENED ||
		p.generation.Load() != reservation.generation ||
		p.registry != reservation.registry {
		p.abortReservationLocked(reservation, allocator)
		return nil, ErrPoolClosed
	}

	entry.pendingSubmits = 1
	entry.outstanding = 1
	reservation.registry.items[reservation.id] = entry
	delete(p.reservations, reservation.id)
	reservation.state = workerIDReservationCommitted
	p.addRunning(1)
	if hook := p.testHooks.duringReservationCapacityConvert; hook != nil {
		hook(reservation.id)
	}
	p.reservedOwners.Add(-1)
	close(reservation.done)
	owner.run()
	return p.submitStop, nil
}

func (p *PoolWithID) abortReservation(reservation *workerIDReservation, allocator uint64) {
	p.lock.Lock()
	p.abortReservationLocked(reservation, allocator)
	p.lock.Unlock()
}

func (p *PoolWithID) abortReservationLocked(
	reservation *workerIDReservation,
	allocator uint64,
) bool {
	if p.reservations[reservation.id] != reservation ||
		reservation.allocator != allocator ||
		reservation.state != workerIDReservationPending {
		return false
	}
	delete(p.reservations, reservation.id)
	p.reservedOwners.Add(-1)
	reservation.state = workerIDReservationAborted
	close(reservation.done)
	p.cond.Broadcast()
	p.maybeManagedDoneLocked()
	return true
}

func (p *PoolWithID) finishSubmit(entry *workerIDEntry, accepted bool) {
	entry.mu.Lock()
	entry.pendingSubmits--
	if !accepted {
		entry.outstanding--
	}
	idleCandidate := entry.pendingSubmits == 0 && entry.outstanding == 0 &&
		entry.taskStartedAt.IsZero()
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
			entry.lastIdleAt = p.clock.Now()
			registry.appendIdle(entry)
			p.clearEscapeBudgetExhausted(entry.id)
		}
		owner = entry.owner
		entry.mu.Unlock()
		registry.expiryMu.Unlock()
	}

	state := atomic.LoadInt32(&p.state)
	if state != OPENED || idleCandidate {
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
	entry.taskStartedAt = p.clock.Now()
	registry.appendRunning(entry)
	return true
}

func (p *PoolWithID) isManagedOwner(owner *goWorkerWithID) bool {
	entry := owner.entry
	entry.mu.Lock()
	managed := entry.owner == owner
	entry.mu.Unlock()
	return managed
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
	entry.taskStartedAt = time.Time{}
	if entry.drained() {
		entry.lastIdleAt = p.clock.Now()
		registry.appendIdle(entry)
		p.clearEscapeBudgetExhausted(entry.id)
	}
	return false
}

func (p *PoolWithID) retireOwnerIfDrained(owner *goWorkerWithID) bool {
	state := atomic.LoadInt32(&p.state)
	capacity := p.Cap()
	reserved := int(p.reservedOwners.Load())
	// Release transitions state and scans entries while holding p.lock, so the
	// normal opened path does not need to contend with Submit on that lock.
	if state == OPENED && (capacity <= 0 || p.Running()+reserved <= capacity) {
		return false
	}

	entry := owner.entry
	p.lock.Lock()
	state = atomic.LoadInt32(&p.state)
	capacity = p.Cap()
	overCapacity := capacity > 0 &&
		len(p.registry.items)+int(p.reservedOwners.Load()) > capacity
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
	state := atomic.LoadInt32(&p.state)
	capacity := p.Cap()
	overCapacity := capacity > 0 &&
		len(p.registry.items)+int(p.reservedOwners.Load()) > capacity
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
	p.clearEscapeBudgetExhausted(entry.id)
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
	if hook := p.testHooks.beforeOwnerExited; hook != nil {
		hook(owner)
	}
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
		entry.taskStartedAt = time.Time{}
		entry.lastIdleAt = p.clock.Now()
		state := atomic.LoadInt32(&p.state)
		overCapacity := p.Cap() > 0 &&
			len(p.registry.items)+int(p.reservedOwners.Load()) > p.Cap()
		if !entry.drained() || (state == OPENED && !overCapacity) {
			replacement := newWorkerWithID(p, entry)
			entry.owner = replacement
			if entry.drained() {
				registry.appendIdle(entry)
				p.clearEscapeBudgetExhausted(entry.id)
			}
			replacement.run()
			entry.mu.Unlock()
			registry.expiryMu.Unlock()
			p.lock.Unlock()
			return
		}
		delete(registry.items, entry.id)
		p.clearEscapeBudgetExhausted(entry.id)
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
		p.reservedOwners.Load() == 0 && p.Running() == 0 && p.Waiting() == 0 {
		p.managedOnce.Do(func() {
			p.managedCloseFence.Store(p.generation.Load())
			close(p.managedDone)
		})
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
	generation := p.generation.Load()
	p.lock.Unlock()

	go p.awaitClosed(generation, managedDone, purgeFinished, tickFinished, closedDone)
	return closedDone, false
}

func (p *PoolWithID) awaitClosed(
	generation uint64,
	managedDone, purgeFinished, tickFinished, closedDone chan struct{},
) {
	<-managedDone
	if hook := p.testHooks.afterManagedCloseFence; hook != nil {
		hook(generation)
	}

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
		p.reservations = make(map[int]*workerIDReservation)
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
	p.lock.Lock()
	capacity := p.Cap()
	if capacity == -1 || size <= 0 || size == capacity {
		p.lock.Unlock()
		return
	}
	atomic.StoreInt32(&p.capacity, int32(size))
	if p.options.MaxEscapedWorkers == 0 {
		globalBudget, perIDBudget := resolvePoolWithIDEscapeBudgets(size, p.options)
		p.escape.transitionMu.Lock()
		p.updateEscapeBudgets(globalBudget, perIDBudget)
		p.escape.transitionMu.Unlock()
		p.signalPurge()
	}
	if size > capacity {
		p.cond.Broadcast()
		p.lock.Unlock()
		return
	}

	if atomic.LoadInt32(&p.state) != OPENED {
		p.lock.Unlock()
		return
	}

	registry := p.registry
	excess := len(registry.items) + int(p.reservedOwners.Load()) - size
	var ownersToStop []*goWorkerWithID
	registry.expiryMu.Lock()
	for entry := registry.idle.head; entry != nil && excess > 0; {
		next := entry.expiryNext
		entry.mu.Lock()
		owner := entry.owner
		if p.detachEntryLocked(entry, owner) {
			ownersToStop = append(ownersToStop, owner)
			excess--
		}
		entry.mu.Unlock()
		entry = next
	}
	registry.expiryMu.Unlock()
	p.lock.Unlock()

	for _, owner := range ownersToStop {
		close(owner.stop)
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
	interval := p.options.ExpiryDuration
	if !p.options.DisablePurgeRunning {
		runningInterval := p.options.RunningTaskTimeout
		if runningInterval > maxPoolWithIDRunningScanInterval {
			runningInterval = maxPoolWithIDRunningScanInterval
		}
		if runningInterval < interval {
			interval = runningInterval
		}
	}
	ticker := time.NewTicker(interval)
	defer func() {
		ticker.Stop()
		close(finished)
	}()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.purgeExpiredNow()
		case <-p.purgeWake:
			p.purgeExpiredNow()
		}
	}
}

func (p *PoolWithID) signalPurge() {
	select {
	case p.purgeWake <- struct{}{}:
	default:
	}
}

func (p *PoolWithID) purgeExpiredNow() {
	p.purgeExpiredWithElapsed(p.clock.Now(), p.clock.Since)
}

// purgeExpired is separated from the ticker so lifecycle races can be tested
// with a synthetic time and explicit synchronization.
func (p *PoolWithID) purgeExpired(now time.Time) {
	p.purgeExpiredWithElapsed(now, now.Sub)
}

func (p *PoolWithID) purgeExpiredWithElapsed(
	now time.Time,
	elapsed func(time.Time) time.Duration,
) {
	if p.options.DisablePurge {
		return
	}

	type transition struct {
		worker      *goWorkerWithID
		replacement *goWorkerWithID
	}
	var transitions []transition
	var events []PoolWithIDEscapeEvent
	var ownersToStop []*goWorkerWithID

	p.lock.Lock()
	p.escape.transitionMu.Lock()
	p.registry.expiryMu.Lock()
	state := atomic.LoadInt32(&p.state)
	generation := p.generation.Load()
	if state == CLOSED || p.managedCloseFence.Load() == generation {
		p.registry.expiryMu.Unlock()
		p.escape.transitionMu.Unlock()
		p.lock.Unlock()
		return
	}

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
		if state != CLOSING && elapsed(entry.lastIdleAt) < p.options.ExpiryDuration {
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
		for entry := p.registry.running.head; entry != nil; {
			next := entry.expiryNext
			if hook := p.testHooks.afterPurgeEntryVisited; hook != nil {
				hook()
			}
			entry.mu.Lock()
			if entry.taskStartedAt.IsZero() {
				p.registry.removeExpiry(entry)
				if entry.drained() {
					p.registry.appendIdle(entry)
					p.clearEscapeBudgetExhausted(entry.id)
				}
				entry.mu.Unlock()
				entry = next
				continue
			}
			if elapsed(entry.taskStartedAt) < p.options.RunningTaskTimeout {
				entry.mu.Unlock()
				break
			}

			oldOwner := entry.owner
			acquiredEvent, exhaustedEvent, acquired := p.tryAcquireEscapePermit(
				oldOwner,
				now,
			)
			if exhaustedEvent.Type != 0 {
				events = append(events, exhaustedEvent)
			}
			if !acquired {
				entry.mu.Unlock()
				entry = next
				continue
			}
			events = append(events, acquiredEvent)
			newOwner := newWorkerWithID(p, entry)
			p.registry.removeExpiry(entry)
			entry.owner = newOwner
			entry.taskStartedAt = time.Time{}
			entry.lastIdleAt = now
			if entry.drained() {
				overCapacity := p.Cap() > 0 &&
					len(p.registry.items)+int(p.reservedOwners.Load()) > p.Cap()
				if state == CLOSING || overCapacity {
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
			})
			entry.mu.Unlock()
			entry = next
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
	if hook := p.testHooks.afterEscapeTransitionsRecorded; hook != nil {
		hook()
	}

	for _, event := range events {
		p.publishEscapeEvent(event)
	}
	for _, item := range transitions {
		close(item.worker.stop)
	}
	p.escape.transitionMu.Unlock()
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
