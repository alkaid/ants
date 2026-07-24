package ants

import (
	"sync"
	"time"
)

const (
	poolWithIDEscapeEventBuffer   = 64
	defaultMaxEscapedWorkers      = 64
	defaultMaxEscapedWorkersPerID = 1
)

// PoolWithIDEscapeEventType identifies a PoolWithID escape-state transition.
type PoolWithIDEscapeEventType uint8

const (
	// PoolWithIDWorkerEscaped reports that a timed-out owner was replaced while
	// its task remained alive.
	PoolWithIDWorkerEscaped PoolWithIDEscapeEventType = iota + 1

	// PoolWithIDEscapedWorkerExited reports that an escaped worker's task later
	// returned or panicked and the worker exited.
	PoolWithIDEscapedWorkerExited

	// PoolWithIDEscapeBudgetExhausted reports that a replacement was not started
	// because one or both escape budgets were exhausted.
	PoolWithIDEscapeBudgetExhausted
)

// PoolWithIDEscapeBudgetReason is a bitmask describing which escape budgets
// prevented a replacement. The zero value means that no budget is exhausted.
type PoolWithIDEscapeBudgetReason uint8

const (
	// PoolWithIDEscapeGlobalBudgetExhausted means the pool-wide escape budget
	// prevented a replacement.
	PoolWithIDEscapeGlobalBudgetExhausted PoolWithIDEscapeBudgetReason = 1 << iota

	// PoolWithIDEscapePerIDBudgetExhausted means the escape budget for one ID
	// prevented a replacement.
	PoolWithIDEscapePerIDBudgetExhausted
)

// PoolWithIDEscapeEvent describes one best-effort escape-state notification.
// Applications should reconcile notifications against EscapeSnapshot rather
// than treating the event stream as authoritative state.
type PoolWithIDEscapeEvent struct {
	// Type identifies the escape-state transition.
	Type PoolWithIDEscapeEventType
	// ID is the task queue associated with the transition.
	ID int
	// Generation is the PoolWithID generation in which the worker started.
	Generation uint64
	// Time is when the escape state was updated.
	Time time.Time
	// Total is the number of escaped workers after this transition.
	Total int
	// ByID is the number of escaped workers for ID after this transition.
	ByID int
	// BudgetReason identifies the budgets newly exhausted by an exhausted event.
	// It is zero for worker start and exit events.
	BudgetReason PoolWithIDEscapeBudgetReason
	// GlobalBudget is the effective pool-wide escape limit at this transition.
	GlobalBudget int
	// PerIDBudget is the effective per-ID escape limit at this transition.
	PerIDBudget int
}

// PoolWithIDEscapeSnapshot is the authoritative current escape state.
type PoolWithIDEscapeSnapshot struct {
	// Total is the number of escaped workers that have not exited.
	Total int
	// ByID maps IDs to their current escaped-worker counts. The map is a copy.
	ByID map[int]int
	// DroppedEvents is the number of notifications dropped because the event
	// channel was full.
	DroppedEvents uint64
	// GlobalBudget is the current effective pool-wide escape limit.
	GlobalBudget int
	// PerIDBudget is the current effective escape limit for one ID.
	PerIDBudget int
	// ExhaustedByID maps IDs to the budgets that most recently prevented their
	// replacement. The map is a copy.
	ExhaustedByID map[int]PoolWithIDEscapeBudgetReason
}

// PoolWithIDEscapeBudgetStatus is an O(1) view of escape-budget use for one ID.
type PoolWithIDEscapeBudgetStatus struct {
	GlobalUsed  int
	GlobalLimit int
	PerIDUsed   int
	PerIDLimit  int
	Reason      PoolWithIDEscapeBudgetReason
}

type poolWithIDEscapePermit struct {
	id         int
	generation uint64
}

type poolWithIDEscapeState struct {
	// transitionMu orders escape-start publication, escaped-worker exits, and
	// authoritative snapshots. Callers that already hold it may use the state
	// helpers below, which only acquire mu.
	transitionMu sync.Mutex
	mu           sync.Mutex

	events        chan PoolWithIDEscapeEvent
	globalLimit   int
	perIDLimit    int
	total         int
	byID          map[int]int
	exhaustedByID map[int]PoolWithIDEscapeBudgetReason
	permits       map[*goWorkerWithID]poolWithIDEscapePermit
	dropped       uint64
}

func newPoolWithIDEscapeState(globalLimit, perIDLimit int) *poolWithIDEscapeState {
	return &poolWithIDEscapeState{
		events:        make(chan PoolWithIDEscapeEvent, poolWithIDEscapeEventBuffer),
		globalLimit:   globalLimit,
		perIDLimit:    perIDLimit,
		byID:          make(map[int]int),
		exhaustedByID: make(map[int]PoolWithIDEscapeBudgetReason),
		permits:       make(map[*goWorkerWithID]poolWithIDEscapePermit),
	}
}

// EscapeEvents returns the nonblocking notification stream for escaped owner
// starts, exits, and budget exhaustion. The channel remains open across Release
// and Reboot and has a fixed capacity of 64. Publishing never waits; a full
// channel drops the notification and increments DroppedEscapeEvents.
//
// One application consumer should read the channel and distribute events when
// multiple observers need them. Applications should use their own context to
// stop consuming and periodically use EscapeSnapshot as the authoritative
// current state. An escape event must not by itself trigger an automatic retry:
// the old task may still run and produce late side effects.
func (p *PoolWithID) EscapeEvents() <-chan PoolWithIDEscapeEvent {
	return p.escape.events
}

// EscapeSnapshot returns the authoritative current escape state. ByID and
// ExhaustedByID are caller-owned copies. Counts and dropped-event totals remain
// continuous across Release and Reboot.
func (p *PoolWithID) EscapeSnapshot() PoolWithIDEscapeSnapshot {
	if hook := p.testHooks.beforeEscapeSnapshotLock; hook != nil {
		hook()
	}
	p.escape.transitionMu.Lock()
	defer p.escape.transitionMu.Unlock()

	p.escape.mu.Lock()
	defer p.escape.mu.Unlock()
	p.escape.clearRecoveredBudgetReasonsLocked()

	byID := make(map[int]int, len(p.escape.byID))
	for id, count := range p.escape.byID {
		byID[id] = count
	}
	exhaustedByID := make(map[int]PoolWithIDEscapeBudgetReason, len(p.escape.exhaustedByID))
	for id, reason := range p.escape.exhaustedByID {
		exhaustedByID[id] = reason
	}
	return PoolWithIDEscapeSnapshot{
		Total:         p.escape.total,
		ByID:          byID,
		DroppedEvents: p.escape.dropped,
		GlobalBudget:  p.escape.globalLimit,
		PerIDBudget:   p.escape.perIDLimit,
		ExhaustedByID: exhaustedByID,
	}
}

// EscapeBudgetStatus returns the current escape-budget use and the most recent
// exhausted reason for id. It runs in O(1) time.
func (p *PoolWithID) EscapeBudgetStatus(id int) PoolWithIDEscapeBudgetStatus {
	p.escape.mu.Lock()
	reason := p.escape.reconciledReasonForIDLocked(id)
	status := PoolWithIDEscapeBudgetStatus{
		GlobalUsed:  p.escape.total,
		GlobalLimit: p.escape.globalLimit,
		PerIDUsed:   p.escape.byID[id],
		PerIDLimit:  p.escape.perIDLimit,
		Reason:      reason,
	}
	p.escape.mu.Unlock()
	return status
}

// Escaped returns the number of escaped workers whose tasks are still alive.
func (p *PoolWithID) Escaped() int {
	p.escape.mu.Lock()
	total := p.escape.total
	p.escape.mu.Unlock()
	return total
}

// TotalWorkers returns managed owners plus escaped workers whose tasks are
// still alive.
func (p *PoolWithID) TotalWorkers() int {
	return p.Running() + p.Escaped()
}

// DroppedEscapeEvents returns the lifetime number of best-effort escape events
// dropped because the event channel was full.
func (p *PoolWithID) DroppedEscapeEvents() uint64 {
	p.escape.mu.Lock()
	dropped := p.escape.dropped
	p.escape.mu.Unlock()
	return dropped
}

func resolvePoolWithIDEscapeBudgets(capacity int, options *Options) (global, perID int) {
	global = options.MaxEscapedWorkers
	if global == 0 {
		if capacity < 0 {
			global = defaultMaxEscapedWorkers
		} else {
			global = capacity / 4
			if global < 1 {
				global = 1
			}
			if global > defaultMaxEscapedWorkers {
				global = defaultMaxEscapedWorkers
			}
		}
	}
	perID = options.MaxEscapedWorkersPerID
	if perID == 0 {
		perID = defaultMaxEscapedWorkersPerID
	}
	return global, perID
}

// tryAcquireEscapePermit requires the caller to hold transitionMu. A denied
// duplicate exhaustion edge returns a zero-Type event. The permit is bound to
// worker and generation until rollback or escapedWorkerExited releases it.
func (p *PoolWithID) tryAcquireEscapePermit(
	worker *goWorkerWithID,
	now time.Time,
) (PoolWithIDEscapeEvent, PoolWithIDEscapeEvent, bool) {
	id := worker.entry.id
	p.escape.mu.Lock()
	defer p.escape.mu.Unlock()

	if _, exists := p.escape.permits[worker]; exists {
		return PoolWithIDEscapeEvent{}, PoolWithIDEscapeEvent{}, false
	}
	reason := p.escape.exhaustedReasonLocked(id)
	if reason != 0 {
		previous := p.escape.reconciledReasonForIDLocked(id)
		added := reason &^ previous
		p.escape.exhaustedByID[id] = reason
		if added == 0 {
			return PoolWithIDEscapeEvent{}, PoolWithIDEscapeEvent{}, false
		}
		exhaustedEvent := p.escape.eventLocked(
			PoolWithIDEscapeBudgetExhausted,
			id,
			worker.generation,
			now,
			added,
		)
		return PoolWithIDEscapeEvent{}, exhaustedEvent, false
	}

	delete(p.escape.exhaustedByID, id)
	p.escape.total++
	p.escape.byID[id]++
	p.escape.permits[worker] = poolWithIDEscapePermit{id: id, generation: worker.generation}
	acquiredEvent := p.escape.eventLocked(
		PoolWithIDWorkerEscaped,
		id,
		worker.generation,
		now,
		0,
	)
	return acquiredEvent, PoolWithIDEscapeEvent{}, true
}

// clearEscapeBudgetExhausted clears the edge-triggered exhaustion state when
// the corresponding scheduler entry is drained or removed.
func (p *PoolWithID) clearEscapeBudgetExhausted(id int) {
	p.escape.mu.Lock()
	delete(p.escape.exhaustedByID, id)
	p.escape.mu.Unlock()
}

// updateEscapeBudgets installs effective limits and clears reasons recovered by
// a dynamic default-budget change. It may be called while p.lock is held because
// it never acquires transitionMu or p.lock. The result reports whether a reason
// was cleared so the caller can wake the purge loop after dropping p.lock.
func (p *PoolWithID) updateEscapeBudgets(globalLimit, perIDLimit int) bool {
	p.escape.mu.Lock()
	p.escape.globalLimit = globalLimit
	p.escape.perIDLimit = perIDLimit
	recovered := p.escape.clearRecoveredBudgetReasonsLocked()
	p.escape.mu.Unlock()
	return recovered
}

func (p *PoolWithID) escapedWorkerExited(worker *goWorkerWithID) {
	p.escape.transitionMu.Lock()
	now := p.clock.Now()
	p.escape.mu.Lock()
	permit, permitFound := p.escape.permits[worker]
	if !permitFound {
		p.escape.mu.Unlock()
		p.escape.transitionMu.Unlock()
		return
	}
	delete(p.escape.permits, worker)
	p.escape.decrementLocked(permit.id)
	p.escape.clearRecoveredBudgetReasonsLocked()
	event := p.escape.eventLocked(
		PoolWithIDEscapedWorkerExited,
		permit.id,
		permit.generation,
		now,
		0,
	)
	p.escape.mu.Unlock()

	p.publishEscapeEvent(event)
	p.escape.transitionMu.Unlock()
	p.signalPurge()
}

func (p *PoolWithID) publishEscapeEvent(event PoolWithIDEscapeEvent) {
	if event.Type == 0 {
		return
	}
	select {
	case p.escape.events <- event:
	default:
		p.escape.mu.Lock()
		p.escape.dropped++
		p.escape.mu.Unlock()
	}
}

func (s *poolWithIDEscapeState) exhaustedReasonLocked(
	id int,
) PoolWithIDEscapeBudgetReason {
	var reason PoolWithIDEscapeBudgetReason
	if s.total >= s.globalLimit {
		reason |= PoolWithIDEscapeGlobalBudgetExhausted
	}
	if s.byID[id] >= s.perIDLimit {
		reason |= PoolWithIDEscapePerIDBudgetExhausted
	}
	return reason
}

func (s *poolWithIDEscapeState) reconciledReasonForIDLocked(
	id int,
) PoolWithIDEscapeBudgetReason {
	reason := s.exhaustedByID[id]
	if s.total < s.globalLimit {
		reason &^= PoolWithIDEscapeGlobalBudgetExhausted
	}
	if s.byID[id] < s.perIDLimit {
		reason &^= PoolWithIDEscapePerIDBudgetExhausted
	}
	if reason == 0 {
		delete(s.exhaustedByID, id)
	} else {
		s.exhaustedByID[id] = reason
	}
	return reason
}

func (s *poolWithIDEscapeState) clearRecoveredBudgetReasonsLocked() bool {
	recovered := false
	for id := range s.exhaustedByID {
		previous := s.exhaustedByID[id]
		if s.reconciledReasonForIDLocked(id) != previous {
			recovered = true
		}
	}
	return recovered
}

func (s *poolWithIDEscapeState) decrementLocked(id int) {
	if s.byID[id] == 0 {
		return
	}
	s.total--
	s.byID[id]--
	if s.byID[id] == 0 {
		delete(s.byID, id)
	}
}

func (s *poolWithIDEscapeState) eventLocked(
	eventType PoolWithIDEscapeEventType,
	id int,
	generation uint64,
	now time.Time,
	reason PoolWithIDEscapeBudgetReason,
) PoolWithIDEscapeEvent {
	return PoolWithIDEscapeEvent{
		Type:         eventType,
		ID:           id,
		Generation:   generation,
		Time:         now,
		Total:        s.total,
		ByID:         s.byID[id],
		BudgetReason: reason,
		GlobalBudget: s.globalLimit,
		PerIDBudget:  s.perIDLimit,
	}
}
