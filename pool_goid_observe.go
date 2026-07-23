package ants

import (
	"sync"
	"time"
)

const poolWithIDEscapeEventBuffer = 64

// PoolWithIDEscapeEventType identifies a PoolWithID worker escape transition.
type PoolWithIDEscapeEventType uint8

const (
	// PoolWithIDWorkerEscaped reports that a timed-out owner was replaced while
	// its task remained alive.
	PoolWithIDWorkerEscaped PoolWithIDEscapeEventType = iota + 1

	// PoolWithIDEscapedWorkerExited reports that an escaped worker's task later
	// returned or panicked and the worker exited.
	PoolWithIDEscapedWorkerExited
)

// PoolWithIDEscapeEvent describes one best-effort worker escape notification.
// Applications should reconcile notifications against EscapeSnapshot rather
// than treating the event stream as authoritative state.
type PoolWithIDEscapeEvent struct {
	// Type identifies whether a worker escaped or later exited.
	Type PoolWithIDEscapeEventType
	// ID is the task queue whose owner escaped.
	ID int
	// Time is when the escape state was updated.
	Time time.Time
	// Total is the number of escaped workers after this transition.
	Total int
	// ByID is the number of escaped workers for ID after this transition.
	ByID int
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
}

type poolWithIDEscapeState struct {
	transitionMu sync.Mutex
	mu           sync.Mutex

	events  chan PoolWithIDEscapeEvent
	total   int
	byID    map[int]int
	dropped uint64
}

func newPoolWithIDEscapeState() *poolWithIDEscapeState {
	return &poolWithIDEscapeState{
		events: make(chan PoolWithIDEscapeEvent, poolWithIDEscapeEventBuffer),
		byID:   make(map[int]int),
	}
}

// EscapeEvents returns the nonblocking notification stream for escaped owner
// starts and exits. The channel remains open across Release and Reboot and has
// a fixed capacity of 64. Publishing never waits; a full channel drops the
// notification and increments EscapeSnapshot().DroppedEvents.
//
// One application consumer should read the channel and distribute events when
// multiple observers need them. Applications should use their own context to
// stop consuming and periodically use EscapeSnapshot as the authoritative
// current state. An escape event must not by itself trigger an automatic retry:
// the old task may still run and produce late side effects.
func (p *PoolWithID) EscapeEvents() <-chan PoolWithIDEscapeEvent {
	return p.escape.events
}

// EscapeSnapshot returns the authoritative current escaped-worker counts and
// the cumulative number of dropped notifications. ByID is a caller-owned copy,
// and DroppedEvents is monotonic for the lifetime of the PoolWithID, including
// across Release and Reboot.
func (p *PoolWithID) EscapeSnapshot() PoolWithIDEscapeSnapshot {
	if hook := p.testHooks.beforeEscapeSnapshotLock; hook != nil {
		hook()
	}
	p.escape.transitionMu.Lock()
	defer p.escape.transitionMu.Unlock()

	p.escape.mu.Lock()
	defer p.escape.mu.Unlock()

	byID := make(map[int]int, len(p.escape.byID))
	for id, count := range p.escape.byID {
		byID[id] = count
	}
	return PoolWithIDEscapeSnapshot{
		Total:         p.escape.total,
		ByID:          byID,
		DroppedEvents: p.escape.dropped,
	}
}

func (p *PoolWithID) recordWorkerEscaped(id int) PoolWithIDEscapeEvent {
	p.escape.mu.Lock()
	p.escape.total++
	p.escape.byID[id]++
	event := PoolWithIDEscapeEvent{
		Type:  PoolWithIDWorkerEscaped,
		ID:    id,
		Time:  time.Now(),
		Total: p.escape.total,
		ByID:  p.escape.byID[id],
	}
	p.escape.mu.Unlock()
	return event
}

func (p *PoolWithID) escapedWorkerExited(worker *goWorkerWithID) {
	id := worker.entry.id
	p.escape.transitionMu.Lock()
	p.escape.mu.Lock()
	if p.escape.byID[id] > 0 {
		p.escape.total--
		p.escape.byID[id]--
		if p.escape.byID[id] == 0 {
			delete(p.escape.byID, id)
		}
	}
	event := PoolWithIDEscapeEvent{
		Type:  PoolWithIDEscapedWorkerExited,
		ID:    id,
		Time:  time.Now(),
		Total: p.escape.total,
		ByID:  p.escape.byID[id],
	}
	p.escape.mu.Unlock()

	p.publishEscapeEvent(event)
	p.escape.transitionMu.Unlock()
	p.logEscapeEvent(event)
}

func (p *PoolWithID) publishEscapeEvent(event PoolWithIDEscapeEvent) {
	select {
	case p.escape.events <- event:
	default:
		p.escape.mu.Lock()
		p.escape.dropped++
		p.escape.mu.Unlock()
	}
}

func (p *PoolWithID) logEscapeEvent(event PoolWithIDEscapeEvent) {
	eventName := "worker_escaped"
	if event.Type == PoolWithIDEscapedWorkerExited {
		eventName = "escaped_worker_exited"
	}
	defer func() { _ = recover() }()
	p.options.Logger.Printf(
		"pool_with_id_escape event=%s id=%d by_id=%d total=%d",
		eventName,
		event.ID,
		event.ByID,
		event.Total,
	)
}
