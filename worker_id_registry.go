package ants

import (
	"sync"
	"time"

	syncx "github.com/alkaid/ants/v2/pkg/sync"
)

// workerIDRegistry owns stable per-ID entries. Its map is protected by the
// PoolWithID registry lock; entry state has a narrower lock for submissions and
// worker state transitions.
type workerIDRegistry struct {
	items    map[int]*workerIDEntry
	expiryMu sync.Locker
	idle     workerIDEntryList
	running  workerIDEntryList
}

func newWorkerIDRegistry() *workerIDRegistry {
	return &workerIDRegistry{
		items:    make(map[int]*workerIDEntry),
		expiryMu: syncx.NewSpinLock(),
	}
}

type workerIDEntry struct {
	mu sync.Mutex

	registry   *workerIDRegistry
	id         int
	generation uint64
	tasks      chan func()
	owner      *goWorkerWithID

	// pendingSubmits counts callers that registered while the pool was open but
	// have not yet completed their channel send attempt. outstanding counts
	// those callers plus accepted tasks not yet taken by an owner. Keeping both
	// closes the receive-to-task-start window for purge and release.
	pendingSubmits int
	outstanding    int

	taskStartedAt time.Time
	lastIdleAt    time.Time
	expiryPending bool

	expiryPrev *workerIDEntry
	expiryNext *workerIDEntry
	expiryList workerIDEntryListKind
}

type workerIDEntryListKind uint8

const (
	workerIDEntryListNone workerIDEntryListKind = iota
	workerIDEntryListIdle
	workerIDEntryListRunning
)

type workerIDEntryList struct {
	head *workerIDEntry
	tail *workerIDEntry
}

func newWorkerIDEntry(
	registry *workerIDRegistry,
	id, taskCapacity int,
	generation uint64,
	now time.Time,
) *workerIDEntry {
	return &workerIDEntry{
		registry:   registry,
		id:         id,
		generation: generation,
		tasks:      make(chan func(), taskCapacity),
		lastIdleAt: now,
	}
}

func (e *workerIDEntry) drained() bool {
	return e.pendingSubmits == 0 && e.outstanding == 0 &&
		e.taskStartedAt.IsZero() && !e.expiryPending
}

// removeExpiry requires expiryMu.
func (r *workerIDRegistry) removeExpiry(entry *workerIDEntry) {
	if entry.expiryList == workerIDEntryListNone {
		return
	}
	list := &r.idle
	if entry.expiryList == workerIDEntryListRunning {
		list = &r.running
	}
	if entry.expiryPrev == nil {
		list.head = entry.expiryNext
	} else {
		entry.expiryPrev.expiryNext = entry.expiryNext
	}
	if entry.expiryNext == nil {
		list.tail = entry.expiryPrev
	} else {
		entry.expiryNext.expiryPrev = entry.expiryPrev
	}
	entry.expiryPrev = nil
	entry.expiryNext = nil
	entry.expiryList = workerIDEntryListNone
}

// appendIdle requires expiryMu and the entry lock.
func (r *workerIDRegistry) appendIdle(entry *workerIDEntry) {
	r.appendExpiry(entry, workerIDEntryListIdle, &r.idle)
}

// appendRunning requires expiryMu and the entry lock.
func (r *workerIDRegistry) appendRunning(entry *workerIDEntry) {
	r.appendExpiry(entry, workerIDEntryListRunning, &r.running)
}

func (r *workerIDRegistry) appendExpiry(
	entry *workerIDEntry,
	kind workerIDEntryListKind,
	list *workerIDEntryList,
) {
	r.removeExpiry(entry)
	entry.expiryPrev = list.tail
	entry.expiryList = kind
	if list.tail == nil {
		list.head = entry
	} else {
		list.tail.expiryNext = entry
	}
	list.tail = entry
}
