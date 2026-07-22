package ants

import "sync"

// workerIDRegistry owns stable per-ID entries. Its map is protected by the
// PoolWithID registry lock; entry state has a narrower lock for submissions and
// worker state transitions.
type workerIDRegistry struct {
	items map[int]*workerIDEntry
}

func newWorkerIDRegistry() *workerIDRegistry {
	return &workerIDRegistry{items: make(map[int]*workerIDEntry)}
}

type workerIDEntry struct {
	mu sync.Mutex

	id    int
	tasks chan func()
	owner *goWorkerWithID

	// pendingSubmits counts callers that registered while the pool was open but
	// have not yet completed their channel send attempt. outstanding counts
	// those callers plus accepted tasks not yet taken by an owner. Keeping both
	// closes the receive-to-task-start window for purge and release.
	pendingSubmits int
	outstanding    int

	taskStartedAt int64
	lastIdleAt    int64
}

func newWorkerIDEntry(id, taskCapacity int, now int64) *workerIDEntry {
	return &workerIDEntry{
		id:         id,
		tasks:      make(chan func(), taskCapacity),
		lastIdleAt: now,
	}
}

func (e *workerIDEntry) drained() bool {
	return e.pendingSubmits == 0 && e.outstanding == 0 && e.taskStartedAt == 0
}
