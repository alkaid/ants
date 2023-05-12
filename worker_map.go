package ants

import (
	"errors"
	"time"
)

type workerMap struct {
	items  map[int]worker
	expiry []worker
}

func newWorkerMap(size int) *workerMap {
	return &workerMap{
		items: map[int]worker{},
	}
}

func (wq *workerMap) len() int {
	return len(wq.items)
}

func (wq *workerMap) isEmpty() bool {
	return len(wq.items) == 0
}

func (wq *workerMap) insert(w worker) error {
	idw, ok := w.(*goWorkerWithID)
	if !ok {
		return errors.New("workerMap only accept goWorkerWithID")
	}
	wq.items[idw.id] = w
	return nil
}

func (wq *workerMap) detach() worker {
	panic("workerMap detach unreachable,instead of get(id)")
}

func (wq *workerMap) get(id int, now time.Time) worker {
	w, ok := wq.items[id]
	if !ok {
		return w
	}
	w.(*goWorkerWithID).lastUsed = now
	return wq.items[id]
}

func (wq *workerMap) detachWithID(id int) worker {
	w := wq.items[id]
	delete(wq.items, id)
	return w
}

func (wq *workerMap) refresh(duration time.Duration) []worker {
	n := wq.len()
	if n == 0 {
		return nil
	}

	expiryTime := time.Now().Add(-duration)
	wq.expiry = wq.expiry[:0]
	for _, w := range wq.items {
		if expiryTime.Before(w.lastUsedTime()) {
			wq.expiry = append(wq.expiry, w)
			delete(wq.items, w.(*goWorkerWithID).id)
		}
	}
	return wq.expiry
}

func (wq *workerMap) reset() {
	for _, w := range wq.items {
		w.finish()
		delete(wq.items, w.(*goWorkerWithID).id)
	}
}
