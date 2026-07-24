package ants

import (
	"context"
	"io"
	"log"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

const poolWithIDObserveTestTimeout = 5 * time.Second

type poolWithIDObserveTestLogger struct {
	events chan string
}

func (l *poolWithIDObserveTestLogger) Printf(_ string, args ...any) {
	if len(args) == 0 {
		return
	}
	event, ok := args[0].(string)
	if !ok {
		return
	}
	select {
	case l.events <- event:
	default:
	}
}

func poolWithIDObserveNewPool(t *testing.T, size int, options ...Option) *PoolWithID {
	t.Helper()
	base := []Option{
		WithExpiryDuration(time.Hour),
		WithTaskBuffer(16),
		WithLogger(log.New(io.Discard, "", 0)),
	}
	p, err := NewPoolWithID(size, append(base, options...)...)
	if err != nil {
		t.Fatalf("NewPoolWithID: %v", err)
	}
	t.Cleanup(func() {
		p.Release()
		select {
		case <-p.closedDone:
		case <-time.After(poolWithIDObserveTestTimeout):
			t.Errorf("PoolWithID did not close during cleanup")
		}
	})
	return p
}

func poolWithIDObserveClose(ch chan struct{}) {
	select {
	case <-ch:
	default:
		close(ch)
	}
}

func poolWithIDObserveReceive[T any](t *testing.T, ch <-chan T, label string) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(poolWithIDObserveTestTimeout):
		var zero T
		t.Fatalf("timed out waiting for %s", label)
		return zero
	}
}

func poolWithIDObserveAssertNoSignal[T any](t *testing.T, ch <-chan T, label string) {
	t.Helper()
	select {
	case <-ch:
		t.Fatalf("%s happened before its explicit barrier", label)
	default:
	}
}

func poolWithIDObserveEventually(t *testing.T, label string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(poolWithIDObserveTestTimeout)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", label)
		}
		runtime.Gosched()
	}
}

func poolWithIDObserveEntryState(t *testing.T, p *PoolWithID, id int) (*workerIDEntry, *goWorkerWithID, time.Time) {
	t.Helper()
	p.lock.Lock()
	entry := p.registry.items[id]
	if entry == nil {
		p.lock.Unlock()
		t.Fatalf("ID %d is not registered", id)
	}
	entry.mu.Lock()
	owner := entry.owner
	startedAt := entry.taskStartedAt
	entry.mu.Unlock()
	p.lock.Unlock()
	return entry, owner, startedAt
}

func poolWithIDObserveAssertEvent(
	t *testing.T,
	event PoolWithIDEscapeEvent,
	wantType PoolWithIDEscapeEventType,
	wantID, wantTotal, wantByID int,
) {
	t.Helper()
	if event.Type != wantType || event.ID != wantID ||
		event.Total != wantTotal || event.ByID != wantByID {
		t.Fatalf(
			"unexpected escape event: got {type:%d id:%d total:%d byID:%d}, want {type:%d id:%d total:%d byID:%d}",
			event.Type, event.ID, event.Total, event.ByID,
			wantType, wantID, wantTotal, wantByID,
		)
	}
	if event.Time.IsZero() {
		t.Fatal("escape event has a zero timestamp")
	}
}

func poolWithIDObserveRelease(t *testing.T, p *PoolWithID) {
	t.Helper()
	done := make(chan error, 1)
	go func() {
		done <- p.ReleaseTimeout(poolWithIDObserveTestTimeout)
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("ReleaseTimeout: %v", err)
		}
	case <-time.After(poolWithIDObserveTestTimeout + time.Second):
		t.Fatal("ReleaseTimeout did not honor its upper bound")
	}
}

func TestPoolWithIDOwnerRemainsSerialBeforeTimeout(t *testing.T) {
	const id = 41
	p := poolWithIDObserveNewPool(t, 1)

	releaseA := make(chan struct{})
	aStarted := make(chan struct{})
	aFinished := make(chan struct{})
	bStarted := make(chan struct{})
	bFinished := make(chan struct{})
	overlapped := make(chan bool, 1)
	t.Cleanup(func() { poolWithIDObserveClose(releaseA) })

	var running atomic.Int32
	if err := p.Submit(id, func() {
		running.Add(1)
		close(aStarted)
		<-releaseA
		running.Add(-1)
		close(aFinished)
	}); err != nil {
		t.Fatalf("submit task A: %v", err)
	}
	poolWithIDObserveReceive(t, aStarted, "task A start")

	if err := p.Submit(id, func() {
		overlapped <- running.Add(1) != 1
		close(bStarted)
		running.Add(-1)
		close(bFinished)
	}); err != nil {
		t.Fatalf("submit task B: %v", err)
	}

	_, _, startedAt := poolWithIDObserveEntryState(t, p, id)
	if startedAt.IsZero() {
		t.Fatal("task A has no recorded start time")
	}
	p.purgeExpired(startedAt.Add(p.options.RunningTaskTimeout - time.Nanosecond))

	poolWithIDObserveAssertNoSignal(t, bStarted, "task B start before timeout")
	poolWithIDObserveAssertNoSignal(t, p.EscapeEvents(), "escape event before timeout")
	if snapshot := p.EscapeSnapshot(); snapshot.Total != 0 || len(snapshot.ByID) != 0 {
		t.Fatalf("unexpected escape snapshot before timeout: %+v", snapshot)
	}

	close(releaseA)
	poolWithIDObserveReceive(t, aFinished, "task A finish")
	poolWithIDObserveReceive(t, bStarted, "task B start")
	if poolWithIDObserveReceive(t, overlapped, "serial execution result") {
		t.Fatal("task B overlapped task A before the escape threshold")
	}
	poolWithIDObserveReceive(t, bFinished, "task B finish")
	poolWithIDObserveRelease(t, p)
}

func TestPoolWithIDTaskCompletionWinsTimeoutRace(t *testing.T) {
	const id = 411
	p := poolWithIDObserveNewPool(t, 1)

	releaseA := make(chan struct{})
	aStarted := make(chan struct{})
	bStarted := make(chan struct{})
	bFinished := make(chan struct{})
	taskStateCleared := make(chan struct{})
	allowOwnerLoop := make(chan struct{})
	secondFinishHook := make(chan struct{})
	t.Cleanup(func() {
		poolWithIDObserveClose(releaseA)
		poolWithIDObserveClose(allowOwnerLoop)
	})

	var finishedCalls atomic.Int32
	p.testHooks.afterTaskFinished = func() {
		switch finishedCalls.Add(1) {
		case 1:
			close(taskStateCleared)
			<-allowOwnerLoop
		case 2:
			close(secondFinishHook)
		}
	}

	if err := p.Submit(id, func() {
		close(aStarted)
		<-releaseA
	}); err != nil {
		t.Fatalf("submit task A: %v", err)
	}
	poolWithIDObserveReceive(t, aStarted, "task A start")
	if err := p.Submit(id, func() {
		close(bStarted)
		close(bFinished)
	}); err != nil {
		t.Fatalf("submit task B: %v", err)
	}

	entry, owner, startedAt := poolWithIDObserveEntryState(t, p, id)
	close(releaseA)
	poolWithIDObserveReceive(t, taskStateCleared, "task A state clear")
	p.purgeExpired(startedAt.Add(p.options.RunningTaskTimeout))

	poolWithIDObserveAssertNoSignal(t, p.EscapeEvents(), "escape after completed task")
	currentEntry, currentOwner, currentStartedAt := poolWithIDObserveEntryState(t, p, id)
	if currentEntry != entry || currentOwner != owner || !currentStartedAt.IsZero() {
		t.Fatalf("completion-first scan changed owner state: entry=%p owner=%p started=%v", currentEntry, currentOwner, currentStartedAt)
	}

	close(allowOwnerLoop)
	poolWithIDObserveReceive(t, bStarted, "task B start")
	poolWithIDObserveReceive(t, bFinished, "task B finish")
	poolWithIDObserveReceive(t, secondFinishHook, "task B finish hook")
	p.testHooks.afterTaskFinished = nil
	if snapshot := p.EscapeSnapshot(); snapshot.Total != 0 || len(snapshot.ByID) != 0 {
		t.Fatalf("completion-first scan created escape state: %+v", snapshot)
	}
	poolWithIDObserveRelease(t, p)
}

func TestPoolWithIDTimeoutTakeoverAndLateOwnerExit(t *testing.T) {
	tests := []struct {
		name       string
		latePanics bool
	}{
		{name: "return"},
		{name: "panic", latePanics: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			const id = 42
			panicSeen := make(chan any, 1)
			p := poolWithIDObserveNewPool(t, 1, WithPanicHandler(func(value any) {
				panicSeen <- value
			}))

			releaseA := make(chan struct{})
			releaseC := make(chan struct{})
			aStarted := make(chan struct{})
			aReturned := make(chan struct{})
			bFinished := make(chan struct{})
			replacementSnapshot := make(chan PoolWithIDEscapeSnapshot, 1)
			cStarted := make(chan struct{})
			cFinished := make(chan struct{})
			dStarted := make(chan struct{})
			dFinished := make(chan struct{})
			t.Cleanup(func() {
				poolWithIDObserveClose(releaseA)
				poolWithIDObserveClose(releaseC)
			})

			if err := p.Submit(id, func() {
				defer close(aReturned)
				close(aStarted)
				<-releaseA
				if tt.latePanics {
					panic("late owner panic")
				}
			}); err != nil {
				t.Fatalf("submit task A: %v", err)
			}
			poolWithIDObserveReceive(t, aStarted, "task A start")

			if err := p.Submit(id, func() {
				replacementSnapshot <- p.EscapeSnapshot()
				close(bFinished)
			}); err != nil {
				t.Fatalf("submit task B: %v", err)
			}
			entry, oldOwner, startedAt := poolWithIDObserveEntryState(t, p, id)
			if startedAt.IsZero() {
				t.Fatal("task A has no recorded start time")
			}

			p.purgeExpired(startedAt.Add(p.options.RunningTaskTimeout))
			startEvent := poolWithIDObserveReceive(t, p.EscapeEvents(), "worker escape event")
			poolWithIDObserveAssertEvent(t, startEvent, PoolWithIDWorkerEscaped, id, 1, 1)
			if snapshot := poolWithIDObserveReceive(t, replacementSnapshot, "replacement owner escape snapshot"); snapshot.Total != 1 || snapshot.ByID[id] != 1 {
				t.Fatalf("replacement owner observed stale escape state: %+v", snapshot)
			}
			poolWithIDObserveReceive(t, bFinished, "replacement owner running task B")
			poolWithIDObserveAssertNoSignal(t, aReturned, "old owner return")

			currentEntry, replacementOwner, _ := poolWithIDObserveEntryState(t, p, id)
			if currentEntry != entry || replacementOwner == oldOwner {
				t.Fatal("timeout did not preserve the entry and replace its owner")
			}

			if err := p.Submit(id, func() {
				close(cStarted)
				<-releaseC
				close(cFinished)
			}); err != nil {
				t.Fatalf("submit task C: %v", err)
			}
			poolWithIDObserveReceive(t, cStarted, "replacement owner running task C")
			if err := p.Submit(id, func() {
				close(dStarted)
				close(dFinished)
			}); err != nil {
				t.Fatalf("submit task D: %v", err)
			}

			close(releaseA)
			poolWithIDObserveReceive(t, aReturned, "old owner return")
			if tt.latePanics {
				if got := poolWithIDObserveReceive(t, panicSeen, "late owner panic recovery"); got != "late owner panic" {
					t.Fatalf("panic handler received %v", got)
				}
			} else {
				poolWithIDObserveAssertNoSignal(t, panicSeen, "panic handler")
			}
			exitEvent := poolWithIDObserveReceive(t, p.EscapeEvents(), "escaped worker exit event")
			poolWithIDObserveAssertEvent(t, exitEvent, PoolWithIDEscapedWorkerExited, id, 0, 0)
			poolWithIDObserveAssertNoSignal(t, dStarted, "old owner reading task D")

			currentEntry, currentOwner, _ := poolWithIDObserveEntryState(t, p, id)
			if currentEntry != entry || currentOwner != replacementOwner {
				t.Fatal("late old-owner exit changed or deleted the replacement owner")
			}
			if snapshot := p.EscapeSnapshot(); snapshot.Total != 0 || len(snapshot.ByID) != 0 {
				t.Fatalf("escaped owner did not leave the snapshot: %+v", snapshot)
			}

			close(releaseC)
			poolWithIDObserveReceive(t, cFinished, "task C finish")
			poolWithIDObserveReceive(t, dStarted, "replacement owner running task D")
			poolWithIDObserveReceive(t, dFinished, "task D finish")
			poolWithIDObserveRelease(t, p)
		})
	}
}

func TestPoolWithIDEscapeSnapshotWaitsForTransition(t *testing.T) {
	const id = 421
	p := poolWithIDObserveNewPool(t, 1)

	releaseA := make(chan struct{})
	aStarted := make(chan struct{})
	bFinished := make(chan struct{})
	transitionPaused := make(chan struct{})
	allowTransition := make(chan struct{})
	snapshotAttempted := make(chan struct{})
	purgeDone := make(chan struct{})
	t.Cleanup(func() {
		p.testHooks.afterEscapeTransitionsRecorded = nil
		p.testHooks.beforeEscapeSnapshotLock = nil
		poolWithIDObserveClose(releaseA)
		poolWithIDObserveClose(allowTransition)
	})

	if err := p.Submit(id, func() {
		close(aStarted)
		<-releaseA
	}); err != nil {
		t.Fatalf("submit task A: %v", err)
	}
	poolWithIDObserveReceive(t, aStarted, "task A start")
	if err := p.Submit(id, func() { close(bFinished) }); err != nil {
		t.Fatalf("submit task B: %v", err)
	}
	_, _, startedAt := poolWithIDObserveEntryState(t, p, id)

	p.testHooks.afterEscapeTransitionsRecorded = func() {
		close(transitionPaused)
		<-allowTransition
	}
	go func() {
		p.purgeExpired(startedAt.Add(p.options.RunningTaskTimeout))
		close(purgeDone)
	}()
	poolWithIDObserveReceive(t, transitionPaused, "escape transition pause")

	p.testHooks.beforeEscapeSnapshotLock = func() { close(snapshotAttempted) }
	snapshotResult := make(chan PoolWithIDEscapeSnapshot, 1)
	go func() { snapshotResult <- p.EscapeSnapshot() }()
	poolWithIDObserveReceive(t, snapshotAttempted, "escape snapshot attempt")
	poolWithIDObserveAssertNoSignal(t, snapshotResult, "snapshot completing inside escape transition")

	p.testHooks.afterEscapeTransitionsRecorded = nil
	close(allowTransition)
	poolWithIDObserveReceive(t, purgeDone, "escape transition completion")
	snapshot := poolWithIDObserveReceive(t, snapshotResult, "linearized escape snapshot")
	p.testHooks.beforeEscapeSnapshotLock = nil
	if snapshot.Total != 1 || snapshot.ByID[id] != 1 {
		t.Fatalf("snapshot after escape transition = %+v, want total=1 byID[%d]=1", snapshot, id)
	}
	poolWithIDObserveReceive(t, bFinished, "replacement owner task B")

	close(releaseA)
	exitEvent := poolWithIDObserveReceive(t, p.EscapeEvents(), "escape start event")
	if exitEvent.Type != PoolWithIDWorkerEscaped {
		t.Fatalf("first escape event type = %v, want %v", exitEvent.Type, PoolWithIDWorkerEscaped)
	}
	exitEvent = poolWithIDObserveReceive(t, p.EscapeEvents(), "escaped worker exit event")
	if exitEvent.Type != PoolWithIDEscapedWorkerExited {
		t.Fatalf("second escape event type = %v, want %v", exitEvent.Type, PoolWithIDEscapedWorkerExited)
	}
	poolWithIDObserveRelease(t, p)
}

func TestPoolWithIDEscapedGoexitOnlyUpdatesEscapeState(t *testing.T) {
	tests := []struct {
		name         string
		panicHandler bool
	}{
		{name: "task"},
		{name: "panic handler", panicHandler: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			const id = 421
			options := []Option(nil)
			if tt.panicHandler {
				options = append(options, WithPanicHandler(func(any) { runtime.Goexit() }))
			}
			p := poolWithIDObserveNewPool(t, 1, options...)

			aStarted := make(chan struct{})
			exitA := make(chan struct{})
			bFinished := make(chan struct{})
			cFinished := make(chan struct{})
			t.Cleanup(func() { poolWithIDObserveClose(exitA) })

			if err := p.Submit(id, func() {
				close(aStarted)
				<-exitA
				if tt.panicHandler {
					panic("panic handler exits")
				}
				runtime.Goexit()
			}); err != nil {
				t.Fatalf("submit task A: %v", err)
			}
			poolWithIDObserveReceive(t, aStarted, "task A start")
			if err := p.Submit(id, func() { close(bFinished) }); err != nil {
				t.Fatalf("submit task B: %v", err)
			}

			_, _, startedAt := poolWithIDObserveEntryState(t, p, id)
			p.purgeExpired(startedAt.Add(p.options.RunningTaskTimeout))
			startEvent := poolWithIDObserveReceive(t, p.EscapeEvents(), "escape start event")
			poolWithIDObserveReceive(t, bFinished, "replacement task B")
			if startEvent.Type != PoolWithIDWorkerEscaped || startEvent.Total != 1 || startEvent.ByID != 1 {
				t.Fatalf("escape start event = %+v", startEvent)
			}
			if got := p.Running(); got != 1 {
				t.Fatalf("Running() after takeover = %d, want 1", got)
			}

			close(exitA)
			exitEvent := poolWithIDObserveReceive(t, p.EscapeEvents(), "escaped Goexit event")
			if exitEvent.Type != PoolWithIDEscapedWorkerExited || exitEvent.Total != 0 || exitEvent.ByID != 0 {
				t.Fatalf("escaped Goexit event = %+v", exitEvent)
			}
			if got := p.Running(); got != 1 {
				t.Fatalf("escaped Goexit changed replacement Running() to %d", got)
			}
			if snapshot := p.EscapeSnapshot(); snapshot.Total != 0 || len(snapshot.ByID) != 0 {
				t.Fatalf("escaped Goexit left stale state: %+v", snapshot)
			}

			if err := p.Submit(id, func() { close(cFinished) }); err != nil {
				t.Fatalf("submit task C: %v", err)
			}
			poolWithIDObserveReceive(t, cFinished, "replacement task C")
			poolWithIDObserveRelease(t, p)
		})
	}
}

func TestPoolWithIDConsecutiveEscapesEventsSnapshotAndRelease(t *testing.T) {
	const id = 43
	p := poolWithIDObserveNewPool(t, 1,
		WithMaxEscapedWorkers(2),
		WithMaxEscapedWorkersPerID(2),
	)

	releaseA := make(chan struct{})
	releaseB := make(chan struct{})
	aStarted := make(chan struct{})
	aReturned := make(chan struct{})
	bStarted := make(chan struct{})
	bReturned := make(chan struct{})
	cFinished := make(chan struct{})
	t.Cleanup(func() {
		poolWithIDObserveClose(releaseA)
		poolWithIDObserveClose(releaseB)
	})

	if err := p.Submit(id, func() {
		defer close(aReturned)
		close(aStarted)
		<-releaseA
	}); err != nil {
		t.Fatalf("submit task A: %v", err)
	}
	poolWithIDObserveReceive(t, aStarted, "task A start")
	if err := p.Submit(id, func() {
		defer close(bReturned)
		close(bStarted)
		<-releaseB
	}); err != nil {
		t.Fatalf("submit task B: %v", err)
	}
	if err := p.Submit(id, func() { close(cFinished) }); err != nil {
		t.Fatalf("submit task C: %v", err)
	}

	_, _, startedAtA := poolWithIDObserveEntryState(t, p, id)
	p.purgeExpired(startedAtA.Add(p.options.RunningTaskTimeout))
	first := poolWithIDObserveReceive(t, p.EscapeEvents(), "first escape event")
	poolWithIDObserveAssertEvent(t, first, PoolWithIDWorkerEscaped, id, 1, 1)
	poolWithIDObserveReceive(t, bStarted, "task B start")

	_, _, startedAtB := poolWithIDObserveEntryState(t, p, id)
	if startedAtB.IsZero() {
		t.Fatal("task B has no recorded start time")
	}
	p.purgeExpired(startedAtB.Add(p.options.RunningTaskTimeout))
	second := poolWithIDObserveReceive(t, p.EscapeEvents(), "second escape event")
	poolWithIDObserveAssertEvent(t, second, PoolWithIDWorkerEscaped, id, 2, 2)
	poolWithIDObserveReceive(t, cFinished, "second replacement owner running task C")

	snapshot := p.EscapeSnapshot()
	if snapshot.Total != 2 || snapshot.ByID[id] != 2 || snapshot.DroppedEvents != 0 {
		t.Fatalf("unexpected snapshot after consecutive escapes: %+v", snapshot)
	}
	snapshot.ByID[id] = 99
	snapshot.ByID[id+1] = 1
	fresh := p.EscapeSnapshot()
	if fresh.Total != 2 || fresh.ByID[id] != 2 {
		t.Fatalf("mutating snapshot map changed internal state: %+v", fresh)
	}
	if _, ok := fresh.ByID[id+1]; ok {
		t.Fatal("snapshot map is not an independent copy")
	}

	poolWithIDObserveRelease(t, p)
	if snapshot = p.EscapeSnapshot(); snapshot.Total != 2 || snapshot.ByID[id] != 2 {
		t.Fatalf("Release waited for or cleared escaped workers: %+v", snapshot)
	}

	close(releaseB)
	poolWithIDObserveReceive(t, bReturned, "task B return")
	firstExit := poolWithIDObserveReceive(t, p.EscapeEvents(), "first escaped worker exit")
	poolWithIDObserveAssertEvent(t, firstExit, PoolWithIDEscapedWorkerExited, id, 1, 1)

	close(releaseA)
	poolWithIDObserveReceive(t, aReturned, "task A return")
	secondExit := poolWithIDObserveReceive(t, p.EscapeEvents(), "second escaped worker exit")
	poolWithIDObserveAssertEvent(t, secondExit, PoolWithIDEscapedWorkerExited, id, 0, 0)
	if snapshot = p.EscapeSnapshot(); snapshot.Total != 0 || len(snapshot.ByID) != 0 {
		t.Fatalf("escaped workers did not drain from snapshot: %+v", snapshot)
	}
}

func TestPoolWithIDFullEscapeEventChannelDropsWithoutBlockingOrLogging(t *testing.T) {
	const id = 44
	logger := &poolWithIDObserveTestLogger{events: make(chan string, 4)}
	p := poolWithIDObserveNewPool(t, 1, WithLogger(logger))

	for i := 0; i < cap(p.escape.events); i++ {
		p.escape.events <- PoolWithIDEscapeEvent{ID: -1}
	}

	releaseA := make(chan struct{})
	aStarted := make(chan struct{})
	aReturned := make(chan struct{})
	bFinished := make(chan struct{})
	t.Cleanup(func() { poolWithIDObserveClose(releaseA) })

	if err := p.Submit(id, func() {
		defer close(aReturned)
		close(aStarted)
		<-releaseA
	}); err != nil {
		t.Fatalf("submit task A: %v", err)
	}
	poolWithIDObserveReceive(t, aStarted, "task A start")
	if err := p.Submit(id, func() { close(bFinished) }); err != nil {
		t.Fatalf("submit task B: %v", err)
	}

	_, _, startedAt := poolWithIDObserveEntryState(t, p, id)
	p.purgeExpired(startedAt.Add(p.options.RunningTaskTimeout))
	poolWithIDObserveReceive(t, bFinished, "replacement owner running task B")
	poolWithIDObserveAssertNoSignal(t, logger.events, "synchronous escape log")
	snapshot := p.EscapeSnapshot()
	if snapshot.Total != 1 || snapshot.ByID[id] != 1 || snapshot.DroppedEvents != 1 {
		t.Fatalf("full event channel lost authoritative escape state: %+v", snapshot)
	}

	close(releaseA)
	poolWithIDObserveReceive(t, aReturned, "task A return")
	poolWithIDObserveEventually(t, "escaped worker exit accounting", func() bool {
		return p.Escaped() == 0 && p.DroppedEscapeEvents() == 2
	})
	poolWithIDObserveAssertNoSignal(t, logger.events, "synchronous escaped-worker exit log")
	snapshot = p.EscapeSnapshot()
	if snapshot.Total != 0 || len(snapshot.ByID) != 0 || snapshot.DroppedEvents != 2 {
		t.Fatalf("full event channel blocked or corrupted worker exit: %+v", snapshot)
	}
	poolWithIDObserveRelease(t, p)
}

func TestPoolWithIDEscapeBatchPublishesStartsBeforeConcurrentExit(t *testing.T) {
	const (
		idA = 49
		idB = 50
		idC = 51
	)
	p := poolWithIDObserveNewPool(t, 3, WithMaxEscapedWorkers(3))

	releaseA := make(chan struct{})
	releaseB := make(chan struct{})
	releaseC := make(chan struct{})
	aStarted := make(chan struct{})
	aReturned := make(chan struct{})
	bStarted := make(chan struct{})
	bReturned := make(chan struct{})
	cStarted := make(chan struct{})
	cReturned := make(chan struct{})
	allowBatchPublish := make(chan struct{})
	t.Cleanup(func() {
		p.testHooks.afterEscapeTransitionsRecorded = nil
		poolWithIDObserveClose(allowBatchPublish)
		poolWithIDObserveClose(releaseA)
		poolWithIDObserveClose(releaseB)
		poolWithIDObserveClose(releaseC)
	})

	if err := p.Submit(idA, func() {
		defer close(aReturned)
		close(aStarted)
		<-releaseA
	}); err != nil {
		t.Fatalf("submit task A: %v", err)
	}
	poolWithIDObserveReceive(t, aStarted, "task A start")
	_, _, startedAtA := poolWithIDObserveEntryState(t, p, idA)
	p.purgeExpired(startedAtA.Add(p.options.RunningTaskTimeout))
	firstEscape := poolWithIDObserveReceive(t, p.EscapeEvents(), "task A escape event")
	poolWithIDObserveAssertEvent(t, firstEscape, PoolWithIDWorkerEscaped, idA, 1, 1)

	if err := p.Submit(idB, func() {
		defer close(bReturned)
		close(bStarted)
		<-releaseB
	}); err != nil {
		t.Fatalf("submit task B: %v", err)
	}
	if err := p.Submit(idC, func() {
		defer close(cReturned)
		close(cStarted)
		<-releaseC
	}); err != nil {
		t.Fatalf("submit task C: %v", err)
	}
	poolWithIDObserveReceive(t, bStarted, "task B start")
	poolWithIDObserveReceive(t, cStarted, "task C start")
	_, _, startedAtB := poolWithIDObserveEntryState(t, p, idB)
	_, _, startedAtC := poolWithIDObserveEntryState(t, p, idC)
	batchNow := startedAtB
	if startedAtC.After(batchNow) {
		batchNow = startedAtC
	}
	batchNow = batchNow.Add(p.options.RunningTaskTimeout)

	batchRecorded := make(chan struct{})
	p.testHooks.afterEscapeTransitionsRecorded = func() {
		close(batchRecorded)
		<-allowBatchPublish
	}
	purgeDone := make(chan struct{})
	go func() {
		p.purgeExpired(batchNow)
		close(purgeDone)
	}()
	poolWithIDObserveReceive(t, batchRecorded, "B/C escape batch record")
	p.testHooks.afterEscapeTransitionsRecorded = nil

	close(releaseA)
	poolWithIDObserveReceive(t, aReturned, "task A return during B/C escape batch")
	poolWithIDObserveAssertNoSignal(t, p.EscapeEvents(), "task A exit overtaking B/C escape starts")
	close(allowBatchPublish)
	poolWithIDObserveReceive(t, purgeDone, "B/C escape batch publish")

	firstBatchStart := poolWithIDObserveReceive(t, p.EscapeEvents(), "first B/C escape start")
	secondBatchStart := poolWithIDObserveReceive(t, p.EscapeEvents(), "second B/C escape start")
	if firstBatchStart.Type != PoolWithIDWorkerEscaped || firstBatchStart.Total != 2 || firstBatchStart.ByID != 1 {
		t.Fatalf("unexpected first batch start event: %+v", firstBatchStart)
	}
	if secondBatchStart.Type != PoolWithIDWorkerEscaped || secondBatchStart.Total != 3 || secondBatchStart.ByID != 1 {
		t.Fatalf("unexpected second batch start event: %+v", secondBatchStart)
	}
	if firstBatchStart.Time.IsZero() || secondBatchStart.Time.IsZero() {
		t.Fatal("B/C escape start event has a zero timestamp")
	}
	if firstBatchStart.ID == secondBatchStart.ID ||
		(firstBatchStart.ID != idB && firstBatchStart.ID != idC) ||
		(secondBatchStart.ID != idB && secondBatchStart.ID != idC) {
		t.Fatalf("batch start IDs = %d, %d; want IDs %d and %d", firstBatchStart.ID, secondBatchStart.ID, idB, idC)
	}
	aExit := poolWithIDObserveReceive(t, p.EscapeEvents(), "task A exit after B/C starts")
	poolWithIDObserveAssertEvent(t, aExit, PoolWithIDEscapedWorkerExited, idA, 2, 0)
	if snapshot := p.EscapeSnapshot(); snapshot.Total != 2 ||
		len(snapshot.ByID) != 2 || snapshot.ByID[idB] != 1 || snapshot.ByID[idC] != 1 {
		t.Fatalf("stale snapshot after ordered B/C starts and A exit: %+v", snapshot)
	}

	close(releaseB)
	poolWithIDObserveReceive(t, bReturned, "task B return")
	bExit := poolWithIDObserveReceive(t, p.EscapeEvents(), "task B exit")
	poolWithIDObserveAssertEvent(t, bExit, PoolWithIDEscapedWorkerExited, idB, 1, 0)
	close(releaseC)
	poolWithIDObserveReceive(t, cReturned, "task C return")
	cExit := poolWithIDObserveReceive(t, p.EscapeEvents(), "task C exit")
	poolWithIDObserveAssertEvent(t, cExit, PoolWithIDEscapedWorkerExited, idC, 0, 0)
	if snapshot := p.EscapeSnapshot(); snapshot.Total != 0 || len(snapshot.ByID) != 0 {
		t.Fatalf("escape state did not drain after B/C returned: %+v", snapshot)
	}
	poolWithIDObserveRelease(t, p)
}

func TestPoolWithIDDisablePurgeOptionsPreventAutomaticEscape(t *testing.T) {
	tests := []struct {
		name   string
		option Option
	}{
		{name: "DisablePurgeRunning", option: WithDisablePurgeRunning(true)},
		{name: "DisablePurge", option: WithDisablePurge(true)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			const id = 45
			p := poolWithIDObserveNewPool(t, 1, tt.option)
			releaseA := make(chan struct{})
			aStarted := make(chan struct{})
			aFinished := make(chan struct{})
			bStarted := make(chan struct{})
			bFinished := make(chan struct{})
			t.Cleanup(func() { poolWithIDObserveClose(releaseA) })

			if err := p.Submit(id, func() {
				close(aStarted)
				<-releaseA
				close(aFinished)
			}); err != nil {
				t.Fatalf("submit task A: %v", err)
			}
			poolWithIDObserveReceive(t, aStarted, "task A start")
			if err := p.Submit(id, func() {
				close(bStarted)
				close(bFinished)
			}); err != nil {
				t.Fatalf("submit task B: %v", err)
			}

			entry, owner, startedAt := poolWithIDObserveEntryState(t, p, id)
			p.purgeExpired(startedAt.Add(p.options.RunningTaskTimeout))
			poolWithIDObserveAssertNoSignal(t, bStarted, "task B start while task A is blocked")
			poolWithIDObserveAssertNoSignal(t, p.EscapeEvents(), "disabled escape event")
			currentEntry, currentOwner, _ := poolWithIDObserveEntryState(t, p, id)
			if currentEntry != entry || currentOwner != owner {
				t.Fatal("disabled running purge still replaced the owner")
			}
			if snapshot := p.EscapeSnapshot(); snapshot.Total != 0 || len(snapshot.ByID) != 0 {
				t.Fatalf("disabled running purge changed escape state: %+v", snapshot)
			}

			close(releaseA)
			poolWithIDObserveReceive(t, aFinished, "task A finish")
			poolWithIDObserveReceive(t, bStarted, "task B serial start")
			poolWithIDObserveReceive(t, bFinished, "task B finish")
			poolWithIDObserveRelease(t, p)
		})
	}
}

func TestPoolWithIDEscapedWorkerLateReturnAcrossReboot(t *testing.T) {
	const id = 46
	p := poolWithIDObserveNewPool(t, 1)

	releaseA := make(chan struct{})
	releaseC := make(chan struct{})
	aStarted := make(chan struct{})
	aReturned := make(chan struct{})
	bFinished := make(chan struct{})
	cStarted := make(chan struct{})
	cFinished := make(chan struct{})
	dStarted := make(chan struct{})
	dFinished := make(chan struct{})
	t.Cleanup(func() {
		poolWithIDObserveClose(releaseA)
		poolWithIDObserveClose(releaseC)
	})

	if err := p.Submit(id, func() {
		defer close(aReturned)
		close(aStarted)
		<-releaseA
	}); err != nil {
		t.Fatalf("submit pre-Reboot task A: %v", err)
	}
	poolWithIDObserveReceive(t, aStarted, "pre-Reboot task A start")
	if err := p.Submit(id, func() { close(bFinished) }); err != nil {
		t.Fatalf("submit pre-Reboot task B: %v", err)
	}
	oldEntry, oldOwner, startedAt := poolWithIDObserveEntryState(t, p, id)
	events := p.EscapeEvents()
	p.purgeExpired(startedAt.Add(p.options.RunningTaskTimeout))
	startEvent := poolWithIDObserveReceive(t, events, "pre-Reboot escape event")
	poolWithIDObserveAssertEvent(t, startEvent, PoolWithIDWorkerEscaped, id, 1, 1)
	poolWithIDObserveReceive(t, bFinished, "pre-Reboot replacement task B")

	poolWithIDObserveRelease(t, p)
	p.Reboot()
	if p.IsClosed() {
		t.Fatal("PoolWithID remained closed after Reboot")
	}
	if p.EscapeEvents() != events {
		t.Fatal("Reboot replaced the escape event stream")
	}
	if snapshot := p.EscapeSnapshot(); snapshot.Total != 1 || snapshot.ByID[id] != 1 {
		t.Fatalf("Reboot reset live escape state: %+v", snapshot)
	}

	if err := p.Submit(id, func() {
		close(cStarted)
		<-releaseC
		close(cFinished)
	}); err != nil {
		t.Fatalf("submit post-Reboot task C: %v", err)
	}
	poolWithIDObserveReceive(t, cStarted, "post-Reboot task C start")
	newEntry, newOwner, _ := poolWithIDObserveEntryState(t, p, id)
	if newEntry == oldEntry || newOwner == oldOwner {
		t.Fatal("Reboot reused the pre-Reboot entry or owner")
	}
	if err := p.Submit(id, func() {
		close(dStarted)
		close(dFinished)
	}); err != nil {
		t.Fatalf("submit post-Reboot task D: %v", err)
	}

	close(releaseA)
	poolWithIDObserveReceive(t, aReturned, "pre-Reboot escaped task return")
	exitEvent := poolWithIDObserveReceive(t, events, "cross-Reboot escaped worker exit")
	poolWithIDObserveAssertEvent(t, exitEvent, PoolWithIDEscapedWorkerExited, id, 0, 0)
	poolWithIDObserveAssertNoSignal(t, dStarted, "old owner consuming post-Reboot task D")
	currentEntry, currentOwner, _ := poolWithIDObserveEntryState(t, p, id)
	if currentEntry != newEntry || currentOwner != newOwner {
		t.Fatal("pre-Reboot escaped worker polluted the new owner")
	}
	if got := p.Running(); got != 1 {
		t.Fatalf("late escaped-worker exit changed new lifecycle Running(): got %d, want 1", got)
	}
	if snapshot := p.EscapeSnapshot(); snapshot.Total != 0 || len(snapshot.ByID) != 0 {
		t.Fatalf("late escaped-worker exit left stale snapshot state: %+v", snapshot)
	}

	close(releaseC)
	poolWithIDObserveReceive(t, cFinished, "post-Reboot task C finish")
	poolWithIDObserveReceive(t, dStarted, "post-Reboot task D start")
	poolWithIDObserveReceive(t, dFinished, "post-Reboot task D finish")
	poolWithIDObserveRelease(t, p)
}

func TestPoolWithIDReleaseEscapesAcceptedRunningTaskWhileClosing(t *testing.T) {
	const id = 48
	p := poolWithIDObserveNewPool(t, 1)

	releaseA := make(chan struct{})
	aStarted := make(chan struct{})
	aReturned := make(chan struct{})
	bFinished := make(chan struct{})
	t.Cleanup(func() { poolWithIDObserveClose(releaseA) })

	if err := p.Submit(id, func() {
		defer close(aReturned)
		close(aStarted)
		<-releaseA
	}); err != nil {
		t.Fatalf("submit accepted task A: %v", err)
	}
	poolWithIDObserveReceive(t, aStarted, "accepted task A start")
	if err := p.Submit(id, func() { close(bFinished) }); err != nil {
		t.Fatalf("submit accepted task B: %v", err)
	}

	_, _, startedAt := poolWithIDObserveEntryState(t, p, id)
	if startedAt.IsZero() {
		t.Fatal("accepted task A has no recorded start time")
	}
	stop := p.submitStop
	closingState := make(chan int32, 1)
	purged := make(chan struct{})
	go func() {
		<-stop
		closingState <- atomic.LoadInt32(&p.state)
		p.purgeExpired(startedAt.Add(p.options.RunningTaskTimeout))
		close(purged)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), poolWithIDObserveTestTimeout)
	defer cancel()
	releaseResult := make(chan error, 1)
	go func() { releaseResult <- p.ReleaseContext(ctx) }()
	if err := poolWithIDObserveReceive(t, releaseResult, "ReleaseContext closing drain"); err != nil {
		t.Fatalf("ReleaseContext: %v", err)
	}
	if state := poolWithIDObserveReceive(t, closingState, "CLOSING state observation"); state != CLOSING {
		t.Fatalf("state at purge barrier = %d, want CLOSING", state)
	}
	poolWithIDObserveReceive(t, purged, "synthetic closing purge")
	poolWithIDObserveReceive(t, bFinished, "replacement owner running accepted task B")
	startEvent := poolWithIDObserveReceive(t, p.EscapeEvents(), "closing worker escape event")
	poolWithIDObserveAssertEvent(t, startEvent, PoolWithIDWorkerEscaped, id, 1, 1)
	if state := atomic.LoadInt32(&p.state); state != CLOSED {
		t.Fatalf("state after ReleaseContext = %d, want CLOSED", state)
	}
	poolWithIDObserveAssertNoSignal(t, aReturned, "escaped task A return before its barrier")
	if snapshot := p.EscapeSnapshot(); snapshot.Total != 1 || snapshot.ByID[id] != 1 {
		t.Fatalf("ReleaseContext waited for or cleared escaped task A: %+v", snapshot)
	}

	close(releaseA)
	poolWithIDObserveReceive(t, aReturned, "escaped task A return")
	exitEvent := poolWithIDObserveReceive(t, p.EscapeEvents(), "closing escaped worker exit event")
	poolWithIDObserveAssertEvent(t, exitEvent, PoolWithIDEscapedWorkerExited, id, 0, 0)
	if snapshot := p.EscapeSnapshot(); snapshot.Total != 0 || len(snapshot.ByID) != 0 {
		t.Fatalf("escaped task A left stale snapshot state: %+v", snapshot)
	}
}

func TestPoolWithIDReleaseDrainsAcceptedTasksAfterPanic(t *testing.T) {
	const id = 47
	panicSeen := make(chan any, 1)
	p := poolWithIDObserveNewPool(t, 1, WithPanicHandler(func(value any) {
		panicSeen <- value
	}))

	releaseA := make(chan struct{})
	aStarted := make(chan struct{})
	aFinished := make(chan struct{})
	cStarted := make(chan struct{})
	cFinished := make(chan struct{})
	t.Cleanup(func() { poolWithIDObserveClose(releaseA) })

	if err := p.Submit(id, func() {
		close(aStarted)
		<-releaseA
		close(aFinished)
	}); err != nil {
		t.Fatalf("submit accepted task A: %v", err)
	}
	poolWithIDObserveReceive(t, aStarted, "accepted task A start")
	if err := p.Submit(id, func() { panic("accepted task panic") }); err != nil {
		t.Fatalf("submit accepted panic task: %v", err)
	}
	if err := p.Submit(id, func() {
		close(cStarted)
		close(cFinished)
	}); err != nil {
		t.Fatalf("submit accepted task C: %v", err)
	}

	releaseIssued := make(chan struct{})
	go func() {
		p.Release()
		close(releaseIssued)
	}()
	poolWithIDObserveReceive(t, releaseIssued, "Release signal phase")
	poolWithIDObserveAssertNoSignal(t, cStarted, "accepted task C before task A finishes")
	if err := p.Submit(id, func() {}); err != ErrPoolClosed {
		t.Fatalf("submit after Release: got %v, want %v", err, ErrPoolClosed)
	}
	releaseDone, alreadyClosed := p.startRelease()
	if alreadyClosed {
		t.Fatal("pool closed before its accepted tasks drained")
	}

	close(releaseA)
	poolWithIDObserveReceive(t, aFinished, "accepted task A finish")
	if got := poolWithIDObserveReceive(t, panicSeen, "accepted task panic recovery"); got != "accepted task panic" {
		t.Fatalf("panic handler received %v", got)
	}
	poolWithIDObserveReceive(t, cStarted, "accepted task C start")
	poolWithIDObserveReceive(t, cFinished, "accepted task C finish")
	poolWithIDObserveReceive(t, releaseDone, "accepted task drain")
	if snapshot := p.EscapeSnapshot(); snapshot.Total != 0 || len(snapshot.ByID) != 0 {
		t.Fatalf("ordinary panic incorrectly created escape state: %+v", snapshot)
	}
}
