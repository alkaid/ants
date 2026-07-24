package ants

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const poolWithIDPhase3TestTimeout = 5 * time.Second

type poolWithIDPhase3FakeClock struct {
	mu      sync.Mutex
	wall    time.Time
	mono    time.Duration
	seq     time.Duration
	samples map[time.Time]time.Duration
}

func newPoolWithIDPhase3FakeClock() *poolWithIDPhase3FakeClock {
	return &poolWithIDPhase3FakeClock{
		wall:    time.Date(2026, time.July, 24, 12, 0, 0, 0, time.UTC),
		samples: make(map[time.Time]time.Duration),
	}
}

func (c *poolWithIDPhase3FakeClock) Now() time.Time {
	c.mu.Lock()
	now := c.wall.Add(c.seq)
	c.seq++
	c.samples[now] = c.mono
	c.mu.Unlock()
	return now
}

func (c *poolWithIDPhase3FakeClock) Since(start time.Time) time.Duration {
	c.mu.Lock()
	startedAt, ok := c.samples[start]
	if !ok {
		c.mu.Unlock()
		panic("phase 3 fake clock received an unknown start time")
	}
	elapsed := c.mono - startedAt
	c.mu.Unlock()
	return elapsed
}

func (c *poolWithIDPhase3FakeClock) Advance(elapsed time.Duration) {
	c.mu.Lock()
	c.wall = c.wall.Add(elapsed)
	c.mono += elapsed
	c.mu.Unlock()
}

func (c *poolWithIDPhase3FakeClock) JumpWall(delta time.Duration) {
	c.mu.Lock()
	c.wall = c.wall.Add(delta)
	c.mu.Unlock()
}

type poolWithIDPhase3BlockingLogger struct {
	calls   atomic.Int32
	entered chan struct{}
	unblock chan struct{}
	once    sync.Once
}

func newPoolWithIDPhase3BlockingLogger() *poolWithIDPhase3BlockingLogger {
	return &poolWithIDPhase3BlockingLogger{
		entered: make(chan struct{}),
		unblock: make(chan struct{}),
	}
}

func (l *poolWithIDPhase3BlockingLogger) Printf(string, ...any) {
	l.calls.Add(1)
	l.once.Do(func() { close(l.entered) })
	<-l.unblock
}

func poolWithIDPhase3NewPool(
	t *testing.T,
	size int,
	clock poolWithIDClock,
	options ...Option,
) *PoolWithID {
	t.Helper()
	previousFactory := poolWithIDClockFactory
	poolWithIDClockFactory = func() poolWithIDClock { return clock }
	p, err := func() (*PoolWithID, error) {
		defer func() { poolWithIDClockFactory = previousFactory }()
		return NewPoolWithID(size, options...)
	}()
	if err != nil {
		t.Fatalf("NewPoolWithID: %v", err)
	}
	t.Cleanup(func() {
		p.Release()
		select {
		case <-p.closedDone:
		case <-time.After(poolWithIDPhase3TestTimeout):
			t.Error("PoolWithID did not close during Phase 3 test cleanup")
		}
	})
	return p
}

func poolWithIDPhase3CloseOnCleanup(t *testing.T, ch chan struct{}) func() {
	t.Helper()
	var once sync.Once
	closeChannel := func() {
		once.Do(func() { close(ch) })
	}
	t.Cleanup(closeChannel)
	return closeChannel
}

func poolWithIDPhase3Receive[T any](t *testing.T, ch <-chan T, label string) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(poolWithIDPhase3TestTimeout):
		var zero T
		t.Fatalf("timed out waiting for %s", label)
		return zero
	}
}

func poolWithIDPhase3AssertNoSignal[T any](t *testing.T, ch <-chan T, label string) {
	t.Helper()
	select {
	case value := <-ch:
		t.Fatalf("unexpected %s: %#v", label, value)
	default:
	}
}

func poolWithIDPhase3Eventually(t *testing.T, label string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(poolWithIDPhase3TestTimeout)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", label)
		}
		runtime.Gosched()
	}
}

func poolWithIDPhase3EntryDrained(p *PoolWithID, id int) bool {
	p.lock.Lock()
	entry := p.registry.items[id]
	if entry == nil {
		p.lock.Unlock()
		return false
	}
	entry.mu.Lock()
	drained := entry.drained()
	entry.mu.Unlock()
	p.lock.Unlock()
	return drained
}

func poolWithIDPhase3HasEntry(p *PoolWithID, id int) bool {
	p.lock.Lock()
	_, ok := p.registry.items[id]
	p.lock.Unlock()
	return ok
}

func poolWithIDPhase3AssertEvent(
	t *testing.T,
	event PoolWithIDEscapeEvent,
	wantType PoolWithIDEscapeEventType,
	wantID int,
	wantGeneration uint64,
	wantTotal, wantByID int,
	wantReason PoolWithIDEscapeBudgetReason,
	wantGlobalBudget, wantPerIDBudget int,
) {
	t.Helper()
	if event.Type != wantType || event.ID != wantID ||
		event.Generation != wantGeneration || event.Total != wantTotal ||
		event.ByID != wantByID || event.BudgetReason != wantReason ||
		event.GlobalBudget != wantGlobalBudget || event.PerIDBudget != wantPerIDBudget {
		t.Fatalf("unexpected escape event: %+v", event)
	}
	if event.Time.IsZero() {
		t.Fatal("escape event has a zero timestamp")
	}
}

func TestPoolWithIDEscapeConfigurationDefaultsAndValidation(t *testing.T) {
	previousHook := poolWithIDBackgroundStartHook
	defer func() { poolWithIDBackgroundStartHook = previousHook }()
	var backgroundStarts atomic.Int32
	poolWithIDBackgroundStartHook = func() { backgroundStarts.Add(1) }

	p, err := NewPoolWithID(8, WithDisablePurge(true))
	if err != nil {
		t.Fatalf("NewPoolWithID(defaults): %v", err)
	}
	if p.options.ExpiryDuration != DefaultPoolWithIDExpiryDuration {
		t.Fatalf("default idle expiry = %v, want %v", p.options.ExpiryDuration, DefaultPoolWithIDExpiryDuration)
	}
	if p.options.RunningTaskTimeout != DefaultRunningTaskTimeout {
		t.Fatalf("default running timeout = %v, want %v", p.options.RunningTaskTimeout, DefaultRunningTaskTimeout)
	}
	status := p.EscapeBudgetStatus(1)
	if status.GlobalLimit != 2 || status.PerIDLimit != 1 {
		t.Fatalf("default escape budgets = global:%d per-ID:%d, want 2/1", status.GlobalLimit, status.PerIDLimit)
	}
	p.Tune(4)
	if status = p.EscapeBudgetStatus(1); status.GlobalLimit != 1 || status.PerIDLimit != 1 {
		t.Fatalf("default escape budgets after Tune(4) = global:%d per-ID:%d, want 1/1", status.GlobalLimit, status.PerIDLimit)
	}
	p.Tune(12)
	if status = p.EscapeBudgetStatus(1); status.GlobalLimit != 3 || status.PerIDLimit != 1 {
		t.Fatalf("default escape budgets after Tune(12) = global:%d per-ID:%d, want 3/1", status.GlobalLimit, status.PerIDLimit)
	}
	if err := p.ReleaseTimeout(poolWithIDPhase3TestTimeout); err != nil {
		t.Fatalf("ReleaseTimeout(default pool): %v", err)
	}

	fixed, err := NewPoolWithID(8,
		WithDisablePurge(true),
		WithMaxEscapedWorkers(5),
	)
	if err != nil {
		t.Fatalf("NewPoolWithID(fixed budget): %v", err)
	}
	fixed.Tune(4)
	if status = fixed.EscapeBudgetStatus(1); status.GlobalLimit != 5 {
		t.Fatalf("explicit global budget after Tune(4) = %d, want 5", status.GlobalLimit)
	}
	if err := fixed.ReleaseTimeout(poolWithIDPhase3TestTimeout); err != nil {
		t.Fatalf("ReleaseTimeout(fixed-budget pool): %v", err)
	}
	if got := backgroundStarts.Load(); got != 2 {
		t.Fatalf("background starts for valid pools = %d, want 2", got)
	}

	backgroundStarts.Store(0)
	tests := []struct {
		name    string
		option  Option
		wantErr error
	}{
		{name: "idle expiry", option: WithExpiryDuration(-time.Nanosecond), wantErr: ErrInvalidPoolExpiry},
		{name: "running timeout", option: WithRunningTaskTimeout(-time.Nanosecond), wantErr: ErrInvalidPoolWithIDRunningTaskTimeout},
		{name: "global budget", option: WithMaxEscapedWorkers(-1), wantErr: ErrInvalidPoolWithIDEscapeBudget},
		{name: "per-ID budget", option: WithMaxEscapedWorkersPerID(-1), wantErr: ErrInvalidPoolWithIDEscapeBudget},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pool, err := NewPoolWithID(1, tt.option)
			if pool != nil || !errors.Is(err, tt.wantErr) {
				t.Fatalf("NewPoolWithID() = (%p, %v), want (nil, %v)", pool, err, tt.wantErr)
			}
		})
	}
	if got := backgroundStarts.Load(); got != 0 {
		t.Fatalf("invalid configurations started %d background loops", got)
	}
}

func TestPoolWithIDClockIgnoresWallJumpsForRunningEscape(t *testing.T) {
	const id = 601
	clock := newPoolWithIDPhase3FakeClock()
	p := poolWithIDPhase3NewPool(t, 1, clock,
		WithExpiryDuration(time.Hour),
		WithMaxEscapedWorkers(4),
		WithMaxEscapedWorkersPerID(4),
	)

	releaseA := make(chan struct{})
	closeReleaseA := poolWithIDPhase3CloseOnCleanup(t, releaseA)
	aStarted := make(chan struct{})
	bStarted := make(chan struct{})
	if err := p.Submit(id, func() {
		close(aStarted)
		<-releaseA
	}); err != nil {
		t.Fatalf("Submit(task A): %v", err)
	}
	poolWithIDPhase3Receive(t, aStarted, "task A start")
	if err := p.Submit(id, func() { close(bStarted) }); err != nil {
		t.Fatalf("Submit(task B): %v", err)
	}

	clock.JumpWall(24 * time.Hour)
	p.purgeExpiredNow()
	poolWithIDPhase3AssertNoSignal(t, bStarted, "task B start after wall-clock advance")
	poolWithIDPhase3AssertNoSignal(t, p.EscapeEvents(), "escape after wall-clock advance")

	clock.JumpWall(-48 * time.Hour)
	clock.Advance(DefaultRunningTaskTimeout - time.Nanosecond)
	p.purgeExpiredNow()
	poolWithIDPhase3AssertNoSignal(t, bStarted, "task B start before monotonic deadline")
	poolWithIDPhase3AssertNoSignal(t, p.EscapeEvents(), "escape before monotonic deadline")

	clock.Advance(time.Nanosecond)
	p.purgeExpiredNow()
	event := poolWithIDPhase3Receive(t, p.EscapeEvents(), "escape at monotonic deadline")
	poolWithIDPhase3AssertEvent(t, event, PoolWithIDWorkerEscaped, id, 1, 1, 1, 0, 4, 4)
	poolWithIDPhase3Receive(t, bStarted, "task B start at monotonic deadline")

	closeReleaseA()
	exitEvent := poolWithIDPhase3Receive(t, p.EscapeEvents(), "escaped task A exit")
	poolWithIDPhase3AssertEvent(t, exitEvent, PoolWithIDEscapedWorkerExited, id, 1, 0, 0, 0, 4, 4)
}

func TestPoolWithIDBlockingPanicHandlerRemainsEscapeEligible(t *testing.T) {
	const id = 602
	clock := newPoolWithIDPhase3FakeClock()
	handlerEntered := make(chan struct{})
	unblockHandler := make(chan struct{})
	closeHandler := poolWithIDPhase3CloseOnCleanup(t, unblockHandler)
	var handlerOnce sync.Once
	p := poolWithIDPhase3NewPool(t, 1, clock,
		WithExpiryDuration(time.Hour),
		WithRunningTaskTimeout(time.Hour),
		WithMaxEscapedWorkers(1),
		WithMaxEscapedWorkersPerID(1),
		WithPanicHandler(func(any) {
			handlerOnce.Do(func() { close(handlerEntered) })
			<-unblockHandler
		}),
	)

	if err := p.Submit(id, func() { panic("blocked panic handler") }); err != nil {
		t.Fatalf("Submit(panicking task): %v", err)
	}
	poolWithIDPhase3Receive(t, handlerEntered, "panic handler entry")
	replacementFinished := make(chan struct{})
	if err := p.Submit(id, func() { close(replacementFinished) }); err != nil {
		t.Fatalf("Submit(replacement task): %v", err)
	}

	clock.Advance(time.Hour)
	p.purgeExpiredNow()
	escape := poolWithIDPhase3Receive(t, p.EscapeEvents(), "panic-handler owner escape")
	poolWithIDPhase3AssertEvent(t, escape, PoolWithIDWorkerEscaped, id, 1, 1, 1, 0, 1, 1)
	poolWithIDPhase3Receive(t, replacementFinished, "replacement behind panic handler")
	if p.Escaped() != 1 || p.TotalWorkers() != 2 {
		t.Fatalf("blocked handler worker totals = escaped:%d total:%d, want 1/2", p.Escaped(), p.TotalWorkers())
	}

	closeHandler()
	exit := poolWithIDPhase3Receive(t, p.EscapeEvents(), "panic-handler owner exit")
	poolWithIDPhase3AssertEvent(t, exit, PoolWithIDEscapedWorkerExited, id, 1, 0, 0, 0, 1, 1)
}

func TestPoolWithIDEscapeDisableMatrix(t *testing.T) {
	tests := []struct {
		name               string
		disablePurge       bool
		disableRunning     bool
		wantIdleRetired    bool
		wantRunningEscaped bool
	}{
		{name: "both enabled", wantIdleRetired: true, wantRunningEscaped: true},
		{name: "running disabled", disableRunning: true, wantIdleRetired: true},
		{name: "purge disabled", disablePurge: true},
		{name: "both disabled", disablePurge: true, disableRunning: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clock := newPoolWithIDPhase3FakeClock()
			p := poolWithIDPhase3NewPool(t, 2, clock,
				WithExpiryDuration(time.Hour),
				WithRunningTaskTimeout(time.Hour),
				WithMaxEscapedWorkers(4),
				WithMaxEscapedWorkersPerID(4),
				WithDisablePurge(tt.disablePurge),
				WithDisablePurgeRunning(tt.disableRunning),
			)

			idleDone := make(chan struct{})
			if err := p.Submit(610, func() { close(idleDone) }); err != nil {
				t.Fatalf("Submit(idle task): %v", err)
			}
			poolWithIDPhase3Receive(t, idleDone, "idle task completion")
			poolWithIDPhase3Eventually(t, "idle entry drain", func() bool {
				return poolWithIDPhase3EntryDrained(p, 610)
			})

			releaseRunning := make(chan struct{})
			closeReleaseRunning := poolWithIDPhase3CloseOnCleanup(t, releaseRunning)
			runningStarted := make(chan struct{})
			replacementStarted := make(chan struct{})
			if err := p.Submit(611, func() {
				close(runningStarted)
				<-releaseRunning
			}); err != nil {
				t.Fatalf("Submit(running task): %v", err)
			}
			poolWithIDPhase3Receive(t, runningStarted, "running task start")
			if err := p.Submit(611, func() { close(replacementStarted) }); err != nil {
				t.Fatalf("Submit(replacement task): %v", err)
			}

			clock.Advance(time.Hour)
			p.purgeExpiredNow()
			if got := !poolWithIDPhase3HasEntry(p, 610); got != tt.wantIdleRetired {
				t.Fatalf("idle retired = %v, want %v", got, tt.wantIdleRetired)
			}
			if tt.wantRunningEscaped {
				event := poolWithIDPhase3Receive(t, p.EscapeEvents(), "running escape event")
				poolWithIDPhase3AssertEvent(t, event, PoolWithIDWorkerEscaped, 611, 1, 1, 1, 0, 4, 4)
				poolWithIDPhase3Receive(t, replacementStarted, "replacement task start")
			} else {
				poolWithIDPhase3AssertNoSignal(t, p.EscapeEvents(), "disabled running escape event")
				poolWithIDPhase3AssertNoSignal(t, replacementStarted, "disabled replacement task start")
			}

			closeReleaseRunning()
			if tt.wantRunningEscaped {
				exit := poolWithIDPhase3Receive(t, p.EscapeEvents(), "escaped running task exit")
				if exit.Type != PoolWithIDEscapedWorkerExited {
					t.Fatalf("escape exit event = %+v", exit)
				}
			} else {
				poolWithIDPhase3Receive(t, replacementStarted, "serial replacement task start")
			}
		})
	}
}

func TestPoolWithIDEscapeBudgetsAndObservation(t *testing.T) {
	t.Run("per-ID exhaustion and recovery", func(t *testing.T) {
		const id = 620
		clock := newPoolWithIDPhase3FakeClock()
		p := poolWithIDPhase3NewPool(t, 1, clock,
			WithExpiryDuration(time.Hour),
			WithRunningTaskTimeout(time.Hour),
			WithMaxEscapedWorkers(4),
			WithMaxEscapedWorkersPerID(1),
		)

		releaseA := make(chan struct{})
		closeReleaseA := poolWithIDPhase3CloseOnCleanup(t, releaseA)
		releaseB := make(chan struct{})
		closeReleaseB := poolWithIDPhase3CloseOnCleanup(t, releaseB)
		aStarted := make(chan struct{})
		bStarted := make(chan struct{})
		cFinished := make(chan struct{})
		if err := p.Submit(id, func() {
			close(aStarted)
			<-releaseA
		}); err != nil {
			t.Fatalf("Submit(task A): %v", err)
		}
		poolWithIDPhase3Receive(t, aStarted, "task A start")
		if err := p.Submit(id, func() {
			close(bStarted)
			<-releaseB
		}); err != nil {
			t.Fatalf("Submit(task B): %v", err)
		}
		if err := p.Submit(id, func() { close(cFinished) }); err != nil {
			t.Fatalf("Submit(task C): %v", err)
		}

		clock.Advance(time.Hour)
		p.purgeExpiredNow()
		startA := poolWithIDPhase3Receive(t, p.EscapeEvents(), "task A escape")
		poolWithIDPhase3AssertEvent(t, startA, PoolWithIDWorkerEscaped, id, 1, 1, 1, 0, 4, 1)
		poolWithIDPhase3Receive(t, bStarted, "task B start")
		if p.Escaped() != 1 || p.TotalWorkers() != 2 {
			t.Fatalf("worker totals after first escape = escaped:%d total:%d, want 1/2", p.Escaped(), p.TotalWorkers())
		}

		clock.Advance(time.Hour)
		p.purgeExpiredNow()
		exhausted := poolWithIDPhase3Receive(t, p.EscapeEvents(), "per-ID budget exhaustion")
		poolWithIDPhase3AssertEvent(
			t, exhausted, PoolWithIDEscapeBudgetExhausted, id, 1, 1, 1,
			PoolWithIDEscapePerIDBudgetExhausted, 4, 1,
		)
		status := p.EscapeBudgetStatus(id)
		if status.GlobalUsed != 1 || status.GlobalLimit != 4 || status.PerIDUsed != 1 ||
			status.PerIDLimit != 1 || status.Reason != PoolWithIDEscapePerIDBudgetExhausted {
			t.Fatalf("per-ID budget status = %+v", status)
		}
		snapshot := p.EscapeSnapshot()
		if snapshot.Total != 1 || snapshot.ByID[id] != 1 || snapshot.GlobalBudget != 4 ||
			snapshot.PerIDBudget != 1 || snapshot.ExhaustedByID[id] != PoolWithIDEscapePerIDBudgetExhausted {
			t.Fatalf("per-ID exhaustion snapshot = %+v", snapshot)
		}
		snapshot.ByID[id] = 99
		snapshot.ExhaustedByID[id] = 0
		fresh := p.EscapeSnapshot()
		if fresh.ByID[id] != 1 || fresh.ExhaustedByID[id] != PoolWithIDEscapePerIDBudgetExhausted {
			t.Fatalf("snapshot maps alias internal state: %+v", fresh)
		}
		for i := 0; i < 16; i++ {
			p.purgeExpiredNow()
		}
		poolWithIDPhase3AssertNoSignal(t, p.EscapeEvents(), "duplicate per-ID exhausted event")
		poolWithIDPhase3AssertNoSignal(t, cFinished, "task C start while per-ID budget is exhausted")

		closeReleaseA()
		exitA := poolWithIDPhase3Receive(t, p.EscapeEvents(), "task A escaped exit")
		poolWithIDPhase3AssertEvent(t, exitA, PoolWithIDEscapedWorkerExited, id, 1, 0, 0, 0, 4, 1)
		startB := poolWithIDPhase3Receive(t, p.EscapeEvents(), "task B escape after quota recovery")
		poolWithIDPhase3AssertEvent(t, startB, PoolWithIDWorkerEscaped, id, 1, 1, 1, 0, 4, 1)
		poolWithIDPhase3Receive(t, cFinished, "task C start after quota recovery")

		closeReleaseB()
		exitB := poolWithIDPhase3Receive(t, p.EscapeEvents(), "task B escaped exit")
		poolWithIDPhase3AssertEvent(t, exitB, PoolWithIDEscapedWorkerExited, id, 1, 0, 0, 0, 4, 1)
		if status = p.EscapeBudgetStatus(id); status.GlobalUsed != 0 || status.PerIDUsed != 0 || status.Reason != 0 {
			t.Fatalf("recovered per-ID budget status = %+v", status)
		}

		for i := 0; i < cap(p.escape.events); i++ {
			p.escape.events <- PoolWithIDEscapeEvent{Type: PoolWithIDWorkerEscaped, ID: -1}
		}
		p.publishEscapeEvent(PoolWithIDEscapeEvent{Type: PoolWithIDEscapeBudgetExhausted, ID: id})
		if got := p.DroppedEscapeEvents(); got != 1 {
			t.Fatalf("DroppedEscapeEvents() = %d, want 1", got)
		}
		if got := p.EscapeSnapshot().DroppedEvents; got != 1 {
			t.Fatalf("snapshot DroppedEvents = %d, want 1", got)
		}
	})

	t.Run("global exhaustion and recovery", func(t *testing.T) {
		clock := newPoolWithIDPhase3FakeClock()
		p := poolWithIDPhase3NewPool(t, 2, clock,
			WithExpiryDuration(time.Hour),
			WithRunningTaskTimeout(time.Hour),
			WithMaxEscapedWorkers(1),
			WithMaxEscapedWorkersPerID(2),
		)

		releaseA1 := make(chan struct{})
		closeReleaseA1 := poolWithIDPhase3CloseOnCleanup(t, releaseA1)
		releaseA2 := make(chan struct{})
		closeReleaseA2 := poolWithIDPhase3CloseOnCleanup(t, releaseA2)
		a1Started := make(chan struct{})
		a2Started := make(chan struct{})
		b1Finished := make(chan struct{})
		b2Finished := make(chan struct{})
		if err := p.Submit(621, func() {
			close(a1Started)
			<-releaseA1
		}); err != nil {
			t.Fatalf("Submit(task A1): %v", err)
		}
		poolWithIDPhase3Receive(t, a1Started, "task A1 start")
		if err := p.Submit(621, func() { close(b1Finished) }); err != nil {
			t.Fatalf("Submit(task B1): %v", err)
		}
		if err := p.Submit(622, func() {
			close(a2Started)
			<-releaseA2
		}); err != nil {
			t.Fatalf("Submit(task A2): %v", err)
		}
		poolWithIDPhase3Receive(t, a2Started, "task A2 start")
		if err := p.Submit(622, func() { close(b2Finished) }); err != nil {
			t.Fatalf("Submit(task B2): %v", err)
		}

		clock.Advance(time.Hour)
		p.purgeExpiredNow()
		first := poolWithIDPhase3Receive(t, p.EscapeEvents(), "global first escape")
		poolWithIDPhase3AssertEvent(t, first, PoolWithIDWorkerEscaped, 621, 1, 1, 1, 0, 1, 2)
		exhausted := poolWithIDPhase3Receive(t, p.EscapeEvents(), "global budget exhaustion")
		poolWithIDPhase3AssertEvent(
			t, exhausted, PoolWithIDEscapeBudgetExhausted, 622, 1, 1, 0,
			PoolWithIDEscapeGlobalBudgetExhausted, 1, 2,
		)
		poolWithIDPhase3Receive(t, b1Finished, "task B1 finish")
		poolWithIDPhase3AssertNoSignal(t, b2Finished, "task B2 start while global budget is exhausted")
		status := p.EscapeBudgetStatus(622)
		if status.GlobalUsed != 1 || status.GlobalLimit != 1 || status.PerIDUsed != 0 ||
			status.PerIDLimit != 2 || status.Reason != PoolWithIDEscapeGlobalBudgetExhausted {
			t.Fatalf("global budget status = %+v", status)
		}
		if p.Escaped() != 1 || p.TotalWorkers() != 3 {
			t.Fatalf("worker totals at global exhaustion = escaped:%d total:%d, want 1/3", p.Escaped(), p.TotalWorkers())
		}
		for i := 0; i < 16; i++ {
			p.purgeExpiredNow()
		}
		poolWithIDPhase3AssertNoSignal(t, p.EscapeEvents(), "duplicate global exhausted event")

		closeReleaseA1()
		exitA1 := poolWithIDPhase3Receive(t, p.EscapeEvents(), "task A1 escaped exit")
		poolWithIDPhase3AssertEvent(t, exitA1, PoolWithIDEscapedWorkerExited, 621, 1, 0, 0, 0, 1, 2)
		second := poolWithIDPhase3Receive(t, p.EscapeEvents(), "task A2 escape after global recovery")
		poolWithIDPhase3AssertEvent(t, second, PoolWithIDWorkerEscaped, 622, 1, 1, 1, 0, 1, 2)
		poolWithIDPhase3Receive(t, b2Finished, "task B2 finish after global recovery")

		closeReleaseA2()
		exitA2 := poolWithIDPhase3Receive(t, p.EscapeEvents(), "task A2 escaped exit")
		poolWithIDPhase3AssertEvent(t, exitA2, PoolWithIDEscapedWorkerExited, 622, 1, 0, 0, 0, 1, 2)
	})
}

func TestPoolWithIDEscapeEventsPreserveMixedScanOrder(t *testing.T) {
	const (
		idA = 623
		idB = 624
	)
	clock := newPoolWithIDPhase3FakeClock()
	p := poolWithIDPhase3NewPool(t, 2, clock,
		WithExpiryDuration(time.Hour),
		WithRunningTaskTimeout(time.Hour),
		WithMaxEscapedWorkers(3),
		WithMaxEscapedWorkersPerID(1),
	)

	releaseA1 := make(chan struct{})
	closeA1 := poolWithIDPhase3CloseOnCleanup(t, releaseA1)
	releaseA2 := make(chan struct{})
	poolWithIDPhase3CloseOnCleanup(t, releaseA2)
	releaseB := make(chan struct{})
	poolWithIDPhase3CloseOnCleanup(t, releaseB)
	a1Started := make(chan struct{})
	a2Started := make(chan struct{})
	bStarted := make(chan struct{})

	if err := p.Submit(idA, func() {
		close(a1Started)
		<-releaseA1
	}); err != nil {
		t.Fatalf("Submit(A1): %v", err)
	}
	poolWithIDPhase3Receive(t, a1Started, "A1 start")
	if err := p.Submit(idA, func() {
		close(a2Started)
		<-releaseA2
	}); err != nil {
		t.Fatalf("Submit(A2): %v", err)
	}
	clock.Advance(time.Hour)
	p.purgeExpiredNow()
	initial := poolWithIDPhase3Receive(t, p.EscapeEvents(), "A1 escape")
	poolWithIDPhase3AssertEvent(t, initial, PoolWithIDWorkerEscaped, idA, 1, 1, 1, 0, 3, 1)
	poolWithIDPhase3Receive(t, a2Started, "A2 start")

	if err := p.Submit(idB, func() {
		close(bStarted)
		<-releaseB
	}); err != nil {
		t.Fatalf("Submit(B): %v", err)
	}
	poolWithIDPhase3Receive(t, bStarted, "B start")
	clock.Advance(time.Hour)
	p.purgeExpiredNow()

	deniedA := poolWithIDPhase3Receive(t, p.EscapeEvents(), "A per-ID exhaustion")
	poolWithIDPhase3AssertEvent(
		t, deniedA, PoolWithIDEscapeBudgetExhausted, idA, 1, 1, 1,
		PoolWithIDEscapePerIDBudgetExhausted, 3, 1,
	)
	escapedB := poolWithIDPhase3Receive(t, p.EscapeEvents(), "B escape")
	poolWithIDPhase3AssertEvent(t, escapedB, PoolWithIDWorkerEscaped, idB, 1, 2, 1, 0, 3, 1)

	closeA1()
}

func TestPoolWithIDReleaseEscapeFenceAndRebootContinuity(t *testing.T) {
	const (
		oldID = 630
		newID = 631
	)
	clock := newPoolWithIDPhase3FakeClock()
	logger := newPoolWithIDPhase3BlockingLogger()
	closeLogger := poolWithIDPhase3CloseOnCleanup(t, logger.unblock)
	p := poolWithIDPhase3NewPool(t, 1, clock,
		WithExpiryDuration(time.Hour),
		WithRunningTaskTimeout(time.Hour),
		WithMaxEscapedWorkers(1),
		WithMaxEscapedWorkersPerID(1),
		WithLogger(logger),
	)
	events := p.EscapeEvents()
	oldGeneration := p.generation.Load()

	fenceReached := make(chan struct{})
	allowFence := make(chan struct{})
	closeAllowFence := poolWithIDPhase3CloseOnCleanup(t, allowFence)
	var fenceOnce sync.Once
	p.testHooks.afterManagedCloseFence = func(generation uint64) {
		if generation != oldGeneration {
			return
		}
		fenceOnce.Do(func() { close(fenceReached) })
		<-allowFence
	}

	releaseOld := make(chan struct{})
	closeReleaseOld := poolWithIDPhase3CloseOnCleanup(t, releaseOld)
	oldStarted := make(chan struct{})
	oldQueuedFinished := make(chan struct{})
	if err := p.Submit(oldID, func() {
		close(oldStarted)
		<-releaseOld
	}); err != nil {
		t.Fatalf("Submit(old running task): %v", err)
	}
	poolWithIDPhase3Receive(t, oldStarted, "old running task start")
	if err := p.Submit(oldID, func() { close(oldQueuedFinished) }); err != nil {
		t.Fatalf("Submit(old queued task): %v", err)
	}

	p.Release()
	ctx, cancel := context.WithTimeout(context.Background(), poolWithIDPhase3TestTimeout)
	defer cancel()
	releaseResult := make(chan error, 1)
	go func() { releaseResult <- p.ReleaseContext(ctx) }()
	clock.Advance(time.Hour)
	purgeDone := make(chan struct{})
	go func() {
		p.purgeExpiredNow()
		close(purgeDone)
	}()
	loggerCalled := false
	select {
	case <-purgeDone:
	case <-logger.entered:
		loggerCalled = true
		closeLogger()
		poolWithIDPhase3Receive(t, purgeDone, "escape scan after unblocking Logger")
	case <-time.After(poolWithIDPhase3TestTimeout):
		closeLogger()
		t.Fatal("escape scan did not complete")
	}
	if loggerCalled {
		t.Fatal("escape scan called Logger synchronously")
	}
	start := poolWithIDPhase3Receive(t, events, "CLOSING escape event")
	poolWithIDPhase3AssertEvent(t, start, PoolWithIDWorkerEscaped, oldID, oldGeneration, 1, 1, 0, 1, 1)
	poolWithIDPhase3Receive(t, oldQueuedFinished, "old queued task drain")
	poolWithIDPhase3Receive(t, fenceReached, "managed close fence")

	p.purgeExpiredNow()
	poolWithIDPhase3AssertNoSignal(t, events, "escape after managed close fence")
	if p.Escaped() != 1 {
		t.Fatalf("Escaped() at managed close fence = %d, want 1", p.Escaped())
	}
	poolWithIDPhase3AssertNoSignal(t, logger.entered, "synchronous escape logger call")
	closeAllowFence()
	if err := poolWithIDPhase3Receive(t, releaseResult, "managed-only ReleaseContext"); err != nil {
		t.Fatalf("ReleaseContext: %v", err)
	}
	if snapshot := p.EscapeSnapshot(); snapshot.Total != 1 || snapshot.ByID[oldID] != 1 {
		t.Fatalf("release cleared escaped state: %+v", snapshot)
	}

	p.Reboot()
	newGeneration := p.generation.Load()
	if newGeneration != oldGeneration+1 {
		t.Fatalf("generation after Reboot = %d, want %d", newGeneration, oldGeneration+1)
	}
	if p.EscapeEvents() != events {
		t.Fatal("Reboot replaced the escape event stream")
	}
	if p.Escaped() != 1 {
		t.Fatalf("Reboot reset escaped permits: %d", p.Escaped())
	}

	releaseNew := make(chan struct{})
	closeReleaseNew := poolWithIDPhase3CloseOnCleanup(t, releaseNew)
	newStarted := make(chan struct{})
	newQueuedFinished := make(chan struct{})
	if err := p.Submit(newID, func() {
		close(newStarted)
		<-releaseNew
	}); err != nil {
		t.Fatalf("Submit(new running task): %v", err)
	}
	poolWithIDPhase3Receive(t, newStarted, "new running task start")
	if err := p.Submit(newID, func() { close(newQueuedFinished) }); err != nil {
		t.Fatalf("Submit(new queued task): %v", err)
	}
	clock.Advance(time.Hour)
	p.purgeExpiredNow()
	exhausted := poolWithIDPhase3Receive(t, events, "post-Reboot budget exhaustion")
	poolWithIDPhase3AssertEvent(
		t, exhausted, PoolWithIDEscapeBudgetExhausted, newID, newGeneration, 1, 0,
		PoolWithIDEscapeGlobalBudgetExhausted, 1, 1,
	)
	poolWithIDPhase3AssertNoSignal(t, newQueuedFinished, "new queued task while old permit is live")
	status := p.EscapeBudgetStatus(newID)
	if status.GlobalUsed != 1 || status.PerIDUsed != 0 ||
		status.Reason != PoolWithIDEscapeGlobalBudgetExhausted {
		t.Fatalf("post-Reboot budget status = %+v", status)
	}
	if p.TotalWorkers() != 2 {
		t.Fatalf("TotalWorkers() across generations = %d, want 2", p.TotalWorkers())
	}

	closeReleaseNew()
	poolWithIDPhase3Receive(t, newQueuedFinished, "new queued task after managed task release")
	if err := p.ReleaseTimeout(poolWithIDPhase3TestTimeout); err != nil {
		t.Fatalf("ReleaseTimeout(new generation): %v", err)
	}
	if status = p.EscapeBudgetStatus(newID); status.Reason != 0 {
		t.Fatalf("drained new-generation entry retained exhausted state: %+v", status)
	}

	closeReleaseOld()
	exit := poolWithIDPhase3Receive(t, events, "old-generation escaped exit")
	poolWithIDPhase3AssertEvent(t, exit, PoolWithIDEscapedWorkerExited, oldID, oldGeneration, 0, 0, 0, 1, 1)
	if p.Escaped() != 0 || p.TotalWorkers() != 0 {
		t.Fatalf("old escaped exit changed current totals incorrectly: escaped:%d total:%d", p.Escaped(), p.TotalWorkers())
	}
	if got := logger.calls.Load(); got != 0 {
		t.Fatalf("escape lifecycle called synchronous Logger %d times", got)
	}
	closeLogger()
}
