package ants

import (
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const poolWithIDBenchmarkTimeout = 2 * time.Minute

var poolWithIDBenchmarkDiagnosticSizes = [...]int{1_000, 10_000}

type poolWithIDBenchmarkLockDiagnostic struct {
	sync.Locker
	heldAt time.Time
	total  atomic.Int64
}

func (l *poolWithIDBenchmarkLockDiagnostic) Lock() {
	l.Locker.Lock()
	l.heldAt = time.Now()
}

func (l *poolWithIDBenchmarkLockDiagnostic) Unlock() {
	l.total.Add(time.Since(l.heldAt).Nanoseconds())
	l.Locker.Unlock()
}

func (l *poolWithIDBenchmarkLockDiagnostic) reset() {
	l.total.Store(0)
}

func poolWithIDBenchmarkNewPool(
	b *testing.B,
	size int,
	options ...Option,
) *PoolWithID {
	b.Helper()
	options = append(options, WithLogger(poolWithIDDiscardLogger{}))
	p, err := NewPoolWithID(size, options...)
	if err != nil {
		b.Fatalf("NewPoolWithID: %v", err)
	}
	return p
}

func poolWithIDBenchmarkNewMode(
	b *testing.B,
	size, taskBuffer int,
	nonblocking, disablePurge bool,
) *PoolWithID {
	b.Helper()
	options := []Option{
		WithExpiryDuration(time.Hour),
		WithRunningTaskTimeout(time.Hour),
		WithTaskBuffer(taskBuffer),
		WithNonblocking(nonblocking),
	}
	if disablePurge {
		options = append(options, WithDisablePurge(true))
	}
	return poolWithIDBenchmarkNewPool(b, size, options...)
}

func poolWithIDBenchmarkNewDiagnostic(
	b *testing.B,
	size int,
) (*PoolWithID, *poolWithIDBenchmarkLockDiagnostic) {
	b.Helper()
	taskBuffer := (1 << 20) / size
	if taskBuffer < 16 {
		taskBuffer = 16
	}
	p := poolWithIDBenchmarkNewMode(b, size, taskBuffer, true, false)
	lock := &poolWithIDBenchmarkLockDiagnostic{Locker: p.lock}
	p.lock = lock
	return p, lock
}

func poolWithIDBenchmarkRelease(b *testing.B, p *PoolWithID) {
	b.Helper()
	if err := p.ReleaseTimeout(poolWithIDBenchmarkTimeout); err != nil &&
		!errors.Is(err, ErrPoolClosed) {
		b.Fatalf("ReleaseTimeout: %v", err)
	}
}

func poolWithIDBenchmarkWaitFor(
	b *testing.B,
	label string,
	condition func() bool,
) {
	b.Helper()
	deadline := time.Now().Add(poolWithIDBenchmarkTimeout)
	for !condition() {
		if time.Now().After(deadline) {
			b.Fatalf("timed out waiting for %s", label)
		}
		runtime.Gosched()
	}
}

func poolWithIDBenchmarkRegistrySize(p *PoolWithID) int {
	p.lock.Lock()
	size := len(p.registry.items)
	p.lock.Unlock()
	return size
}

func poolWithIDBenchmarkWaitForIdle(b *testing.B, p *PoolWithID, count int) {
	b.Helper()
	poolWithIDBenchmarkWaitFor(b, fmt.Sprintf("%d idle entries", count), func() bool {
		idle := 0
		p.lock.Lock()
		if len(p.registry.items) == count {
			for _, entry := range p.registry.items {
				entry.mu.Lock()
				if entry.drained() {
					idle++
				}
				entry.mu.Unlock()
			}
		}
		p.lock.Unlock()
		return idle == count
	})
}

func poolWithIDBenchmarkPopulateIdle(b *testing.B, p *PoolWithID, count int) {
	b.Helper()
	var finished sync.WaitGroup
	finished.Add(count)
	for id := 0; id < count; id++ {
		if err := p.Submit(id, finished.Done); err != nil {
			b.Fatalf("Submit idle ID %d: %v", id, err)
		}
	}
	finished.Wait()
	poolWithIDBenchmarkWaitForIdle(b, p, count)
}

func poolWithIDBenchmarkPopulateRunning(
	b *testing.B,
	p *PoolWithID,
	count int,
) func() {
	b.Helper()
	release := make(chan struct{})
	var started sync.WaitGroup
	started.Add(count)
	allStarted := make(chan struct{})
	go func() {
		started.Wait()
		close(allStarted)
	}()
	task := func() {
		started.Done()
		<-release
	}
	for id := 0; id < count; id++ {
		if err := p.Submit(id, task); err != nil {
			close(release)
			b.Fatalf("Submit running ID %d: %v", id, err)
		}
	}
	select {
	case <-allStarted:
	case <-time.After(poolWithIDBenchmarkTimeout):
		close(release)
		b.Fatalf("timed out waiting for %d running owners", count)
	}
	var once sync.Once
	return func() { once.Do(func() { close(release) }) }
}

func poolWithIDBenchmarkTask(cpu bool) func() {
	if !cpu {
		return func() {}
	}
	return func() {
		value := uint64(0x9e3779b97f4a7c15)
		for i := 0; i < 64; i++ {
			value ^= value << 7
			value ^= value >> 9
			value *= 0xbf58476d1ce4e5b9
		}
		runtime.KeepAlive(value)
	}
}

type poolWithIDBenchmarkSubmitResult struct {
	accepted   int64
	rejected   int64
	unexpected error
}

func poolWithIDBenchmarkRunThroughput(
	b *testing.B,
	p *PoolWithID,
	idCount int,
	task func(),
	allowOverload bool,
) {
	b.Helper()
	results := make(chan poolWithIDBenchmarkSubmitResult, runtime.GOMAXPROCS(0))

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		result := poolWithIDBenchmarkSubmitResult{}
		id := 0
		for pb.Next() {
			err := p.Submit(id, task)
			switch {
			case err == nil:
				result.accepted++
			case errors.Is(err, ErrPoolOverload):
				result.rejected++
			case result.unexpected == nil:
				result.unexpected = err
			}
			id++
			if id == idCount {
				id = 0
			}
		}
		results <- result
	})
	b.StopTimer()
	close(results)

	var accepted, rejected int64
	for result := range results {
		accepted += result.accepted
		rejected += result.rejected
		if result.unexpected != nil {
			b.Fatalf("Submit returned unexpected error: %v", result.unexpected)
		}
	}
	if !allowOverload && rejected != 0 {
		b.Fatalf("Submit rejected %d benchmark tasks", rejected)
	}
	b.ReportMetric(float64(accepted)/float64(b.N), "accepted/op")
	b.ReportMetric(float64(rejected)/float64(b.N), "rejected/op")
}

func BenchmarkPoolWithIDThroughput(b *testing.B) {
	scenarios := [...]struct {
		name          string
		ids           int
		taskBuffer    int
		nonblocking   bool
		disablePurge  bool
		allowOverload bool
	}{
		{name: "single", ids: 1, taskBuffer: 1_024},
		{name: "multi", ids: 1_000, taskBuffer: 64},
		{name: "multi-disable-purge", ids: 1_000, taskBuffer: 64, disablePurge: true},
		{
			name:          "saturated-nonblocking",
			ids:           1,
			taskBuffer:    1,
			nonblocking:   true,
			disablePurge:  true,
			allowOverload: true,
		},
	}
	taskKinds := [...]struct {
		name string
		cpu  bool
	}{
		{name: "empty"},
		{name: "cpu", cpu: true},
	}

	for _, scenario := range scenarios {
		for _, kind := range taskKinds {
			b.Run(scenario.name+"/"+kind.name, func(b *testing.B) {
				p := poolWithIDBenchmarkNewMode(
					b,
					scenario.ids,
					scenario.taskBuffer,
					scenario.nonblocking,
					scenario.disablePurge,
				)
				defer poolWithIDBenchmarkRelease(b, p)
				poolWithIDBenchmarkPopulateIdle(b, p, scenario.ids)
				poolWithIDBenchmarkRunThroughput(
					b,
					p,
					scenario.ids,
					poolWithIDBenchmarkTask(kind.cpu),
					scenario.allowOverload,
				)
			})
		}
	}
}

func BenchmarkPoolWithIDLockDiagnostics(b *testing.B) {
	for _, count := range poolWithIDBenchmarkDiagnosticSizes {
		name := benchmarkPoolWithIDSizeName(count)
		b.Run("submit/"+name, func(b *testing.B) {
			p, lock := poolWithIDBenchmarkNewDiagnostic(b, count)
			defer poolWithIDBenchmarkRelease(b, p)
			poolWithIDBenchmarkPopulateIdle(b, p, count)
			lock.reset()
			poolWithIDBenchmarkRunThroughput(b, p, count, func() {}, false)
			b.ReportMetric(float64(lock.total.Load())/float64(b.N), "lock-ns/op")
		})

		b.Run("purge-idle/"+name, func(b *testing.B) {
			p, lock := poolWithIDBenchmarkNewDiagnostic(b, count)
			defer poolWithIDBenchmarkRelease(b, p)
			poolWithIDBenchmarkPopulateIdle(b, p, count)
			now := time.Now()
			lock.reset()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				p.purgeExpired(now)
			}
			b.StopTimer()
			b.ReportMetric(float64(lock.total.Load())/float64(b.N), "lock-ns/op")
		})

		b.Run("purge-running/"+name, func(b *testing.B) {
			p, lock := poolWithIDBenchmarkNewDiagnostic(b, count)
			release := poolWithIDBenchmarkPopulateRunning(b, p, count)
			defer func() {
				release()
				poolWithIDBenchmarkRelease(b, p)
			}()
			now := time.Now()
			lock.reset()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				p.purgeExpired(now)
			}
			b.StopTimer()
			b.ReportMetric(float64(lock.total.Load())/float64(b.N), "lock-ns/op")
		})
	}
}

func poolWithIDBenchmarkSetIdleEligibility(
	b *testing.B,
	p *PoolWithID,
	eligible int,
	now time.Time,
) {
	b.Helper()
	p.registry.expiryMu.Lock()
	visited := 0
	for entry := p.registry.idle.head; entry != nil; entry = entry.expiryNext {
		entry.mu.Lock()
		if visited < eligible {
			entry.lastIdleAt = now.Add(-2 * time.Hour)
		} else {
			entry.lastIdleAt = now
		}
		entry.mu.Unlock()
		visited++
	}
	p.registry.expiryMu.Unlock()
	if visited != poolWithIDBenchmarkRegistrySize(p) {
		b.Fatalf("expiry list has %d entries, registry has %d", visited,
			poolWithIDBenchmarkRegistrySize(p))
	}
}

func poolWithIDBenchmarkNewPurgeState(
	b *testing.B,
	entries, eligible int,
) (*PoolWithID, time.Time) {
	b.Helper()
	p := poolWithIDBenchmarkNewMode(b, entries, 1, false, false)
	poolWithIDBenchmarkPopulateIdle(b, p, entries)
	now := time.Now()
	poolWithIDBenchmarkSetIdleEligibility(b, p, eligible, now)
	return p, now
}

func BenchmarkPoolWithIDPurgeTransitions(b *testing.B) {
	const entries = 256

	b.Run("none", func(b *testing.B) {
		p, now := poolWithIDBenchmarkNewPurgeState(b, entries, 0)
		defer poolWithIDBenchmarkRelease(b, p)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			p.purgeExpired(now)
		}
		b.StopTimer()
		b.ReportMetric(0, "retired/op")
	})

	for _, scenario := range []struct {
		name     string
		eligible int
	}{
		{name: "half", eligible: entries / 2},
		{name: "all", eligible: entries},
	} {
		b.Run(scenario.name, func(b *testing.B) {
			var retired int64
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				p, now := poolWithIDBenchmarkNewPurgeState(b, entries, scenario.eligible)
				b.StartTimer()
				p.purgeExpired(now)
				b.StopTimer()
				retired += int64(entries - poolWithIDBenchmarkRegistrySize(p))
				poolWithIDBenchmarkRelease(b, p)
			}
			b.ReportMetric(float64(retired)/float64(b.N), "retired/op")
		})
	}
}

func BenchmarkPoolWithIDEscapeReplacement(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		p := poolWithIDBenchmarkNewPool(
			b,
			1,
			WithExpiryDuration(time.Hour),
			WithRunningTaskTimeout(time.Hour),
			WithTaskBuffer(1),
			WithMaxEscapedWorkers(1),
			WithMaxEscapedWorkersPerID(1),
		)
		release := make(chan struct{})
		started := make(chan struct{})
		if err := p.Submit(0, func() {
			close(started)
			<-release
		}); err != nil {
			close(release)
			b.Fatalf("Submit running task: %v", err)
		}
		<-started
		if err := p.Submit(0, func() {}); err != nil {
			close(release)
			b.Fatalf("Submit queued task: %v", err)
		}
		now := time.Now().Add(2 * time.Hour)
		b.StartTimer()
		p.purgeExpired(now)
		b.StopTimer()
		close(release)
		poolWithIDBenchmarkWaitFor(b, "escaped owner exit", func() bool {
			return p.Escaped() == 0
		})
		poolWithIDBenchmarkRelease(b, p)
	}
	b.ReportMetric(1, "replacements/op")
}

func BenchmarkPoolWithIDEscapeEventChannelFull(b *testing.B) {
	p := poolWithIDBenchmarkNewMode(b, 1, 1, false, true)
	defer poolWithIDBenchmarkRelease(b, p)
	event := PoolWithIDEscapeEvent{Type: PoolWithIDWorkerEscaped, ID: 1}
	for i := 0; i < cap(p.escape.events); i++ {
		p.escape.events <- event
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.publishEscapeEvent(event)
	}
	b.StopTimer()
	b.ReportMetric(float64(p.DroppedEscapeEvents())/float64(b.N), "dropped/op")
}

func BenchmarkPoolWithIDEscapeSnapshotHighCardinality(b *testing.B) {
	for _, cardinality := range []int{1_000, 10_000} {
		b.Run(benchmarkPoolWithIDSizeName(cardinality), func(b *testing.B) {
			p := poolWithIDBenchmarkNewPool(
				b,
				1,
				WithDisablePurge(true),
				WithMaxEscapedWorkers(cardinality+1),
				WithMaxEscapedWorkersPerID(1),
			)
			defer poolWithIDBenchmarkRelease(b, p)
			p.escape.mu.Lock()
			p.escape.total = cardinality
			for id := 0; id < cardinality; id++ {
				p.escape.byID[id] = 1
				p.escape.exhaustedByID[id] = PoolWithIDEscapePerIDBudgetExhausted
			}
			p.escape.mu.Unlock()

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				snapshot := p.EscapeSnapshot()
				runtime.KeepAlive(snapshot)
			}
			b.StopTimer()
			b.ReportMetric(float64(cardinality), "ids/op")
		})
	}
}

func BenchmarkPoolWithIDReleaseStorm(b *testing.B) {
	const entries = 64
	for _, callers := range []int{1, 8, 64} {
		b.Run(fmt.Sprintf("callers-%d", callers), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				p := poolWithIDBenchmarkNewMode(b, entries, 1, false, true)
				poolWithIDBenchmarkPopulateIdle(b, p, entries)
				start := make(chan struct{})
				results := make(chan error, callers)
				var ready, finished sync.WaitGroup
				ready.Add(callers)
				finished.Add(callers)
				for caller := 0; caller < callers; caller++ {
					go func() {
						defer finished.Done()
						ready.Done()
						<-start
						results <- p.ReleaseTimeout(poolWithIDBenchmarkTimeout)
					}()
				}
				ready.Wait()
				b.StartTimer()
				close(start)
				finished.Wait()
				b.StopTimer()
				for caller := 0; caller < callers; caller++ {
					if err := <-results; err != nil && !errors.Is(err, ErrPoolClosed) {
						b.Fatalf("ReleaseTimeout storm: %v", err)
					}
				}
			}
			b.ReportMetric(float64(callers), "callers/op")
		})
	}
}

func BenchmarkPoolWithIDLifecycle(b *testing.B) {
	const ids = 64
	for _, cpu := range []bool{false, true} {
		name := "empty"
		if cpu {
			name = "cpu"
		}
		b.Run("reboot/"+name, func(b *testing.B) {
			p := poolWithIDBenchmarkNewMode(b, ids, ids, false, true)
			defer poolWithIDBenchmarkRelease(b, p)
			work := poolWithIDBenchmarkTask(cpu)
			b.ReportAllocs()
			b.ResetTimer()
			for generation := 0; generation < b.N; generation++ {
				var finished sync.WaitGroup
				finished.Add(ids)
				task := func() {
					work()
					finished.Done()
				}
				for id := 0; id < ids; id++ {
					if err := p.Submit(id, task); err != nil {
						b.Fatalf("Submit lifecycle task: %v", err)
					}
				}
				finished.Wait()
				if err := p.ReleaseTimeout(poolWithIDBenchmarkTimeout); err != nil {
					b.Fatalf("ReleaseTimeout lifecycle generation: %v", err)
				}
				p.Reboot()
			}
			b.StopTimer()
			b.ReportMetric(ids, "tasks/op")
		})
	}
}

func benchmarkPoolWithIDSizeName(size int) string {
	switch size {
	case 1_000:
		return "1k"
	case 10_000:
		return "10k"
	case 100_000:
		return "100k"
	default:
		return fmt.Sprintf("%d", size)
	}
}
