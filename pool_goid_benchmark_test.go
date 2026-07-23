package ants

import (
	"errors"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const (
	poolWithIDBenchmarkTimeout   = 2 * time.Minute
	poolWithIDBenchmarkMaxSample = 64 * 1024
)

var poolWithIDBenchmarkSizes = [...]int{1_000, 10_000, 100_000}

type poolWithIDBenchmarkLock struct {
	sync.Locker
	heldAt time.Time
	total  atomic.Int64
}

func (l *poolWithIDBenchmarkLock) Lock() {
	l.Locker.Lock()
	l.heldAt = time.Now()
}

func (l *poolWithIDBenchmarkLock) Unlock() {
	l.total.Add(time.Since(l.heldAt).Nanoseconds())
	l.Locker.Unlock()
}

func (l *poolWithIDBenchmarkLock) reset() {
	l.total.Store(0)
}

func poolWithIDBenchmarkNew(b *testing.B, size int) (*PoolWithID, *poolWithIDBenchmarkLock) {
	b.Helper()
	taskBuffer := (1 << 20) / size
	if taskBuffer < 16 {
		taskBuffer = 16
	}
	p, err := NewPoolWithID(
		size,
		WithExpiryDuration(time.Hour),
		WithTaskBuffer(taskBuffer),
		WithNonblocking(true),
		WithLogger(poolWithIDDiscardLogger{}),
	)
	if err != nil {
		b.Fatalf("NewPoolWithID: %v", err)
	}
	measuredLock := &poolWithIDBenchmarkLock{Locker: p.lock}
	p.lock = measuredLock
	return p, measuredLock
}

func poolWithIDBenchmarkRelease(b *testing.B, p *PoolWithID) {
	b.Helper()
	if err := p.ReleaseTimeout(poolWithIDBenchmarkTimeout); err != nil &&
		!errors.Is(err, ErrPoolClosed) {
		b.Fatalf("ReleaseTimeout: %v", err)
	}
}

func poolWithIDBenchmarkWaitForIdle(b *testing.B, p *PoolWithID, count int) {
	b.Helper()
	deadline := time.Now().Add(poolWithIDBenchmarkTimeout)
	for {
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
		if idle == count {
			return
		}
		if time.Now().After(deadline) {
			b.Fatalf("timed out waiting for %d idle entries; got %d", count, idle)
		}
		runtime.Gosched()
	}
}

func poolWithIDBenchmarkPopulateIdle(b *testing.B, p *PoolWithID, count int) {
	b.Helper()
	var finished sync.WaitGroup
	finished.Add(count)
	task := finished.Done
	for id := 0; id < count; id++ {
		if err := p.Submit(id, task); err != nil {
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
	allStarted := make(chan struct{})
	var started atomic.Int64
	task := func() {
		if started.Add(1) == int64(count) {
			close(allStarted)
		}
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

func poolWithIDBenchmarkSamples(n int) []int64 {
	if n > poolWithIDBenchmarkMaxSample {
		n = poolWithIDBenchmarkMaxSample
	}
	return make([]int64, n)
}

func poolWithIDBenchmarkReport(
	b *testing.B,
	lock *poolWithIDBenchmarkLock,
	scanned int64,
	samples []int64,
	elapsed time.Duration,
) {
	b.Helper()
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	percentile := func(p int) float64 {
		return float64(samples[(len(samples)-1)*p/100])
	}
	b.ReportMetric(float64(scanned)/float64(b.N), "scanned/op")
	b.ReportMetric(float64(lock.total.Load())/float64(b.N), "lock-ns/op")
	b.ReportMetric(percentile(95), "p95-ns/op")
	b.ReportMetric(percentile(99), "p99-ns/op")
	b.ReportMetric(float64(b.N)/elapsed.Seconds(), "ops/s")
}

func BenchmarkPoolWithIDPurge(b *testing.B) {
	for _, count := range poolWithIDBenchmarkSizes {
		b.Run("idle/"+benchmarkPoolWithIDSizeName(count), func(b *testing.B) {
			p, measuredLock := poolWithIDBenchmarkNew(b, count)
			defer poolWithIDBenchmarkRelease(b, p)
			poolWithIDBenchmarkPopulateIdle(b, p, count)

			var scanned atomic.Int64
			p.testHooks.afterPurgeEntryVisited = func() { scanned.Add(1) }
			now := time.Now().UnixNano()
			samples := poolWithIDBenchmarkSamples(b.N)
			b.ReportAllocs()
			measuredLock.reset()
			b.ResetTimer()
			startedAt := time.Now()
			for i := 0; i < b.N; i++ {
				started := time.Now()
				p.purgeExpired(now)
				samples[i%len(samples)] = time.Since(started).Nanoseconds()
			}
			elapsed := time.Since(startedAt)
			b.StopTimer()
			poolWithIDBenchmarkReport(b, measuredLock, scanned.Load(), samples, elapsed)
		})

		b.Run("running/"+benchmarkPoolWithIDSizeName(count), func(b *testing.B) {
			p, measuredLock := poolWithIDBenchmarkNew(b, count)
			releaseTasks := poolWithIDBenchmarkPopulateRunning(b, p, count)
			defer func() {
				releaseTasks()
				poolWithIDBenchmarkRelease(b, p)
			}()

			var scanned atomic.Int64
			p.testHooks.afterPurgeEntryVisited = func() { scanned.Add(1) }
			now := time.Now().UnixNano()
			samples := poolWithIDBenchmarkSamples(b.N)
			b.ReportAllocs()
			measuredLock.reset()
			b.ResetTimer()
			startedAt := time.Now()
			for i := 0; i < b.N; i++ {
				started := time.Now()
				p.purgeExpired(now)
				samples[i%len(samples)] = time.Since(started).Nanoseconds()
			}
			elapsed := time.Since(startedAt)
			b.StopTimer()
			poolWithIDBenchmarkReport(b, measuredLock, scanned.Load(), samples, elapsed)
		})
	}
}

func BenchmarkPoolWithIDSubmit(b *testing.B) {
	for _, count := range poolWithIDBenchmarkSizes {
		b.Run(benchmarkPoolWithIDSizeName(count), func(b *testing.B) {
			p, measuredLock := poolWithIDBenchmarkNew(b, count)
			defer poolWithIDBenchmarkRelease(b, p)
			poolWithIDBenchmarkPopulateIdle(b, p, count)

			samples := poolWithIDBenchmarkSamples(b.N)
			var sequence atomic.Int64
			var sampleCount atomic.Int64
			var submitted atomic.Int64
			var completed atomic.Int64
			var rejected atomic.Int64
			task := func() { completed.Add(1) }

			b.ReportAllocs()
			measuredLock.reset()
			b.ResetTimer()
			startedAt := time.Now()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					index := sequence.Add(1) - 1
					started := time.Now()
					err := p.Submit(int(index%int64(count)), task)
					elapsed := time.Since(started).Nanoseconds()
					if sample := sampleCount.Add(1) - 1; sample < int64(len(samples)) {
						samples[sample] = elapsed
					}
					if err != nil {
						rejected.Add(1)
						continue
					}
					submitted.Add(1)
				}
			})
			elapsed := time.Since(startedAt)
			b.StopTimer()

			deadline := time.Now().Add(poolWithIDBenchmarkTimeout)
			for completed.Load() != submitted.Load() {
				if time.Now().After(deadline) {
					b.Fatalf("timed out waiting for submitted tasks: completed=%d submitted=%d",
						completed.Load(), submitted.Load())
				}
				runtime.Gosched()
			}
			if got := rejected.Load(); got != 0 {
				b.Fatalf("Submit rejected %d benchmark tasks", got)
			}
			poolWithIDBenchmarkReport(b, measuredLock, 0, samples, elapsed)
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
		return "unknown"
	}
}
