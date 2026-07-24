package ants_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	ants "github.com/alkaid/ants/v2"
)

func BenchmarkPoolWithIDComparison(b *testing.B) {
	for _, ids := range []int{1, 64, 1_024} {
		b.Run(fmt.Sprintf("ids-%d", ids), func(b *testing.B) {
			p, err := ants.NewPoolWithID(
				ids,
				ants.WithTaskBuffer(1_024),
				ants.WithDisablePurge(true),
			)
			if err != nil {
				b.Fatal(err)
			}
			var warmed sync.WaitGroup
			warmed.Add(ids)
			for id := 0; id < ids; id++ {
				if err := p.Submit(id, warmed.Done); err != nil {
					b.Fatalf("warm ID %d: %v", id, err)
				}
			}
			warmed.Wait()
			task := func() {}
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				id := 0
				for pb.Next() {
					if err := p.Submit(id, task); err != nil {
						b.Fatalf("Submit: %v", err)
					}
					id++
					if id == ids {
						id = 0
					}
				}
			})
			b.StopTimer()
			if err := p.ReleaseTimeout(2 * time.Minute); err != nil {
				b.Fatal(err)
			}
		})
	}
}
