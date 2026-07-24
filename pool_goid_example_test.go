package ants_test

import (
	"context"
	"log"
	"sync/atomic"
	"time"

	ants "github.com/alkaid/ants/v2"
)

// MonitorPoolWithID demonstrates one event consumer, authoritative snapshot
// reconciliation, per-ID diagnostics, and a low-cardinality total gauge.
func MonitorPoolWithID(
	ctx context.Context,
	pool *ants.PoolWithID,
	recordByID func(id, escaped int),
	setEscapedGauge func(total int),
) {
	knownIDs := make(map[int]struct{})
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case event := <-pool.EscapeEvents():
			recordByID(event.ID, event.ByID)
			if event.ByID == 0 {
				delete(knownIDs, event.ID)
			} else {
				knownIDs[event.ID] = struct{}{}
			}
			log.Printf("pool escape type=%d id=%d generation=%d reason=%d by_id=%d total=%d",
				event.Type, event.ID, event.Generation, event.BudgetReason,
				event.ByID, event.Total)
		case <-ticker.C:
			// Events notify promptly; the snapshot repairs missed notifications.
			snapshot := pool.EscapeSnapshot()
			for id := range knownIDs {
				if snapshot.ByID[id] == 0 {
					recordByID(id, 0)
					delete(knownIDs, id)
				}
			}
			for id, count := range snapshot.ByID {
				recordByID(id, count)
				knownIDs[id] = struct{}{}
			}
			setEscapedGauge(snapshot.Total)
			if snapshot.DroppedEvents != 0 {
				log.Printf("pool escape notifications dropped=%d", snapshot.DroppedEvents)
			}
		case <-ctx.Done():
			return
		}
	}
}

func ExamplePoolWithID_escapeObservability() {
	pool, err := ants.NewPoolWithID(32, ants.WithExpiryDuration(time.Minute))
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Release()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// A monitoring SDK can export this as one low-cardinality gauge. Keep IDs in
	// logs or an internal diagnostic store instead of metric labels. Do not
	// retry a task solely because an escape notification was received: the old
	// task may still run.
	var escapedWorkers atomic.Int64
	go MonitorPoolWithID(
		ctx,
		pool,
		func(id, escaped int) {
			log.Printf("pool escape state id=%d escaped=%d", id, escaped)
		},
		func(total int) { escapedWorkers.Store(int64(total)) },
	)
}
