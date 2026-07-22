package ants_test

import (
	"errors"
	"testing"
	"time"

	ants "github.com/alkaid/ants/v2"
	"github.com/stretchr/testify/require"
)

func poolWithIDAcceptEscapeEventType(ants.PoolWithIDEscapeEventType) {}

func poolWithIDAcceptEscapeEventChannel(<-chan ants.PoolWithIDEscapeEvent) {}

func TestPoolWithIDPublicAPI(t *testing.T) {
	eventType := ants.PoolWithIDWorkerEscaped
	poolWithIDAcceptEscapeEventType(eventType)
	require.Equal(t, ants.PoolWithIDWorkerEscaped, eventType)
	require.NotEqual(t, ants.PoolWithIDWorkerEscaped, ants.PoolWithIDEscapedWorkerExited)
	require.NotEqual(t, ants.OPENED, ants.CLOSING)
	require.Equal(t, 10, ants.MinTaskBuffer)

	now := time.Now()
	event := ants.PoolWithIDEscapeEvent{
		Type:  ants.PoolWithIDEscapedWorkerExited,
		ID:    7,
		Time:  now,
		Total: 2,
		ByID:  1,
	}
	require.Equal(t, ants.PoolWithIDEscapedWorkerExited, event.Type)
	require.Equal(t, 7, event.ID)
	require.Equal(t, now, event.Time)
	require.Equal(t, 2, event.Total)
	require.Equal(t, 1, event.ByID)

	options := []ants.Option{
		ants.WithOptions(ants.Options{
			ExpiryDuration:      time.Second,
			Nonblocking:         true,
			TaskBuffer:          4,
			DisablePurgeRunning: true,
		}),
		ants.WithDisablePurgeRunning(false),
		ants.WithDisablePurge(true),
	}
	pool, err := ants.NewPoolWithID(1, options...)
	require.NoError(t, err)

	events := pool.EscapeEvents()
	poolWithIDAcceptEscapeEventChannel(events)
	require.NotNil(t, events)
	snapshot := pool.EscapeSnapshot()
	require.Equal(t, ants.PoolWithIDEscapeSnapshot{
		Total:         0,
		ByID:          map[int]int{},
		DroppedEvents: 0,
	}, snapshot)

	require.NoError(t, pool.ReleaseTimeout(time.Second))
	require.ErrorIs(t, pool.ReleaseTimeout(time.Second), ants.ErrPoolClosed)

	_, err = ants.NewPoolWithID(1, ants.WithTaskBuffer(-1))
	require.True(t, errors.Is(err, ants.ErrInvalidPoolWithIDTaskBuffer))
}
