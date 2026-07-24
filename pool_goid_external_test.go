package ants_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	ants "github.com/alkaid/ants/v2"
	"github.com/stretchr/testify/require"
)

func poolWithIDAcceptEscapeEventType(ants.PoolWithIDEscapeEventType) {}

func poolWithIDAcceptEscapeBudgetReason(ants.PoolWithIDEscapeBudgetReason) {}

func poolWithIDAcceptEscapeEventChannel(<-chan ants.PoolWithIDEscapeEvent) {}

func poolWithIDAcceptOption(ants.Option) {}

func poolWithIDAcceptOptions(...ants.Option) {}

// A local defined type keeps go vet's composites check satisfied while the
// conversion to ants.Options locks the current fork's exact field layout.
type poolWithIDOptionsLayout ants.Options

func poolWithIDUnkeyedOptions() ants.Options {
	return ants.Options(poolWithIDOptionsLayout{
		time.Second,
		true,
		1,
		true,
		nil,
		nil,
		true,
		4,
		false,
		time.Minute,
		2,
		1,
	})
}

func poolWithIDCloseSignal() (chan struct{}, func()) {
	ch := make(chan struct{})
	var once sync.Once
	return ch, func() { once.Do(func() { close(ch) }) }
}

func poolWithIDWaitSignal(t *testing.T, ch <-chan struct{}, name string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for %s", name)
	}
}

func TestPoolWithIDPublicAPI(t *testing.T) {
	eventType := ants.PoolWithIDEscapeBudgetExhausted
	poolWithIDAcceptEscapeEventType(eventType)
	require.Equal(t, ants.PoolWithIDEscapeBudgetExhausted, eventType)
	require.NotEqual(t, ants.PoolWithIDWorkerEscaped, eventType)
	require.NotEqual(t, ants.PoolWithIDWorkerEscaped, ants.PoolWithIDEscapedWorkerExited)
	require.NotEqual(t, ants.OPENED, ants.CLOSING)
	require.Equal(t, 10, ants.MinTaskBuffer)
	require.Equal(t, 100, ants.DefaultTaskBuffer)
	require.Equal(t, 64*1024, ants.MaxTaskBuffer)
	require.Equal(t, 30*time.Second, ants.DefaultPoolWithIDExpiryDuration)
	require.Equal(t, 5*time.Minute, ants.DefaultRunningTaskTimeout)

	budgetReason := ants.PoolWithIDEscapeGlobalBudgetExhausted |
		ants.PoolWithIDEscapePerIDBudgetExhausted
	poolWithIDAcceptEscapeBudgetReason(budgetReason)

	now := time.Now()
	event := ants.PoolWithIDEscapeEvent{
		Type:         ants.PoolWithIDEscapeBudgetExhausted,
		ID:           7,
		Generation:   3,
		Time:         now,
		Total:        2,
		ByID:         1,
		BudgetReason: budgetReason,
		GlobalBudget: 4,
		PerIDBudget:  2,
	}
	require.Equal(t, ants.PoolWithIDEscapeBudgetExhausted, event.Type)
	require.Equal(t, 7, event.ID)
	require.Equal(t, uint64(3), event.Generation)
	require.Equal(t, now, event.Time)
	require.Equal(t, 2, event.Total)
	require.Equal(t, 1, event.ByID)
	require.Equal(t, budgetReason, event.BudgetReason)
	require.Equal(t, 4, event.GlobalBudget)
	require.Equal(t, 2, event.PerIDBudget)

	directOption := ants.Option(func(options *ants.Options) {
		options.TaskBuffer = 4
	})
	poolWithIDAcceptOption(directOption)

	options := []ants.Option{
		directOption,
		ants.WithDisablePurge(true),
		ants.WithDisablePurgeRunning(false),
		ants.WithRunningTaskTimeout(time.Minute),
		ants.WithMaxEscapedWorkers(2),
		ants.WithMaxEscapedWorkersPerID(1),
		ants.WithMaxBlockingTasks(2),
	}
	poolWithIDAcceptOptions(options...)
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
		GlobalBudget:  2,
		PerIDBudget:   1,
		ExhaustedByID: map[int]ants.PoolWithIDEscapeBudgetReason{},
	}, snapshot)
	require.Zero(t, pool.Escaped())
	require.Equal(t, pool.Running(), pool.TotalWorkers())
	require.Zero(t, pool.Waiting())
	require.Zero(t, pool.DroppedEscapeEvents())
	require.Equal(t, ants.PoolWithIDEscapeBudgetStatus{
		GlobalLimit: 2,
		PerIDLimit:  1,
	}, pool.EscapeBudgetStatus(1))

	pool.Tune(2)
	require.NoError(t, pool.ReleaseContext(context.Background()))
	require.ErrorIs(t, pool.ReleaseTimeout(time.Second), ants.ErrPoolClosed)
	pool.Reboot()
	require.NoError(t, pool.ReleaseTimeout(time.Second))

	_, err = ants.NewPoolWithID(1, ants.WithTaskBuffer(-1))
	require.True(t, errors.Is(err, ants.ErrInvalidPoolWithIDTaskBuffer))
	_, err = ants.NewPoolWithID(1, ants.WithTaskBuffer(ants.MaxTaskBuffer+1))
	require.ErrorIs(t, err, ants.ErrInvalidPoolWithIDTaskBuffer)
	_, err = ants.NewPoolWithID(1, ants.WithRunningTaskTimeout(-time.Nanosecond))
	require.ErrorIs(t, err, ants.ErrInvalidPoolWithIDRunningTaskTimeout)
	_, err = ants.NewPoolWithID(1, ants.WithMaxEscapedWorkers(-1))
	require.ErrorIs(t, err, ants.ErrInvalidPoolWithIDEscapeBudget)
	_, err = ants.NewPoolWithID(1, ants.WithMaxEscapedWorkersPerID(-1))
	require.ErrorIs(t, err, ants.ErrInvalidPoolWithIDEscapeBudget)
}

func TestPoolWithIDOptionsCompatibility(t *testing.T) {
	keyed := ants.Options{
		ExpiryDuration:         time.Second,
		PreAlloc:               true,
		MaxBlockingTasks:       1,
		Nonblocking:            true,
		DisablePurge:           true,
		TaskBuffer:             4,
		DisablePurgeRunning:    false,
		RunningTaskTimeout:     time.Minute,
		MaxEscapedWorkers:      2,
		MaxEscapedWorkersPerID: 1,
	}

	cases := []struct {
		name    string
		options []ants.Option
	}{
		{name: "direct Option", options: []ants.Option{func(options *ants.Options) {
			options.TaskBuffer = 4
			options.DisablePurge = true
		}}},
		{name: "expanded []Option", options: []ants.Option{
			ants.WithTaskBuffer(4),
			ants.WithDisablePurge(true),
		}},
		{name: "WithOptions keyed literal", options: []ants.Option{ants.WithOptions(keyed)}},
		{name: "WithOptions current unkeyed layout", options: []ants.Option{
			ants.WithOptions(poolWithIDUnkeyedOptions()),
		}},
		{name: "PreAlloc remains accepted", options: []ants.Option{
			ants.WithPreAlloc(true),
			ants.WithTaskBuffer(4),
			ants.WithDisablePurge(true),
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pool, err := ants.NewPoolWithID(1, tc.options...)
			require.NoError(t, err)
			require.NoError(t, pool.ReleaseTimeout(time.Second))
		})
	}
}

func TestPoolWithIDTaskBufferPublicContract(t *testing.T) {
	for _, tc := range []struct {
		name      string
		options   []ants.Option
		admission int
	}{
		{
			name: "zero uses default admission limit",
			options: []ants.Option{
				ants.WithNonblocking(true),
				ants.WithDisablePurge(true),
			},
			admission: ants.DefaultTaskBuffer,
		},
		{
			name: "positive value is admission limit",
			options: []ants.Option{
				ants.WithTaskBuffer(2),
				ants.WithNonblocking(true),
				ants.WithDisablePurge(true),
			},
			admission: 2,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pool, err := ants.NewPoolWithID(1, tc.options...)
			require.NoError(t, err)

			started := make(chan struct{})
			unblock, closeUnblock := poolWithIDCloseSignal()
			t.Cleanup(closeUnblock)
			require.NoError(t, pool.Submit(7, func() {
				close(started)
				<-unblock
			}))
			poolWithIDWaitSignal(t, started, "owner start")

			for i := 0; i < tc.admission; i++ {
				require.NoError(t, pool.Submit(7, func() {}))
			}
			require.ErrorIs(t, pool.Submit(7, func() {}), ants.ErrPoolOverload)

			closeUnblock()
			require.NoError(t, pool.ReleaseTimeout(time.Second))
		})
	}

	t.Run("blocking mode uses twice the configured capacity", func(t *testing.T) {
		const admission = 2
		pool, err := ants.NewPoolWithID(
			1,
			ants.WithTaskBuffer(admission),
			ants.WithDisablePurge(true),
		)
		require.NoError(t, err)

		started := make(chan struct{})
		unblock, closeUnblock := poolWithIDCloseSignal()
		t.Cleanup(closeUnblock)
		require.NoError(t, pool.Submit(9, func() {
			close(started)
			<-unblock
		}))
		poolWithIDWaitSignal(t, started, "owner start")

		for i := 0; i < 2*admission; i++ {
			require.NoError(t, pool.Submit(9, func() {}))
		}

		closeUnblock()
		require.NoError(t, pool.ReleaseTimeout(time.Second))
	})
}
