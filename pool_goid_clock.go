package ants

import "time"

const maxPoolWithIDRunningScanInterval = 30 * time.Second

type poolWithIDClock interface {
	Now() time.Time
	Since(time.Time) time.Duration
}

type systemPoolWithIDClock struct{}

func (systemPoolWithIDClock) Now() time.Time {
	return time.Now()
}

func (systemPoolWithIDClock) Since(start time.Time) time.Duration {
	return time.Since(start)
}

// poolWithIDClockFactory is replaced by deterministic tests. Production uses
// time.Time's monotonic component for all expiry and running-time comparisons.
var poolWithIDClockFactory = func() poolWithIDClock {
	return systemPoolWithIDClock{}
}
