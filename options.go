/*
 * Copyright (c) 2018. Andy Pan. All rights reserved.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package ants

import "time"

// Option represents the optional function.
type Option func(opts *Options)

func loadOptions(options ...Option) *Options {
	opts := new(Options)
	for _, option := range options {
		option(opts)
	}
	return opts
}

// Options contains all options which will be applied when instantiating an ants
// pool.
//
// This fork adds PoolWithID-specific fields to the upstream layout. Prefer
// keyed literals or Option functions: unkeyed Options literals are compatible
// only when they contain every field in this version's exact order.
type Options struct {
	// ExpiryDuration is a period for the scavenger goroutine to clean up those expired workers,
	// the scavenger scans all workers every `ExpiryDuration` and clean up those workers that haven't been
	// used for more than `ExpiryDuration`. For PoolWithID it is also measured
	// from the start of each task's execution. Once a running task reaches this
	// escape threshold, a replacement owner takes over the same ID queue unless
	// running purge is disabled. This is not an end-to-end deadline measured
	// from Submit, and it cannot stop the escaped task or prevent late side
	// effects.
	ExpiryDuration time.Duration

	// PreAlloc indicates whether to make memory pre-allocation when initializing Pool.
	// It is accepted but has no effect for PoolWithID.
	PreAlloc bool

	// Max number of goroutine blocking on pool.Submit.
	// 0 (default value) means no such limit.
	// For PoolWithID this only limits callers waiting for owner capacity for a
	// new ID; it does not limit queue-space waits for an existing ID.
	MaxBlockingTasks int

	// When Nonblocking is true, Pool.Submit will never be blocked.
	// ErrPoolOverload will be returned when Pool.Submit cannot be done at once.
	// When Nonblocking is true, MaxBlockingTasks is inoperative.
	// PoolWithID also returns ErrPoolOverload when an existing ID's observed
	// queue length reaches TaskBuffer, or when its final nonblocking send finds
	// the physical queue full. Concurrent submissions may use the reserved half
	// of that queue because the admission check and send are not serialized.
	Nonblocking bool

	// PanicHandler is used to handle panics from each worker goroutine.
	// If nil, the default behavior is to capture the value given to panic
	// and resume normal execution and print that value along with the
	// stack trace of the goroutine
	PanicHandler func(any)

	// Logger is the customized logger for logging info, if it is not set,
	// default standard logger from log package is used.
	Logger Logger

	// When DisablePurge is true, workers are not purged and are resident. For
	// PoolWithID this also disables automatic escape of long-running owners.
	DisablePurge bool

	// TaskBuffer is the PoolWithID admission limit for each ID. The physical
	// task channel has twice this capacity. Zero uses DefaultTaskBuffer and a
	// physical capacity of 2*DefaultTaskBuffer. Negative values and values above
	// MaxTaskBuffer are rejected by NewPoolWithID.
	//
	// Nonblocking submissions reject when the observed queue length reaches this
	// limit. The check and send are not serialized, so concurrent submissions
	// may use the reserved half between TaskBuffer and 2*TaskBuffer; a final
	// nonblocking send still rejects if the physical channel is full. Blocking
	// submissions may use the full physical capacity and then wait for space or
	// pool closure. MaxBlockingTasks does not limit this existing-ID wait, and a
	// blocking task that recursively submits to its own full queue is not
	// guaranteed to make progress.
	TaskBuffer int

	// DisablePurgeRunning prevents PoolWithID from escaping an owner whose task
	// exceeds ExpiryDuration. A permanently blocked task can then block that ID
	// permanently. This option has no effect on other pool types.
	DisablePurgeRunning bool
}

// WithOptions accepts the whole Options config. Prefer a keyed Options literal
// because this fork's PoolWithID fields extend the upstream struct layout.
func WithOptions(options Options) Option {
	return func(opts *Options) {
		*opts = options
	}
}

// WithExpiryDuration sets up the interval time of cleaning up goroutines. For
// PoolWithID it also sets the running-task escape threshold, measured from the
// start of task execution rather than from Submit.
func WithExpiryDuration(expiryDuration time.Duration) Option {
	return func(opts *Options) {
		opts.ExpiryDuration = expiryDuration
	}
}

// WithPreAlloc indicates whether it should malloc for workers. PoolWithID
// accepts this option but does not preallocate or reuse ID workers.
func WithPreAlloc(preAlloc bool) Option {
	return func(opts *Options) {
		opts.PreAlloc = preAlloc
	}
}

// WithMaxBlockingTasks sets up the maximum number of goroutines that are
// blocked when a pool reaches owner capacity. For PoolWithID it does not apply
// to queue-space waits for an ID that already exists.
func WithMaxBlockingTasks(maxBlockingTasks int) Option {
	return func(opts *Options) {
		opts.MaxBlockingTasks = maxBlockingTasks
	}
}

// WithNonblocking indicates that pool submissions return ErrPoolOverload
// instead of waiting. PoolWithID applies this to both new-ID owner capacity and
// existing-ID queue admission.
func WithNonblocking(nonblocking bool) Option {
	return func(opts *Options) {
		opts.Nonblocking = nonblocking
	}
}

// WithPanicHandler sets up panic handler.
func WithPanicHandler(panicHandler func(any)) Option {
	return func(opts *Options) {
		opts.PanicHandler = panicHandler
	}
}

// WithLogger sets up a customized logger.
func WithLogger(logger Logger) Option {
	return func(opts *Options) {
		opts.Logger = logger
	}
}

// WithDisablePurge indicates whether we turn off automatic purge. It also
// disables PoolWithID's automatic escape of long-running owners.
func WithDisablePurge(disable bool) Option {
	return func(opts *Options) {
		opts.DisablePurge = disable
	}
}

// WithDisablePurgeRunning controls whether PoolWithID replaces an owner whose
// running task reaches ExpiryDuration. Setting disable to true turns off that
// automatic recovery behavior.
func WithDisablePurgeRunning(disable bool) Option {
	return func(opts *Options) {
		opts.DisablePurgeRunning = disable
	}
}

// WithTaskBuffer sets the PoolWithID admission limit for each ID. Its physical
// task channel capacity is twice taskBuffer; zero selects DefaultTaskBuffer.
// Negative values and values above MaxTaskBuffer make NewPoolWithID return
// ErrInvalidPoolWithIDTaskBuffer.
//
// In nonblocking mode, submissions reject once the observed queue length
// reaches the admission limit and always use a final nonblocking send. Because
// the check and send are not serialized, concurrent submissions may use the
// reserved half of the channel. In blocking mode, submissions may use the full
// channel and wait for space or closure; MaxBlockingTasks does not limit this
// existing-ID wait. A task that recursively submits to its own full queue in
// blocking mode is not guaranteed to make progress.
func WithTaskBuffer(taskBuffer int) Option {
	return func(opts *Options) {
		opts.TaskBuffer = taskBuffer
	}
}
