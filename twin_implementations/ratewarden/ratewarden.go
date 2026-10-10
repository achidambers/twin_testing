// Package ratewarden is an in-process, keyed rate limiter.
//
// A Limiter tracks independent limits per key (a user ID, an API key, a
// client IP, ...). Three algorithms are provided:
//
//   - NewTokenBucket: GCRA token bucket. Smooth refill with a burst allowance.
//     Exact integer arithmetic, one int64 of state per key.
//   - NewFixedWindow: at most limit requests per aligned window. Cheapest,
//     but allows up to 2x limit across a window boundary.
//   - NewSlidingWindow: sliding window counter. Weights the previous window's
//     count by how much of it still overlaps the sliding window, removing
//     most of the fixed window's boundary burst.
//
// All methods are safe for concurrent use. Keys are spread over mutex-guarded
// shards; a background janitor drops keys whose state has fully replenished,
// and WithMaxKeys bounds memory against key-flooding.
package ratewarden

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// Never is the RetryAfter of a request that can never succeed because its
// cost exceeds the limiter's capacity.
const Never time.Duration = -1

var (
	// ErrInvalidConfig wraps every constructor and option validation error.
	ErrInvalidConfig = errors.New("ratewarden: invalid config")
	// ErrExceedsLimit is returned by Wait when n is larger than the
	// limiter's capacity, so waiting would never succeed.
	ErrExceedsLimit = errors.New("ratewarden: request cost exceeds limiter capacity")
	// ErrDeadlineTooShort is returned by Wait when the context deadline
	// would pass before the request could be allowed.
	ErrDeadlineTooShort = errors.New("ratewarden: context deadline would pass before request is allowed")
)

// Decision is the outcome of one AllowN call.
type Decision struct {
	Allowed bool
	// Limit is the key's capacity: the burst for a token bucket, the
	// per-window limit otherwise.
	Limit int
	// Remaining is how many more units could be taken right now.
	Remaining int
	// RetryAfter is 0 when allowed. When denied it is how long until the
	// same request would be allowed, assuming no other traffic on the key,
	// or Never if it can never be allowed.
	RetryAfter time.Duration
	// ResetAfter is how long until the key is back to full capacity.
	ResetAfter time.Duration
}

// Clock supplies time. The default is a monotonic wall clock, which is
// immune to system clock jumps. ratewardentest.Clock is a manual clock for
// tests.
type Clock interface {
	Now() time.Time
	After(d time.Duration) <-chan time.Time
}

type monoClock struct{ base time.Time }

// Now is base plus monotonic elapsed time, so UnixNano never goes backwards
// even if the system clock is changed.
func (c monoClock) Now() time.Time                       { return c.base.Add(time.Since(c.base)) }
func (monoClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

// Option configures a Limiter.
type Option func(*options) error

type options struct {
	clock   Clock
	shards  int
	maxKeys int
	cleanup time.Duration
}

func defaultOptions() options {
	return options{clock: monoClock{time.Now()}, shards: 64, cleanup: time.Minute}
}

// WithClock replaces the time source.
func WithClock(c Clock) Option {
	return func(o *options) error {
		if c == nil {
			return fmt.Errorf("%w: nil clock", ErrInvalidConfig)
		}
		o.clock = c
		return nil
	}
}

// WithShards sets the number of lock shards, rounded up to a power of two.
// More shards reduce contention across keys. Default 64.
func WithShards(n int) Option {
	return func(o *options) error {
		if n < 1 || n > 1<<16 {
			return fmt.Errorf("%w: shards must be in [1, 65536], got %d", ErrInvalidConfig, n)
		}
		o.shards = n
		return nil
	}
}

// WithMaxKeys bounds how many keys are tracked. When a shard is full, a new
// key evicts a random existing key in that shard (whose limit then starts
// over). The bound is enforced per shard, so the effective maximum is
// maxKeys rounded up to a multiple of the shard count. 0 means unbounded,
// which is the default; set this for any key derived from client input.
func WithMaxKeys(n int) Option {
	return func(o *options) error {
		if n < 0 {
			return fmt.Errorf("%w: max keys must be >= 0, got %d", ErrInvalidConfig, n)
		}
		o.maxKeys = n
		return nil
	}
}

// WithCleanupInterval sets how often idle keys are swept. 0 disables the
// background janitor. Default one minute.
func WithCleanupInterval(d time.Duration) Option {
	return func(o *options) error {
		if d < 0 {
			return fmt.Errorf("%w: cleanup interval must be >= 0, got %v", ErrInvalidConfig, d)
		}
		o.cleanup = d
		return nil
	}
}

// backend is the type-erased view of a store[S, A].
type backend interface {
	take(key string, n int, now int64) Decision
	reset(key string)
	sweep(now int64) int
	len() int
}

// Limiter is a keyed rate limiter. Create one with NewTokenBucket,
// NewFixedWindow or NewSlidingWindow and call Close when done with it to
// stop its janitor goroutine.
type Limiter struct {
	b         backend
	clock     Clock
	done      chan struct{}
	wg        sync.WaitGroup
	closeOnce sync.Once
}

func newLimiter[S any, A algorithm[S]](alg A, opts []Option) (*Limiter, error) {
	o := defaultOptions()
	for _, opt := range opts {
		if err := opt(&o); err != nil {
			return nil, err
		}
	}
	l := &Limiter{
		b:     newStore[S](alg, o.shards, o.maxKeys),
		clock: o.clock,
		done:  make(chan struct{}),
	}
	if o.cleanup > 0 {
		l.wg.Add(1)
		go l.janitor(o.cleanup)
	}
	return l, nil
}

func (l *Limiter) janitor(every time.Duration) {
	defer l.wg.Done()
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-l.done:
			return
		case <-t.C:
			l.b.sweep(l.now())
		}
	}
}

func (l *Limiter) now() int64 { return l.clock.Now().UnixNano() }

// Allow takes one unit for key.
func (l *Limiter) Allow(key string) Decision { return l.AllowN(key, 1) }

// AllowN takes n units for key if they are all available, and otherwise
// takes nothing. n <= 0 reports the key's state without consuming anything.
func (l *Limiter) AllowN(key string, n int) Decision {
	if n < 0 {
		n = 0
	}
	return l.b.take(key, n, l.now())
}

// Peek reports key's state without consuming anything.
func (l *Limiter) Peek(key string) Decision { return l.AllowN(key, 0) }

// Wait blocks until one unit for key is taken or ctx is done.
func (l *Limiter) Wait(ctx context.Context, key string) error { return l.WaitN(ctx, key, 1) }

// WaitN blocks until n units for key are taken or ctx is done. It returns
// ErrExceedsLimit if n exceeds capacity, and ErrDeadlineTooShort without
// waiting if ctx's deadline would pass first. Waiters are not queued: under
// contention a waiter may be overtaken and wait again.
func (l *Limiter) WaitN(ctx context.Context, key string, n int) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		d := l.AllowN(key, n)
		if d.Allowed {
			return nil
		}
		if d.RetryAfter == Never {
			return ErrExceedsLimit
		}
		if dl, ok := ctx.Deadline(); ok && time.Until(dl) < d.RetryAfter {
			return ErrDeadlineTooShort
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-l.clock.After(d.RetryAfter):
		}
	}
}

// Reset forgets key, restoring it to full capacity.
func (l *Limiter) Reset(key string) { l.b.reset(key) }

// Len reports how many keys are currently tracked.
func (l *Limiter) Len() int { return l.b.len() }

// Close stops the janitor. The Limiter stays usable afterwards, but idle
// keys are no longer swept. Close is idempotent.
func (l *Limiter) Close() error {
	l.closeOnce.Do(func() {
		close(l.done)
		l.wg.Wait()
	})
	return nil
}
