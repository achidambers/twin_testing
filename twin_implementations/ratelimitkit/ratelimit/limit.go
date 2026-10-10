// Package ratelimit provides keyed, in-memory rate limiters for Go services.
//
// Two algorithms are available:
//
//   - GCRA (generic cell rate algorithm): an exact, integer-only token bucket.
//     It allows a sustained Rate events per Period, with bursts of up to Burst.
//     Each key costs a single int64 of state.
//   - Sliding window counter: at most Rate events per Period, estimated from
//     the current and previous fixed windows. Burst is ignored. The count
//     within any aligned window never exceeds Rate.
//
// Both are exposed through the Limiter interface, so a distributed backend
// (for example Redis) can be substituted without changing callers such as
// the httplimit middleware.
package ratelimit

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"
)

var (
	// ErrInvalidLimit is returned when a Limit fails validation.
	ErrInvalidLimit = errors.New("ratelimit: invalid limit")
	// ErrInvalidCost is returned when n <= 0.
	ErrInvalidCost = errors.New("ratelimit: cost must be positive")
	// ErrCostExceedsLimit is returned when n is larger than the limiter's
	// capacity, so the request could never be allowed however long it waits.
	ErrCostExceedsLimit = errors.New("ratelimit: cost exceeds limiter capacity")
	// ErrTooManyKeys is returned when a new key would exceed WithMaxKeys.
	ErrTooManyKeys = errors.New("ratelimit: key capacity reached")
	// ErrClosed is returned by a limiter after Close.
	ErrClosed = errors.New("ratelimit: limiter closed")
	// ErrWouldExceedDeadline is returned by Wait when the required delay is
	// longer than the context's remaining time.
	ErrWouldExceedDeadline = errors.New("ratelimit: wait would exceed context deadline")
)

// maxSpan bounds Period and Burst*interval so nanosecond timestamp
// arithmetic cannot overflow int64.
const maxSpan = math.MaxInt64 / 4

// Limit describes an allowed rate: Rate events per Period, with bursts of up
// to Burst. A zero Burst defaults to Rate.
type Limit struct {
	Rate   int
	Period time.Duration
	Burst  int
}

// PerSecond returns a limit of rate events per second with burst equal to rate.
func PerSecond(rate int) Limit { return Limit{Rate: rate, Period: time.Second} }

// PerMinute returns a limit of rate events per minute with burst equal to rate.
func PerMinute(rate int) Limit { return Limit{Rate: rate, Period: time.Minute} }

// PerHour returns a limit of rate events per hour with burst equal to rate.
func PerHour(rate int) Limit { return Limit{Rate: rate, Period: time.Hour} }

// WithBurst returns a copy of l with the given burst.
func (l Limit) WithBurst(burst int) Limit {
	l.Burst = burst
	return l
}

func (l Limit) normalized() Limit {
	if l.Burst == 0 {
		l.Burst = l.Rate
	}
	return l
}

// interval is the time it takes to replenish one event.
func (l Limit) interval() time.Duration {
	return time.Duration(int64(l.Period) / int64(l.Rate))
}

// Validate reports whether l is usable.
func (l Limit) Validate() error {
	l = l.normalized()
	switch {
	case l.Rate <= 0:
		return fmt.Errorf("%w: rate must be positive, got %d", ErrInvalidLimit, l.Rate)
	case l.Period <= 0:
		return fmt.Errorf("%w: period must be positive, got %v", ErrInvalidLimit, l.Period)
	case l.Burst < 0:
		return fmt.Errorf("%w: burst must not be negative, got %d", ErrInvalidLimit, l.Burst)
	case int64(l.Period) > maxSpan:
		return fmt.Errorf("%w: period %v is too long", ErrInvalidLimit, l.Period)
	}
	iv := int64(l.interval())
	if iv == 0 {
		return fmt.Errorf("%w: %d per %v is finer than 1ns per event", ErrInvalidLimit, l.Rate, l.Period)
	}
	if iv > maxSpan/int64(l.Burst) {
		return fmt.Errorf("%w: burst %d at %v per event is too long", ErrInvalidLimit, l.Burst, time.Duration(iv))
	}
	return nil
}

// String implements fmt.Stringer.
func (l Limit) String() string {
	l = l.normalized()
	return fmt.Sprintf("%d/%v (burst %d)", l.Rate, l.Period, l.Burst)
}

// Result is the outcome of a rate limit decision.
type Result struct {
	// Allowed reports whether the request may proceed.
	Allowed bool
	// Limit is the limiter's capacity: Burst for GCRA, Rate for the sliding window.
	Limit int
	// Remaining is how many more events of cost 1 would be allowed right now.
	Remaining int
	// RetryAfter is how long to wait before the same request would be
	// allowed. It is zero when Allowed is true.
	RetryAfter time.Duration
	// ResetAfter is how long until the key is back to full capacity.
	ResetAfter time.Duration
}

// Limiter makes rate limit decisions for keys.
//
// AllowN reports whether n events for key may happen now, and records them
// if so. A denied request consumes nothing. Implementations must be safe for
// concurrent use.
type Limiter interface {
	AllowN(ctx context.Context, key string, n int) (Result, error)
}
