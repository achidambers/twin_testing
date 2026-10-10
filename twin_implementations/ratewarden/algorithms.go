package ratewarden

import (
	"fmt"
	"math"
	"time"
)

// ---- token bucket (GCRA) ----

// gcra is the Generic Cell Rate Algorithm, an exact token bucket that keeps a
// single timestamp per key: the theoretical arrival time (tat) at which the
// bucket would be full again. One unit costs interval ns; the bucket holds
// burst units, i.e. tau = burst*interval ns of credit.
type gcra struct {
	interval int64
	burst    int
	tau      int64
}

type gcraState struct{ tat int64 }

// NewTokenBucket allows limit units per `per` on average, with up to burst
// units at once. A fresh key starts with a full bucket. Rate precision is one
// nanosecond per unit (per/limit is truncated to whole nanoseconds).
func NewTokenBucket(limit int, per time.Duration, burst int, opts ...Option) (*Limiter, error) {
	if limit < 1 || per <= 0 || burst < 1 {
		return nil, fmt.Errorf("%w: token bucket needs limit >= 1, per > 0, burst >= 1 (got %d, %v, %d)",
			ErrInvalidConfig, limit, per, burst)
	}
	interval := int64(per) / int64(limit)
	if interval < 1 {
		return nil, fmt.Errorf("%w: rate above one unit per nanosecond", ErrInvalidConfig)
	}
	if int64(burst) > math.MaxInt64/4/interval {
		return nil, fmt.Errorf("%w: burst*per/limit overflows", ErrInvalidConfig)
	}
	alg := gcra{interval: interval, burst: burst, tau: int64(burst) * interval}
	return newLimiter[gcraState](alg, opts)
}

func (g gcra) take(st gcraState, now int64, n int) (gcraState, Decision) {
	s := &st
	tat := max(s.tat, now)
	// Units available now: how much of tau is not yet committed.
	avail := func(tat int64) int { return clampInt(int((now-(tat-g.tau))/g.interval), 0, g.burst) }
	d := Decision{Limit: g.burst}
	if n > g.burst {
		d.Remaining, d.RetryAfter, d.ResetAfter = avail(tat), Never, time.Duration(tat-now)
		return st, d
	}
	newTat := tat + int64(n)*g.interval
	allowAt := newTat - g.tau
	if now < allowAt {
		d.Remaining, d.RetryAfter, d.ResetAfter = avail(tat), time.Duration(allowAt-now), time.Duration(tat-now)
		return st, d
	}
	s.tat = newTat
	d.Allowed, d.Remaining, d.ResetAfter = true, avail(newTat), time.Duration(newTat-now)
	return st, d
}

func (g gcra) expired(s gcraState, now int64) bool { return s.tat <= now }

// ---- fixed window ----

type fixedWindow struct {
	limit  int
	window int64
}

type fixedState struct {
	idx   int64 // now / window of the window count belongs to
	count int
}

// NewFixedWindow allows limit units per window, with windows aligned to
// multiples of window since the Unix epoch. Up to 2*limit can pass in a
// short span straddling a window boundary; use NewSlidingWindow or
// NewTokenBucket where that matters.
func NewFixedWindow(limit int, window time.Duration, opts ...Option) (*Limiter, error) {
	if limit < 1 || window <= 0 {
		return nil, fmt.Errorf("%w: fixed window needs limit >= 1, window > 0 (got %d, %v)", ErrInvalidConfig, limit, window)
	}
	return newLimiter[fixedState](fixedWindow{limit: limit, window: int64(window)}, opts)
}

func (f fixedWindow) take(st fixedState, now int64, n int) (fixedState, Decision) {
	s := &st
	if idx := now / f.window; idx > s.idx { // never roll back if time does
		s.idx, s.count = idx, 0
	}
	reset := time.Duration((s.idx+1)*f.window - now)
	d := Decision{Limit: f.limit}
	switch {
	case n > f.limit:
		d.RetryAfter = Never
	case s.count+n > f.limit:
		d.RetryAfter = reset
	default:
		s.count += n
		d.Allowed = true
	}
	d.Remaining = f.limit - s.count
	if s.count > 0 {
		d.ResetAfter = reset
	}
	return st, d
}

func (f fixedWindow) expired(s fixedState, now int64) bool {
	return s.count == 0 || now/f.window > s.idx
}

// ---- sliding window counter ----

type slidingWindow struct {
	limit  int
	window int64
}

type slidingState struct {
	idx       int64 // index of the current aligned window
	cur, prev int   // units taken in window idx and idx-1
}

// NewSlidingWindow allows about limit units in any window-long span. It keeps
// counts for the current and previous aligned windows and estimates usage as
//
//	prev * (fraction of previous window still inside the span) + cur
//
// which assumes the previous window's traffic was evenly spread. No aligned
// window ever admits more than limit.
func NewSlidingWindow(limit int, window time.Duration, opts ...Option) (*Limiter, error) {
	if limit < 1 || window <= 0 {
		return nil, fmt.Errorf("%w: sliding window needs limit >= 1, window > 0 (got %d, %v)", ErrInvalidConfig, limit, window)
	}
	return newLimiter[slidingState](slidingWindow{limit: limit, window: int64(window)}, opts)
}

// eps absorbs float rounding so that waiting exactly RetryAfter succeeds.
const eps = 1e-9

func (w slidingWindow) take(st slidingState, now int64, n int) (slidingState, Decision) {
	s := &st
	switch idx := now / w.window; {
	case idx == s.idx+1:
		s.idx, s.prev, s.cur = idx, s.cur, 0
	case idx > s.idx+1:
		s.idx, s.prev, s.cur = idx, 0, 0
	}
	start := s.idx * w.window
	elapsed := clamp64(now-start, 0, w.window)
	weight := 1 - float64(elapsed)/float64(w.window)
	est := float64(s.prev)*weight + float64(s.cur)

	d := Decision{Limit: w.limit}
	switch {
	case n > w.limit:
		d.RetryAfter = Never
	case est+float64(n) > float64(w.limit)+eps:
		d.RetryAfter = w.retryAfter(s, now, n)
	default:
		s.cur += n
		est += float64(n)
		d.Allowed = true
	}
	d.Remaining = clampInt(int(math.Floor(float64(w.limit)-est+eps)), 0, w.limit)
	switch {
	case s.cur > 0:
		d.ResetAfter = time.Duration(start + 2*w.window - now)
	case s.prev > 0:
		d.ResetAfter = time.Duration(start + w.window - now)
	}
	return st, d
}

// retryAfter solves prev*(1-e/window) + cur + n <= limit for the earliest
// elapsed e, rolling into the next window if the current one cannot fit.
func (w slidingWindow) retryAfter(s *slidingState, now int64, n int) time.Duration {
	start := s.idx * w.window
	var at int64
	if room := w.limit - s.cur - n; room >= 0 {
		// Fits in this window once enough of prev has slid out. prev > 0
		// here, or the request would have been allowed.
		frac := 1 - float64(room)/float64(s.prev)
		at = start + int64(math.Ceil(frac*float64(w.window)))
	} else {
		// Next window: prev' = cur, cur' = 0. cur > 0 since n <= limit.
		next := start + w.window
		frac := math.Max(0, 1-float64(w.limit-n)/float64(s.cur))
		at = next + int64(math.Ceil(frac*float64(w.window)))
	}
	return time.Duration(max(at-now, 1))
}

func (w slidingWindow) expired(s slidingState, now int64) bool {
	idx := now / w.window
	return (s.cur == 0 && s.prev == 0) || idx > s.idx+1 || (idx == s.idx+1 && s.cur == 0)
}

func clampInt(v, lo, hi int) int    { return min(max(v, lo), hi) }
func clamp64(v, lo, hi int64) int64 { return min(max(v, lo), hi) }
