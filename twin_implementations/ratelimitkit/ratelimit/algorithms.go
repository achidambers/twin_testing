package ratelimit

import (
	"math"
	"time"
)

// state is the per-key record. All times are Unix nanoseconds.
// GCRA uses tat; the sliding window uses windowStart, prev and curr.
type state struct {
	tat                     int64
	windowStart, prev, curr int64
	// expiresAt is when the state becomes equivalent to a fresh key and can
	// be dropped without changing any future decision.
	expiresAt int64
}

// algorithm is a per-key decision function. Callers must serialize access
// to a given state.
type algorithm interface {
	capacity() int
	take(st *state, now int64, n int) Result
}

// gcra implements the generic cell rate algorithm. It tracks a theoretical
// arrival time (TAT): the time at which the bucket would be full again.
type gcra struct {
	interval    int64 // time to replenish one event
	burstOffset int64 // interval * burst: how far TAT may run ahead of now
	burst       int
}

func newGCRA(l Limit) gcra {
	iv := int64(l.interval())
	return gcra{interval: iv, burstOffset: iv * int64(l.Burst), burst: l.Burst}
}

func (g gcra) capacity() int { return g.burst }

func (g gcra) take(st *state, now int64, n int) Result {
	tat := max(st.tat, now)
	newTAT := tat + g.interval*int64(n)
	allowAt := newTAT - g.burstOffset
	if now < allowAt {
		return Result{
			Limit:      g.burst,
			Remaining:  g.remaining(now, tat),
			RetryAfter: time.Duration(allowAt - now),
			ResetAfter: time.Duration(tat - now),
		}
	}
	st.tat = newTAT
	st.expiresAt = newTAT
	return Result{
		Allowed:    true,
		Limit:      g.burst,
		Remaining:  g.remaining(now, newTAT),
		ResetAfter: time.Duration(newTAT - now),
	}
}

func (g gcra) remaining(now, tat int64) int {
	// If the clock stepped backwards, tat can be more than burstOffset ahead.
	return int(min(max((now+g.burstOffset-tat)/g.interval, 0), int64(g.burst)))
}

// slidingWindow implements the sliding window counter: the previous fixed
// window's count is weighted by how much of it still overlaps the sliding
// window ending now.
type slidingWindow struct {
	window int64
	rate   int
}

func newSlidingWindow(l Limit) slidingWindow {
	return slidingWindow{window: int64(l.Period), rate: l.Rate}
}

func (w slidingWindow) capacity() int { return w.rate }

func (w slidingWindow) take(st *state, now int64, n int) Result {
	start := now - now%w.window
	if now < 0 && now%w.window != 0 {
		start -= w.window
	}
	// If the clock stepped back into an earlier window, keep counting in the
	// latest one rather than forgetting what was recorded.
	if start > st.windowStart {
		if start-st.windowStart == w.window {
			st.prev = st.curr
		} else {
			st.prev = 0
		}
		st.curr = 0
		st.windowStart = start
	}
	elapsed := min(max(now-st.windowStart, 0), w.window-1)

	used := w.used(st, elapsed)
	if used+float64(n) > float64(w.rate) {
		return Result{
			Limit:      w.rate,
			Remaining:  w.remaining(used),
			RetryAfter: w.retryAfter(st, elapsed, n),
			ResetAfter: w.resetAfter(st, elapsed),
		}
	}
	st.curr += int64(n)
	st.expiresAt = st.windowStart + 2*w.window
	return Result{
		Allowed:    true,
		Limit:      w.rate,
		Remaining:  w.remaining(used + float64(n)),
		ResetAfter: w.resetAfter(st, elapsed),
	}
}

func (w slidingWindow) used(st *state, elapsed int64) float64 {
	weight := float64(w.window-elapsed) / float64(w.window)
	return float64(st.prev)*weight + float64(st.curr)
}

func (w slidingWindow) remaining(used float64) int {
	return int(max(math.Floor(float64(w.rate)-used), 0))
}

// retryAfter returns the earliest time at which n more events would fit.
// One extra nanosecond absorbs floating point rounding in used.
func (w slidingWindow) retryAfter(st *state, elapsed int64, n int) time.Duration {
	win := float64(w.window)
	budget := float64(w.rate-n) - float64(st.curr)
	if budget >= 0 && st.prev > 0 {
		// Wait in this window until prev*(win-e)/win <= budget.
		e := int64(math.Ceil(win*(1-budget/float64(st.prev)))) + 1
		if e < w.window {
			return time.Duration(max(e-elapsed, 1))
		}
	}
	// Wait into the next window, where curr becomes prev and decays.
	toNext := w.window - elapsed
	if st.curr == 0 {
		return time.Duration(toNext)
	}
	e := int64(math.Ceil(win*(1-float64(w.rate-n)/float64(st.curr)))) + 1
	return time.Duration(toNext + max(e, 0))
}

func (w slidingWindow) resetAfter(st *state, elapsed int64) time.Duration {
	switch {
	case st.curr > 0:
		return time.Duration(2*w.window - elapsed)
	case st.prev > 0:
		return time.Duration(w.window - elapsed)
	}
	return 0
}
