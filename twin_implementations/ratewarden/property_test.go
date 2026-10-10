package ratewarden_test

import (
	"fmt"
	"math/rand/v2"
	"testing"
	"time"

	"ratewarden"
	"ratewarden/ratewardentest"
)

type event struct {
	at int64 // UnixNano
	n  int
}

// walkParams shapes a random walk over one key.
type walkParams struct {
	steps   int
	maxN    int           // request sizes drawn from [1, maxN]
	maxStep time.Duration // clock advance drawn from [0, maxStep) between requests
	tight   bool          // RetryAfter must be exact: still denied 1ns earlier
}

// walk drives l with random requests and clock advances, checks per-decision
// invariants, and returns every allowed request. On some denials it waits
// exactly RetryAfter and asserts the same request is then allowed.
func walk(t *testing.T, l *ratewarden.Limiter, clk *ratewardentest.Clock, rng *rand.Rand, p walkParams) []event {
	t.Helper()
	var allowed []event
	for i := 0; i < p.steps; i++ {
		n := 1 + rng.IntN(p.maxN)
		d := l.AllowN("k", n)
		checkDecision(t, i, n, d)
		if d.Allowed {
			allowed = append(allowed, event{clk.Now().UnixNano(), n})
		} else if d.RetryAfter > 0 && rng.IntN(3) == 0 {
			if p.tight && d.RetryAfter > 1 {
				clk.Advance(d.RetryAfter - 1)
				if l.AllowN("k", n).Allowed {
					t.Fatalf("step %d: allowed 1ns before RetryAfter %v", i, d.RetryAfter)
				}
				clk.Advance(1)
			} else {
				clk.Advance(d.RetryAfter)
			}
			d2 := l.AllowN("k", n)
			if !d2.Allowed {
				t.Fatalf("step %d: n=%d still denied after waiting RetryAfter %v: %+v", i, n, d.RetryAfter, d2)
			}
			allowed = append(allowed, event{clk.Now().UnixNano(), n})
		}
		clk.Advance(time.Duration(rng.Int64N(int64(p.maxStep))))
	}
	return allowed
}

func checkDecision(t *testing.T, step, n int, d ratewarden.Decision) {
	t.Helper()
	switch {
	case d.Remaining < 0 || d.Remaining > d.Limit:
		t.Fatalf("step %d: Remaining %d outside [0, %d]", step, d.Remaining, d.Limit)
	case d.ResetAfter < 0:
		t.Fatalf("step %d: negative ResetAfter %v", step, d.ResetAfter)
	case d.Allowed && d.RetryAfter != 0:
		t.Fatalf("step %d: allowed with RetryAfter %v", step, d.RetryAfter)
	case !d.Allowed && d.RetryAfter <= 0 && d.RetryAfter != ratewarden.Never:
		t.Fatalf("step %d: denied with RetryAfter %v", step, d.RetryAfter)
	case !d.Allowed && (d.RetryAfter == ratewarden.Never) != (n > d.Limit):
		t.Fatalf("step %d: Never=%v but n=%d limit=%d", step, d.RetryAfter == ratewarden.Never, n, d.Limit)
	case !d.Allowed && d.Remaining >= n:
		t.Fatalf("step %d: denied n=%d with %d remaining", step, n, d.Remaining)
	}
}

func forSeeds(t *testing.T, fn func(t *testing.T, rng *rand.Rand)) {
	seeds := 30
	if testing.Short() {
		seeds = 5
	}
	for s := 0; s < seeds; s++ {
		t.Run(fmt.Sprint("seed", s), func(t *testing.T) {
			fn(t, rand.New(rand.NewPCG(uint64(s), 0x5eed)))
		})
	}
}

func TestPropertyTokenBucket(t *testing.T) {
	forSeeds(t, func(t *testing.T, rng *rand.Rand) {
		burst := 1 + rng.IntN(20)
		interval := time.Duration(1+rng.IntN(1000)) * time.Millisecond
		clk := newClock()
		l := tokenBucket(t, 1, interval, burst, ratewarden.WithClock(clk))
		evs := walk(t, l, clk, rng, walkParams{steps: 600, maxN: burst + 1, maxStep: interval * 2, tight: true})
		// Over any span [t_i, t_j], at most burst + span/interval units.
		for i := range evs {
			sum := 0
			for j := i; j < len(evs); j++ {
				sum += evs[j].n
				if bound := burst + int((evs[j].at-evs[i].at)/int64(interval)); sum > bound {
					t.Fatalf("events %d..%d took %d units, bound %d", i, j, sum, bound)
				}
			}
		}
	})
}

func TestPropertyFixedWindow(t *testing.T) {
	forSeeds(t, func(t *testing.T, rng *rand.Rand) {
		limit := 1 + rng.IntN(20)
		window := time.Duration(1+rng.IntN(1000)) * time.Millisecond
		clk := newClock()
		l := fixedWindow(t, limit, window, ratewarden.WithClock(clk))
		evs := walk(t, l, clk, rng, walkParams{steps: 600, maxN: limit + 1, maxStep: window / 3, tight: true})
		for idx, sum := range sumByWindow(evs, window) {
			if sum > limit {
				t.Fatalf("window %d admitted %d, limit %d", idx, sum, limit)
			}
		}
	})
}

func TestPropertySlidingWindow(t *testing.T) {
	forSeeds(t, func(t *testing.T, rng *rand.Rand) {
		limit := 1 + rng.IntN(20)
		window := time.Duration(1+rng.IntN(1000)) * time.Millisecond
		w := int64(window)
		clk := newClock()
		l := slidingWindow(t, limit, window, ratewarden.WithClock(clk))
		evs := walk(t, l, clk, rng, walkParams{steps: 600, maxN: limit + 1, maxStep: window / 3})

		// Independently recompute the estimate at every allowed event
		// from the event log alone.
		byWin := map[int64]int{}
		for _, e := range evs {
			idx := e.at / w
			byWin[idx] += e.n
			weight := 1 - float64(e.at-idx*w)/float64(w)
			est := float64(byWin[idx-1])*weight + float64(byWin[idx])
			if est > float64(limit)+1e-6 {
				t.Fatalf("at %d estimate %.4f exceeds limit %d", e.at, est, limit)
			}
		}
		for idx, sum := range sumByWindow(evs, window) {
			if sum > limit {
				t.Fatalf("aligned window %d admitted %d, limit %d", idx, sum, limit)
			}
		}
	})
}

func sumByWindow(evs []event, window time.Duration) map[int64]int {
	out := map[int64]int{}
	for _, e := range evs {
		out[e.at/int64(window)] += e.n
	}
	return out
}

// Under saturating demand, a limiter should admit close to its nominal rate:
// not just safe, but not needlessly stingy either.
func TestPropertyThroughputUnderSaturation(t *testing.T) {
	const window = time.Second
	const limit = 100
	cases := map[string]func(*ratewardentest.Clock) *ratewarden.Limiter{
		"token-bucket": func(c *ratewardentest.Clock) *ratewarden.Limiter {
			return tokenBucket(t, limit, window, limit, ratewarden.WithClock(c))
		},
		"fixed-window": func(c *ratewardentest.Clock) *ratewarden.Limiter {
			return fixedWindow(t, limit, window, ratewarden.WithClock(c))
		},
		"sliding-window": func(c *ratewardentest.Clock) *ratewarden.Limiter {
			return slidingWindow(t, limit, window, ratewarden.WithClock(c))
		},
	}
	for name, mk := range cases {
		t.Run(name, func(t *testing.T) {
			clk := newClock()
			l := mk(clk)
			admitted := 0
			const seconds = 60
			for i := 0; i < seconds*1000; i++ { // one request per ms, 10x the rate
				if l.Allow("k").Allowed {
					admitted++
				}
				clk.Advance(time.Millisecond)
			}
			rate := float64(admitted) / seconds
			if rate < 0.95*limit || rate > 1.05*limit+float64(limit)/seconds {
				t.Fatalf("admitted %.1f/s under saturation, want ~%d/s", rate, limit)
			}
		})
	}
}
