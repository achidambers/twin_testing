package ratelimit_test

import (
	"math/rand"
	"testing"
	"time"

	"github.com/example/ratelimitkit/ratelimit"
)

func TestWindowAllowsRatePerWindow(t *testing.T) {
	lim, _ := newLimiter(t, ratelimit.NewSlidingWindow, ratelimit.PerSecond(10))
	for i := 0; i < 10; i++ {
		if res := allow(t, lim, "k", 1); !res.Allowed || res.Remaining != 9-i {
			t.Fatalf("request %d: %+v", i, res)
		}
	}
	res := allow(t, lim, "k", 1)
	if res.Allowed || res.Remaining != 0 || res.Limit != 10 {
		t.Fatalf("11th request: %+v, want denied", res)
	}
	// All 10 are in the current window, so it takes the rest of this window
	// plus a tenth of the next for the weighted count to drop below 10.
	if want := time.Second + 100*time.Millisecond + time.Nanosecond; res.RetryAfter != want {
		t.Errorf("RetryAfter = %v, want %v", res.RetryAfter, want)
	}
	if res.ResetAfter != 2*time.Second {
		t.Errorf("ResetAfter = %v, want 2s", res.ResetAfter)
	}
}

func TestWindowWeightsPreviousWindow(t *testing.T) {
	lim, clock := newLimiter(t, ratelimit.NewSlidingWindow, ratelimit.PerSecond(10))
	allow(t, lim, "k", 10)
	clock.Advance(1500 * time.Millisecond) // half of the previous window still overlaps
	res := allow(t, lim, "k", 1)
	if !res.Allowed || res.Remaining != 4 {
		t.Fatalf("got %+v, want allowed with 4 remaining (10*0.5 + 1 used)", res)
	}
	allow(t, lim, "k", 4)
	if res := allow(t, lim, "k", 1); res.Allowed {
		t.Fatalf("allowed beyond weighted capacity: %+v", res)
	}
}

func TestWindowForgetsAfterTwoWindows(t *testing.T) {
	lim, clock := newLimiter(t, ratelimit.NewSlidingWindow, ratelimit.PerMinute(5))
	allow(t, lim, "k", 5)
	clock.Advance(2 * time.Minute)
	if res := allow(t, lim, "k", 5); !res.Allowed {
		t.Fatalf("full capacity not restored after two windows: %+v", res)
	}
}

func TestWindowDeniedRequestsConsumeNothing(t *testing.T) {
	lim, clock := newLimiter(t, ratelimit.NewSlidingWindow, ratelimit.PerSecond(3))
	allow(t, lim, "k", 3)
	var last ratelimit.Result
	for i := 0; i < 50; i++ {
		if last = allow(t, lim, "k", 1); last.Allowed {
			t.Fatal("allowed while full")
		}
	}
	clock.Advance(last.RetryAfter)
	if !allow(t, lim, "k", 1).Allowed {
		t.Fatal("denials were counted")
	}
}

// TestWindowInvariants drives random traffic and checks, at every step, that
// no aligned window exceeds Rate and that waiting RetryAfter always suffices.
func TestWindowInvariants(t *testing.T) {
	const rate = 7
	lim, clock := newLimiter(t, ratelimit.NewSlidingWindow, ratelimit.PerSecond(rate))
	rng := rand.New(rand.NewSource(1))
	perWindow := map[int64]int{}
	for i := 0; i < 20_000; i++ {
		clock.Advance(time.Duration(rng.Intn(300)) * time.Millisecond)
		n := 1 + rng.Intn(rate)
		res := allow(t, lim, "k", n)
		if !res.Allowed {
			if res.RetryAfter <= 0 {
				t.Fatalf("step %d: denied with RetryAfter %v", i, res.RetryAfter)
			}
			clock.Advance(res.RetryAfter)
			if res = allow(t, lim, "k", n); !res.Allowed {
				t.Fatalf("step %d: still denied after waiting RetryAfter: %+v", i, res)
			}
		}
		if res.Remaining < 0 || res.Remaining > rate {
			t.Fatalf("step %d: Remaining %d out of range", i, res.Remaining)
		}
		w := clock.Now().Unix()
		if perWindow[w] += n; perWindow[w] > rate {
			t.Fatalf("step %d: %d events in window %d, limit %d", i, perWindow[w], w, rate)
		}
	}
}

func TestWindowClockMovingBackwards(t *testing.T) {
	lim, clock := newLimiter(t, ratelimit.NewSlidingWindow, ratelimit.PerSecond(2))
	allow(t, lim, "k", 2)
	clock.Set(epoch.Add(-10 * time.Second))
	if res := allow(t, lim, "k", 1); res.Allowed {
		t.Fatalf("stepping the clock back reset the window: %+v", res)
	}
}
