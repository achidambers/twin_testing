package ratewarden_test

import (
	"fmt"
	"testing"
	"time"

	"ratewarden"
)

// ---- token bucket ----

func TestTokenBucketBurstThenDeny(t *testing.T) {
	clk := newClock()
	// 10/s => one token per 100ms, bucket of 5.
	l := tokenBucket(t, 10, time.Second, 5, ratewarden.WithClock(clk))
	for i := 0; i < 5; i++ {
		check(t, fmt.Sprintf("req %d", i), l.Allow("k"),
			want{allowed: true, remaining: 4 - i, resetAfter: time.Duration(i+1) * 100 * time.Millisecond})
	}
	check(t, "6th", l.Allow("k"), want{allowed: false, remaining: 0, retryAfter: 100 * time.Millisecond, resetAfter: 500 * time.Millisecond})
	if d := l.Allow("k"); d.Limit != 5 {
		t.Errorf("Limit = %d, want burst 5", d.Limit)
	}
}

func TestTokenBucketRefill(t *testing.T) {
	clk := newClock()
	l := tokenBucket(t, 10, time.Second, 5, ratewarden.WithClock(clk))
	l.AllowN("k", 5)

	clk.Advance(99 * time.Millisecond)
	check(t, "at 99ms", l.Allow("k"), want{allowed: false, remaining: 0, retryAfter: time.Millisecond, resetAfter: skip})
	clk.Advance(time.Millisecond)
	check(t, "at 100ms", l.Allow("k"), want{allowed: true, remaining: 0, retryAfter: 0, resetAfter: 500 * time.Millisecond})

	clk.Advance(250 * time.Millisecond) // 2.5 tokens back
	check(t, "peek at 350ms", l.Peek("k"), want{allowed: true, remaining: 2, retryAfter: 0, resetAfter: 250 * time.Millisecond})
	check(t, "take 3 of 2.5", l.AllowN("k", 3), want{allowed: false, remaining: 2, retryAfter: 50 * time.Millisecond, resetAfter: skip})
	check(t, "take 2", l.AllowN("k", 2), want{allowed: true, remaining: 0, retryAfter: 0, resetAfter: skip})
}

func TestTokenBucketSteadyRateNeverDenied(t *testing.T) {
	clk := newClock()
	l := tokenBucket(t, 100, time.Second, 1, ratewarden.WithClock(clk))
	for i := 0; i < 10_000; i++ {
		if d := l.Allow("k"); !d.Allowed {
			t.Fatalf("request %d at the exact rate was denied: %+v", i, d)
		}
		clk.Advance(10 * time.Millisecond)
	}
}

func TestTokenBucketCapsAtBurst(t *testing.T) {
	clk := newClock()
	l := tokenBucket(t, 10, time.Second, 5, ratewarden.WithClock(clk))
	l.Allow("k")
	clk.Advance(24 * time.Hour)
	check(t, "after idle", l.Peek("k"), want{allowed: true, remaining: 5, retryAfter: 0, resetAfter: 0})
	if d := l.AllowN("k", 5); !d.Allowed {
		t.Fatal("full burst denied after idle")
	}
	if d := l.Allow("k"); d.Allowed {
		t.Fatal("idle time accumulated beyond burst")
	}
}

func TestTokenBucketCostAboveBurstIsNever(t *testing.T) {
	clk := newClock()
	l := tokenBucket(t, 10, time.Second, 5, ratewarden.WithClock(clk))
	check(t, "n=6", l.AllowN("k", 6), want{allowed: false, remaining: 5, retryAfter: ratewarden.Never, resetAfter: 0})
	check(t, "state untouched", l.AllowN("k", 5), want{allowed: true, remaining: 0, retryAfter: 0, resetAfter: skip})
}

func TestTokenBucketDenialConsumesNothing(t *testing.T) {
	clk := newClock()
	l := tokenBucket(t, 1, time.Second, 3, ratewarden.WithClock(clk))
	l.AllowN("k", 2)
	for i := 0; i < 100; i++ {
		if l.AllowN("k", 2).Allowed {
			t.Fatal("allowed 2 with 1 token")
		}
	}
	if !l.Allow("k").Allowed {
		t.Fatal("denials consumed the remaining token")
	}
}

func TestTokenBucketUnevenRate(t *testing.T) {
	// 3 per second: interval is 333333333ns, truncated from 1s/3.
	clk := newClock()
	l := tokenBucket(t, 3, time.Second, 1, ratewarden.WithClock(clk))
	l.Allow("k")
	d := l.Allow("k")
	if d.RetryAfter != 333333333 {
		t.Fatalf("RetryAfter = %d, want 333333333", d.RetryAfter)
	}
	clk.Advance(d.RetryAfter)
	if !l.Allow("k").Allowed {
		t.Fatal("denied after waiting RetryAfter")
	}
}

// ---- fixed window ----

func TestFixedWindowBasic(t *testing.T) {
	clk := newClock()
	clk.Advance(10 * time.Second) // 50s left in the first minute
	l := fixedWindow(t, 3, time.Minute, ratewarden.WithClock(clk))
	check(t, "peek fresh", l.Peek("k"), want{allowed: true, remaining: 3, retryAfter: 0, resetAfter: 0})
	for i := 0; i < 3; i++ {
		check(t, fmt.Sprintf("req %d", i), l.Allow("k"), want{allowed: true, remaining: 2 - i, retryAfter: 0, resetAfter: 50 * time.Second})
	}
	check(t, "4th", l.Allow("k"), want{allowed: false, remaining: 0, retryAfter: 50 * time.Second, resetAfter: 50 * time.Second})

	clk.Advance(50*time.Second - 1)
	check(t, "1ns before boundary", l.Allow("k"), want{allowed: false, remaining: 0, retryAfter: 1, resetAfter: skip})
	clk.Advance(1)
	check(t, "at boundary", l.Allow("k"), want{allowed: true, remaining: 2, retryAfter: 0, resetAfter: time.Minute})
}

func TestFixedWindowBoundaryBurst(t *testing.T) {
	// Documented weakness: 2x limit can pass across a boundary.
	clk := newClock()
	l := fixedWindow(t, 5, time.Minute, ratewarden.WithClock(clk))
	clk.Advance(time.Minute - time.Millisecond)
	if !l.AllowN("k", 5).Allowed {
		t.Fatal("first window denied")
	}
	clk.Advance(time.Millisecond)
	if !l.AllowN("k", 5).Allowed {
		t.Fatal("second window denied")
	}
}

func TestFixedWindowPartialAndNever(t *testing.T) {
	clk := newClock()
	l := fixedWindow(t, 5, time.Minute, ratewarden.WithClock(clk))
	l.AllowN("k", 3)
	check(t, "3 of 2", l.AllowN("k", 3), want{allowed: false, remaining: 2, retryAfter: time.Minute, resetAfter: skip})
	check(t, "6 > limit", l.AllowN("k", 6), want{allowed: false, remaining: 2, retryAfter: ratewarden.Never, resetAfter: skip})
	check(t, "2 of 2", l.AllowN("k", 2), want{allowed: true, remaining: 0, retryAfter: 0, resetAfter: skip})
}

func TestFixedWindowClockGoingBackwards(t *testing.T) {
	clk := newClock()
	clk.Advance(2 * time.Minute)
	l := fixedWindow(t, 2, time.Minute, ratewarden.WithClock(clk))
	l.AllowN("k", 2)
	clk.Set(epoch.Add(time.Minute + 30*time.Second)) // previous window
	if l.Allow("k").Allowed {
		t.Fatal("clock going backwards reset the window")
	}
}

// ---- sliding window ----

func TestSlidingWindowWeightsPreviousWindow(t *testing.T) {
	clk := newClock()
	l := slidingWindow(t, 10, time.Minute, ratewarden.WithClock(clk))
	for i := 0; i < 10; i++ {
		if !l.Allow("k").Allowed {
			t.Fatalf("req %d denied", i)
		}
	}
	// Full window: next window must slide 10% out (6s) before one fits.
	check(t, "11th", l.Allow("k"), want{allowed: false, remaining: 0, retryAfter: 66 * time.Second, resetAfter: 2 * time.Minute})

	// Half way through the next window, prev contributes 10*0.5 = 5.
	clk.Advance(90 * time.Second)
	check(t, "peek mid window", l.Peek("k"), want{allowed: true, remaining: 5, retryAfter: 0, resetAfter: 30 * time.Second})
	for i := 0; i < 5; i++ {
		if !l.Allow("k").Allowed {
			t.Fatalf("mid-window req %d denied", i)
		}
	}
	d := l.Allow("k")
	check(t, "6th mid window", d, want{allowed: false, remaining: 0, retryAfter: skip, resetAfter: 90 * time.Second})
	// est = 10*(1-e/60) + 5 <= 9  => e >= 36s, 6s from now.
	if d.RetryAfter != 6*time.Second {
		t.Errorf("RetryAfter = %v, want 6s", d.RetryAfter)
	}
}

func TestSlidingWindowNoBoundaryBurst(t *testing.T) {
	clk := newClock()
	l := slidingWindow(t, 5, time.Minute, ratewarden.WithClock(clk))
	clk.Advance(time.Minute - time.Millisecond)
	if !l.AllowN("k", 5).Allowed {
		t.Fatal("first batch denied")
	}
	clk.Advance(time.Millisecond)
	if l.Allow("k").Allowed {
		t.Fatal("sliding window allowed a burst across the boundary")
	}
}

func TestSlidingWindowForgetsAfterTwoWindows(t *testing.T) {
	clk := newClock()
	l := slidingWindow(t, 5, time.Minute, ratewarden.WithClock(clk))
	l.AllowN("k", 5)
	clk.Advance(2 * time.Minute)
	check(t, "after 2 windows", l.Peek("k"), want{allowed: true, remaining: 5, retryAfter: 0, resetAfter: 0})
}

func TestSlidingWindowNever(t *testing.T) {
	clk := newClock()
	l := slidingWindow(t, 5, time.Minute, ratewarden.WithClock(clk))
	check(t, "6 > 5", l.AllowN("k", 6), want{allowed: false, remaining: 5, retryAfter: ratewarden.Never, resetAfter: 0})
}

// ---- shared behaviour ----

func allAlgorithms(t *testing.T, limit int, opts ...ratewarden.Option) map[string]*ratewarden.Limiter {
	return map[string]*ratewarden.Limiter{
		"token-bucket":   tokenBucket(t, limit, time.Minute, limit, opts...),
		"fixed-window":   fixedWindow(t, limit, time.Minute, opts...),
		"sliding-window": slidingWindow(t, limit, time.Minute, opts...),
	}
}

func TestKeysAreIndependent(t *testing.T) {
	for name, l := range allAlgorithms(t, 2, ratewarden.WithClock(newClock())) {
		t.Run(name, func(t *testing.T) {
			l.AllowN("a", 2)
			if l.Allow("a").Allowed {
				t.Fatal("a over limit")
			}
			if !l.AllowN("b", 2).Allowed {
				t.Fatal("b affected by a")
			}
		})
	}
}

func TestPeekAndNegativeNDoNotConsume(t *testing.T) {
	for name, l := range allAlgorithms(t, 3, ratewarden.WithClock(newClock())) {
		t.Run(name, func(t *testing.T) {
			for i := 0; i < 10; i++ {
				l.Peek("k")
				l.AllowN("k", -5)
			}
			if d := l.AllowN("k", 3); !d.Allowed {
				t.Fatalf("peek consumed capacity: %+v", d)
			}
		})
	}
}

func TestReset(t *testing.T) {
	for name, l := range allAlgorithms(t, 1, ratewarden.WithClock(newClock())) {
		t.Run(name, func(t *testing.T) {
			l.Allow("k")
			l.Reset("k")
			if !l.Allow("k").Allowed {
				t.Fatal("Reset did not restore capacity")
			}
			l.Reset("missing") // no-op
		})
	}
}
