package ratelimit_test

import (
	"testing"
	"time"

	"github.com/example/ratelimitkit/ratelimit"
)

func TestGCRABurstThenDeny(t *testing.T) {
	lim, _ := newLimiter(t, ratelimit.NewGCRA, ratelimit.PerSecond(10))
	for i := 0; i < 10; i++ {
		res := allow(t, lim, "k", 1)
		if !res.Allowed || res.Remaining != 9-i || res.RetryAfter != 0 {
			t.Fatalf("request %d: %+v, want allowed with %d remaining", i, res, 9-i)
		}
	}
	res := allow(t, lim, "k", 1)
	if res.Allowed || res.Remaining != 0 || res.RetryAfter != 100*time.Millisecond {
		t.Fatalf("11th request: %+v, want denied, retry after 100ms", res)
	}
	if res.ResetAfter != time.Second {
		t.Errorf("ResetAfter = %v, want 1s", res.ResetAfter)
	}
}

func TestGCRARetryAfterIsExact(t *testing.T) {
	lim, clock := newLimiter(t, ratelimit.NewGCRA, ratelimit.PerMinute(3).WithBurst(2))
	allow(t, lim, "k", 2)
	res := allow(t, lim, "k", 2)
	if res.Allowed || res.RetryAfter != 40*time.Second {
		t.Fatalf("got %+v, want denied with RetryAfter 40s", res)
	}
	clock.Advance(res.RetryAfter - time.Nanosecond)
	if res := allow(t, lim, "k", 2); res.Allowed {
		t.Fatalf("allowed 1ns before RetryAfter: %+v", res)
	}
	clock.Advance(time.Nanosecond)
	if res := allow(t, lim, "k", 2); !res.Allowed {
		t.Fatalf("denied at RetryAfter: %+v", res)
	}
}

func TestGCRARefillsGradually(t *testing.T) {
	lim, clock := newLimiter(t, ratelimit.NewGCRA, ratelimit.PerSecond(4))
	allow(t, lim, "k", 4)
	clock.Advance(250 * time.Millisecond)
	if res := allow(t, lim, "k", 1); !res.Allowed || res.Remaining != 0 {
		t.Fatalf("after one interval: %+v, want allowed with 0 remaining", res)
	}
	clock.Advance(600 * time.Millisecond) // 2.4 intervals: two whole tokens
	res := allow(t, lim, "k", 3)
	if res.Allowed || res.Remaining != 2 {
		t.Fatalf("cost 3 with 2 tokens: %+v, want denied with 2 remaining", res)
	}
	if res.RetryAfter != 150*time.Millisecond {
		t.Errorf("RetryAfter = %v, want 150ms", res.RetryAfter)
	}
	if res := allow(t, lim, "k", 2); !res.Allowed || res.Remaining != 0 {
		t.Fatalf("cost 2 with 2 tokens: %+v", res)
	}
}

func TestGCRADoesNotRefillBeyondBurst(t *testing.T) {
	lim, clock := newLimiter(t, ratelimit.NewGCRA, ratelimit.PerSecond(10).WithBurst(3))
	allow(t, lim, "k", 1)
	clock.Advance(time.Hour)
	for i := 0; i < 3; i++ {
		if !allow(t, lim, "k", 1).Allowed {
			t.Fatalf("request %d denied after idle period", i)
		}
	}
	if allow(t, lim, "k", 1).Allowed {
		t.Fatal("idle time accumulated more than Burst tokens")
	}
}

func TestGCRADeniedRequestsConsumeNothing(t *testing.T) {
	lim, clock := newLimiter(t, ratelimit.NewGCRA, ratelimit.PerSecond(1))
	allow(t, lim, "k", 1)
	var last ratelimit.Result
	for i := 0; i < 100; i++ {
		last = allow(t, lim, "k", 1)
		if last.Allowed {
			t.Fatal("allowed while empty")
		}
	}
	clock.Advance(last.RetryAfter)
	if !allow(t, lim, "k", 1).Allowed {
		t.Fatal("repeated denials pushed back the refill")
	}
}

func TestGCRASustainedRate(t *testing.T) {
	// 10/s, no burst beyond one, polled every millisecond for 10s.
	lim, clock := newLimiter(t, ratelimit.NewGCRA, ratelimit.PerSecond(10).WithBurst(1))
	allowed := 0
	for i := 0; i < 10_000; i++ {
		if allow(t, lim, "k", 1).Allowed {
			allowed++
		}
		clock.Advance(time.Millisecond)
	}
	if allowed != 100 {
		t.Fatalf("allowed %d in 10s, want exactly 100", allowed)
	}
}

func TestGCRAKeysAreIndependent(t *testing.T) {
	lim, _ := newLimiter(t, ratelimit.NewGCRA, ratelimit.PerSecond(1))
	if !allow(t, lim, "a", 1).Allowed || allow(t, lim, "a", 1).Allowed {
		t.Fatal("key a: want one allowed then denied")
	}
	if !allow(t, lim, "b", 1).Allowed {
		t.Fatal("key b was limited by key a")
	}
	if lim.Len() != 2 {
		t.Fatalf("Len() = %d, want 2", lim.Len())
	}
}

func TestGCRAClockMovingBackwards(t *testing.T) {
	lim, clock := newLimiter(t, ratelimit.NewGCRA, ratelimit.PerSecond(2))
	allow(t, lim, "k", 2)
	clock.Set(epoch.Add(-time.Hour))
	res := allow(t, lim, "k", 1)
	if res.Allowed || res.Remaining != 0 || res.RetryAfter <= 0 {
		t.Fatalf("after clock stepped back: %+v, want denied with no free tokens", res)
	}
	clock.Set(epoch.Add(time.Second))
	if res := allow(t, lim, "k", 2); !res.Allowed {
		t.Fatalf("did not recover once the clock caught up: %+v", res)
	}
}
