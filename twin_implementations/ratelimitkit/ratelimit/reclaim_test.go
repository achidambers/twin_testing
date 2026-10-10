package ratelimit

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

// keyOutsideShard returns a key that hashes to a different shard than other,
// so reclaiming other's slot requires a full sweep rather than a local one.
func keyOutsideShard(l *MemoryLimiter, other string) string {
	for i := 0; ; i++ {
		if k := fmt.Sprint("key-", i); l.shardFor(k) != l.shardFor(other) {
			return k
		}
	}
}

func newReclaimLimiter(t *testing.T, maxKeys int) (*MemoryLimiter, *ManualClock) {
	t.Helper()
	clock := NewManualClock(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
	// One event per millisecond, burst 1: each key expires 1ms after use.
	lim, err := NewGCRA(PerSecond(1000).WithBurst(1),
		WithClock(clock), WithMaxKeys(maxKeys), WithCleanupInterval(0))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { lim.Close() })
	return lim, clock
}

func TestReclaimAcrossShards(t *testing.T) {
	lim, clock := newReclaimLimiter(t, 1)
	ctx := context.Background()
	if _, err := lim.Allow(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Millisecond)
	b := keyOutsideShard(lim, "a")
	if res, err := lim.Allow(ctx, b); err != nil || !res.Allowed {
		t.Fatalf("Allow(%q) = %+v, %v; want room reclaimed from another shard", b, res, err)
	}
	if lim.Len() != 1 {
		t.Fatalf("Len() = %d, want 1", lim.Len())
	}
}

func TestReclaimIsThrottled(t *testing.T) {
	lim, clock := newReclaimLimiter(t, 1)
	ctx := context.Background()
	if _, err := lim.Allow(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	b := keyOutsideShard(lim, "a")

	// "a" is live: the full sweep runs, frees nothing, and starts the window.
	if _, err := lim.Allow(ctx, b); !errors.Is(err, ErrTooManyKeys) {
		t.Fatalf("err = %v, want ErrTooManyKeys", err)
	}

	// "a" has expired, but another full sweep is not allowed yet.
	clock.Advance(reclaimInterval / 2)
	if _, err := lim.Allow(ctx, b); !errors.Is(err, ErrTooManyKeys) {
		t.Fatalf("inside throttle window: err = %v, want ErrTooManyKeys", err)
	}

	clock.Advance(reclaimInterval / 2)
	if res, err := lim.Allow(ctx, b); err != nil || !res.Allowed {
		t.Fatalf("after throttle window: %+v, %v; want allowed", res, err)
	}
}

// TestReclaimRaceWithSameKey has many goroutines race to create the same new
// key at capacity; the key must be created once and the count stay exact.
func TestReclaimRaceWithSameKey(t *testing.T) {
	lim, clock := newReclaimLimiter(t, 1)
	ctx := context.Background()
	if _, err := lim.Allow(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Second)
	b := keyOutsideShard(lim, "a")
	done := make(chan bool)
	for i := 0; i < 50; i++ {
		go func() {
			res, err := lim.Allow(ctx, b)
			done <- err == nil && res.Allowed
		}()
	}
	allowed := 0
	for i := 0; i < 50; i++ {
		if <-done {
			allowed++
		}
	}
	if allowed != 1 || lim.Len() != 1 {
		t.Fatalf("allowed=%d Len=%d, want 1 and 1", allowed, lim.Len())
	}
}
