package ratelimit_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/example/ratelimitkit/ratelimit"
)

func TestSweepRemovesOnlyExpiredKeys(t *testing.T) {
	for name, ctor := range constructors {
		t.Run(name, func(t *testing.T) {
			lim, clock := newLimiter(t, ctor, ratelimit.PerSecond(1))
			allow(t, lim, "old", 1)
			clock.Advance(2500 * time.Millisecond)
			allow(t, lim, "new", 1)
			if n := lim.Sweep(); n != 1 {
				t.Fatalf("Sweep() removed %d, want 1", n)
			}
			if lim.Len() != 1 {
				t.Fatalf("Len() = %d, want 1", lim.Len())
			}
			if res := allow(t, lim, "new", 1); res.Allowed {
				t.Fatalf("sweep dropped live state for key new: %+v", res)
			}
		})
	}
}

// TestSweepNeverChangesDecisions compares a limiter that is swept after every
// call with one that is never swept.
func TestSweepNeverChangesDecisions(t *testing.T) {
	for name, ctor := range constructors {
		t.Run(name, func(t *testing.T) {
			swept, c1 := newLimiter(t, ctor, ratelimit.PerSecond(3))
			kept, c2 := newLimiter(t, ctor, ratelimit.PerSecond(3))
			steps := []time.Duration{0, 0, 0, 0, 300 * time.Millisecond, 700 * time.Millisecond, 999 * time.Millisecond,
				1 * time.Millisecond, 2 * time.Second, 0, 0, 0, 0, 3 * time.Second, 100 * time.Millisecond}
			for i, d := range steps {
				c1.Advance(d)
				c2.Advance(d)
				a, b := allow(t, swept, "k", 1), allow(t, kept, "k", 1)
				if a != b {
					t.Fatalf("step %d: swept %+v != unswept %+v", i, a, b)
				}
				swept.Sweep()
			}
		})
	}
}

func TestMaxKeys(t *testing.T) {
	lim, clock := newLimiter(t, ratelimit.NewGCRA, ratelimit.PerSecond(1), ratelimit.WithMaxKeys(2))
	ctx := context.Background()
	allow(t, lim, "a", 1)
	allow(t, lim, "b", 1)
	if _, err := lim.AllowN(ctx, "c", 1); !errors.Is(err, ratelimit.ErrTooManyKeys) {
		t.Fatalf("third key: err = %v, want ErrTooManyKeys", err)
	}
	if res := allow(t, lim, "a", 1); res.Allowed {
		t.Fatal("existing key should still be tracked (and limited) at capacity")
	}
	clock.Advance(time.Second) // a and b expire and can be reclaimed on demand
	if res := allow(t, lim, "c", 1); !res.Allowed {
		t.Fatalf("new key after expiry: %+v", res)
	}
	if lim.Len() > 2 {
		t.Fatalf("Len() = %d, exceeds max 2", lim.Len())
	}
}

func TestMaxKeysIsExactUnderConcurrency(t *testing.T) {
	const maxKeys = 50
	lim, _ := newLimiter(t, ratelimit.NewGCRA, ratelimit.PerSecond(1), ratelimit.WithMaxKeys(maxKeys))
	var ok, full atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := lim.AllowN(context.Background(), fmt.Sprint("key-", i), 1)
			switch {
			case err == nil:
				ok.Add(1)
			case errors.Is(err, ratelimit.ErrTooManyKeys):
				full.Add(1)
			default:
				t.Error(err)
			}
		}(i)
	}
	wg.Wait()
	if ok.Load() != maxKeys || full.Load() != 1000-maxKeys || lim.Len() != maxKeys {
		t.Fatalf("ok=%d full=%d Len=%d, want %d/%d/%d", ok.Load(), full.Load(), lim.Len(), maxKeys, 1000-maxKeys, maxKeys)
	}
}

func TestReset(t *testing.T) {
	lim, _ := newLimiter(t, ratelimit.NewGCRA, ratelimit.PerSecond(1))
	allow(t, lim, "k", 1)
	lim.Reset("k")
	lim.Reset("missing")
	if lim.Len() != 0 {
		t.Fatalf("Len() = %d after Reset, want 0", lim.Len())
	}
	if !allow(t, lim, "k", 1).Allowed {
		t.Fatal("Reset did not restore capacity")
	}
}

func TestClose(t *testing.T) {
	lim, err := ratelimit.NewGCRA(ratelimit.PerSecond(1), ratelimit.WithCleanupInterval(time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	if err := lim.Close(); err != nil {
		t.Fatal(err)
	}
	if err := lim.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if _, err := lim.Allow(context.Background(), "k"); !errors.Is(err, ratelimit.ErrClosed) {
		t.Fatalf("Allow after Close: %v, want ErrClosed", err)
	}
}

func TestBackgroundCleanup(t *testing.T) {
	clock := ratelimit.NewManualClock(epoch)
	lim, err := ratelimit.NewGCRA(ratelimit.PerSecond(1),
		ratelimit.WithClock(clock), ratelimit.WithCleanupInterval(time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	defer lim.Close()
	allow(t, lim, "k", 1)
	clock.Advance(time.Second)
	deadline := time.Now().Add(2 * time.Second)
	for lim.Len() != 0 {
		if time.Now().After(deadline) {
			t.Fatal("background cleanup never removed the expired key")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestCanceledContext(t *testing.T) {
	lim, _ := newLimiter(t, ratelimit.NewGCRA, ratelimit.PerSecond(1))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := lim.Allow(ctx, "k"); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if lim.Len() != 0 {
		t.Fatal("canceled call created state")
	}
}

// TestConcurrentSingleKey checks that contention cannot let more than the
// capacity through while the clock is frozen.
func TestConcurrentSingleKey(t *testing.T) {
	for name, ctor := range constructors {
		t.Run(name, func(t *testing.T) {
			lim, _ := newLimiter(t, ctor, ratelimit.PerMinute(100))
			var allowed atomic.Int64
			var wg sync.WaitGroup
			for i := 0; i < 1000; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					res, err := lim.Allow(context.Background(), "shared")
					if err != nil {
						t.Error(err)
					}
					if res.Allowed {
						allowed.Add(1)
					}
				}()
			}
			wg.Wait()
			if allowed.Load() != 100 {
				t.Fatalf("allowed %d, want exactly 100", allowed.Load())
			}
		})
	}
}

// TestConcurrentMixedOperations exercises every method at once under -race.
func TestConcurrentMixedOperations(t *testing.T) {
	for name, ctor := range constructors {
		t.Run(name, func(t *testing.T) {
			lim, clock := newLimiter(t, ctor, ratelimit.PerSecond(50), ratelimit.WithMaxKeys(64))
			ctx := context.Background()
			var wg sync.WaitGroup
			for g := 0; g < 16; g++ {
				wg.Add(1)
				go func(g int) {
					defer wg.Done()
					for i := 0; i < 2000; i++ {
						key := fmt.Sprint("k", (g*7+i)%100)
						switch i % 50 {
						case 0:
							lim.Sweep()
						case 1:
							lim.Reset(key)
						case 2:
							clock.Advance(10 * time.Millisecond)
						default:
							_, err := lim.AllowN(ctx, key, 1+i%3)
							if err != nil && !errors.Is(err, ratelimit.ErrTooManyKeys) {
								t.Error(err)
								return
							}
						}
						if n := lim.Len(); n < 0 || n > 64 {
							t.Errorf("Len() = %d out of [0, 64]", n)
							return
						}
					}
				}(g)
			}
			wg.Wait()
		})
	}
}
