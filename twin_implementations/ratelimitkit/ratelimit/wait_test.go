package ratelimit_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/example/ratelimitkit/ratelimit"
)

// waitAsync runs Wait in a goroutine and returns its result channel once the
// call is blocked on the clock.
func waitAsync(t *testing.T, ctx context.Context, lim *ratelimit.MemoryLimiter, clock *ratelimit.ManualClock, n int) <-chan error {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- lim.Wait(ctx, "k", n) }()
	deadline := time.Now().Add(2 * time.Second)
	for clock.PendingTimers() == 0 {
		select {
		case err := <-done:
			t.Fatalf("Wait returned %v before blocking", err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("Wait never blocked on the clock")
		}
		time.Sleep(time.Millisecond)
	}
	return done
}

func expectDone(t *testing.T, done <-chan error, want error) {
	t.Helper()
	select {
	case err := <-done:
		if !errors.Is(err, want) {
			t.Fatalf("Wait returned %v, want %v", err, want)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Wait did not return")
	}
}

func TestWaitReturnsImmediatelyWhenAllowed(t *testing.T) {
	lim, _ := newLimiter(t, ratelimit.NewGCRA, ratelimit.PerSecond(2))
	if err := lim.Wait(context.Background(), "k", 2); err != nil {
		t.Fatal(err)
	}
}

func TestWaitBlocksUntilAllowed(t *testing.T) {
	for name, ctor := range constructors {
		t.Run(name, func(t *testing.T) {
			lim, clock := newLimiter(t, ctor, ratelimit.PerSecond(1))
			allow(t, lim, "k", 1)
			done := waitAsync(t, context.Background(), lim, clock, 1)
			clock.Advance(10 * time.Millisecond)
			select {
			case err := <-done:
				t.Fatalf("Wait returned %v too early", err)
			case <-time.After(20 * time.Millisecond):
			}
			clock.Advance(2 * time.Second)
			expectDone(t, done, nil)
			if res := allow(t, lim, "k", 1); res.Allowed {
				t.Fatalf("Wait did not consume its event: %+v", res)
			}
		})
	}
}

func TestWaitCanceled(t *testing.T) {
	lim, clock := newLimiter(t, ratelimit.NewGCRA, ratelimit.PerSecond(1))
	allow(t, lim, "k", 1)
	ctx, cancel := context.WithCancel(context.Background())
	done := waitAsync(t, ctx, lim, clock, 1)
	cancel()
	expectDone(t, done, context.Canceled)
}

func TestWaitDeadlineTooShort(t *testing.T) {
	lim, _ := newLimiter(t, ratelimit.NewGCRA, ratelimit.PerMinute(1))
	allow(t, lim, "k", 1)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	start := time.Now()
	if err := lim.Wait(ctx, "k", 1); !errors.Is(err, ratelimit.ErrWouldExceedDeadline) {
		t.Fatalf("err = %v, want ErrWouldExceedDeadline", err)
	}
	if time.Since(start) > 500*time.Millisecond {
		t.Fatal("Wait slept before reporting the deadline problem")
	}
}

func TestWaitUnblocksOnClose(t *testing.T) {
	lim, clock := newLimiter(t, ratelimit.NewGCRA, ratelimit.PerSecond(1))
	allow(t, lim, "k", 1)
	done := waitAsync(t, context.Background(), lim, clock, 1)
	lim.Close()
	expectDone(t, done, ratelimit.ErrClosed)
}

func TestWaitPropagatesErrors(t *testing.T) {
	lim, _ := newLimiter(t, ratelimit.NewGCRA, ratelimit.PerSecond(1))
	if err := lim.Wait(context.Background(), "k", 2); !errors.Is(err, ratelimit.ErrCostExceedsLimit) {
		t.Fatalf("err = %v, want ErrCostExceedsLimit", err)
	}
}

func TestManualClock(t *testing.T) {
	c := ratelimit.NewManualClock(epoch)
	if !c.Now().Equal(epoch) {
		t.Fatalf("Now() = %v", c.Now())
	}
	select {
	case <-c.After(0):
	default:
		t.Fatal("After(0) did not fire immediately")
	}
	late, early := c.After(2*time.Second), c.After(time.Second)
	c.Advance(time.Second)
	select {
	case <-early:
	default:
		t.Fatal("1s timer did not fire at 1s")
	}
	select {
	case <-late:
		t.Fatal("2s timer fired at 1s")
	default:
	}
	if c.PendingTimers() != 1 {
		t.Fatalf("PendingTimers() = %d, want 1", c.PendingTimers())
	}
	c.Set(epoch) // moving backwards fires nothing
	if c.PendingTimers() != 1 {
		t.Fatal("Set backwards fired a timer")
	}
	c.Advance(3 * time.Second)
	<-late
}

func TestSystemClockIsMonotonic(t *testing.T) {
	c := ratelimit.SystemClock()
	prev := c.Now()
	for i := 0; i < 10_000; i++ {
		now := c.Now()
		if now.Before(prev) {
			t.Fatalf("clock went backwards: %v then %v", prev, now)
		}
		prev = now
	}
}
