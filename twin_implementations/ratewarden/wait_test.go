package ratewarden_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"ratewarden"
)

func TestWaitImmediate(t *testing.T) {
	l := tokenBucket(t, 1, time.Hour, 1, ratewarden.WithClock(newClock()))
	if err := l.Wait(context.Background(), "k"); err != nil {
		t.Fatal(err)
	}
}

func TestWaitBlocksUntilCapacity(t *testing.T) {
	clk := newClock()
	l := tokenBucket(t, 1, time.Second, 1, ratewarden.WithClock(clk))
	l.Allow("k")

	done := make(chan error, 1)
	go func() { done <- l.Wait(context.Background(), "k") }()
	waitFor(t, func() bool { return clk.Waiters() == 1 }, "Wait to block")
	select {
	case err := <-done:
		t.Fatalf("Wait returned %v before time advanced", err)
	default:
	}
	clk.Advance(time.Second)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if l.Allow("k").Allowed {
		t.Fatal("Wait did not consume the token")
	}
}

func TestWaitRetriesWhenOvertaken(t *testing.T) {
	clk := newClock()
	l := tokenBucket(t, 1, time.Second, 1, ratewarden.WithClock(clk))
	l.Allow("k")
	done := make(chan error, 1)
	go func() { done <- l.Wait(context.Background(), "k") }()
	waitFor(t, func() bool { return clk.Waiters() == 1 }, "Wait to block")

	// Wake the waiter and race it for the token. If we win, it must go
	// back to waiting rather than return early or error.
	clk.Advance(time.Second)
	if l.Allow("k").Allowed {
		waitFor(t, func() bool { return clk.Waiters() == 1 }, "Wait to block again")
		clk.Advance(time.Second)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestWaitContextCancelled(t *testing.T) {
	clk := newClock()
	l := tokenBucket(t, 1, time.Hour, 1, ratewarden.WithClock(clk))
	l.Allow("k")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- l.Wait(ctx, "k") }()
	waitFor(t, func() bool { return clk.Waiters() == 1 }, "Wait to block")
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}

func TestWaitAlreadyCancelled(t *testing.T) {
	l := tokenBucket(t, 1, time.Hour, 1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := l.Wait(ctx, "k"); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if l.Peek("k").Remaining != 1 {
		t.Fatal("cancelled Wait consumed a token")
	}
}

func TestWaitDeadlineTooShortFailsFast(t *testing.T) {
	l := tokenBucket(t, 1, time.Hour, 1)
	l.Allow("k")
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	start := time.Now()
	if err := l.Wait(ctx, "k"); !errors.Is(err, ratewarden.ErrDeadlineTooShort) {
		t.Fatalf("err = %v, want ErrDeadlineTooShort", err)
	}
	if time.Since(start) > time.Second {
		t.Fatal("did not fail fast")
	}
}

func TestWaitExceedsLimit(t *testing.T) {
	l := fixedWindow(t, 3, time.Second)
	if err := l.WaitN(context.Background(), "k", 4); !errors.Is(err, ratewarden.ErrExceedsLimit) {
		t.Fatalf("err = %v, want ErrExceedsLimit", err)
	}
}

func TestWaitRealClockPacing(t *testing.T) {
	if testing.Short() {
		t.Skip("uses real time")
	}
	l := tokenBucket(t, 1, 20*time.Millisecond, 1)
	start := time.Now()
	for i := 0; i < 5; i++ {
		if err := l.Wait(context.Background(), "k"); err != nil {
			t.Fatal(err)
		}
	}
	// First is immediate, then 4 intervals of 20ms.
	if el := time.Since(start); el < 75*time.Millisecond || el > 2*time.Second {
		t.Fatalf("5 waits took %v, want ~80ms", el)
	}
}
