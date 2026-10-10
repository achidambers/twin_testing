package ratewarden_test

import (
	"errors"
	"fmt"
	"math"
	"runtime"
	"testing"
	"time"

	"ratewarden"
)

func TestConstructorValidation(t *testing.T) {
	cases := []struct {
		name string
		make func() (*ratewarden.Limiter, error)
	}{
		{"tb zero limit", func() (*ratewarden.Limiter, error) { return ratewarden.NewTokenBucket(0, time.Second, 1) }},
		{"tb zero per", func() (*ratewarden.Limiter, error) { return ratewarden.NewTokenBucket(1, 0, 1) }},
		{"tb negative per", func() (*ratewarden.Limiter, error) { return ratewarden.NewTokenBucket(1, -time.Second, 1) }},
		{"tb zero burst", func() (*ratewarden.Limiter, error) { return ratewarden.NewTokenBucket(1, time.Second, 0) }},
		{"tb sub-ns rate", func() (*ratewarden.Limiter, error) { return ratewarden.NewTokenBucket(2, time.Nanosecond, 1) }},
		{"tb overflow", func() (*ratewarden.Limiter, error) { return ratewarden.NewTokenBucket(1, time.Hour, math.MaxInt) }},
		{"fw zero limit", func() (*ratewarden.Limiter, error) { return ratewarden.NewFixedWindow(0, time.Second) }},
		{"fw zero window", func() (*ratewarden.Limiter, error) { return ratewarden.NewFixedWindow(1, 0) }},
		{"sw zero limit", func() (*ratewarden.Limiter, error) { return ratewarden.NewSlidingWindow(0, time.Second) }},
		{"sw zero window", func() (*ratewarden.Limiter, error) { return ratewarden.NewSlidingWindow(1, 0) }},
		{"nil clock", func() (*ratewarden.Limiter, error) {
			return ratewarden.NewFixedWindow(1, time.Second, ratewarden.WithClock(nil))
		}},
		{"zero shards", func() (*ratewarden.Limiter, error) {
			return ratewarden.NewFixedWindow(1, time.Second, ratewarden.WithShards(0))
		}},
		{"too many shards", func() (*ratewarden.Limiter, error) {
			return ratewarden.NewFixedWindow(1, time.Second, ratewarden.WithShards(1<<17))
		}},
		{"negative max keys", func() (*ratewarden.Limiter, error) {
			return ratewarden.NewFixedWindow(1, time.Second, ratewarden.WithMaxKeys(-1))
		}},
		{"negative cleanup", func() (*ratewarden.Limiter, error) {
			return ratewarden.NewFixedWindow(1, time.Second, ratewarden.WithCleanupInterval(-1))
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			l, err := c.make()
			if l != nil {
				l.Close()
			}
			if !errors.Is(err, ratewarden.ErrInvalidConfig) {
				t.Fatalf("err = %v, want ErrInvalidConfig", err)
			}
		})
	}
}

func TestValidEdgeConfigs(t *testing.T) {
	// One unit per nanosecond is the finest rate supported.
	tokenBucket(t, 1, time.Nanosecond, 1)
	// Non-power-of-two shard counts are rounded up, not rejected.
	fixedWindow(t, 1, time.Second, ratewarden.WithShards(3))
	fixedWindow(t, 1, time.Second, ratewarden.WithCleanupInterval(0), ratewarden.WithMaxKeys(0))
}

func TestMaxKeysBoundsMemory(t *testing.T) {
	clk := newClock()
	for name, l := range allAlgorithms(t, 5, ratewarden.WithClock(clk), ratewarden.WithShards(1), ratewarden.WithMaxKeys(3)) {
		t.Run(name, func(t *testing.T) {
			for i := 0; i < 100; i++ {
				if !l.Allow(fmt.Sprint("key-", i)).Allowed {
					t.Fatalf("fresh key %d denied", i)
				}
				if n := l.Len(); n > 3 {
					t.Fatalf("Len = %d after %d keys, want <= 3", n, i+1)
				}
			}
			if l.Len() != 3 {
				t.Fatalf("Len = %d, want 3", l.Len())
			}
		})
	}
}

func TestMaxKeysAcrossShards(t *testing.T) {
	// 10 keys over 4 shards => 3 per shard, at most 12 in total.
	l := fixedWindow(t, 5, time.Minute, ratewarden.WithClock(newClock()), ratewarden.WithShards(4), ratewarden.WithMaxKeys(10))
	for i := 0; i < 10_000; i++ {
		l.Allow(fmt.Sprint(i))
	}
	if n := l.Len(); n > 12 {
		t.Fatalf("Len = %d, want <= 12", n)
	}
}

func TestOnlyConsumingRequestsCreateKeys(t *testing.T) {
	clk := newClock()
	for name, l := range allAlgorithms(t, 5, ratewarden.WithClock(clk)) {
		t.Run(name, func(t *testing.T) {
			for i := 0; i < 50; i++ {
				l.Peek(fmt.Sprint("peek-", i))
				l.AllowN(fmt.Sprint("huge-", i), 6) // exceeds capacity: denied
			}
			if n := l.Len(); n != 0 {
				t.Fatalf("Len = %d, want 0: peeks/denials must not store keys", n)
			}
		})
	}
}

func TestJanitorSweepsOnlyIdleKeys(t *testing.T) {
	clk := newClock()
	ls := allAlgorithms(t, 5, ratewarden.WithClock(clk), ratewarden.WithCleanupInterval(time.Millisecond))
	for _, l := range ls {
		for i := 0; i < 20; i++ {
			l.Allow(fmt.Sprint(i))
		}
	}
	time.Sleep(20 * time.Millisecond) // many sweeps at unchanged time
	for name, l := range ls {
		if n := l.Len(); n != 20 {
			t.Fatalf("%s: janitor removed active keys: Len = %d, want 20", name, n)
		}
	}
	clk.Advance(3 * time.Minute) // every key fully replenished
	for name, l := range ls {
		waitFor(t, func() bool { return l.Len() == 0 }, name+" janitor to sweep idle keys")
	}
}

func TestCloseIsIdempotentAndStopsJanitor(t *testing.T) {
	before := runtime.NumGoroutine()
	var ls []*ratewarden.Limiter
	for i := 0; i < 50; i++ {
		l, err := ratewarden.NewFixedWindow(1, time.Second, ratewarden.WithCleanupInterval(time.Millisecond))
		if err != nil {
			t.Fatal(err)
		}
		ls = append(ls, l)
	}
	if runtime.NumGoroutine() < before+50 {
		t.Fatal("janitors not started")
	}
	for _, l := range ls {
		l.Close()
		l.Close()
	}
	waitFor(t, func() bool { return runtime.NumGoroutine() <= before }, "janitor goroutines to exit")
	if !ls[0].Allow("k").Allowed {
		t.Fatal("limiter unusable after Close")
	}
}

func TestAllowDoesNotAllocate(t *testing.T) {
	for name, l := range allAlgorithms(t, 1_000_000) {
		t.Run(name, func(t *testing.T) {
			l.Allow("k") // create the key first; inserting a key may allocate
			if n := testing.AllocsPerRun(1000, func() { l.Allow("k"); l.Peek("other") }); n != 0 {
				t.Fatalf("%v allocs per Allow+Peek, want 0", n)
			}
		})
	}
}
