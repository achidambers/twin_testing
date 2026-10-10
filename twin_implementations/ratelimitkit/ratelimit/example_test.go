package ratelimit_test

import (
	"context"
	"fmt"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/example/ratelimitkit/ratelimit"
)

func ExampleNewGCRA() {
	clock := ratelimit.NewManualClock(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
	lim, err := ratelimit.NewGCRA(ratelimit.PerSecond(2), ratelimit.WithClock(clock))
	if err != nil {
		panic(err)
	}
	defer lim.Close()

	ctx := context.Background()
	for i := 0; i < 3; i++ {
		res, _ := lim.Allow(ctx, "user-42")
		fmt.Println(res.Allowed, res.Remaining, res.RetryAfter)
	}
	clock.Advance(500 * time.Millisecond)
	res, _ := lim.Allow(ctx, "user-42")
	fmt.Println(res.Allowed, res.Remaining, res.RetryAfter)
	// Output:
	// true 1 0s
	// true 0 0s
	// false 0 500ms
	// true 0 0s
}

func benchLimiters(b *testing.B) map[string]*ratelimit.MemoryLimiter {
	b.Helper()
	out := map[string]*ratelimit.MemoryLimiter{}
	for name, ctor := range constructors {
		lim, err := ctor(ratelimit.PerSecond(1_000_000_000))
		if err != nil {
			b.Fatal(err)
		}
		b.Cleanup(func() { lim.Close() })
		out[name] = lim
	}
	return out
}

func BenchmarkAllowSingleKey(b *testing.B) {
	for name, lim := range benchLimiters(b) {
		b.Run(name, func(b *testing.B) {
			ctx := context.Background()
			for i := 0; i < b.N; i++ {
				lim.Allow(ctx, "k")
			}
		})
	}
}

func BenchmarkAllowParallelSharedKey(b *testing.B) {
	for name, lim := range benchLimiters(b) {
		b.Run(name, func(b *testing.B) {
			ctx := context.Background()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					lim.Allow(ctx, "shared")
				}
			})
		})
	}
}

func BenchmarkAllowParallelManyKeys(b *testing.B) {
	keys := make([]string, 10_000)
	for i := range keys {
		keys[i] = "client-" + strconv.Itoa(i)
	}
	for name, lim := range benchLimiters(b) {
		b.Run(name, func(b *testing.B) {
			ctx := context.Background()
			var next atomic.Int64
			b.RunParallel(func(pb *testing.PB) {
				i := int(next.Add(1)) * 7919
				for pb.Next() {
					lim.Allow(ctx, keys[i%len(keys)])
					i++
				}
			})
		})
	}
}
