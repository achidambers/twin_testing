package ratewarden_test

import (
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"ratewarden"
)

func benchLimiters(b *testing.B) map[string]*ratewarden.Limiter {
	return map[string]*ratewarden.Limiter{
		"token-bucket":   tokenBucket(b, 1_000_000, time.Second, 1000),
		"fixed-window":   fixedWindow(b, 1_000_000, time.Second),
		"sliding-window": slidingWindow(b, 1_000_000, time.Second),
	}
}

func BenchmarkAllowSingleKey(b *testing.B) {
	for name, l := range benchLimiters(b) {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				l.Allow("k")
			}
		})
	}
}

func BenchmarkAllowSingleKeyParallel(b *testing.B) {
	for name, l := range benchLimiters(b) {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					l.Allow("hot")
				}
			})
		})
	}
}

func BenchmarkAllowManyKeysParallel(b *testing.B) {
	keys := make([]string, 100_000)
	for i := range keys {
		keys[i] = "user-" + strconv.Itoa(i)
	}
	for name, l := range benchLimiters(b) {
		b.Run(name, func(b *testing.B) {
			var ctr atomic.Uint64
			b.ReportAllocs()
			b.RunParallel(func(pb *testing.PB) {
				i := ctr.Add(1) * 7919
				for pb.Next() {
					l.Allow(keys[i%uint64(len(keys))])
					i++
				}
			})
		})
	}
}
