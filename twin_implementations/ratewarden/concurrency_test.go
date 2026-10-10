package ratewarden_test

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"ratewarden"
)

// With time frozen, exactly `limit` requests must win no matter how many
// goroutines race for one key.
func TestConcurrentSingleKeyExactCount(t *testing.T) {
	const limit, goroutines, perG = 1000, 64, 100
	for name, l := range allAlgorithms(t, limit, ratewarden.WithClock(newClock())) {
		t.Run(name, func(t *testing.T) {
			var allowed atomic.Int64
			var wg sync.WaitGroup
			start := make(chan struct{})
			for g := 0; g < goroutines; g++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-start
					for i := 0; i < perG; i++ {
						if l.Allow("hot").Allowed {
							allowed.Add(1)
						}
					}
				}()
			}
			close(start)
			wg.Wait()
			if got := allowed.Load(); got != limit {
				t.Fatalf("allowed %d, want exactly %d", got, limit)
			}
		})
	}
}

func TestConcurrentAllowNExactCount(t *testing.T) {
	// Mixed sizes: total admitted units never exceeds capacity, and with
	// only size-1 stragglers at the end it is filled exactly.
	const limit = 997
	for name, l := range allAlgorithms(t, limit, ratewarden.WithClock(newClock())) {
		t.Run(name, func(t *testing.T) {
			var units atomic.Int64
			var wg sync.WaitGroup
			for g := 0; g < 32; g++ {
				wg.Add(1)
				go func(g int) {
					defer wg.Done()
					for i := 0; i < 200; i++ {
						n := 1 + (g+i)%7
						if l.AllowN("k", n).Allowed {
							units.Add(int64(n))
						}
					}
				}(g)
			}
			wg.Wait()
			for l.Allow("k").Allowed {
				units.Add(1)
			}
			if got := units.Load(); got != limit {
				t.Fatalf("admitted %d units, want exactly %d", got, limit)
			}
		})
	}
}

// Hammer every API at once under -race with real time. Correctness is
// checked elsewhere; this is about data races and deadlocks.
func TestConcurrentMixedOperations(t *testing.T) {
	for name, l := range allAlgorithms(t, 50,
		ratewarden.WithShards(8), ratewarden.WithMaxKeys(100), ratewarden.WithCleanupInterval(time.Millisecond)) {
		t.Run(name, func(t *testing.T) {
			var wg sync.WaitGroup
			stop := make(chan struct{})
			op := func(f func(i int)) {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for i := 0; ; i++ {
						select {
						case <-stop:
							return
						default:
							f(i)
						}
					}
				}()
			}
			for g := 0; g < 8; g++ {
				op(func(i int) { l.AllowN(fmt.Sprint("k", i%300), 1+i%3) })
			}
			op(func(i int) { l.Peek(fmt.Sprint("k", i%300)) })
			op(func(i int) { l.Reset(fmt.Sprint("k", i%300)) })
			op(func(int) { l.Len() })
			time.Sleep(100 * time.Millisecond)
			close(stop)
			wg.Wait()
			if n := l.Len(); n > 104 { // 100 rounded up to a multiple of 8 shards
				t.Fatalf("Len %d exceeds MaxKeys bound", n)
			}
		})
	}
}
