package ratelimit_test

import (
	"testing"
	"time"

	"github.com/example/ratelimitkit/ratelimit"
)

// checkResult verifies properties every Result must have.
func checkResult(t *testing.T, res ratelimit.Result, capacity int) {
	t.Helper()
	if res.Remaining < 0 || res.Remaining > capacity {
		t.Fatalf("Remaining %d outside [0, %d]", res.Remaining, capacity)
	}
	if res.Allowed && res.RetryAfter != 0 {
		t.Fatalf("allowed with RetryAfter %v", res.RetryAfter)
	}
	if !res.Allowed && res.RetryAfter <= 0 {
		t.Fatalf("denied with RetryAfter %v", res.RetryAfter)
	}
	if res.ResetAfter < 0 {
		t.Fatalf("negative ResetAfter %v", res.ResetAfter)
	}
}

// FuzzGCRA checks GCRA conformance: after time elapsed, at most
// burst + elapsed/interval events have been allowed, and waiting RetryAfter
// always suffices.
func FuzzGCRA(f *testing.F) {
	f.Add(uint8(10), uint8(5), []byte{0, 1, 2, 255, 7, 0, 0, 0, 13, 64, 9})
	f.Add(uint8(1), uint8(1), []byte{0, 0, 8, 16})
	f.Add(uint8(255), uint8(255), []byte{255, 255, 0, 3})
	f.Fuzz(func(t *testing.T, rate, burst uint8, ops []byte) {
		if rate == 0 || burst == 0 {
			t.Skip()
		}
		lim, clock := newLimiter(t, ratelimit.NewGCRA, ratelimit.PerSecond(int(rate)).WithBurst(int(burst)))
		interval := time.Second / time.Duration(rate)
		var elapsed time.Duration
		var consumed int64
		advance := func(d time.Duration) {
			clock.Advance(d)
			elapsed += d
		}
		for _, op := range ops {
			advance(time.Duration(op>>3) * interval / 4)
			n := 1 + int(op&7)%int(burst)
			res := allow(t, lim, "k", n)
			checkResult(t, res, int(burst))
			if !res.Allowed {
				advance(res.RetryAfter)
				if res = allow(t, lim, "k", n); !res.Allowed {
					t.Fatalf("denied after waiting RetryAfter: %+v", res)
				}
			}
			consumed += int64(n)
			if earned := int64(burst) + int64(elapsed/interval); consumed > earned {
				t.Fatalf("consumed %d > burst %d + %v/%v", consumed, burst, elapsed, interval)
			}
		}
	})
}

// FuzzSlidingWindow checks that no aligned window ever exceeds the rate and
// that waiting RetryAfter always suffices.
func FuzzSlidingWindow(f *testing.F) {
	f.Add(uint8(10), []byte{0, 1, 2, 255, 7, 0, 0, 0, 13, 64, 9})
	f.Add(uint8(1), []byte{0, 0, 8, 16})
	f.Add(uint8(200), []byte{255, 255, 0, 3, 128})
	f.Fuzz(func(t *testing.T, rate uint8, ops []byte) {
		if rate == 0 {
			t.Skip()
		}
		lim, clock := newLimiter(t, ratelimit.NewSlidingWindow, ratelimit.PerSecond(int(rate)))
		perWindow := map[int64]int{}
		for _, op := range ops {
			clock.Advance(time.Duration(op>>2) * 20 * time.Millisecond)
			n := 1 + int(op&3)%int(rate)
			res := allow(t, lim, "k", n)
			checkResult(t, res, int(rate))
			if !res.Allowed {
				clock.Advance(res.RetryAfter)
				if res = allow(t, lim, "k", n); !res.Allowed {
					t.Fatalf("denied after waiting RetryAfter: %+v", res)
				}
			}
			w := clock.Now().Unix()
			if perWindow[w] += n; perWindow[w] > int(rate) {
				t.Fatalf("%d events in one window, rate %d", perWindow[w], rate)
			}
		}
	})
}
