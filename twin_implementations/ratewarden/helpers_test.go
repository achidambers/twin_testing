package ratewarden_test

import (
	"testing"
	"time"

	"ratewarden"
	"ratewarden/ratewardentest"
)

// epoch is aligned to every window size used in tests, so window boundaries
// fall on round offsets from it.
var epoch = time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)

func newClock() *ratewardentest.Clock { return ratewardentest.NewClock(epoch) }

func mustLimiter(t testing.TB, l *ratewarden.Limiter, err error) *ratewarden.Limiter {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	return l
}

func tokenBucket(t testing.TB, limit int, per time.Duration, burst int, opts ...ratewarden.Option) *ratewarden.Limiter {
	l, err := ratewarden.NewTokenBucket(limit, per, burst, opts...)
	return mustLimiter(t, l, err)
}

func fixedWindow(t testing.TB, limit int, window time.Duration, opts ...ratewarden.Option) *ratewarden.Limiter {
	l, err := ratewarden.NewFixedWindow(limit, window, opts...)
	return mustLimiter(t, l, err)
}

func slidingWindow(t testing.TB, limit int, window time.Duration, opts ...ratewarden.Option) *ratewarden.Limiter {
	l, err := ratewarden.NewSlidingWindow(limit, window, opts...)
	return mustLimiter(t, l, err)
}

// want asserts the fields of d that the test cares about; negative ints and
// durations mean "don't check".
type want struct {
	allowed    bool
	remaining  int
	retryAfter time.Duration
	resetAfter time.Duration
}

func check(t *testing.T, label string, d ratewarden.Decision, w want) {
	t.Helper()
	if d.Allowed != w.allowed {
		t.Errorf("%s: Allowed = %v, want %v (decision %+v)", label, d.Allowed, w.allowed, d)
	}
	if w.remaining >= 0 && d.Remaining != w.remaining {
		t.Errorf("%s: Remaining = %d, want %d", label, d.Remaining, w.remaining)
	}
	if w.retryAfter != skip && d.RetryAfter != w.retryAfter {
		t.Errorf("%s: RetryAfter = %v, want %v", label, d.RetryAfter, w.retryAfter)
	}
	if w.resetAfter != skip && d.ResetAfter != w.resetAfter {
		t.Errorf("%s: ResetAfter = %v, want %v", label, d.ResetAfter, w.resetAfter)
	}
}

const skip time.Duration = -2 // distinct from ratewarden.Never (-1)

func waitFor(t *testing.T, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}
