package ratelimit_test

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/example/ratelimitkit/ratelimit"
)

// epoch is aligned to every window size used in these tests.
var epoch = time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

type constructor func(ratelimit.Limit, ...ratelimit.Option) (*ratelimit.MemoryLimiter, error)

var constructors = map[string]constructor{
	"gcra":           ratelimit.NewGCRA,
	"sliding-window": ratelimit.NewSlidingWindow,
}

// newLimiter returns a limiter on a manual clock with background cleanup off.
func newLimiter(t testing.TB, c constructor, l ratelimit.Limit, opts ...ratelimit.Option) (*ratelimit.MemoryLimiter, *ratelimit.ManualClock) {
	t.Helper()
	clock := ratelimit.NewManualClock(epoch)
	base := []ratelimit.Option{ratelimit.WithClock(clock), ratelimit.WithCleanupInterval(0)}
	lim, err := c(l, append(base, opts...)...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { lim.Close() })
	return lim, clock
}

func allow(t testing.TB, l *ratelimit.MemoryLimiter, key string, n int) ratelimit.Result {
	t.Helper()
	r, err := l.AllowN(context.Background(), key, n)
	if err != nil {
		t.Fatalf("AllowN(%q, %d): %v", key, n, err)
	}
	return r
}

func TestLimitValidate(t *testing.T) {
	cases := []struct {
		name string
		l    ratelimit.Limit
		ok   bool
	}{
		{"per second", ratelimit.PerSecond(10), true},
		{"per minute with burst", ratelimit.PerMinute(60).WithBurst(5), true},
		{"burst above rate", ratelimit.PerSecond(1).WithBurst(100), true},
		{"one per nanosecond", ratelimit.Limit{Rate: 1, Period: 1}, true},
		{"zero rate", ratelimit.Limit{Period: time.Second}, false},
		{"negative rate", ratelimit.Limit{Rate: -1, Period: time.Second}, false},
		{"zero period", ratelimit.Limit{Rate: 1}, false},
		{"negative period", ratelimit.Limit{Rate: 1, Period: -time.Second}, false},
		{"negative burst", ratelimit.PerSecond(1).WithBurst(-1), false},
		{"finer than 1ns", ratelimit.Limit{Rate: 2, Period: 1}, false},
		{"period overflow", ratelimit.Limit{Rate: 1, Period: math.MaxInt64}, false},
		{"burst overflow", ratelimit.PerHour(1).WithBurst(math.MaxInt32), false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := c.l.Validate()
			if (err == nil) != c.ok {
				t.Fatalf("Validate() = %v, want ok=%v", err, c.ok)
			}
			if err != nil && !errors.Is(err, ratelimit.ErrInvalidLimit) {
				t.Fatalf("error %v does not wrap ErrInvalidLimit", err)
			}
			for name, ctor := range constructors {
				lim, err := ctor(c.l, ratelimit.WithCleanupInterval(0))
				if (err == nil) != c.ok {
					t.Errorf("%s constructor error = %v, want ok=%v", name, err, c.ok)
				}
				if lim != nil {
					lim.Close()
				}
			}
		})
	}
}

func TestLimitDefaultsAndString(t *testing.T) {
	if got, want := ratelimit.PerSecond(5).String(), "5/1s (burst 5)"; got != want {
		t.Errorf("String() = %q, want %q", got, want)
	}
	lim, _ := newLimiter(t, ratelimit.NewGCRA, ratelimit.Limit{Rate: 3, Period: time.Minute})
	if got := lim.Limit().Burst; got != 3 {
		t.Errorf("zero burst normalized to %d, want 3", got)
	}
	if got := allow(t, lim, "k", 1).Limit; got != 3 {
		t.Errorf("Result.Limit = %d, want 3", got)
	}
}

func TestInvalidOptions(t *testing.T) {
	for name, ctor := range constructors {
		if _, err := ctor(ratelimit.PerSecond(1), ratelimit.WithMaxKeys(-1)); err == nil {
			t.Errorf("%s: negative max keys accepted", name)
		}
		if _, err := ctor(ratelimit.PerSecond(1), ratelimit.WithClock(nil)); err == nil {
			t.Errorf("%s: nil clock accepted", name)
		}
	}
}

func TestCostValidation(t *testing.T) {
	for name, ctor := range constructors {
		t.Run(name, func(t *testing.T) {
			lim, _ := newLimiter(t, ctor, ratelimit.PerSecond(5))
			ctx := context.Background()
			for _, n := range []int{0, -1} {
				if _, err := lim.AllowN(ctx, "k", n); !errors.Is(err, ratelimit.ErrInvalidCost) {
					t.Errorf("AllowN(n=%d) error = %v, want ErrInvalidCost", n, err)
				}
			}
			res, err := lim.AllowN(ctx, "k", 6)
			if !errors.Is(err, ratelimit.ErrCostExceedsLimit) || res.Allowed || res.Limit != 5 {
				t.Errorf("AllowN(n=6) = %+v, %v; want ErrCostExceedsLimit with Limit 5", res, err)
			}
			if lim.Len() != 0 {
				t.Errorf("rejected calls created %d keys", lim.Len())
			}
			if res := allow(t, lim, "k", 5); !res.Allowed {
				t.Errorf("full-capacity request after invalid ones was denied: %+v", res)
			}
		})
	}
}
