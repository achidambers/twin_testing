package ratewarden_test

import (
	"testing"
	"time"

	"ratewarden"
)

// FuzzLimiters feeds arbitrary request/advance sequences to every algorithm
// and checks the decision invariants plus each algorithm's hard bound.
// Each input byte pair is (request size, clock advance).
//
//	go test -fuzz=FuzzLimiters -fuzztime=30s
func FuzzLimiters(f *testing.F) {
	f.Add(uint8(5), uint16(100), []byte{1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 6, 255})
	f.Add(uint8(1), uint16(1), []byte{1, 1, 1, 1, 2, 0})
	f.Add(uint8(20), uint16(1000), []byte{20, 250, 20, 250, 20, 250, 20, 250, 19, 3})
	f.Fuzz(func(t *testing.T, limit uint8, windowMs uint16, ops []byte) {
		lim := int(limit%50) + 1
		window := time.Duration(int(windowMs)%5000+1) * time.Millisecond
		interval := int64(window) / int64(lim)
		if len(ops) > 2000 {
			ops = ops[:2000]
		}
		type mk func(*testing.T, ...ratewarden.Option) *ratewarden.Limiter
		algs := map[string]mk{
			"tb": func(t *testing.T, o ...ratewarden.Option) *ratewarden.Limiter {
				return tokenBucket(t, lim, window, lim, o...)
			},
			"fw": func(t *testing.T, o ...ratewarden.Option) *ratewarden.Limiter {
				return fixedWindow(t, lim, window, o...)
			},
			"sw": func(t *testing.T, o ...ratewarden.Option) *ratewarden.Limiter {
				return slidingWindow(t, lim, window, o...)
			},
		}
		for name, newL := range algs {
			clk := newClock()
			l := newL(t, ratewarden.WithClock(clk), ratewarden.WithCleanupInterval(0))
			var evs []event
			for i := 0; i+1 < len(ops); i += 2 {
				n := int(ops[i]) % (lim + 2) // occasionally above capacity
				d := l.AllowN("k", n)
				if n > 0 {
					checkDecision(t, i, n, d)
				}
				if d.Allowed && n > 0 {
					evs = append(evs, event{clk.Now().UnixNano(), n})
				}
				clk.Advance(time.Duration(ops[i+1]) * window / 128)
			}
			switch name {
			case "tb":
				for i := range evs {
					sum := 0
					for j := i; j < len(evs); j++ {
						sum += evs[j].n
						if bound := lim + int((evs[j].at-evs[i].at)/interval); sum > bound {
							t.Fatalf("tb: %d units in span, bound %d", sum, bound)
						}
					}
				}
			default:
				for idx, sum := range sumByWindow(evs, window) {
					if sum > lim {
						t.Fatalf("%s: window %d admitted %d > %d", name, idx, sum, lim)
					}
				}
			}
		}
	})
}
