package ratewarden

import (
	"math/rand/v2"
	"testing"
	"time"
)

// The janitor may drop a key only if doing so cannot change any future
// decision. For random states, whenever expired(s, now) holds, every take on
// s must match a take on the zero state, both in the decision and in the
// resulting state's future behaviour.
func checkExpiredEquivalence[S any, A algorithm[S]](t *testing.T, alg A, limit int, step time.Duration) {
	t.Helper()
	now := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano()
	for seed := uint64(0); seed < 20; seed++ {
		rng := rand.New(rand.NewPCG(seed, seed))
		var s S
		for i := 0; i < 2000; i++ {
			now += rng.Int64N(int64(step))
			if alg.expired(s, now) {
				for n := 0; n <= limit+1; n++ {
					a, da := alg.take(s, now, n)
					b, db := alg.take(*new(S), now, n)
					if da != db {
						t.Fatalf("seed %d step %d: expired state %+v decides %+v for n=%d, fresh decides %+v", seed, i, s, da, n, db)
					}
					// And the next decision after that take matches too.
					later := now + rng.Int64N(int64(step))
					_, da2 := alg.take(a, later, 1)
					_, db2 := alg.take(b, later, 1)
					if da2 != db2 {
						t.Fatalf("seed %d step %d: divergence after take: %+v vs %+v", seed, i, da2, db2)
					}
				}
			}
			n := rng.IntN(limit + 1)
			if c, d := alg.take(s, now, n); d.Allowed && n > 0 {
				s = c // mirror the store: persist only consuming allows
			}
		}
	}
}

func TestExpiredMeansFresh(t *testing.T) {
	t.Run("token-bucket", func(t *testing.T) {
		checkExpiredEquivalence[gcraState](t, gcra{interval: int64(100 * time.Millisecond), burst: 5, tau: int64(500 * time.Millisecond)}, 5, 300*time.Millisecond)
	})
	t.Run("fixed-window", func(t *testing.T) {
		checkExpiredEquivalence[fixedState](t, fixedWindow{limit: 5, window: int64(time.Second)}, 5, 700*time.Millisecond)
	})
	t.Run("sliding-window", func(t *testing.T) {
		checkExpiredEquivalence[slidingState](t, slidingWindow{limit: 5, window: int64(time.Second)}, 5, 700*time.Millisecond)
	})
}

func TestShardCountRoundsUpToPowerOfTwo(t *testing.T) {
	for in, want := range map[int]int{1: 1, 2: 2, 3: 4, 64: 64, 65: 128} {
		st := newStore[fixedState](fixedWindow{limit: 1, window: 1}, in, 0)
		if len(st.shards) != want || st.mask != uint32(want-1) {
			t.Errorf("shards(%d) = %d mask %d, want %d", in, len(st.shards), st.mask, want)
		}
	}
}

func TestFNV32a(t *testing.T) {
	// Reference values from the FNV spec.
	for in, want := range map[string]uint32{"": 0x811c9dc5, "a": 0xe40c292c, "foobar": 0xbf9cf968} {
		if got := fnv32a(in); got != want {
			t.Errorf("fnv32a(%q) = %#x, want %#x", in, got, want)
		}
	}
}
