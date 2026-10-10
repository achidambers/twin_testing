package ratewarden

import "sync"

// algorithm is one rate limiting policy over a per-key state S. The zero S
// must mean "fresh key, full capacity". take returns the updated state; the
// store only persists it when the request was allowed and consumed
// something, so denials and peeks never allocate or change state. State is
// passed by value so it never escapes to the heap.
type algorithm[S any] interface {
	take(s S, now int64, n int) (S, Decision)
	// expired reports whether s is equivalent to the zero state at now, so
	// the key can be dropped without changing any future decision.
	expired(s S, now int64) bool
}

type shard[S any] struct {
	mu sync.Mutex
	m  map[string]S
	_  [48]byte // keep neighbouring shards' mutexes off the same cache line
}

type store[S any, A algorithm[S]] struct {
	alg         A
	shards      []shard[S]
	mask        uint32
	maxPerShard int
}

func newStore[S any, A algorithm[S]](alg A, shards, maxKeys int) *store[S, A] {
	n := 1
	for n < shards {
		n <<= 1
	}
	st := &store[S, A]{alg: alg, shards: make([]shard[S], n), mask: uint32(n - 1)}
	for i := range st.shards {
		st.shards[i].m = make(map[string]S)
	}
	if maxKeys > 0 {
		st.maxPerShard = (maxKeys + n - 1) / n
	}
	return st
}

// fnv32a, inlined to avoid the allocation of hash/fnv's interface.
func fnv32a(s string) uint32 {
	h := uint32(2166136261)
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= 16777619
	}
	return h
}

func (st *store[S, A]) shardFor(key string) *shard[S] {
	return &st.shards[fnv32a(key)&st.mask]
}

func (st *store[S, A]) take(key string, n int, now int64) Decision {
	sh := st.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	s, ok := sh.m[key]
	s, d := st.alg.take(s, now, n)
	if d.Allowed && n > 0 {
		if !ok && st.maxPerShard > 0 && len(sh.m) >= st.maxPerShard {
			for k := range sh.m { // map iteration order is randomised
				delete(sh.m, k)
				break
			}
		}
		sh.m[key] = s
	}
	return d
}

func (st *store[S, A]) reset(key string) {
	sh := st.shardFor(key)
	sh.mu.Lock()
	delete(sh.m, key)
	sh.mu.Unlock()
}

func (st *store[S, A]) sweep(now int64) int {
	removed := 0
	for i := range st.shards {
		sh := &st.shards[i]
		sh.mu.Lock()
		for k, s := range sh.m {
			if st.alg.expired(s, now) {
				delete(sh.m, k)
				removed++
			}
		}
		sh.mu.Unlock()
	}
	return removed
}

func (st *store[S, A]) len() int {
	total := 0
	for i := range st.shards {
		sh := &st.shards[i]
		sh.mu.Lock()
		total += len(sh.m)
		sh.mu.Unlock()
	}
	return total
}
