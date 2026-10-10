package ratelimit

import (
	"context"
	"errors"
	"hash/maphash"
	"sync"
	"sync/atomic"
	"time"
)

// numShards splits the key space so unrelated keys rarely contend on a lock.
const numShards = 64

// DefaultCleanupInterval is how often expired keys are swept by default.
const DefaultCleanupInterval = time.Minute

// reclaimInterval is the minimum time between capacity-triggered sweeps.
const reclaimInterval = 100 * time.Millisecond

// Option configures a MemoryLimiter.
type Option func(*options)

type options struct {
	clock           Clock
	maxKeys         int
	cleanupInterval time.Duration
}

// WithClock sets the time source. The default is SystemClock.
func WithClock(c Clock) Option { return func(o *options) { o.clock = c } }

// WithMaxKeys caps the number of keys tracked at once. At the cap, a new key
// first triggers reclamation of expired keys (a full sweep at most once per
// 100ms); if there is still no room, AllowN returns ErrTooManyKeys. Existing
// keys are unaffected. Zero, the default, means unlimited.
func WithMaxKeys(n int) Option { return func(o *options) { o.maxKeys = n } }

// WithCleanupInterval sets how often a background goroutine removes expired
// keys. Zero or negative disables it; call Sweep manually instead.
func WithCleanupInterval(d time.Duration) Option {
	return func(o *options) { o.cleanupInterval = d }
}

type shard struct {
	mu      sync.Mutex
	entries map[string]*state
}

// MemoryLimiter is a Limiter that keeps per-key state in process memory.
// It is safe for concurrent use. Call Close to stop background cleanup.
type MemoryLimiter struct {
	limit   Limit
	algo    algorithm
	clock   Clock
	maxKeys int64
	seed    maphash.Seed
	shards  [numShards]shard
	keys    atomic.Int64
	// lastReclaim is when the last capacity-triggered full sweep ran.
	lastReclaim atomic.Int64

	closed    atomic.Bool
	stop      chan struct{}
	stopOnce  sync.Once
	janitorWG sync.WaitGroup
}

var _ Limiter = (*MemoryLimiter)(nil)

// NewGCRA returns a limiter using the generic cell rate algorithm: a token
// bucket allowing limit.Rate events per limit.Period with bursts of up to
// limit.Burst.
func NewGCRA(limit Limit, opts ...Option) (*MemoryLimiter, error) {
	if err := limit.Validate(); err != nil {
		return nil, err
	}
	limit = limit.normalized()
	return newMemory(limit, newGCRA(limit), opts)
}

// NewSlidingWindow returns a limiter allowing at most limit.Rate events per
// limit.Period using a sliding window counter. limit.Burst is ignored.
func NewSlidingWindow(limit Limit, opts ...Option) (*MemoryLimiter, error) {
	if err := limit.Validate(); err != nil {
		return nil, err
	}
	limit = limit.normalized()
	return newMemory(limit, newSlidingWindow(limit), opts)
}

func newMemory(limit Limit, algo algorithm, opts []Option) (*MemoryLimiter, error) {
	o := options{clock: SystemClock(), cleanupInterval: DefaultCleanupInterval}
	for _, opt := range opts {
		opt(&o)
	}
	if o.clock == nil {
		return nil, errors.New("ratelimit: nil clock")
	}
	if o.maxKeys < 0 {
		return nil, errors.New("ratelimit: max keys must not be negative")
	}
	l := &MemoryLimiter{
		limit:   limit,
		algo:    algo,
		clock:   o.clock,
		maxKeys: int64(o.maxKeys),
		seed:    maphash.MakeSeed(),
		stop:    make(chan struct{}),
	}
	for i := range l.shards {
		l.shards[i].entries = make(map[string]*state)
	}
	if o.cleanupInterval > 0 {
		l.janitorWG.Add(1)
		go l.janitor(o.cleanupInterval)
	}
	return l, nil
}

// Limit returns the limiter's configuration.
func (l *MemoryLimiter) Limit() Limit { return l.limit }

// Allow is shorthand for AllowN(ctx, key, 1).
func (l *MemoryLimiter) Allow(ctx context.Context, key string) (Result, error) {
	return l.AllowN(ctx, key, 1)
}

// AllowN reports whether n events for key may happen now, and records them
// if so. A denied request consumes nothing.
func (l *MemoryLimiter) AllowN(ctx context.Context, key string, n int) (Result, error) {
	if l.closed.Load() {
		return Result{}, ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	capacity := l.algo.capacity()
	if n <= 0 {
		return Result{Limit: capacity}, ErrInvalidCost
	}
	if n > capacity {
		return Result{Limit: capacity}, ErrCostExceedsLimit
	}

	now := l.clock.Now().UnixNano()
	s := l.shardFor(key)
	s.mu.Lock()
	st, err := l.entryLocked(s, key, now)
	if errors.Is(err, ErrTooManyKeys) && l.claimReclaim(now) {
		// Expired keys may live in other shards. Sweep without holding this
		// shard's lock, so two goroutines cannot deadlock on each other's.
		s.mu.Unlock()
		l.Sweep()
		s.mu.Lock()
		st, err = l.entryLocked(s, key, now)
	}
	if err != nil {
		s.mu.Unlock()
		return Result{Limit: capacity}, err
	}
	res := l.algo.take(st, now, n)
	s.mu.Unlock()
	return res, nil
}

// entryLocked returns key's state, creating it if there is room. The caller
// holds s.mu.
func (l *MemoryLimiter) entryLocked(s *shard, key string, now int64) (*state, error) {
	if st, ok := s.entries[key]; ok {
		return st, nil
	}
	if !l.reserveKey(s, now) {
		return nil, ErrTooManyKeys
	}
	st := &state{}
	s.entries[key] = st
	return st, nil
}

// claimReclaim reports whether this caller may run a full sweep to make room
// for a new key. Full sweeps are O(keys), so they are throttled to keep a
// flood of new keys at capacity from turning into a CPU exhaustion attack.
func (l *MemoryLimiter) claimReclaim(now int64) bool {
	last := l.lastReclaim.Load()
	return now-last >= int64(reclaimInterval) && l.lastReclaim.CompareAndSwap(last, now)
}

// reserveKey claims room for one more key, first reclaiming expired entries
// from s (whose lock the caller holds) if the cap has been reached.
func (l *MemoryLimiter) reserveKey(s *shard, now int64) bool {
	if l.maxKeys == 0 {
		l.keys.Add(1)
		return true
	}
	for swept := false; ; {
		c := l.keys.Load()
		if c < l.maxKeys {
			if l.keys.CompareAndSwap(c, c+1) {
				return true
			}
			continue
		}
		if swept {
			return false
		}
		l.sweepShardLocked(s, now)
		swept = true
	}
}

// Wait blocks until n events for key are allowed, ctx is done, or the
// limiter is closed. It returns ErrWouldExceedDeadline without waiting if
// ctx's deadline is too close. Waiters are not queued in arrival order.
func (l *MemoryLimiter) Wait(ctx context.Context, key string, n int) error {
	for {
		res, err := l.AllowN(ctx, key, n)
		if err != nil {
			return err
		}
		if res.Allowed {
			return nil
		}
		if deadline, ok := ctx.Deadline(); ok && res.RetryAfter > time.Until(deadline) {
			return ErrWouldExceedDeadline
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-l.stop:
			return ErrClosed
		case <-l.clock.After(res.RetryAfter):
		}
	}
}

// Reset forgets all state for key, restoring its full capacity.
func (l *MemoryLimiter) Reset(key string) {
	s := l.shardFor(key)
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.entries[key]; ok {
		delete(s.entries, key)
		l.keys.Add(-1)
	}
}

// Len returns the number of keys currently tracked.
func (l *MemoryLimiter) Len() int { return int(l.keys.Load()) }

// Sweep removes keys whose state has fully expired and returns how many were
// removed. Removing them never changes a future decision.
func (l *MemoryLimiter) Sweep() int {
	now := l.clock.Now().UnixNano()
	removed := 0
	for i := range l.shards {
		s := &l.shards[i]
		s.mu.Lock()
		removed += l.sweepShardLocked(s, now)
		s.mu.Unlock()
	}
	return removed
}

func (l *MemoryLimiter) sweepShardLocked(s *shard, now int64) int {
	removed := 0
	for k, st := range s.entries {
		if st.expiresAt <= now {
			delete(s.entries, k)
			removed++
		}
	}
	l.keys.Add(int64(-removed))
	return removed
}

// Close stops background cleanup and makes further calls return ErrClosed.
// Blocked Wait calls return ErrClosed. Close is idempotent.
func (l *MemoryLimiter) Close() error {
	l.stopOnce.Do(func() {
		l.closed.Store(true)
		close(l.stop)
	})
	l.janitorWG.Wait()
	return nil
}

func (l *MemoryLimiter) shardFor(key string) *shard {
	return &l.shards[maphash.String(l.seed, key)%numShards]
}

func (l *MemoryLimiter) janitor(interval time.Duration) {
	defer l.janitorWG.Done()
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-l.stop:
			return
		case <-t.C:
			l.Sweep()
		}
	}
}
