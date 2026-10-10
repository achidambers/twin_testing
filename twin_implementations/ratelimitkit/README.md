# ratelimitkit

A keyed rate limiter for Go services, with HTTP middleware. Standard library only.

```
ratelimit/              core package: algorithms, in-memory store, clocks
httplimit/              net/http middleware and client key extraction
cmd/ratelimit-demo/     example server
```

## Algorithms

| Constructor        | Semantics                                                          | Use when                                |
|--------------------|--------------------------------------------------------------------|-----------------------------------------|
| `NewGCRA`          | Token bucket: `Rate` per `Period`, bursts up to `Burst`. Exact, integer-only. | You want smooth pacing with controlled bursts (the usual choice). |
| `NewSlidingWindow` | At most `Rate` per `Period`, weighted over the previous window. `Burst` ignored. | You need "N per minute" quota semantics. |

Both return `*MemoryLimiter`, which implements the `Limiter` interface:

```go
type Limiter interface {
	AllowN(ctx context.Context, key string, n int) (Result, error)
}
```

`Result` carries `Allowed`, `Limit`, `Remaining`, `RetryAfter` and `ResetAfter`.
A denied request never consumes capacity. A distributed store (e.g. Redis)
can be added by implementing `Limiter`; the middleware only depends on it.

## Usage

```go
lim, err := ratelimit.NewGCRA(ratelimit.PerMinute(60).WithBurst(10),
	ratelimit.WithMaxKeys(1_000_000))
if err != nil { ... }
defer lim.Close()

// Direct
res, err := lim.Allow(ctx, userID)
err = lim.Wait(ctx, userID, 1) // block until allowed, ctx done, or Close

// HTTP
mw := httplimit.Middleware(lim, httplimit.Options{
	KeyFunc: httplimit.ClientIP(httplimit.IPOptions{
		TrustedProxies: []netip.Prefix{netip.MustParsePrefix("10.0.0.0/8")},
	}),
})
http.ListenAndServe(":8080", mw(handler))
```

## Production behaviour

- **Concurrency:** 64 lock shards selected by a per-limiter random `maphash`
  seed, so unrelated keys rarely contend and callers cannot aim collisions.
  ~30-45 ns/op, zero allocations (Apple M-series).
- **Memory:** each key's state records when it becomes equivalent to a fresh
  key; a background sweep (default every minute) drops such keys without ever
  changing a decision. `WithMaxKeys` caps tracked keys exactly; at the cap a
  new key triggers a reclaim sweep (throttled to once per 100ms so a key flood
  cannot force O(n) work per request), then `ErrTooManyKeys`.
- **Time:** the default clock derives time from Go's monotonic clock, so NTP
  steps or manual clock changes cannot refill or drain buckets. If a clock
  does go backwards, both algorithms stay conservative (no free capacity).
- **HTTP headers:** `RateLimit-Limit`, `RateLimit-Remaining`, `RateLimit-Reset`
  on every decided request, and `Retry-After` on 429s. Seconds round up so
  clients never retry too early.
- **Failure policy:** limiter errors (store down, key cap reached, closed)
  return 503 by default, or pass through with `FailOpen: true`. A cost that can
  never fit returns 429 without `Retry-After`. A request that cannot be keyed
  returns 400 regardless of `FailOpen`, so a missing key never bypasses limits.
  `OnError` sees every error for logging and metrics.
- **Client identity:** `X-Forwarded-For` is only honoured through configured
  trusted proxies, walked right to left so client-supplied entries are ignored.
  IPv6 clients are grouped by /64 by default, since one client typically owns
  a whole /64 and could otherwise rotate addresses to evade limits.

### Trade-offs to know

- `FailOpen: true` also passes requests when the key cap is reached, so under a
  key-flood attack it disables limiting for new clients. Keep it off unless
  availability matters more than protection.
- `Wait` does not queue waiters in arrival order; under heavy contention a
  waiter can be overtaken.
- State is per process. Behind a load balancer with N replicas, each enforces
  its own limit unless you implement `Limiter` over a shared store.

## Testing

```sh
go test -race ./...                               # 59 tests, examples and fuzz seeds
go test -cover ./...                              # ~98% coverage
go test -run='^$' -fuzz=FuzzGCRA ./ratelimit          # conformance fuzzing
go test -run='^$' -fuzz=FuzzSlidingWindow ./ratelimit
go test -run='^$' -bench=. -benchmem ./ratelimit
```

Tests run on a `ManualClock`, so timing assertions are exact to the nanosecond
and nothing sleeps. They cover: exact `RetryAfter`, refill, burst caps,
sustained throughput, clock steps backwards, eviction never changing
decisions, exact key caps under 1000 concurrent inserts, exactly-`Burst`
admissions under 1000 concurrent requests, `Wait` cancellation / deadline /
`Close`, every middleware failure mode, and X-Forwarded-For spoofing. The
fuzzers check the algorithms' guarantees (GCRA never admits more than
`Burst + elapsed/interval`; no window exceeds `Rate`; waiting `RetryAfter`
always suffices) over arbitrary traffic.

## Demo

```sh
go run ./cmd/ratelimit-demo -rate 3 -period 1m
# flags: -addr -algorithm gcra|sliding-window -rate -period -burst
#        -max-keys -trusted-proxies 10.0.0.0/8,... -fail-open
```
