# ratewarden

An in-process, keyed rate limiter for Go with HTTP middleware. Standard library only.

```go
l, err := ratewarden.NewTokenBucket(100, time.Second, 20, // 100/s, bursts of 20
	ratewarden.WithMaxKeys(1_000_000))
if err != nil { ... }
defer l.Close()

if d := l.Allow(userID); !d.Allowed {
	// d.RetryAfter says when to come back
}
```

## Algorithms

| constructor | behaviour | state per key | use when |
|---|---|---|---|
| `NewTokenBucket(limit, per, burst)` | GCRA: smooth refill at `limit/per`, up to `burst` at once | 8 bytes | default choice; APIs, outbound pacing |
| `NewSlidingWindow(limit, window)` | ~`limit` per rolling window (weighted two-window counter) | 24 bytes | "N per minute" quotas without boundary bursts |
| `NewFixedWindow(limit, window)` | `limit` per epoch-aligned window | 16 bytes | cheapest; 2x burst across a boundary is acceptable |

Every call returns a `Decision`: `Allowed`, `Limit`, `Remaining`, `RetryAfter` (exact: waiting that long guarantees the same request succeeds, absent other traffic; `Never` if the cost exceeds capacity) and `ResetAfter`.

## API

- `Allow(key)`, `AllowN(key, n)`: all-or-nothing; a denied request consumes nothing
- `Peek(key)`: state without consuming
- `Wait(ctx, key)`, `WaitN`: block until allowed; fail fast with `ErrDeadlineTooShort` if the ctx deadline would pass first, `ErrExceedsLimit` if it can never succeed
- `Reset(key)`, `Len()`, `Close()`

Options: `WithClock`, `WithShards` (default 64), `WithMaxKeys` (default unbounded, so **set it when keys come from client input**), `WithCleanupInterval` (default 1m, 0 disables).

## HTTP middleware

```go
mw, err := httpmw.New(httpmw.Config{
	Limiter: l,
	KeyFunc: httpmw.FirstNonEmpty(
		httpmw.KeyByHeader("X-API-Key"),
		httpmw.KeyByForwardedFor(netip.MustParsePrefix("10.0.0.0/8")),
	),
})
http.ListenAndServe(":8080", mw(handler))
```

Sets `RateLimit-Limit`, `RateLimit-Remaining`, `RateLimit-Reset` on every response, plus `429` with `Retry-After` (rounded up to whole seconds) on denial. Hooks: `Cost`, `OnLimited`, `OnDecision`, `DisableHeaders`.

Key functions:
- `KeyByIP` uses the peer address. IPv6 is grouped by /64.
- `KeyByForwardedFor(trusted...)` trusts `X-Forwarded-For` only through the chain of trusted proxies, walking right to left. Entries a client prepends are ignored.
- `KeyByHeader(name)` returns `""` when the header is missing, and an empty key means *unlimited*. Wrap it with `FirstNonEmpty` and an IP fallback.

## Design notes

- **Concurrency.** Keys hash (FNV-1a) onto cache-line-padded, mutex-guarded shards. ~40 ns/op and 0 allocations per call on an M4, single-key or 100k keys in parallel.
- **Time.** The default clock is wall time anchored to Go's monotonic clock, so NTP steps or manual clock changes cannot reset or freeze limits. Windows never roll backwards.
- **Memory.** Only allowed, consuming requests store a key, so denials and peeks on unknown keys cost nothing. The janitor drops a key only once its state is exactly equivalent to a fresh key, which is property-tested. `WithMaxKeys` evicts a random key when a shard is full: under a key flood that key's limit restarts, but memory stays bounded.
- **Scope.** The limiter is in-process. With N replicas behind a load balancer, each enforces its own limit.

## Testing

```sh
go test -race ./...                       # full suite
go test -short ./...                      # skip real-time tests
go test -fuzz=FuzzLimiters -fuzztime=1m .
go test -fuzz=FuzzKeyByForwardedFor -fuzztime=1m ./httpmw
go test -bench . -benchmem .
```

The suite includes:
- exact decision tables per algorithm
- randomized property tests that check each algorithm's bound against an independent recomputation from the log of allowed requests
- RetryAfter checks: waiting that long always succeeds, and for the token bucket and fixed window, 1 ns earlier still fails
- an exact-count race test with 64 goroutines on one key
- a test that expired state is equivalent to a fresh key
- Wait tests covering cancellation and overtaking
- key-spoofing tests for `X-Forwarded-For`
- a zero-allocation regression test

Statement coverage is 99%. Planting 9 realistic bugs (off-by-one admission, premature expiry, dropped window weighting, rounding-down RetryAfter, spoofable XFF walk, ...) makes the suite fail every time.

`ratewardentest.Clock` is a manual clock you can use to test your own code deterministically.
