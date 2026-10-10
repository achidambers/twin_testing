// Package httpmw applies a ratewarden limiter to HTTP handlers.
//
// Allowed and denied responses carry the IETF draft rate limit headers
// (RateLimit-Limit, RateLimit-Remaining, RateLimit-Reset, all integers, the
// reset in seconds). Denied requests get 429 Too Many Requests and a
// Retry-After header.
package httpmw

import (
	"errors"
	"math"
	"net/http"
	"strconv"
	"time"

	"ratewarden"
)

// Allower is the part of *ratewarden.Limiter the middleware needs.
type Allower interface {
	AllowN(key string, n int) ratewarden.Decision
}

// Config configures New. Only Limiter is required.
type Config struct {
	Limiter Allower
	// KeyFunc maps a request to its limit key. A request whose key is ""
	// is passed through unlimited, so fall back to an IP key (see
	// FirstNonEmpty) if clients must not be able to opt out. Default KeyByIP.
	KeyFunc func(*http.Request) string
	// Cost is how many units a request takes. Default 1.
	Cost func(*http.Request) int
	// OnLimited writes the response for denied requests, after the rate
	// limit headers are set. Default: 429 with a short plain-text body.
	OnLimited http.Handler
	// OnDecision, if set, observes every decision (for metrics or logs).
	OnDecision func(r *http.Request, key string, d ratewarden.Decision)
	// DisableHeaders omits the RateLimit-* headers (Retry-After is still
	// sent on 429).
	DisableHeaders bool
}

// New returns middleware enforcing cfg.
func New(cfg Config) (func(http.Handler) http.Handler, error) {
	if cfg.Limiter == nil {
		return nil, errors.New("httpmw: Config.Limiter is required")
	}
	if cfg.KeyFunc == nil {
		cfg.KeyFunc = KeyByIP
	}
	if cfg.OnLimited == nil {
		cfg.OnLimited = http.HandlerFunc(defaultLimited)
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			key := cfg.KeyFunc(r)
			if key == "" {
				next.ServeHTTP(w, r)
				return
			}
			cost := 1
			if cfg.Cost != nil {
				cost = cfg.Cost(r)
			}
			d := cfg.Limiter.AllowN(key, cost)
			if cfg.OnDecision != nil {
				cfg.OnDecision(r, key, d)
			}
			h := w.Header()
			if !cfg.DisableHeaders {
				h.Set("RateLimit-Limit", strconv.Itoa(d.Limit))
				h.Set("RateLimit-Remaining", strconv.Itoa(d.Remaining))
				h.Set("RateLimit-Reset", strconv.FormatInt(ceilSeconds(d.ResetAfter), 10))
			}
			if d.Allowed {
				next.ServeHTTP(w, r)
				return
			}
			if d.RetryAfter != ratewarden.Never {
				h.Set("Retry-After", strconv.FormatInt(max(ceilSeconds(d.RetryAfter), 1), 10))
			}
			cfg.OnLimited.ServeHTTP(w, r)
		})
	}, nil
}

func defaultLimited(w http.ResponseWriter, _ *http.Request) {
	http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
}

// ceilSeconds rounds up so clients never retry early.
func ceilSeconds(d time.Duration) int64 {
	if d <= 0 {
		return 0
	}
	return int64(math.Ceil(d.Seconds()))
}
