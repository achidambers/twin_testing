// Package httplimit provides HTTP middleware backed by a ratelimit.Limiter.
package httplimit

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/example/ratelimitkit/ratelimit"
)

// Options configures Middleware. The zero value is usable.
type Options struct {
	// KeyFunc identifies the client. Default: ClientIP(IPOptions{}).
	// Requests it cannot key are rejected with 400 regardless of FailOpen,
	// so a missing key can never bypass the limit.
	KeyFunc KeyFunc
	// Cost returns how many events a request consumes. Default: 1.
	Cost func(*http.Request) int
	// FailOpen lets requests through when the limiter returns an error
	// (e.g. a backing store is down). By default they get 503.
	FailOpen bool
	// DeniedHandler writes the response for rate limited requests, after
	// the rate limit headers are set. Default: 429 with a short text body.
	DeniedHandler http.Handler
	// OnError is called for every key or limiter error, for logging and
	// metrics. Optional.
	OnError func(r *http.Request, err error)
}

// Middleware returns middleware that rate limits requests with l.
//
// Every decided request carries RateLimit-Limit, RateLimit-Remaining and
// RateLimit-Reset (seconds) headers; denied requests also carry Retry-After.
func Middleware(l ratelimit.Limiter, o Options) func(http.Handler) http.Handler {
	if l == nil {
		panic("httplimit: nil Limiter")
	}
	if o.KeyFunc == nil {
		o.KeyFunc = ClientIP(IPOptions{})
	}
	if o.Cost == nil {
		o.Cost = func(*http.Request) int { return 1 }
	}
	if o.DeniedHandler == nil {
		o.DeniedHandler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, http.StatusText(http.StatusTooManyRequests), http.StatusTooManyRequests)
		})
	}
	onError := o.OnError
	if onError == nil {
		onError = func(*http.Request, error) {}
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			key, err := o.KeyFunc(r)
			if err != nil {
				onError(r, err)
				http.Error(w, "unable to identify client", http.StatusBadRequest)
				return
			}

			res, err := l.AllowN(r.Context(), key, o.Cost(r))
			switch {
			case err == nil:
			case errors.Is(err, context.Canceled) && r.Context().Err() != nil:
				// The client went away; there is nobody to respond to.
				return
			case errors.Is(err, ratelimit.ErrCostExceedsLimit), errors.Is(err, ratelimit.ErrInvalidCost):
				// Retrying cannot help, so no Retry-After.
				onError(r, err)
				w.Header().Set("RateLimit-Limit", strconv.Itoa(res.Limit))
				o.DeniedHandler.ServeHTTP(w, r)
				return
			default:
				onError(r, err)
				if o.FailOpen {
					next.ServeHTTP(w, r)
				} else {
					http.Error(w, "rate limiter unavailable", http.StatusServiceUnavailable)
				}
				return
			}

			h := w.Header()
			h.Set("RateLimit-Limit", strconv.Itoa(res.Limit))
			h.Set("RateLimit-Remaining", strconv.Itoa(res.Remaining))
			h.Set("RateLimit-Reset", strconv.FormatInt(ceilSeconds(res.ResetAfter), 10))
			if !res.Allowed {
				h.Set("Retry-After", strconv.FormatInt(max(ceilSeconds(res.RetryAfter), 1), 10))
				o.DeniedHandler.ServeHTTP(w, r)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// ceilSeconds rounds d up to whole seconds so clients never retry too early.
func ceilSeconds(d time.Duration) int64 {
	if d <= 0 {
		return 0
	}
	return int64((d + time.Second - 1) / time.Second)
}
