package httplimit_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/example/ratelimitkit/httplimit"
	"github.com/example/ratelimitkit/ratelimit"
)

// limiterFunc adapts a function to ratelimit.Limiter.
type limiterFunc func(ctx context.Context, key string, n int) (ratelimit.Result, error)

func (f limiterFunc) AllowN(ctx context.Context, key string, n int) (ratelimit.Result, error) {
	return f(ctx, key, n)
}

// countingHandler records how many requests reached the protected handler.
type countingHandler struct{ calls atomic.Int64 }

func (h *countingHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.calls.Add(1)
	io.WriteString(w, "ok")
}

func newGCRA(t *testing.T, l ratelimit.Limit) *ratelimit.MemoryLimiter {
	t.Helper()
	clock := ratelimit.NewManualClock(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
	lim, err := ratelimit.NewGCRA(l, ratelimit.WithClock(clock), ratelimit.WithCleanupInterval(0))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { lim.Close() })
	return lim
}

func do(h http.Handler, remote string) *httptest.ResponseRecorder {
	r := httptest.NewRequest("GET", "/", nil)
	r.RemoteAddr = remote
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func checkHeaders(t *testing.T, w *httptest.ResponseRecorder, want map[string]string) {
	t.Helper()
	for k, v := range want {
		if got := w.Header().Get(k); got != v {
			t.Errorf("header %s = %q, want %q", k, got, v)
		}
	}
}

func TestAllowedRequestGetsHeaders(t *testing.T) {
	next := &countingHandler{}
	h := httplimit.Middleware(newGCRA(t, ratelimit.PerMinute(2)), httplimit.Options{})(next)
	w := do(h, "192.0.2.1:1234")
	if w.Code != http.StatusOK || next.calls.Load() != 1 {
		t.Fatalf("code %d, handler calls %d", w.Code, next.calls.Load())
	}
	checkHeaders(t, w, map[string]string{
		"RateLimit-Limit": "2", "RateLimit-Remaining": "1", "RateLimit-Reset": "30", "Retry-After": "",
	})
}

func TestDeniedRequestGets429(t *testing.T) {
	next := &countingHandler{}
	h := httplimit.Middleware(newGCRA(t, ratelimit.PerMinute(2)), httplimit.Options{})(next)
	do(h, "192.0.2.1:1234")
	do(h, "192.0.2.1:1234")
	w := do(h, "192.0.2.1:1234")
	if w.Code != http.StatusTooManyRequests || next.calls.Load() != 2 {
		t.Fatalf("code %d, handler calls %d; want 429 and 2", w.Code, next.calls.Load())
	}
	checkHeaders(t, w, map[string]string{"RateLimit-Remaining": "0", "Retry-After": "30", "RateLimit-Reset": "60"})

	if w := do(h, "192.0.2.2:1234"); w.Code != http.StatusOK {
		t.Fatalf("a different client was limited: %d", w.Code)
	}
}

func TestHeaderSecondsRoundUp(t *testing.T) {
	cases := []struct {
		retry, reset         time.Duration
		wantRetry, wantReset string
	}{
		{1200 * time.Millisecond, 1500 * time.Millisecond, "2", "2"},
		{time.Nanosecond, time.Nanosecond, "1", "1"},
		{time.Second, 3 * time.Second, "1", "3"},
	}
	for _, c := range cases {
		lim := limiterFunc(func(context.Context, string, int) (ratelimit.Result, error) {
			return ratelimit.Result{Limit: 5, RetryAfter: c.retry, ResetAfter: c.reset}, nil
		})
		w := do(httplimit.Middleware(lim, httplimit.Options{})(&countingHandler{}), "192.0.2.1:1")
		checkHeaders(t, w, map[string]string{"Retry-After": c.wantRetry, "RateLimit-Reset": c.wantReset})
	}
}

func TestLimiterErrorFailsClosedByDefault(t *testing.T) {
	storeDown := errors.New("store unavailable")
	for _, err := range []error{storeDown, ratelimit.ErrTooManyKeys, ratelimit.ErrClosed} {
		lim := limiterFunc(func(context.Context, string, int) (ratelimit.Result, error) {
			return ratelimit.Result{}, err
		})
		next := &countingHandler{}
		var reported error
		h := httplimit.Middleware(lim, httplimit.Options{OnError: func(_ *http.Request, e error) { reported = e }})(next)
		w := do(h, "192.0.2.1:1")
		if w.Code != http.StatusServiceUnavailable || next.calls.Load() != 0 || !errors.Is(reported, err) {
			t.Errorf("%v: code %d, calls %d, reported %v; want 503, 0, the error", err, w.Code, next.calls.Load(), reported)
		}
	}
}

func TestLimiterErrorFailOpen(t *testing.T) {
	lim := limiterFunc(func(context.Context, string, int) (ratelimit.Result, error) {
		return ratelimit.Result{}, errors.New("store unavailable")
	})
	next := &countingHandler{}
	var reported int
	h := httplimit.Middleware(lim, httplimit.Options{FailOpen: true, OnError: func(*http.Request, error) { reported++ }})(next)
	if w := do(h, "192.0.2.1:1"); w.Code != http.StatusOK || next.calls.Load() != 1 || reported != 1 {
		t.Fatalf("code %d, calls %d, reported %d; want 200, 1, 1", w.Code, next.calls.Load(), reported)
	}
}

func TestCostExceedingCapacity(t *testing.T) {
	next := &countingHandler{}
	var reported error
	h := httplimit.Middleware(newGCRA(t, ratelimit.PerSecond(5)), httplimit.Options{
		Cost:     func(*http.Request) int { return 10 },
		FailOpen: true, // must not apply: this is a client problem, not an outage
		OnError:  func(_ *http.Request, e error) { reported = e },
	})(next)
	w := do(h, "192.0.2.1:1")
	if w.Code != http.StatusTooManyRequests || next.calls.Load() != 0 {
		t.Fatalf("code %d, calls %d; want 429, 0", w.Code, next.calls.Load())
	}
	checkHeaders(t, w, map[string]string{"RateLimit-Limit": "5", "Retry-After": ""})
	if !errors.Is(reported, ratelimit.ErrCostExceedsLimit) {
		t.Errorf("reported %v, want ErrCostExceedsLimit", reported)
	}
}

func TestCostFunction(t *testing.T) {
	h := httplimit.Middleware(newGCRA(t, ratelimit.PerMinute(5)), httplimit.Options{
		Cost: func(*http.Request) int { return 3 },
	})(&countingHandler{})
	checkHeaders(t, do(h, "192.0.2.1:1"), map[string]string{"RateLimit-Remaining": "2"})
	if w := do(h, "192.0.2.1:1"); w.Code != http.StatusTooManyRequests {
		t.Fatalf("second cost-3 request: code %d, want 429", w.Code)
	}
}

func TestKeyErrorRejectsEvenWhenFailOpen(t *testing.T) {
	var limiterCalls int
	lim := limiterFunc(func(context.Context, string, int) (ratelimit.Result, error) {
		limiterCalls++
		return ratelimit.Result{Allowed: true}, nil
	})
	next := &countingHandler{}
	h := httplimit.Middleware(lim, httplimit.Options{KeyFunc: httplimit.Header("X-API-Key"), FailOpen: true})(next)
	w := do(h, "192.0.2.1:1")
	if w.Code != http.StatusBadRequest || next.calls.Load() != 0 || limiterCalls != 0 {
		t.Fatalf("code %d, handler calls %d, limiter calls %d; want 400, 0, 0", w.Code, next.calls.Load(), limiterCalls)
	}
}

func TestCustomDeniedHandler(t *testing.T) {
	denied := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusTooManyRequests)
		io.WriteString(w, `{"error":"slow down"}`)
	})
	h := httplimit.Middleware(newGCRA(t, ratelimit.PerMinute(1)), httplimit.Options{DeniedHandler: denied})(&countingHandler{})
	do(h, "192.0.2.1:1")
	w := do(h, "192.0.2.1:1")
	if w.Body.String() != `{"error":"slow down"}` || w.Header().Get("Retry-After") != "60" {
		t.Fatalf("body %q, Retry-After %q", w.Body.String(), w.Header().Get("Retry-After"))
	}
}

func TestClientDisconnected(t *testing.T) {
	lim, err := ratelimit.NewGCRA(ratelimit.PerSecond(5), ratelimit.WithCleanupInterval(0))
	if err != nil {
		t.Fatal(err)
	}
	defer lim.Close()
	next := &countingHandler{}
	var reported int
	h := httplimit.Middleware(lim, httplimit.Options{OnError: func(*http.Request, error) { reported++ }})(next)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r := httptest.NewRequest("GET", "/", nil).WithContext(ctx)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if next.calls.Load() != 0 || reported != 0 || w.Body.Len() != 0 {
		t.Fatalf("calls %d, reported %d, body %q; want the request dropped quietly", next.calls.Load(), reported, w.Body.String())
	}
}

func TestNilLimiterPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("Middleware(nil) did not panic")
		}
	}()
	httplimit.Middleware(nil, httplimit.Options{})
}

func TestEndToEnd(t *testing.T) {
	lim, err := ratelimit.NewGCRA(ratelimit.PerMinute(3))
	if err != nil {
		t.Fatal(err)
	}
	defer lim.Close()
	srv := httptest.NewServer(httplimit.Middleware(lim, httplimit.Options{})(&countingHandler{}))
	defer srv.Close()

	want := []int{200, 200, 200, 429, 429}
	for i, code := range want {
		resp, err := http.Get(srv.URL)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != code {
			t.Fatalf("request %d: status %d, want %d", i, resp.StatusCode, code)
		}
		if code == 429 && resp.Header.Get("Retry-After") == "" {
			t.Fatalf("request %d: 429 without Retry-After", i)
		}
	}
}
