package httpmw

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	"ratewarden"
	"ratewarden/ratewardentest"
)

// stub returns a fixed decision and records what it was asked.
type stub struct {
	mu    sync.Mutex
	d     ratewarden.Decision
	keys  []string
	costs []int
}

func (s *stub) AllowN(key string, n int) ratewarden.Decision {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.keys = append(s.keys, key)
	s.costs = append(s.costs, n)
	return s.d
}

var okHandler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok"))
})

func serve(t *testing.T, cfg Config, r *http.Request) *httptest.ResponseRecorder {
	t.Helper()
	mw, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	rec := httptest.NewRecorder()
	mw(okHandler).ServeHTTP(rec, r)
	return rec
}

func req() *http.Request {
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.RemoteAddr = "203.0.113.7:5555"
	return r
}

func TestNewRequiresLimiter(t *testing.T) {
	if _, err := New(Config{}); err == nil {
		t.Fatal("expected error for missing limiter")
	}
}

func TestAllowedSetsHeadersAndCallsNext(t *testing.T) {
	s := &stub{d: ratewarden.Decision{Allowed: true, Limit: 10, Remaining: 7, ResetAfter: 1500 * time.Millisecond}}
	rec := serve(t, Config{Limiter: s}, req())
	if rec.Code != 200 || rec.Body.String() != "ok" {
		t.Fatalf("got %d %q", rec.Code, rec.Body.String())
	}
	for h, want := range map[string]string{
		"RateLimit-Limit": "10", "RateLimit-Remaining": "7", "RateLimit-Reset": "2", "Retry-After": "",
	} {
		if got := rec.Header().Get(h); got != want {
			t.Errorf("%s = %q, want %q", h, got, want)
		}
	}
	if len(s.keys) != 1 || s.keys[0] != "ip:203.0.113.7" || s.costs[0] != 1 {
		t.Errorf("limiter called with %v %v", s.keys, s.costs)
	}
}

func TestDeniedReturns429WithRetryAfter(t *testing.T) {
	cases := []struct {
		retry time.Duration
		want  string
	}{
		{1200 * time.Millisecond, "2"}, // rounded up
		{100 * time.Millisecond, "1"},  // never below 1
		{time.Nanosecond, "1"},
		{30 * time.Second, "30"},
	}
	for _, c := range cases {
		s := &stub{d: ratewarden.Decision{Limit: 5, RetryAfter: c.retry, ResetAfter: 30 * time.Second}}
		rec := serve(t, Config{Limiter: s}, req())
		if rec.Code != http.StatusTooManyRequests {
			t.Fatalf("code %d, want 429", rec.Code)
		}
		if got := rec.Header().Get("Retry-After"); got != c.want {
			t.Errorf("retry %v: Retry-After = %q, want %q", c.retry, got, c.want)
		}
		if rec.Header().Get("RateLimit-Remaining") != "0" || rec.Header().Get("RateLimit-Reset") != "30" {
			t.Errorf("rate limit headers wrong on 429: %v", rec.Header())
		}
		if rec.Body.String() == "ok" {
			t.Fatal("next handler ran for a denied request")
		}
	}
}

func TestNeverOmitsRetryAfter(t *testing.T) {
	s := &stub{d: ratewarden.Decision{Limit: 5, Remaining: 5, RetryAfter: ratewarden.Never}}
	rec := serve(t, Config{Limiter: s}, req())
	if rec.Code != 429 || rec.Header().Get("Retry-After") != "" {
		t.Fatalf("code %d Retry-After %q", rec.Code, rec.Header().Get("Retry-After"))
	}
}

func TestDisableHeaders(t *testing.T) {
	s := &stub{d: ratewarden.Decision{Limit: 5, RetryAfter: time.Second}}
	rec := serve(t, Config{Limiter: s, DisableHeaders: true}, req())
	if rec.Header().Get("RateLimit-Limit") != "" {
		t.Fatal("RateLimit headers sent despite DisableHeaders")
	}
	if rec.Header().Get("Retry-After") != "1" {
		t.Fatal("Retry-After must still be sent on 429")
	}
}

func TestCustomOnLimited(t *testing.T) {
	s := &stub{d: ratewarden.Decision{Limit: 5, RetryAfter: time.Second}}
	rec := serve(t, Config{Limiter: s, OnLimited: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte(`{"error":"slow down"}`))
	})}, req())
	if rec.Code != 503 || rec.Body.String() != `{"error":"slow down"}` {
		t.Fatalf("got %d %q", rec.Code, rec.Body.String())
	}
	if rec.Header().Get("Retry-After") != "1" {
		t.Fatal("headers must be set before OnLimited runs")
	}
}

func TestEmptyKeyBypassesLimiter(t *testing.T) {
	s := &stub{d: ratewarden.Decision{Limit: 5, RetryAfter: time.Second}}
	rec := serve(t, Config{Limiter: s, KeyFunc: func(*http.Request) string { return "" }}, req())
	if rec.Code != 200 || len(s.keys) != 0 {
		t.Fatalf("code %d, limiter calls %d", rec.Code, len(s.keys))
	}
	if rec.Header().Get("RateLimit-Limit") != "" {
		t.Fatal("headers set for unlimited request")
	}
}

func TestCostAndOnDecision(t *testing.T) {
	s := &stub{d: ratewarden.Decision{Allowed: true, Limit: 100, Remaining: 90}}
	var seenKey string
	var seen ratewarden.Decision
	serve(t, Config{
		Limiter:    s,
		KeyFunc:    func(*http.Request) string { return "custom" },
		Cost:       func(r *http.Request) int { return len(r.URL.Path) },
		OnDecision: func(_ *http.Request, k string, d ratewarden.Decision) { seenKey, seen = k, d },
	}, httptest.NewRequest(http.MethodGet, "/abcd", nil))
	if s.costs[0] != 5 || s.keys[0] != "custom" {
		t.Errorf("limiter got key %q cost %d", s.keys[0], s.costs[0])
	}
	if seenKey != "custom" || seen.Remaining != 90 {
		t.Errorf("OnDecision got %q %+v", seenKey, seen)
	}
}

// End to end with a real limiter: burst of 2, then 429 until refill.
func TestIntegrationWithTokenBucket(t *testing.T) {
	clk := ratewardentest.NewClock(time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC))
	l, err := ratewarden.NewTokenBucket(1, time.Second, 2, ratewarden.WithClock(clk))
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	mw, _ := New(Config{Limiter: l})
	h := mw(okHandler)
	do := func(addr string) *httptest.ResponseRecorder {
		r := req()
		r.RemoteAddr = addr
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, r)
		return rec
	}
	codes := []int{do("1.1.1.1:1").Code, do("1.1.1.1:2").Code, do("1.1.1.1:3").Code}
	if codes[0] != 200 || codes[1] != 200 || codes[2] != 429 {
		t.Fatalf("codes %v, want [200 200 429]", codes)
	}
	if do("2.2.2.2:1").Code != 200 {
		t.Fatal("other client limited")
	}
	rec := do("1.1.1.1:4")
	if ra, _ := strconv.Atoi(rec.Header().Get("Retry-After")); ra != 1 {
		t.Fatalf("Retry-After %q", rec.Header().Get("Retry-After"))
	}
	clk.Advance(time.Second)
	if do("1.1.1.1:5").Code != 200 {
		t.Fatal("not allowed after refill")
	}
}

func TestCeilSeconds(t *testing.T) {
	for d, want := range map[time.Duration]int64{
		-time.Second: 0, 0: 0, 1: 1, time.Second: 1, time.Second + 1: 2, 90 * time.Second: 90,
	} {
		if got := ceilSeconds(d); got != want {
			t.Errorf("ceilSeconds(%v) = %d, want %d", d, got, want)
		}
	}
}
