// Command ratelimit-demo serves a hello-world endpoint behind the rate
// limiting middleware.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/netip"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/example/ratelimitkit/httplimit"
	"github.com/example/ratelimitkit/ratelimit"
)

func main() {
	addr := flag.String("addr", ":8080", "listen address")
	algo := flag.String("algorithm", "gcra", "gcra | sliding-window")
	rate := flag.Int("rate", 10, "events allowed per period")
	period := flag.Duration("period", time.Second, "rate period")
	burst := flag.Int("burst", 0, "burst size for gcra (0 = rate)")
	maxKeys := flag.Int("max-keys", 1_000_000, "maximum tracked clients (0 = unlimited)")
	trusted := flag.String("trusted-proxies", "", "comma-separated CIDRs allowed to set X-Forwarded-For")
	failOpen := flag.Bool("fail-open", false, "allow requests when the limiter errors")
	flag.Parse()

	var proxies []netip.Prefix
	for _, s := range strings.Split(*trusted, ",") {
		if s = strings.TrimSpace(s); s == "" {
			continue
		}
		p, err := netip.ParsePrefix(s)
		if err != nil {
			log.Fatalf("bad -trusted-proxies entry %q: %v", s, err)
		}
		proxies = append(proxies, p)
	}

	limit := ratelimit.Limit{Rate: *rate, Period: *period, Burst: *burst}
	opts := []ratelimit.Option{ratelimit.WithMaxKeys(*maxKeys)}
	var (
		limiter *ratelimit.MemoryLimiter
		err     error
	)
	switch *algo {
	case "gcra":
		limiter, err = ratelimit.NewGCRA(limit, opts...)
	case "sliding-window":
		limiter, err = ratelimit.NewSlidingWindow(limit, opts...)
	default:
		err = fmt.Errorf("unknown algorithm %q", *algo)
	}
	if err != nil {
		log.Fatal(err)
	}
	defer limiter.Close()

	mw := httplimit.Middleware(limiter, httplimit.Options{
		KeyFunc:  httplimit.ClientIP(httplimit.IPOptions{TrustedProxies: proxies}),
		FailOpen: *failOpen,
		OnError:  func(r *http.Request, err error) { log.Printf("rate limit error for %s: %v", r.RemoteAddr, err) },
	})
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "hello")
	})

	srv := &http.Server{
		Addr:              *addr,
		Handler:           mw(mux),
		ReadHeaderTimeout: 10 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		log.Printf("listening on %s, %s limit %v", *addr, *algo, limiter.Limit())
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatal(err)
		}
	}()
	<-ctx.Done()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("shutdown: %v", err)
	}
}
