package httpmw

import (
	"net"
	"net/http"
	"net/netip"
	"strings"
)

// Keys are namespaced ("ip:", "hdr:<name>:") so different key functions
// combined with FirstNonEmpty can never collide.

// KeyByIP keys on the connection's peer address. IPv6 addresses are grouped
// by /64, since a single client typically controls a whole /64. Use
// KeyByForwardedFor instead when behind a reverse proxy.
func KeyByIP(r *http.Request) string {
	if a, ok := peerAddr(r); ok {
		return ipKey(a)
	}
	return "ip:" + r.RemoteAddr // e.g. a unix socket; still a stable key
}

// KeyByForwardedFor returns a key func for servers behind reverse proxies.
// It trusts X-Forwarded-For only as far as the chain of trusted proxies
// goes: starting from the peer, it walks the header right to left, skipping
// addresses inside trusted, and keys on the first address outside it. A
// client can prepend anything it likes to the header, but cannot get past
// the entry the first trusted proxy appended for it.
//
// If the peer itself is not trusted, the header is ignored entirely.
func KeyByForwardedFor(trusted ...netip.Prefix) func(*http.Request) string {
	isTrusted := func(a netip.Addr) bool {
		for _, p := range trusted {
			if p.Contains(a) {
				return true
			}
		}
		return false
	}
	return func(r *http.Request) string {
		peer, ok := peerAddr(r)
		if !ok {
			return KeyByIP(r)
		}
		if !isTrusted(peer) {
			return ipKey(peer)
		}
		hops := forwardedHops(r.Header.Values("X-Forwarded-For"))
		client := peer
		for i := len(hops) - 1; i >= 0; i-- {
			a, err := netip.ParseAddr(hops[i])
			if err != nil {
				break // garbage behind a trusted proxy: use the nearest valid hop
			}
			client = a.Unmap()
			if !isTrusted(client) {
				break
			}
		}
		return ipKey(client)
	}
}

// KeyByHeader keys on a request header's value, e.g. an API key. Requests
// without the header get "" (unlimited); wrap with FirstNonEmpty to fall
// back to an IP key.
func KeyByHeader(name string) func(*http.Request) string {
	prefix := "hdr:" + http.CanonicalHeaderKey(name) + ":"
	return func(r *http.Request) string {
		if v := strings.TrimSpace(r.Header.Get(name)); v != "" {
			return prefix + v
		}
		return ""
	}
}

// FirstNonEmpty returns the first non-empty key produced by fns.
func FirstNonEmpty(fns ...func(*http.Request) string) func(*http.Request) string {
	return func(r *http.Request) string {
		for _, f := range fns {
			if k := f(r); k != "" {
				return k
			}
		}
		return ""
	}
}

func peerAddr(r *http.Request) (netip.Addr, bool) {
	host := r.RemoteAddr
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	a, err := netip.ParseAddr(host)
	if err != nil {
		return netip.Addr{}, false
	}
	return a.Unmap(), true
}

func ipKey(a netip.Addr) string {
	a = a.WithZone("")
	if a.Is6() {
		p, _ := a.Prefix(64)
		return "ip:" + p.String()
	}
	return "ip:" + a.String()
}

func forwardedHops(values []string) []string {
	var hops []string
	for _, v := range values {
		for _, h := range strings.Split(v, ",") {
			if h = strings.TrimSpace(h); h != "" {
				hops = append(hops, h)
			}
		}
	}
	return hops
}
