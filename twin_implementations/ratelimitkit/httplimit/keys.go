package httplimit

import (
	"errors"
	"fmt"
	"net/http"
	"net/netip"
	"strings"
)

// ErrNoKey is returned by a KeyFunc that cannot identify the client.
var ErrNoKey = errors.New("httplimit: cannot determine rate limit key")

// KeyFunc extracts the rate limit key from a request.
type KeyFunc func(*http.Request) (string, error)

// IPOptions configures ClientIP.
type IPOptions struct {
	// TrustedProxies lists the networks of reverse proxies in front of this
	// server. X-Forwarded-For is only honoured for hops inside them; without
	// any, the header is ignored because clients can set it to anything.
	TrustedProxies []netip.Prefix
	// IPv6PrefixLen groups IPv6 clients by network, since a single client
	// usually controls a whole /64. Zero means 64; 128 keys each address.
	IPv6PrefixLen int
}

// ClientIP returns a KeyFunc keyed on the client's IP address. It panics if
// IPv6PrefixLen is outside 0..128.
func ClientIP(o IPOptions) KeyFunc {
	bits := o.IPv6PrefixLen
	if bits == 0 {
		bits = 64
	}
	if bits < 1 || bits > 128 {
		panic(fmt.Sprintf("httplimit: IPv6PrefixLen %d out of range", o.IPv6PrefixLen))
	}
	trusted := append([]netip.Prefix(nil), o.TrustedProxies...)
	return func(r *http.Request) (string, error) {
		ip, err := clientAddr(r, trusted)
		if err != nil {
			return "", err
		}
		if ip.Is6() && bits < 128 {
			p, err := ip.Prefix(bits)
			if err != nil {
				return "", fmt.Errorf("%w: %v", ErrNoKey, err)
			}
			return p.String(), nil
		}
		return ip.String(), nil
	}
}

// clientAddr walks X-Forwarded-For from right to left, starting at the
// connection's peer, and returns the first address that is not a trusted
// proxy. Entries left of that address are client-controlled and ignored.
func clientAddr(r *http.Request, trusted []netip.Prefix) (netip.Addr, error) {
	ap, err := netip.ParseAddrPort(r.RemoteAddr)
	if err != nil {
		return netip.Addr{}, fmt.Errorf("%w: bad RemoteAddr %q", ErrNoKey, r.RemoteAddr)
	}
	ip := normalize(ap.Addr())
	if !isTrusted(ip, trusted) {
		return ip, nil
	}
	hops := forwardedFor(r.Header)
	for i := len(hops) - 1; i >= 0; i-- {
		hop, err := netip.ParseAddr(hops[i])
		if err != nil {
			// Can't see past a malformed hop; key on the last proxy reached.
			return ip, nil
		}
		ip = normalize(hop)
		if !isTrusted(ip, trusted) {
			return ip, nil
		}
	}
	return ip, nil
}

func forwardedFor(h http.Header) []string {
	var hops []string
	for _, v := range h.Values("X-Forwarded-For") {
		for _, part := range strings.Split(v, ",") {
			if part = strings.TrimSpace(part); part != "" {
				hops = append(hops, part)
			}
		}
	}
	return hops
}

func normalize(ip netip.Addr) netip.Addr { return ip.Unmap().WithZone("") }

func isTrusted(ip netip.Addr, trusted []netip.Prefix) bool {
	for _, p := range trusted {
		if p.Contains(ip) {
			return true
		}
	}
	return false
}

// Header returns a KeyFunc keyed on the value of the named header, such as
// an API key. Requests without the header fail with ErrNoKey.
func Header(name string) KeyFunc {
	return func(r *http.Request) (string, error) {
		v := strings.TrimSpace(r.Header.Get(name))
		if v == "" {
			return "", fmt.Errorf("%w: missing %s header", ErrNoKey, name)
		}
		return v, nil
	}
}
