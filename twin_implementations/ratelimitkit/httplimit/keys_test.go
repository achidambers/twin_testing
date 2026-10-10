package httplimit_test

import (
	"errors"
	"net/http/httptest"
	"net/netip"
	"testing"

	"github.com/example/ratelimitkit/httplimit"
)

func TestClientIP(t *testing.T) {
	trusted := []netip.Prefix{netip.MustParsePrefix("10.0.0.0/8"), netip.MustParsePrefix("fd00::/8")}
	const v6 = "[2001:db8:1:2:3:4:5:6]:443"
	cases := []struct {
		name    string
		remote  string
		xff     []string
		trusted []netip.Prefix
		v6Bits  int
		want    string
	}{
		{"direct ipv4", "203.0.113.7:5000", nil, nil, 0, "203.0.113.7"},
		{"xff ignored without trusted proxies", "203.0.113.7:5000", []string{"1.1.1.1"}, nil, 0, "203.0.113.7"},
		{"xff ignored from untrusted peer", "203.0.113.7:5000", []string{"1.1.1.1"}, trusted, 0, "203.0.113.7"},
		{"client behind trusted proxy", "10.0.0.2:80", []string{"198.51.100.4"}, trusted, 0, "198.51.100.4"},
		{"client-supplied entries skipped", "10.0.0.2:80", []string{"6.6.6.6, 198.51.100.4"}, trusted, 0, "198.51.100.4"},
		{"multiple trusted hops and headers", "10.0.0.2:80", []string{"198.51.100.4, 10.1.1.1", "10.2.2.2"}, trusted, 0, "198.51.100.4"},
		{"all hops trusted", "10.0.0.2:80", []string{"10.9.9.9"}, trusted, 0, "10.9.9.9"},
		{"trusted proxy without header", "10.0.0.2:80", nil, trusted, 0, "10.0.0.2"},
		{"malformed hop stops the walk", "10.0.0.2:80", []string{"garbage, 10.1.1.1"}, trusted, 0, "10.1.1.1"},
		{"ipv4-mapped ipv6 unmapped", "[::ffff:203.0.113.7]:1", nil, nil, 0, "203.0.113.7"},
		{"ipv6 grouped by /64", v6, nil, nil, 0, "2001:db8:1:2::/64"},
		{"ipv6 custom prefix", v6, nil, nil, 48, "2001:db8:1::/48"},
		{"ipv6 per address", v6, nil, nil, 128, "2001:db8:1:2:3:4:5:6"},
		{"ipv6 zone dropped", "[fe80::1%eth0]:1", nil, nil, 0, "fe80::/64"},
		{"trusted ipv6 proxy", "[fd00::1]:80", []string{"2001:db8::9"}, trusted, 0, "2001:db8::/64"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := httptest.NewRequest("GET", "/", nil)
			r.RemoteAddr = c.remote
			for _, v := range c.xff {
				r.Header.Add("X-Forwarded-For", v)
			}
			got, err := httplimit.ClientIP(httplimit.IPOptions{TrustedProxies: c.trusted, IPv6PrefixLen: c.v6Bits})(r)
			if err != nil || got != c.want {
				t.Fatalf("got %q, %v; want %q", got, err, c.want)
			}
		})
	}
}

func TestClientIPBadRemoteAddr(t *testing.T) {
	r := httptest.NewRequest("GET", "/", nil)
	r.RemoteAddr = "not-an-address"
	if _, err := httplimit.ClientIP(httplimit.IPOptions{})(r); !errors.Is(err, httplimit.ErrNoKey) {
		t.Fatalf("err = %v, want ErrNoKey", err)
	}
}

func TestClientIPRejectsBadPrefixLen(t *testing.T) {
	for _, bits := range []int{-1, 129} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("IPv6PrefixLen %d did not panic", bits)
				}
			}()
			httplimit.ClientIP(httplimit.IPOptions{IPv6PrefixLen: bits})
		}()
	}
}

func TestClientIPCopiesTrustedProxies(t *testing.T) {
	trusted := []netip.Prefix{netip.MustParsePrefix("10.0.0.0/8")}
	key := httplimit.ClientIP(httplimit.IPOptions{TrustedProxies: trusted})
	trusted[0] = netip.MustParsePrefix("192.0.2.0/24")
	r := httptest.NewRequest("GET", "/", nil)
	r.RemoteAddr = "10.0.0.2:80"
	r.Header.Set("X-Forwarded-For", "198.51.100.4")
	if got, _ := key(r); got != "198.51.100.4" {
		t.Fatalf("got %q; mutating the caller's slice changed the trusted set", got)
	}
}

func TestHeaderKey(t *testing.T) {
	key := httplimit.Header("X-API-Key")
	r := httptest.NewRequest("GET", "/", nil)
	if _, err := key(r); !errors.Is(err, httplimit.ErrNoKey) {
		t.Fatalf("missing header: err = %v, want ErrNoKey", err)
	}
	r.Header.Set("X-API-Key", "   ")
	if _, err := key(r); !errors.Is(err, httplimit.ErrNoKey) {
		t.Fatalf("blank header: err = %v, want ErrNoKey", err)
	}
	r.Header.Set("X-API-Key", " abc123 ")
	if got, err := key(r); err != nil || got != "abc123" {
		t.Fatalf("got %q, %v; want abc123", got, err)
	}
}
