package httpmw

import (
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"testing"
)

func reqFrom(remote string, xff ...string) *http.Request {
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.RemoteAddr = remote
	for _, v := range xff {
		r.Header.Add("X-Forwarded-For", v)
	}
	return r
}

func TestKeyByIP(t *testing.T) {
	cases := map[string]string{
		"203.0.113.7:1234":                  "ip:203.0.113.7",
		"[2001:db8:1:2:3:4:5:6]:443":        "ip:2001:db8:1:2::/64",
		"[2001:db8:1:2:ffff:ffff:ffff:1]:1": "ip:2001:db8:1:2::/64", // same /64
		"[::ffff:198.51.100.1]:80":          "ip:198.51.100.1",      // v4-mapped
		"[fe80::1%en0]:80":                  "ip:fe80::/64",         // zone dropped
		"198.51.100.2":                      "ip:198.51.100.2",      // no port
		"@":                                 "ip:@",                 // unix socket
	}
	for remote, want := range cases {
		if got := KeyByIP(reqFrom(remote)); got != want {
			t.Errorf("KeyByIP(%q) = %q, want %q", remote, got, want)
		}
	}
}

func TestKeyByForwardedFor(t *testing.T) {
	trusted := []netip.Prefix{netip.MustParsePrefix("10.0.0.0/8"), netip.MustParsePrefix("fd00::/8")}
	key := KeyByForwardedFor(trusted...)
	cases := []struct {
		name   string
		remote string
		xff    []string
		want   string
	}{
		{"untrusted peer ignores header", "203.0.113.9:1", []string{"1.2.3.4"}, "ip:203.0.113.9"},
		{"one trusted proxy", "10.0.0.1:1", []string{"198.51.100.5"}, "ip:198.51.100.5"},
		{"chain of trusted proxies", "10.0.0.1:1", []string{"198.51.100.5, 10.1.1.1, 10.2.2.2"}, "ip:198.51.100.5"},
		{"client-spoofed prefix ignored", "10.0.0.1:1", []string{"6.6.6.6, 7.7.7.7, 198.51.100.5"}, "ip:198.51.100.5"},
		{"spoofed trusted address in prefix", "10.0.0.1:1", []string{"10.9.9.9, 198.51.100.5"}, "ip:198.51.100.5"},
		{"multiple header lines", "10.0.0.1:1", []string{"6.6.6.6", "198.51.100.5, 10.3.3.3"}, "ip:198.51.100.5"},
		{"no header falls back to peer", "10.0.0.1:1", nil, "ip:10.0.0.1"},
		{"all hops trusted", "10.0.0.1:1", []string{"10.5.5.5, 10.6.6.6"}, "ip:10.5.5.5"},
		{"garbage stops the walk", "10.0.0.1:1", []string{"198.51.100.5, not-an-ip, 10.7.7.7"}, "ip:10.7.7.7"},
		{"empty entries skipped", "10.0.0.1:1", []string{" , 198.51.100.5 ,, "}, "ip:198.51.100.5"},
		{"ipv6 client grouped by /64", "[fd00::1]:1", []string{"2001:db8:a:b::99"}, "ip:2001:db8:a:b::/64"},
		{"v4-mapped hop", "10.0.0.1:1", []string{"::ffff:198.51.100.5"}, "ip:198.51.100.5"},
		{"unparsable peer", "@", []string{"1.2.3.4"}, "ip:@"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := key(reqFrom(c.remote, c.xff...)); got != c.want {
				t.Fatalf("got %q, want %q", got, c.want)
			}
		})
	}
}

func TestKeyByForwardedForNoTrustedIsKeyByIP(t *testing.T) {
	r := reqFrom("10.0.0.1:1", "1.2.3.4")
	if got := KeyByForwardedFor()(r); got != "ip:10.0.0.1" {
		t.Fatalf("got %q", got)
	}
}

func TestKeyByHeaderAndFirstNonEmpty(t *testing.T) {
	byKey := KeyByHeader("x-api-key")
	r := reqFrom("203.0.113.7:1")
	if got := byKey(r); got != "" {
		t.Fatalf("missing header gave %q", got)
	}
	r.Header.Set("X-Api-Key", "  secret  ")
	if got := byKey(r); got != "hdr:X-Api-Key:secret" {
		t.Fatalf("got %q", got)
	}

	combined := FirstNonEmpty(byKey, KeyByIP)
	if got := combined(reqFrom("203.0.113.7:1")); got != "ip:203.0.113.7" {
		t.Fatalf("fallback gave %q", got)
	}
	if got := combined(r); got != "hdr:X-Api-Key:secret" {
		t.Fatalf("header gave %q", got)
	}
	if got := FirstNonEmpty()(r); got != "" {
		t.Fatalf("empty FirstNonEmpty gave %q", got)
	}
}

func TestKeyNamespacesDoNotCollide(t *testing.T) {
	r := reqFrom("1.2.3.4:1")
	r.Header.Set("X-Api-Key", "1.2.3.4")
	if KeyByHeader("X-Api-Key")(r) == KeyByIP(r) {
		t.Fatal("header value equal to an IP collides with the IP key")
	}
}

func FuzzKeyByForwardedFor(f *testing.F) {
	f.Add("10.0.0.1:1", "1.2.3.4, 10.0.0.2")
	f.Add("[fd00::1]:9", "::1,,garbage, 2001:db8::1")
	f.Add("@", "")
	trusted := []netip.Prefix{netip.MustParsePrefix("10.0.0.0/8"), netip.MustParsePrefix("fd00::/8")}
	key := KeyByForwardedFor(trusted...)
	f.Fuzz(func(t *testing.T, remote, xff string) {
		r := reqFrom(remote)
		r.Header.Set("X-Forwarded-For", xff)
		k := key(r)
		if !strings.HasPrefix(k, "ip:") {
			t.Fatalf("key %q lacks ip: prefix", k)
		}
		// An untrusted peer's key never depends on the header.
		if a, ok := peerAddr(r); !ok || (!trusted[0].Contains(a) && !trusted[1].Contains(a)) {
			if want := KeyByIP(r); k != want {
				t.Fatalf("untrusted peer %q: header changed key %q -> %q", remote, want, k)
			}
		}
	})
}
