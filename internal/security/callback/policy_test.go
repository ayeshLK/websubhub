// Copyright 2026 Ayesh Almeida
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package callback

import (
	"context"
	"net/netip"
	"testing"
)

func TestSecureDefaultRejectsUnsafeCallbacks(t *testing.T) {
	policy, err := New([]uint16{443}, nil, nil, fixedResolver{"public.example": {netip.MustParseAddr("8.8.8.8")}, "rebind.example": {netip.MustParseAddr("10.0.0.1")}})
	if err != nil {
		t.Fatal(err)
	}
	for _, rawURL := range []string{"http://public.example/callback", "https://127.0.0.1/callback", "https://169.254.169.254/latest", "https://rebind.example/callback", "https://user:pass@public.example/callback", "https://public.example:8443/callback", "https://public.example/callback#fragment"} {
		if err := policy.ValidateURL(context.Background(), rawURL); err == nil {
			t.Errorf("unsafe callback accepted: %s", rawURL)
		}
	}
	if err := policy.ValidateURL(context.Background(), "https://public.example/callback?capability=opaque"); err != nil {
		t.Fatalf("public HTTPS callback rejected: %v", err)
	}
}

func TestExplicitAllowlistPermitsTrustedPrivateAndHTTP(t *testing.T) {
	policy, err := New([]uint16{80, 443, 8443}, []string{"subscriber.internal"}, []string{"10.20.0.0/16"}, fixedResolver{"subscriber.internal": {netip.MustParseAddr("10.1.2.3")}, "cidr.internal": {netip.MustParseAddr("10.20.1.2")}})
	if err != nil {
		t.Fatal(err)
	}
	if err := policy.ValidateURL(context.Background(), "http://subscriber.internal/callback"); err != nil {
		t.Fatal(err)
	}
	if err := policy.ValidateURL(context.Background(), "https://cidr.internal:8443/callback"); err != nil {
		t.Fatal(err)
	}
	if err := policy.ValidateURL(context.Background(), "http://cidr.internal/callback"); err == nil {
		t.Fatal("HTTP allowed by CIDR without explicit host")
	}
}

func TestDialTimeResolutionRejectsDNSRebinding(t *testing.T) {
	resolver := &sequenceResolver{addresses: [][]netip.Addr{
		{netip.MustParseAddr("8.8.8.8")},
		{netip.MustParseAddr("10.0.0.1")},
	}}
	policy, err := New([]uint16{443}, nil, nil, resolver)
	if err != nil {
		t.Fatal(err)
	}
	if err := policy.ValidateURL(context.Background(), "https://rebind.example/callback"); err != nil {
		t.Fatalf("admission unexpectedly failed: %v", err)
	}
	if _, err := policy.DialContext(context.Background(), "tcp", "rebind.example:443"); err == nil {
		t.Fatal("dial-time private rebinding was accepted")
	}
	if resolver.calls != 2 {
		t.Fatalf("resolver calls = %d, want admission and dial resolution", resolver.calls)
	}
}

func TestRestrictedAddressClasses(t *testing.T) {
	for _, value := range []string{"0.0.0.0", "10.0.0.1", "100.64.0.1", "127.0.0.1", "169.254.169.254", "172.16.0.1", "192.0.2.1", "192.168.1.1", "198.18.0.1", "198.51.100.1", "203.0.113.1", "224.0.0.1", "240.0.0.1", "::", "::1", "fc00::1", "fe80::1", "ff00::1", "2001:db8::1"} {
		if !restricted(netip.MustParseAddr(value)) {
			t.Errorf("restricted address accepted: %s", value)
		}
	}
	if restricted(netip.MustParseAddr("8.8.8.8")) || restricted(netip.MustParseAddr("2606:4700:4700::1111")) {
		t.Fatal("public address rejected")
	}
}

type fixedResolver map[string][]netip.Addr

func (r fixedResolver) LookupNetIP(_ context.Context, _, host string) ([]netip.Addr, error) {
	return r[host], nil
}

type sequenceResolver struct {
	addresses [][]netip.Addr
	calls     int
}

func (r *sequenceResolver) LookupNetIP(context.Context, string, string) ([]netip.Addr, error) {
	index := r.calls
	r.calls++
	if index >= len(r.addresses) {
		index = len(r.addresses) - 1
	}
	return r.addresses[index], nil
}
