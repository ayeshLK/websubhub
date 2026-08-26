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

// Package callback enforces outbound subscriber destination policy.
package callback

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/ayeshLK/websubhub/internal/config"
)

type Resolver interface {
	LookupNetIP(context.Context, string, string) ([]netip.Addr, error)
}

func FromConfig(cfg config.Callback, resolver Resolver) (*Policy, *http.Client, error) {
	ports := make([]uint16, 0, len(cfg.AllowedPorts))
	for _, port := range cfg.AllowedPorts {
		if port < 1 || port > 65535 {
			return nil, nil, fmt.Errorf("invalid callback port %d", port)
		}
		ports = append(ports, uint16(port))
	}
	policy, err := New(ports, cfg.AllowedHosts, cfg.AllowedCIDRs, resolver)
	if err != nil {
		return nil, nil, err
	}
	client := policy.HTTPClient(cfg.ConnectTimeout.Value(), cfg.TLSHandshakeTimeout.Value(), cfg.ResponseHeaderTimeout.Value(), 0)
	return policy, client, nil
}

func (p *Policy) HTTPClient(connectTimeout, tlsTimeout, responseHeaderTimeout, requestTimeout time.Duration) *http.Client {
	dialer := &net.Dialer{Timeout: connectTimeout}
	transport := &http.Transport{
		Proxy: nil,
		DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
			return p.dialContext(ctx, network, address, dialer)
		},
		DisableKeepAlives:     true,
		TLSHandshakeTimeout:   tlsTimeout,
		ResponseHeaderTimeout: responseHeaderTimeout,
	}
	return &http.Client{
		Transport: transport,
		Timeout:   requestTimeout,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return errors.New("callback redirects are forbidden")
		},
	}
}

type Policy struct {
	AllowedPorts []uint16
	AllowedHosts []string
	AllowedCIDRs []netip.Prefix
	Resolver     Resolver
}

func New(ports []uint16, hosts []string, cidrs []string, resolver Resolver) (*Policy, error) {
	if len(ports) == 0 {
		return nil, errors.New("at least one callback port is required")
	}
	if resolver == nil {
		resolver = net.DefaultResolver
	}
	policy := &Policy{AllowedPorts: slices.Clone(ports), Resolver: resolver}
	for _, host := range hosts {
		host = strings.ToLower(strings.TrimSuffix(strings.TrimSpace(host), "."))
		if host == "" || strings.ContainsAny(host, "/:@[]") {
			return nil, fmt.Errorf("invalid callback allowed host %q", host)
		}
		policy.AllowedHosts = append(policy.AllowedHosts, host)
	}
	for _, value := range cidrs {
		prefix, err := netip.ParsePrefix(value)
		if err != nil {
			return nil, fmt.Errorf("invalid callback allowed CIDR %q: %w", value, err)
		}
		policy.AllowedCIDRs = append(policy.AllowedCIDRs, prefix.Masked())
	}
	return policy, nil
}

func (p *Policy) ValidateURL(ctx context.Context, rawURL string) error {
	parsed, err := url.Parse(rawURL)
	if err != nil || parsed.Host == "" || (parsed.Scheme != "https" && parsed.Scheme != "http") {
		return errors.New("callback must be an absolute HTTP or HTTPS URL")
	}
	if parsed.User != nil || parsed.Fragment != "" {
		return errors.New("callback userinfo and fragments are forbidden")
	}
	host := normalizedHost(parsed.Hostname())
	trustedHost := p.hostAllowed(host)
	if parsed.Scheme != "https" && !trustedHost {
		return errors.New("HTTP callback requires an explicitly allowed host")
	}
	port, err := callbackPort(parsed)
	if err != nil || !slices.Contains(p.AllowedPorts, port) {
		return errors.New("callback port is not allowed")
	}
	_, err = p.resolveAllowed(ctx, host, trustedHost)
	return err
}

func (p *Policy) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	return p.dialContext(ctx, network, address, &net.Dialer{})
}

func (p *Policy) dialContext(ctx context.Context, network, address string, dialer *net.Dialer) (net.Conn, error) {
	if network != "tcp" && network != "tcp4" && network != "tcp6" {
		return nil, errors.New("callback network is not allowed")
	}
	host, portText, err := net.SplitHostPort(address)
	if err != nil {
		return nil, errors.New("invalid callback dial address")
	}
	portNumber, err := strconv.ParseUint(portText, 10, 16)
	if err != nil || !slices.Contains(p.AllowedPorts, uint16(portNumber)) {
		return nil, errors.New("callback port is not allowed")
	}
	host = normalizedHost(host)
	addresses, err := p.resolveAllowed(ctx, host, p.hostAllowed(host))
	if err != nil {
		return nil, err
	}
	var joined error
	for _, address := range addresses {
		connection, dialErr := dialer.DialContext(ctx, network, net.JoinHostPort(address.String(), portText))
		if dialErr == nil {
			return connection, nil
		}
		joined = errors.Join(joined, dialErr)
	}
	return nil, fmt.Errorf("connect to allowed callback destination: %w", joined)
}

func (p *Policy) resolveAllowed(ctx context.Context, host string, trustedHost bool) ([]netip.Addr, error) {
	if address, err := netip.ParseAddr(host); err == nil {
		address = address.Unmap()
		if !trustedHost && !p.addressAllowed(address) {
			return nil, errors.New("callback address is denied")
		}
		return []netip.Addr{address}, nil
	}
	addresses, err := p.Resolver.LookupNetIP(ctx, "ip", host)
	if err != nil || len(addresses) == 0 {
		return nil, errors.New("resolve callback destination")
	}
	result := make([]netip.Addr, 0, len(addresses))
	for _, address := range addresses {
		address = address.Unmap()
		if trustedHost || p.addressAllowed(address) {
			result = append(result, address)
		}
	}
	if len(result) == 0 {
		return nil, errors.New("callback resolved only to denied addresses")
	}
	return result, nil
}

func (p *Policy) addressAllowed(address netip.Addr) bool {
	for _, prefix := range p.AllowedCIDRs {
		if prefix.Contains(address) {
			return true
		}
	}
	return !restricted(address)
}

func (p *Policy) hostAllowed(host string) bool {
	return slices.Contains(p.AllowedHosts, normalizedHost(host))
}

func callbackPort(value *url.URL) (uint16, error) {
	if value.Port() == "" {
		if value.Scheme == "https" {
			return 443, nil
		}
		return 80, nil
	}
	port, err := strconv.ParseUint(value.Port(), 10, 16)
	return uint16(port), err
}

func normalizedHost(host string) string { return strings.ToLower(strings.TrimSuffix(host, ".")) }

var deniedPrefixes = mustPrefixes(
	"0.0.0.0/8", "10.0.0.0/8", "100.64.0.0/10", "127.0.0.0/8", "169.254.0.0/16",
	"172.16.0.0/12", "192.0.0.0/24", "192.0.2.0/24", "192.168.0.0/16", "198.18.0.0/15",
	"198.51.100.0/24", "203.0.113.0/24", "224.0.0.0/4", "240.0.0.0/4",
	"::/128", "::1/128", "fc00::/7", "fe80::/10", "ff00::/8", "2001:db8::/32",
)

func restricted(address netip.Addr) bool {
	if !address.IsValid() || address.IsUnspecified() || address.IsLoopback() || address.IsPrivate() || address.IsLinkLocalUnicast() || address.IsLinkLocalMulticast() || address.IsMulticast() {
		return true
	}
	for _, prefix := range deniedPrefixes {
		if prefix.Contains(address) {
			return true
		}
	}
	return false
}

func mustPrefixes(values ...string) []netip.Prefix {
	result := make([]netip.Prefix, 0, len(values))
	for _, value := range values {
		result = append(result, netip.MustParsePrefix(value))
	}
	return result
}
