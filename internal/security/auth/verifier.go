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

// Package auth authenticates JWT bearer tokens and authorizes product scopes.
package auth

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/lestrrat-go/jwx/v3/jwa"
	"github.com/lestrrat-go/jwx/v3/jwk"
	"github.com/lestrrat-go/jwx/v3/jws"
	"github.com/lestrrat-go/jwx/v3/jwt"

	"github.com/ayeshLK/websubhub/internal/config"
)

const (
	ScopeTopicRegister      = "websubhub:topic:register"
	ScopeTopicDeregister    = "websubhub:topic:deregister"
	ScopeContentPublish     = "websubhub:content:publish"
	ScopeSubscriptionCreate = "websubhub:subscription:create"
	ScopeSubscriptionDelete = "websubhub:subscription:delete"
	ScopeOperationsRead     = "websubhub:ops:read"
	ScopeOperationsWrite    = "websubhub:ops:write"
)

var (
	ErrUnauthenticated = errors.New("request is not authenticated")
	ErrForbidden       = errors.New("request is not authorized")
)

type Principal struct {
	Subject string
	Scopes  map[string]struct{}
}

type principalContextKey struct{}

type KeySource interface {
	Keys(context.Context, bool) (jwk.Set, error)
}

type Verifier struct {
	cfg        config.JWT
	keys       KeySource
	algorithms map[string]jwa.SignatureAlgorithm
}

func New(cfg config.JWT, keys KeySource) (*Verifier, error) {
	if keys == nil {
		return nil, errors.New("JWT key source is required")
	}
	algorithms := make(map[string]jwa.SignatureAlgorithm, len(cfg.Algorithms))
	for _, name := range cfg.Algorithms {
		algorithm, ok := jwa.LookupSignatureAlgorithm(name)
		if !ok {
			return nil, fmt.Errorf("unknown JWT algorithm %q", name)
		}
		algorithms[name] = algorithm
	}
	return &Verifier{cfg: cfg, keys: keys, algorithms: algorithms}, nil
}

func (v *Verifier) Authenticate(request *http.Request, requiredScope string) (Principal, error) {
	principal, err := v.Verify(request)
	if err != nil {
		return Principal{}, err
	}
	if _, ok := principal.Scopes[requiredScope]; !ok {
		return Principal{}, ErrForbidden
	}
	return principal, nil
}

func (v *Verifier) Verify(request *http.Request) (Principal, error) {
	tokenText, err := bearerToken(request.Header.Values("Authorization"), v.cfg.MaxTokenBytes)
	if err != nil {
		return Principal{}, err
	}
	provider := &keyProvider{ctx: request.Context(), source: v.keys, algorithms: v.algorithms}
	token, err := jwt.Parse([]byte(tokenText), jwt.WithKeyProvider(provider), jwt.WithIssuer(v.cfg.Issuer), jwt.WithAudience(v.cfg.Audience), jwt.WithAcceptableSkew(v.cfg.ClockSkew.Value()), jwt.WithRequiredClaim(jwt.ExpirationKey), jwt.WithRequiredClaim(jwt.SubjectKey))
	if err != nil {
		return Principal{}, ErrUnauthenticated
	}
	subject, ok := token.Subject()
	if !ok || subject == "" {
		return Principal{}, ErrUnauthenticated
	}
	scopes, err := tokenScopes(token, v.cfg.ScopeClaim)
	if err != nil {
		return Principal{}, ErrUnauthenticated
	}
	return Principal{Subject: subject, Scopes: scopes}, nil
}

func (v *Verifier) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		principal, err := v.Verify(request)
		if err != nil {
			response.Header().Set("WWW-Authenticate", `Bearer realm="websubhub"`)
			http.Error(response, "unauthorized", http.StatusUnauthorized)
			return
		}
		ctx := context.WithValue(request.Context(), principalContextKey{}, principal)
		next.ServeHTTP(response, request.WithContext(ctx))
	})
}

func RequireScope(ctx context.Context, scope string) (Principal, error) {
	principal, ok := ctx.Value(principalContextKey{}).(Principal)
	if !ok || principal.Subject == "" {
		return Principal{}, ErrUnauthenticated
	}
	if _, ok := principal.Scopes[scope]; !ok {
		return Principal{}, ErrForbidden
	}
	return principal, nil
}

func (v *Verifier) Authorize(ctx context.Context, scope string) (string, error) {
	principal, err := RequireScope(ctx, scope)
	if err != nil {
		return "", err
	}
	return principal.Subject, nil
}

func bearerToken(values []string, maximum int64) (string, error) {
	if len(values) != 1 {
		return "", ErrUnauthenticated
	}
	parts := strings.Fields(values[0])
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") || len(parts[1]) == 0 || int64(len(parts[1])) > maximum {
		return "", ErrUnauthenticated
	}
	return parts[1], nil
}

func tokenScopes(token jwt.Token, claim string) (map[string]struct{}, error) {
	var value any
	if err := token.Get(claim, &value); err != nil {
		return nil, err
	}
	result := make(map[string]struct{})
	switch value := value.(type) {
	case string:
		for _, scope := range strings.Fields(value) {
			result[scope] = struct{}{}
		}
	case []string:
		for _, scope := range value {
			if scope != "" {
				result[scope] = struct{}{}
			}
		}
	case []any:
		for _, item := range value {
			scope, ok := item.(string)
			if !ok || scope == "" {
				return nil, errors.New("invalid JWT scope claim")
			}
			result[scope] = struct{}{}
		}
	default:
		return nil, errors.New("invalid JWT scope claim")
	}
	return result, nil
}

type keyProvider struct {
	ctx        context.Context
	source     KeySource
	algorithms map[string]jwa.SignatureAlgorithm
}

func (p *keyProvider) FetchKeys(_ context.Context, sink jws.KeySink, signature *jws.Signature, _ *jws.Message) error {
	algorithm, ok := signature.ProtectedHeaders().Algorithm()
	if !ok {
		return errors.New("JWT protected algorithm is required")
	}
	allowed, ok := p.algorithms[algorithm.String()]
	if !ok || allowed != algorithm {
		return errors.New("JWT algorithm is not allowed")
	}
	keyID, ok := signature.ProtectedHeaders().KeyID()
	if !ok || keyID == "" {
		return errors.New("JWT protected key ID is required")
	}
	set, err := p.source.Keys(p.ctx, false)
	if err != nil {
		return err
	}
	key, found := set.LookupKeyID(keyID)
	if !found {
		set, err = p.source.Keys(p.ctx, true)
		if err != nil {
			return err
		}
		key, found = set.LookupKeyID(keyID)
	}
	if !found {
		return errors.New("JWT signing key is unavailable")
	}
	sink.Key(algorithm, key)
	return nil
}

type RemoteKeySource struct {
	url       string
	client    *http.Client
	ttl       time.Duration
	mu        sync.Mutex
	set       jwk.Set
	fetchedAt time.Time
}

func NewRemoteKeySource(cfg config.JWT, client *http.Client) *RemoteKeySource {
	if client == nil {
		client = &http.Client{Timeout: cfg.RequestTimeout.Value(), CheckRedirect: func(*http.Request, []*http.Request) error { return errors.New("JWKS redirects are forbidden") }}
	}
	return &RemoteKeySource{url: cfg.JWKSURL, client: client, ttl: cfg.CacheTTL.Value()}
}

func (s *RemoteKeySource) Keys(ctx context.Context, force bool) (jwk.Set, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !force && s.set != nil && time.Since(s.fetchedAt) < s.ttl {
		return s.set, nil
	}
	set, err := jwk.Fetch(ctx, s.url, jwk.WithHTTPClient(s.client), jwk.WithMaxFetchBodySize(1<<20))
	if err != nil {
		return nil, errors.New("fetch JWT signing keys")
	}
	s.set, s.fetchedAt = set, time.Now()
	return set, nil
}
