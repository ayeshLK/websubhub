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

package auth

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/lestrrat-go/jwx/v3/jwa"
	"github.com/lestrrat-go/jwx/v3/jwk"
	"github.com/lestrrat-go/jwx/v3/jwt"

	"github.com/ayeshLK/websubhub/internal/config"
)

func TestAuthenticateValidTokenAndScope(t *testing.T) {
	fixture := newJWTFixture(t)
	request := httptest.NewRequest("GET", "https://hub.example.test/v1/system/capabilities", nil)
	request.Header.Set("Authorization", "Bearer "+fixture.sign(t, jwa.RS256(), ScopeOperationsRead, time.Now().Add(time.Minute), fixture.cfg.Issuer, fixture.cfg.Audience))
	principal, err := fixture.verifier.Authenticate(request, ScopeOperationsRead)
	if err != nil || principal.Subject != "subject-1" {
		t.Fatalf("principal=%#v err=%v", principal, err)
	}
}

func TestMiddlewareRejectsMissingBearerBeforeHandler(t *testing.T) {
	fixture := newJWTFixture(t)
	called := false
	handler := fixture.verifier.Middleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { called = true }))
	request := httptest.NewRequest(http.MethodGet, "https://hub.example.test/websub", nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized || called || response.Header().Get("WWW-Authenticate") == "" {
		t.Fatalf("status=%d called=%v authenticate=%q", response.Code, called, response.Header().Get("WWW-Authenticate"))
	}
}

func TestAuthenticateFailsClosed(t *testing.T) {
	fixture := newJWTFixture(t)
	tests := []struct {
		name  string
		token string
		scope string
		want  error
	}{
		{"wrong scope", fixture.sign(t, jwa.RS256(), ScopeContentPublish, time.Now().Add(time.Minute), fixture.cfg.Issuer, fixture.cfg.Audience), ScopeOperationsRead, ErrForbidden},
		{"expired", fixture.sign(t, jwa.RS256(), ScopeOperationsRead, time.Now().Add(-time.Minute), fixture.cfg.Issuer, fixture.cfg.Audience), ScopeOperationsRead, ErrUnauthenticated},
		{"wrong issuer", fixture.sign(t, jwa.RS256(), ScopeOperationsRead, time.Now().Add(time.Minute), "https://wrong.example", fixture.cfg.Audience), ScopeOperationsRead, ErrUnauthenticated},
		{"wrong audience", fixture.sign(t, jwa.RS256(), ScopeOperationsRead, time.Now().Add(time.Minute), fixture.cfg.Issuer, "wrong"), ScopeOperationsRead, ErrUnauthenticated},
		{"wrong algorithm", fixture.sign(t, jwa.RS512(), ScopeOperationsRead, time.Now().Add(time.Minute), fixture.cfg.Issuer, fixture.cfg.Audience), ScopeOperationsRead, ErrUnauthenticated},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest("GET", "https://hub.example.test", nil)
			request.Header.Set("Authorization", "Bearer "+test.token)
			_, err := fixture.verifier.Authenticate(request, test.scope)
			if !errors.Is(err, test.want) {
				t.Fatalf("error=%v want=%v", err, test.want)
			}
		})
	}
}

func TestAuthenticateRejectsMissingDuplicateAndOversizedBearer(t *testing.T) {
	fixture := newJWTFixture(t)
	for _, values := range [][]string{nil, {"Basic value"}, {"Bearer one", "Bearer two"}, {"Bearer " + string(make([]byte, fixture.cfg.MaxTokenBytes+1))}} {
		request := httptest.NewRequest("GET", "https://hub.example.test", nil)
		for _, value := range values {
			request.Header.Add("Authorization", value)
		}
		if _, err := fixture.verifier.Authenticate(request, ScopeOperationsRead); !errors.Is(err, ErrUnauthenticated) {
			t.Fatalf("headers=%q error=%v", values, err)
		}
	}
}

func TestUnknownKeyForcesOneRefresh(t *testing.T) {
	fixture := newJWTFixture(t)
	source := &rotatingSource{sets: []jwk.Set{jwk.NewSet(), fixture.set}}
	verifier, err := New(fixture.cfg, source)
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest("GET", "https://hub.example.test", nil)
	request.Header.Set("Authorization", "Bearer "+fixture.sign(t, jwa.RS256(), ScopeOperationsRead, time.Now().Add(time.Minute), fixture.cfg.Issuer, fixture.cfg.Audience))
	if _, err := verifier.Authenticate(request, ScopeOperationsRead); err != nil {
		t.Fatal(err)
	}
	if source.calls != 2 || !source.forces[1] {
		t.Fatalf("calls=%d forces=%v", source.calls, source.forces)
	}
}

type jwtFixture struct {
	cfg      config.JWT
	private  jwk.Key
	set      jwk.Set
	verifier *Verifier
}

func newJWTFixture(t *testing.T) jwtFixture {
	t.Helper()
	raw, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	private, err := jwk.Import(raw)
	if err != nil {
		t.Fatal(err)
	}
	if err := private.Set(jwk.KeyIDKey, "key-1"); err != nil {
		t.Fatal(err)
	}
	public, err := jwk.PublicKeyOf(private)
	if err != nil {
		t.Fatal(err)
	}
	set := jwk.NewSet()
	if err := set.AddKey(public); err != nil {
		t.Fatal(err)
	}
	cfg := config.HubDefaults().Security.JWT
	verifier, err := New(cfg, fixedSource{set})
	if err != nil {
		t.Fatal(err)
	}
	return jwtFixture{cfg: cfg, private: private, set: set, verifier: verifier}
}

func (f jwtFixture) sign(t *testing.T, algorithm jwa.SignatureAlgorithm, scope string, expiration time.Time, issuer, audience string) string {
	t.Helper()
	token := jwt.New()
	for name, value := range map[string]any{jwt.SubjectKey: "subject-1", jwt.IssuerKey: issuer, jwt.AudienceKey: []string{audience}, jwt.ExpirationKey: expiration, "scope": scope} {
		if err := token.Set(name, value); err != nil {
			t.Fatal(err)
		}
	}
	signed, err := jwt.Sign(token, jwt.WithKey(algorithm, f.private))
	if err != nil {
		t.Fatal(err)
	}
	return string(signed)
}

type fixedSource struct{ set jwk.Set }

func (s fixedSource) Keys(context.Context, bool) (jwk.Set, error) { return s.set, nil }

type rotatingSource struct {
	sets   []jwk.Set
	calls  int
	forces []bool
}

func (s *rotatingSource) Keys(_ context.Context, force bool) (jwk.Set, error) {
	index := s.calls
	s.calls++
	s.forces = append(s.forces, force)
	return s.sets[index], nil
}
