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

// Command fixture is a Compose-only JWT issuer and controlled WebSub subscriber.
package main

import (
	"context"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/lestrrat-go/jwx/v3/jwa"
	"github.com/lestrrat-go/jwx/v3/jwk"
	"github.com/lestrrat-go/jwx/v3/jwt"
)

const (
	issuer   = "https://fixture:8443"
	audience = "websubhub"
	keyID    = "compose-key"
)

var allScopes = strings.Join([]string{
	"websubhub:topic:register", "websubhub:topic:deregister", "websubhub:content:publish",
	"websubhub:subscription:create", "websubhub:subscription:delete", "websubhub:ops:read", "websubhub:ops:write",
}, " ")

type receipt struct {
	Path        string `json:"path"`
	Count       int    `json:"count"`
	BodyBase64  string `json:"body_base64"`
	ContentType string `json:"content_type"`
	MessageID   string `json:"message_id"`
	Signature   string `json:"signature"`
}

type subscriber struct {
	mu       sync.Mutex
	statuses map[string]int
	receipts map[string]receipt
}

func main() {
	privateKey, err := loadPrivateKey(required("FIXTURE_JWT_KEY"))
	if err != nil {
		log.Fatal(err)
	}
	identity, err := identityHandler(privateKey)
	if err != nil {
		log.Fatal(err)
	}
	callbacks := &subscriber{statuses: make(map[string]int), receipts: make(map[string]receipt)}
	identityServer := &http.Server{Addr: ":8443", Handler: identity, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 30 * time.Second}
	callbackServer := &http.Server{Addr: ":8082", Handler: callbacks, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 30 * time.Second}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	errorsChannel := make(chan error, 2)
	go func() {
		errorsChannel <- identityServer.ListenAndServeTLS(required("FIXTURE_TLS_CERT"), required("FIXTURE_TLS_KEY"))
	}()
	go func() { errorsChannel <- callbackServer.ListenAndServe() }()
	select {
	case <-ctx.Done():
	case err := <-errorsChannel:
		if !errors.Is(err, http.ErrServerClosed) {
			log.Printf("fixture server: %v", err)
		}
	}
	shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = identityServer.Shutdown(shutdown)
	_ = callbackServer.Shutdown(shutdown)
}

func identityHandler(privateKey *rsa.PrivateKey) (http.Handler, error) {
	public, err := jwk.Import(&privateKey.PublicKey)
	if err != nil {
		return nil, err
	}
	if err := public.Set(jwk.KeyIDKey, keyID); err != nil {
		return nil, err
	}
	if err := public.Set(jwk.AlgorithmKey, jwa.RS256()); err != nil {
		return nil, err
	}
	set := jwk.NewSet()
	if err := set.AddKey(public); err != nil {
		return nil, err
	}
	private, err := jwk.Import(privateKey)
	if err != nil {
		return nil, err
	}
	if err := private.Set(jwk.KeyIDKey, keyID); err != nil {
		return nil, err
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/.well-known/jwks.json", func(response http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet {
			http.Error(response, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		response.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(response).Encode(set)
	})
	mux.HandleFunc("/token", func(response http.ResponseWriter, request *http.Request) {
		token, err := jwt.NewBuilder().Issuer(issuer).Audience([]string{audience}).Subject("compose-operator").IssuedAt(time.Now()).Expiration(time.Now().Add(15*time.Minute)).Claim("scope", allScopes).Build()
		if err != nil {
			http.Error(response, "token unavailable", http.StatusInternalServerError)
			return
		}
		signed, err := jwt.Sign(token, jwt.WithKey(jwa.RS256(), private))
		if err != nil {
			http.Error(response, "token unavailable", http.StatusInternalServerError)
			return
		}
		response.Header().Set("Content-Type", "text/plain")
		_, _ = response.Write(signed)
	})
	return mux, nil
}

func (s *subscriber) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	switch {
	case request.URL.Path == "/health":
		response.WriteHeader(http.StatusNoContent)
	case request.URL.Path == "/control":
		s.control(response, request)
	case request.URL.Path == "/received":
		s.received(response, request)
	case strings.HasPrefix(request.URL.Path, "/callback-"):
		s.callback(response, request)
	default:
		http.NotFound(response, request)
	}
}

func (s *subscriber) control(response http.ResponseWriter, request *http.Request) {
	status, err := strconv.Atoi(request.URL.Query().Get("status"))
	path := request.URL.Query().Get("path")
	if request.Method != http.MethodPost || !strings.HasPrefix(path, "/callback-") || err != nil || status < 200 || status > 599 {
		http.Error(response, "invalid control", http.StatusBadRequest)
		return
	}
	s.mu.Lock()
	s.statuses[path] = status
	s.mu.Unlock()
	response.WriteHeader(http.StatusNoContent)
}

func (s *subscriber) received(response http.ResponseWriter, request *http.Request) {
	s.mu.Lock()
	result := make([]receipt, 0, len(s.receipts))
	for _, value := range s.receipts {
		result = append(result, value)
	}
	s.mu.Unlock()
	response.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(response).Encode(result)
}

func (s *subscriber) callback(response http.ResponseWriter, request *http.Request) {
	if request.Method == http.MethodGet {
		_, _ = response.Write([]byte(request.URL.Query().Get("hub.challenge")))
		return
	}
	if request.Method != http.MethodPost {
		http.Error(response, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	body, err := io.ReadAll(io.LimitReader(request.Body, (1<<20)+1))
	if err != nil || len(body) > 1<<20 {
		http.Error(response, "body rejected", http.StatusRequestEntityTooLarge)
		return
	}
	s.mu.Lock()
	current := s.receipts[request.URL.Path]
	current.Path, current.Count = request.URL.Path, current.Count+1
	current.BodyBase64, current.ContentType = base64.StdEncoding.EncodeToString(body), request.Header.Get("Content-Type")
	current.MessageID, current.Signature = request.Header.Get("X-Hub-MessageId"), request.Header.Get("X-Hub-Signature")
	s.receipts[request.URL.Path] = current
	status := s.statuses[request.URL.Path]
	if status == 0 {
		status = http.StatusNoContent
	}
	s.mu.Unlock()
	response.WriteHeader(status)
}

func loadPrivateKey(path string) (*rsa.PrivateKey, error) {
	body, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	block, _ := pem.Decode(body)
	if block == nil {
		return nil, errors.New("JWT key is not PEM")
	}
	if key, err := x509.ParsePKCS8PrivateKey(block.Bytes); err == nil {
		if rsaKey, ok := key.(*rsa.PrivateKey); ok {
			return rsaKey, nil
		}
	}
	return x509.ParsePKCS1PrivateKey(block.Bytes)
}

func required(name string) string {
	value := os.Getenv(name)
	if value == "" {
		log.Fatalf("%s is required", name)
	}
	return value
}
