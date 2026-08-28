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

// Package admin serves the bounded protected operations surface.
package admin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"

	"github.com/ayeshLK/websubhub/internal/management"
	"github.com/ayeshLK/websubhub/internal/persistence/messagestore"
	"github.com/ayeshLK/websubhub/internal/security/auth"
	"github.com/ayeshLK/websubhub/internal/state"
)

type Authentication interface {
	Middleware(http.Handler) http.Handler
	Authorize(context.Context, string) (string, error)
}

type Readiness interface {
	Ready() bool
}

type SnapshotSource interface {
	Snapshot() state.Snapshot
}

type CapabilitySource interface {
	Capabilities(context.Context) (messagestore.Capabilities, error)
}

type Dependencies struct {
	Authentication Authentication
	Readiness      Readiness
	Projection     SnapshotSource
	Capabilities   CapabilitySource
	Queries        management.QueryClient
}

type Service struct {
	dependencies Dependencies
	handler      http.Handler
}

func New(dependencies Dependencies) (*Service, error) {
	if dependencies.Authentication == nil || dependencies.Readiness == nil || dependencies.Projection == nil || dependencies.Capabilities == nil || dependencies.Queries == nil {
		return nil, errors.New("authentication, readiness, projection, capabilities, and management queries are required")
	}
	service := &Service{dependencies: dependencies}
	mux := http.NewServeMux()
	mux.HandleFunc("/health/live", service.live)
	mux.HandleFunc("/health/ready", service.ready)
	mux.Handle("/v1/system/capabilities", service.protected(service.capabilities))
	mux.Handle("/v1/topics", service.protected(service.topics))
	mux.Handle("/v1/topics/", service.protected(service.topic))
	mux.Handle("/v1/subscriptions", service.protected(service.subscriptions))
	mux.Handle("/v1/subscriptions/", service.protected(service.subscription))
	mux.Handle("/metrics", service.protected(service.metrics))
	service.handler = mux
	return service, nil
}

func (s *Service) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	s.handler.ServeHTTP(response, request)
}

func (s *Service) protected(handler http.HandlerFunc) http.Handler {
	return s.dependencies.Authentication.Middleware(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if _, err := s.dependencies.Authentication.Authorize(request.Context(), auth.ScopeOperationsRead); err != nil {
			http.Error(response, "forbidden", http.StatusForbidden)
			return
		}
		handler(response, request)
	}))
}

func (s *Service) live(response http.ResponseWriter, request *http.Request) {
	if !onlyGET(response, request) {
		return
	}
	response.WriteHeader(http.StatusNoContent)
}

func (s *Service) ready(response http.ResponseWriter, request *http.Request) {
	if !onlyGET(response, request) {
		return
	}
	if !s.dependencies.Readiness.Ready() {
		writeJSON(response, http.StatusServiceUnavailable, map[string]any{"ready": false, "reasons": []string{"state_projection_unavailable"}})
		return
	}
	writeJSON(response, http.StatusOK, map[string]any{"ready": true, "reasons": []string{}})
}

func (s *Service) capabilities(response http.ResponseWriter, request *http.Request) {
	if !onlyGET(response, request) {
		return
	}
	capabilities, err := s.dependencies.Capabilities.Capabilities(request.Context())
	if err != nil || capabilities.Validate() != nil {
		http.Error(response, "capabilities unavailable", http.StatusServiceUnavailable)
		return
	}
	type status struct {
		Name    string               `json:"name"`
		Support messagestore.Support `json:"support"`
		Detail  string               `json:"detail,omitempty"`
	}
	statuses := make([]status, 0, len(capabilities.Statuses))
	for name, value := range capabilities.Statuses {
		statuses = append(statuses, status{Name: string(name), Support: value.Support, Detail: value.Detail})
	}
	sort.Slice(statuses, func(i, j int) bool { return statuses[i].Name < statuses[j].Name })
	writeJSON(response, http.StatusOK, map[string]any{"provider": capabilities.Provider, "message_store": statuses, "resource_topics": map[string]bool{"renewal": false, "lease_expiry": false, "automatic_failover": false, "event_only_fetch": false}, "delivery": map[string]any{"guarantee": "at_least_once", "replay": "unsupported"}})
}

func (s *Service) topics(response http.ResponseWriter, request *http.Request) {
	if !onlyGET(response, request) || !onlyQueryParameters(response, request, "limit", "status", "cursor") {
		return
	}
	limit, ok := pageLimit(response, request)
	if !ok {
		return
	}
	result, err := s.dependencies.Queries.ListTopics(request.Context(), management.TopicQuery{Limit: limit, Status: request.URL.Query().Get("status")})
	if err != nil {
		writeQueryError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, result)
}

func (s *Service) topic(response http.ResponseWriter, request *http.Request) {
	if !onlyGET(response, request) || !onlyQueryParameters(response, request) {
		return
	}
	id, ok := pathID(response, request, "/v1/topics/")
	if !ok {
		return
	}
	result, err := s.dependencies.Queries.GetTopic(request.Context(), id)
	if err != nil {
		writeQueryError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, result)
}

func (s *Service) subscriptions(response http.ResponseWriter, request *http.Request) {
	if !onlyGET(response, request) || !onlyQueryParameters(response, request, "limit", "status", "topic_id", "cursor") {
		return
	}
	limit, ok := pageLimit(response, request)
	if !ok {
		return
	}
	result, err := s.dependencies.Queries.ListSubscriptions(request.Context(), management.SubscriptionQuery{Limit: limit, Status: request.URL.Query().Get("status"), TopicID: request.URL.Query().Get("topic_id")})
	if err != nil {
		writeQueryError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, result)
}

func (s *Service) subscription(response http.ResponseWriter, request *http.Request) {
	if !onlyGET(response, request) || !onlyQueryParameters(response, request) {
		return
	}
	id, ok := pathID(response, request, "/v1/subscriptions/")
	if !ok {
		return
	}
	result, err := s.dependencies.Queries.GetSubscription(request.Context(), id)
	if err != nil {
		writeQueryError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, result)
}

func (s *Service) metrics(response http.ResponseWriter, request *http.Request) {
	if !onlyGET(response, request) {
		return
	}
	snapshot := s.dependencies.Projection.Snapshot()
	counts := map[state.SubscriptionStatus]int{}
	for _, subscription := range snapshot.Subscriptions {
		counts[subscription.Status]++
	}
	ready := 0
	if s.dependencies.Readiness.Ready() {
		ready = 1
	}
	response.Header().Set("Content-Type", "text/plain; version=0.0.4")
	_, _ = fmt.Fprintf(response, "# TYPE websubhub_ready gauge\nwebsubhub_ready %d\n# TYPE websubhub_state_revision gauge\nwebsubhub_state_revision %d\n# TYPE websubhub_subscriptions gauge\n", ready, snapshot.Revision)
	for _, status := range []state.SubscriptionStatus{state.SubscriptionActive, state.SubscriptionStale, state.SubscriptionRemoved} {
		_, _ = fmt.Fprintf(response, "websubhub_subscriptions{status=%q} %d\n", status, counts[status])
	}
}

func onlyGET(response http.ResponseWriter, request *http.Request) bool {
	if request.Method == http.MethodGet {
		return true
	}
	response.Header().Set("Allow", http.MethodGet)
	http.Error(response, "method not allowed", http.StatusMethodNotAllowed)
	return false
}

func pageLimit(response http.ResponseWriter, request *http.Request) (int, bool) {
	if request.URL.Query().Has("cursor") {
		http.Error(response, "cursor is not supported in v0.5", http.StatusBadRequest)
		return 0, false
	}
	value := request.URL.Query().Get("limit")
	if value == "" {
		return management.MaximumPageSize, true
	}
	limit, err := strconv.Atoi(value)
	if err != nil || limit < 1 || limit > management.MaximumPageSize {
		http.Error(response, "limit must be between 1 and 100", http.StatusBadRequest)
		return 0, false
	}
	return limit, true
}

func onlyQueryParameters(response http.ResponseWriter, request *http.Request, allowed ...string) bool {
	accepted := make(map[string]struct{}, len(allowed))
	for _, name := range allowed {
		accepted[name] = struct{}{}
	}
	for name := range request.URL.Query() {
		if _, ok := accepted[name]; !ok {
			http.Error(response, "unsupported query parameter", http.StatusBadRequest)
			return false
		}
	}
	return true
}

func pathID(response http.ResponseWriter, request *http.Request, prefix string) (string, bool) {
	escapedID := strings.TrimPrefix(request.URL.EscapedPath(), prefix)
	// A literal slash denotes another route segment. An escaped slash is part
	// of the stable product ID, which is required for URL-valued topic IDs.
	if escapedID == "" || strings.Contains(escapedID, "/") {
		http.NotFound(response, request)
		return "", false
	}
	id, err := url.PathUnescape(escapedID)
	if err != nil || id == "" {
		http.NotFound(response, request)
		return "", false
	}
	return id, true
}

func writeQueryError(response http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, management.ErrInvalidQuery):
		http.Error(response, "invalid management query", http.StatusBadRequest)
	case errors.Is(err, management.ErrNotFound):
		http.Error(response, "management resource not found", http.StatusNotFound)
	default:
		http.Error(response, "management state unavailable", http.StatusServiceUnavailable)
	}
}

func writeJSON(response http.ResponseWriter, status int, value any) {
	response.Header().Set("Content-Type", "application/json")
	response.WriteHeader(status)
	_ = json.NewEncoder(response).Encode(value)
}
