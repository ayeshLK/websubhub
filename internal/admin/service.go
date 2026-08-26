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

	"github.com/ayeshLK/websubhub/internal/persistence/messagestore"
	"github.com/ayeshLK/websubhub/internal/security/auth"
	"github.com/ayeshLK/websubhub/internal/state"
)

const maximumPageSize = 100

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

type DLQEntry struct {
	MessageID      string `json:"message_id"`
	TopicID        string `json:"topic_id"`
	SubscriptionID string `json:"subscription_id"`
	FailureClass   string `json:"failure_class"`
	Attempt        uint32 `json:"attempt"`
	ContentType    string `json:"content_type"`
	BodyBytes      int64  `json:"body_bytes"`
}

type DLQInspector interface {
	List(context.Context, int) ([]DLQEntry, error)
}

type Dependencies struct {
	Authentication Authentication
	Readiness      Readiness
	Projection     SnapshotSource
	Capabilities   CapabilitySource
	DLQ            DLQInspector
}

type Service struct {
	dependencies Dependencies
	handler      http.Handler
}

func New(dependencies Dependencies) (*Service, error) {
	if dependencies.Authentication == nil || dependencies.Readiness == nil || dependencies.Projection == nil || dependencies.Capabilities == nil || dependencies.DLQ == nil {
		return nil, errors.New("authentication, readiness, projection, capabilities, and DLQ inspector are required")
	}
	service := &Service{dependencies: dependencies}
	mux := http.NewServeMux()
	mux.HandleFunc("/health/live", service.live)
	mux.HandleFunc("/health/ready", service.ready)
	mux.Handle("/v1/system/capabilities", service.protected(service.capabilities))
	mux.Handle("/v1/subscriptions", service.protected(service.subscriptions))
	mux.Handle("/v1/dlq", service.protected(service.dlq))
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
	writeJSON(response, http.StatusOK, map[string]any{"provider": capabilities.Provider, "message_store": statuses, "resource_topics": map[string]bool{"renewal": false, "lease_expiry": false, "automatic_failover": false, "event_only_fetch": false}, "delivery": map[string]any{"guarantee": "at_least_once", "replay": "manual_reactivation_only"}})
}

func (s *Service) subscriptions(response http.ResponseWriter, request *http.Request) {
	if !onlyGET(response, request) {
		return
	}
	limit, ok := pageLimit(response, request)
	if !ok {
		return
	}
	snapshot := s.dependencies.Projection.Snapshot()
	type item struct {
		ID          string                   `json:"id"`
		TopicID     string                   `json:"topic_id"`
		Callback    string                   `json:"callback"`
		ServerID    string                   `json:"server_id"`
		ConsumerID  string                   `json:"consumer_id"`
		Status      state.SubscriptionStatus `json:"status"`
		StaleReason string                   `json:"stale_reason,omitempty"`
	}
	items := make([]item, 0, len(snapshot.Subscriptions))
	for _, subscription := range snapshot.Subscriptions {
		items = append(items, item{ID: subscription.ID, TopicID: subscription.TopicID, Callback: redactURL(subscription.CallbackURL), ServerID: subscription.ServerID, ConsumerID: subscription.ConsumerID, Status: subscription.Status, StaleReason: subscription.StaleReason})
	}
	sort.Slice(items, func(i, j int) bool { return items[i].ID < items[j].ID })
	if len(items) > limit {
		items = items[:limit]
	}
	writeJSON(response, http.StatusOK, map[string]any{"revision": snapshot.Revision, "subscriptions": items})
}

func (s *Service) dlq(response http.ResponseWriter, request *http.Request) {
	if !onlyGET(response, request) {
		return
	}
	limit, ok := pageLimit(response, request)
	if !ok {
		return
	}
	entries, err := s.dependencies.DLQ.List(request.Context(), limit)
	if err != nil {
		http.Error(response, "DLQ unavailable", http.StatusServiceUnavailable)
		return
	}
	if len(entries) > limit {
		entries = entries[:limit]
	}
	writeJSON(response, http.StatusOK, map[string]any{"entries": entries})
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
		return maximumPageSize, true
	}
	limit, err := strconv.Atoi(value)
	if err != nil || limit < 1 || limit > maximumPageSize {
		http.Error(response, "limit must be between 1 and 100", http.StatusBadRequest)
		return 0, false
	}
	return limit, true
}

func redactURL(raw string) string {
	parsed, err := url.Parse(raw)
	if err != nil {
		return "redacted"
	}
	parsed.RawQuery, parsed.ForceQuery, parsed.Fragment, parsed.User = "", false, "", nil
	return parsed.String()
}

func writeJSON(response http.ResponseWriter, status int, value any) {
	response.Header().Set("Content-Type", "application/json")
	response.WriteHeader(status)
	_ = json.NewEncoder(response).Encode(value)
}
