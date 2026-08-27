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

package admin

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/ayeshLK/websubhub/internal/management"
	"github.com/ayeshLK/websubhub/internal/persistence/messagestore"
	"github.com/ayeshLK/websubhub/internal/state"
)

func TestHealthIsSeparateFromProtectedOperations(t *testing.T) {
	authentication := &testAuthentication{allowed: false}
	service := testService(t, authentication, true, testCanonicalSnapshots{snapshot: canonicalSnapshot()})
	if response := serve(service, "/health/live"); response.Code != http.StatusNoContent {
		t.Fatalf("live status = %d", response.Code)
	}
	if response := serve(service, "/health/ready"); response.Code != http.StatusOK {
		t.Fatalf("ready status = %d", response.Code)
	}
	if authentication.calls != 0 {
		t.Fatalf("health invoked authentication %d times", authentication.calls)
	}
	if response := serve(service, "/v1/system/capabilities"); response.Code != http.StatusForbidden {
		t.Fatalf("protected status = %d", response.Code)
	}
	if authentication.calls != 1 {
		t.Fatalf("protected authentication calls = %d", authentication.calls)
	}
}

func TestReadinessReportsOnlyBoundedReason(t *testing.T) {
	service := testService(t, &testAuthentication{allowed: true}, false, testCanonicalSnapshots{snapshot: canonicalSnapshot()})
	response := serve(service, "/health/ready")
	if response.Code != http.StatusServiceUnavailable || response.Body.String() != "{\"ready\":false,\"reasons\":[\"state_projection_unavailable\"]}\n" {
		t.Fatalf("readiness = %d %q", response.Code, response.Body.String())
	}
}

func TestManagementCollectionsUseCanonicalSnapshotAndRedactSecrets(t *testing.T) {
	service := testService(t, &testAuthentication{allowed: true}, true, testCanonicalSnapshots{snapshot: canonicalSnapshot()})
	topics := serve(service, "/v1/topics?status=active&limit=1")
	topicURL := "https://publisher.example/resource"
	if topics.Code != http.StatusOK || !strings.Contains(topics.Body.String(), `"revision":7`) || !strings.Contains(topics.Body.String(), `"id":"`+topicURL+`"`) || strings.Contains(topics.Body.String(), "internal-content") {
		t.Fatalf("topics = %d %s", topics.Code, topics.Body.String())
	}
	subscriptions := serve(service, "/v1/subscriptions?topic_id="+url.QueryEscape(topicURL)+"&status=active&limit=1")
	body := subscriptions.Body.String()
	if subscriptions.Code != http.StatusOK || !strings.Contains(body, `"revision":7`) || !strings.Contains(body, `"callback":"https://subscriber.example/callback"`) {
		t.Fatalf("subscriptions = %d %s", subscriptions.Code, body)
	}
	for _, forbidden := range []string{"opaque-token", "plaintext-secret", "ciphertext-value", "consumer-1"} {
		if strings.Contains(body, forbidden) {
			t.Fatalf("response disclosed %q: %s", forbidden, body)
		}
	}
}

func TestManagementDetailAndNotFoundResponses(t *testing.T) {
	service := testService(t, &testAuthentication{allowed: true}, true, testCanonicalSnapshots{snapshot: canonicalSnapshot()})
	topicURL := "https://publisher.example/resource"
	if response := serve(service, "/v1/topics/"+url.PathEscape(topicURL)); response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"topic":{"id":"`+topicURL+`"`) {
		t.Fatalf("URL topic detail = %d %s", response.Code, response.Body.String())
	}
	if response := serve(service, "/v1/subscriptions/subscription-1"); response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"subscription":{"id":"subscription-1"`) {
		t.Fatalf("subscription detail = %d %s", response.Code, response.Body.String())
	}
	if response := serve(service, "/v1/topics/unknown"); response.Code != http.StatusNotFound || strings.Contains(response.Body.String(), "unknown") {
		t.Fatalf("unknown topic = %d %s", response.Code, response.Body.String())
	}
	if response := serve(service, "/v1/topics/not/a/single/id"); response.Code != http.StatusNotFound {
		t.Fatalf("multi-segment topic ID = %d", response.Code)
	}
	if response := serve(service, "/v1/subscriptions/"); response.Code != http.StatusNotFound {
		t.Fatalf("empty subscription ID = %d", response.Code)
	}
}

func TestManagementQueryValidationAndConsolidatorFailure(t *testing.T) {
	service := testService(t, &testAuthentication{allowed: true}, true, testCanonicalSnapshots{snapshot: canonicalSnapshot()})
	for _, target := range []string{"/v1/topics?limit=101", "/v1/topics?cursor=opaque", "/v1/topics?unknown=value", "/v1/subscriptions?status=unknown"} {
		if response := serve(service, target); response.Code != http.StatusBadRequest {
			t.Fatalf("%s status = %d: %s", target, response.Code, response.Body.String())
		}
	}
	failing := testService(t, &testAuthentication{allowed: true}, true, testCanonicalSnapshots{err: errors.New("secret consolidator detail")})
	response := serve(failing, "/v1/topics")
	if response.Code != http.StatusServiceUnavailable || strings.Contains(response.Body.String(), "secret consolidator detail") {
		t.Fatalf("unavailable = %d %s", response.Code, response.Body.String())
	}
}

func TestCapabilitiesAndRemovedDLQRoute(t *testing.T) {
	service := testService(t, &testAuthentication{allowed: true}, true, testCanonicalSnapshots{snapshot: canonicalSnapshot()})
	response := serve(service, "/v1/system/capabilities")
	body := response.Body.String()
	if response.Code != http.StatusOK || !strings.Contains(body, `"renewal":false`) || !strings.Contains(body, `"guarantee":"at_least_once"`) || !strings.Contains(body, `"replay":"unsupported"`) {
		t.Fatalf("capabilities = %d %s", response.Code, body)
	}
	if response := serve(service, "/v1/dlq"); response.Code != http.StatusNotFound {
		t.Fatalf("removed DLQ route = %d", response.Code)
	}
}

func TestMetricsNeverExposeRawIdentifiersAsLabels(t *testing.T) {
	service := testService(t, &testAuthentication{allowed: true}, true, testCanonicalSnapshots{snapshot: canonicalSnapshot()})
	metrics := serve(service, "/metrics").Body.String()
	for _, forbidden := range []string{"subscription-local", "subscriber.example", "topic-local"} {
		if strings.Contains(metrics, forbidden) {
			t.Fatalf("metrics disclosed %q: %s", forbidden, metrics)
		}
	}
	if !strings.Contains(metrics, `websubhub_subscriptions{status="active"} 1`) {
		t.Fatalf("metrics = %s", metrics)
	}
}

func testService(t *testing.T, authentication *testAuthentication, ready bool, source testCanonicalSnapshots) *Service {
	t.Helper()
	projection := state.EmptySnapshot()
	projection.Revision = 99
	projection.Subscriptions["subscription-local"] = state.Subscription{ID: "subscription-local", TopicID: "topic-local", TopicURL: "https://publisher.example/local", CallbackURL: "https://subscriber.example/local", ServerID: "hub-local", ConsumerID: "consumer-local", Status: state.SubscriptionActive}
	queries, err := management.New(source)
	if err != nil {
		t.Fatal(err)
	}
	service, err := New(Dependencies{Authentication: authentication, Readiness: testReadiness(ready), Projection: testProjection{projection}, Capabilities: testCapabilities{}, Queries: queries})
	if err != nil {
		t.Fatal(err)
	}
	return service
}

func canonicalSnapshot() state.Snapshot {
	snapshot := state.EmptySnapshot()
	snapshot.Revision = 7
	topicURL := "https://publisher.example/resource"
	snapshot.Topics[topicURL] = state.Topic{ID: topicURL, CanonicalURL: topicURL, ContentDestination: "internal-content", Status: state.TopicActive, Revision: 1}
	snapshot.Subscriptions["subscription-1"] = state.Subscription{ID: "subscription-1", TopicID: topicURL, TopicURL: topicURL, CallbackURL: "https://subscriber.example/callback?token=opaque-token", SecretCiphertext: []byte("ciphertext-value"), SecretKeyID: "key-1", ServerID: "hub-1", ConsumerID: "consumer-1", Status: state.SubscriptionActive, Revision: 2}
	return snapshot
}

func serve(handler http.Handler, target string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(http.MethodGet, target, nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

type testAuthentication struct {
	allowed bool
	calls   int
}

func (a *testAuthentication) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		a.calls++
		next.ServeHTTP(response, request)
	})
}
func (a *testAuthentication) Authorize(context.Context, string) (string, error) {
	if !a.allowed {
		return "", errors.New("forbidden")
	}
	return "operator-1", nil
}

type testReadiness bool

func (r testReadiness) Ready() bool { return bool(r) }

type testProjection struct{ snapshot state.Snapshot }

func (p testProjection) Snapshot() state.Snapshot { return p.snapshot }

type testCanonicalSnapshots struct {
	snapshot state.Snapshot
	err      error
}

func (s testCanonicalSnapshots) Snapshot(context.Context) (state.Snapshot, error) {
	return s.snapshot, s.err
}

type testCapabilities struct{}

func (testCapabilities) Capabilities(context.Context) (messagestore.Capabilities, error) {
	statuses := make(map[messagestore.Capability]messagestore.CapabilityStatus)
	for _, capability := range []messagestore.Capability{messagestore.DurablePublish, messagestore.Ordering, messagestore.DurableSubscription, messagestore.Acknowledgement, messagestore.Replay, messagestore.Retention, messagestore.DeadLettering, messagestore.DelayedDelivery, messagestore.Transactions, messagestore.Provisioning, messagestore.ConsumerScaling} {
		statuses[capability] = messagestore.CapabilityStatus{Support: messagestore.Native}
	}
	return messagestore.Capabilities{Provider: "test", Statuses: statuses}, nil
}
