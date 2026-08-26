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
	"strings"
	"testing"

	"github.com/ayeshLK/websubhub/internal/persistence/messagestore"
	"github.com/ayeshLK/websubhub/internal/state"
)

func TestHealthIsSeparateFromProtectedOperations(t *testing.T) {
	authentication := &testAuthentication{allowed: false}
	service := testService(t, authentication, true)
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
	service := testService(t, &testAuthentication{allowed: true}, false)
	response := serve(service, "/health/ready")
	if response.Code != http.StatusServiceUnavailable || response.Body.String() != "{\"ready\":false,\"reasons\":[\"state_projection_unavailable\"]}\n" {
		t.Fatalf("readiness = %d %q", response.Code, response.Body.String())
	}
}

func TestSubscriptionInspectionRedactsSecretsAndCallbackCapabilities(t *testing.T) {
	service := testService(t, &testAuthentication{allowed: true}, true)
	response := serve(service, "/v1/subscriptions?limit=1")
	body := response.Body.String()
	if response.Code != http.StatusOK || !strings.Contains(body, `"callback":"https://subscriber.example/callback"`) {
		t.Fatalf("response = %d %s", response.Code, body)
	}
	for _, forbidden := range []string{"opaque-token", "plaintext-secret", "ciphertext-value"} {
		if strings.Contains(body, forbidden) {
			t.Fatalf("response disclosed %q: %s", forbidden, body)
		}
	}
}

func TestCapabilitiesReportPreviewLimitations(t *testing.T) {
	service := testService(t, &testAuthentication{allowed: true}, true)
	response := serve(service, "/v1/system/capabilities")
	body := response.Body.String()
	if response.Code != http.StatusOK || !strings.Contains(body, `"renewal":false`) || !strings.Contains(body, `"automatic_failover":false`) || !strings.Contains(body, `"guarantee":"at_least_once"`) {
		t.Fatalf("capabilities = %d %s", response.Code, body)
	}
}

func TestDLQAndMetricsNeverExposePayloadOrRawIdentifiersAsLabels(t *testing.T) {
	service := testService(t, &testAuthentication{allowed: true}, true)
	dlq := serve(service, "/v1/dlq?limit=1").Body.String()
	if strings.Contains(dlq, "customer-payload") || !strings.Contains(dlq, `"failure_class":"http_400"`) {
		t.Fatalf("DLQ = %s", dlq)
	}
	metrics := serve(service, "/metrics").Body.String()
	for _, forbidden := range []string{"subscription-1", "subscriber.example", "topic-1"} {
		if strings.Contains(metrics, forbidden) {
			t.Fatalf("metrics disclosed %q: %s", forbidden, metrics)
		}
	}
	if !strings.Contains(metrics, `websubhub_subscriptions{status="active"} 1`) {
		t.Fatalf("metrics = %s", metrics)
	}
}

func testService(t *testing.T, authentication *testAuthentication, ready bool) *Service {
	t.Helper()
	snapshot := state.EmptySnapshot()
	snapshot.Revision = 7
	snapshot.Subscriptions["subscription-1"] = state.Subscription{ID: "subscription-1", TopicID: "topic-1", CallbackURL: "https://subscriber.example/callback?token=opaque-token", SecretCiphertext: []byte("ciphertext-value"), SecretKeyID: "key-1", ServerID: "hub-1", ConsumerID: "consumer-1", Status: state.SubscriptionActive}
	service, err := New(Dependencies{Authentication: authentication, Readiness: testReadiness(ready), Projection: testProjection{snapshot}, Capabilities: testCapabilities{}, DLQ: testDLQ{}})
	if err != nil {
		t.Fatal(err)
	}
	return service
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

type testCapabilities struct{}

func (testCapabilities) Capabilities(context.Context) (messagestore.Capabilities, error) {
	statuses := make(map[messagestore.Capability]messagestore.CapabilityStatus)
	for _, capability := range []messagestore.Capability{messagestore.DurablePublish, messagestore.Ordering, messagestore.DurableSubscription, messagestore.Acknowledgement, messagestore.Replay, messagestore.Retention, messagestore.DeadLettering, messagestore.DelayedDelivery, messagestore.Transactions, messagestore.Provisioning, messagestore.ConsumerScaling} {
		statuses[capability] = messagestore.CapabilityStatus{Support: messagestore.Native}
	}
	return messagestore.Capabilities{Provider: "test", Statuses: statuses}, nil
}

type testDLQ struct{}

func (testDLQ) List(context.Context, int) ([]DLQEntry, error) {
	return []DLQEntry{{MessageID: "message-1", TopicID: "topic-1", SubscriptionID: "subscription-1", FailureClass: "http_400", Attempt: 1, ContentType: "application/json", BodyBytes: 16}}, nil
}
