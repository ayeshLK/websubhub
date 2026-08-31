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

package resourcehub

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ayeshLK/websubhub/internal/config"
	"github.com/ayeshLK/websubhub/internal/persistence"
	"github.com/ayeshLK/websubhub/internal/persistence/messagestore"
	"github.com/ayeshLK/websubhub/internal/state"
)

var adapterTime = time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)

type eventAppender struct {
	events chan state.Event
}

func (a *eventAppender) Append(ctx context.Context, event state.Event) error {
	select {
	case a.events <- event:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type projection struct {
	mu       sync.RWMutex
	snapshot state.Snapshot
}

func (p *projection) Snapshot() state.Snapshot {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.snapshot
}

func (p *projection) set(snapshot state.Snapshot) {
	p.mu.Lock()
	p.snapshot = snapshot
	p.mu.Unlock()
}

type sealer struct {
	mu        sync.Mutex
	plaintext []byte
}

type allowCallbacks struct{}

func (allowCallbacks) ValidateURL(context.Context, string) error { return nil }

type allowSubscriptions struct{}

func (allowSubscriptions) ValidateSubscription(context.Context, messagestore.Destination, messagestore.SubscriptionOptions) error {
	return nil
}

type subscriptionValidatorFunc func(context.Context, messagestore.Destination, messagestore.SubscriptionOptions) error

func (f subscriptionValidatorFunc) ValidateSubscription(ctx context.Context, destination messagestore.Destination, options messagestore.SubscriptionOptions) error {
	return f(ctx, destination, options)
}

type contentSinkFunc func(context.Context, ContentUpdate) error

func (f contentSinkFunc) Persist(ctx context.Context, update ContentUpdate) error {
	return f(ctx, update)
}

type allowAuthorization struct{}

func (allowAuthorization) Middleware(next http.Handler) http.Handler         { return next }
func (allowAuthorization) Authorize(context.Context, string) (string, error) { return "subject-1", nil }

func (s *sealer) Seal(_ context.Context, plaintext []byte) ([]byte, string, error) {
	s.mu.Lock()
	s.plaintext = append([]byte(nil), plaintext...)
	s.mu.Unlock()
	return []byte("sealed-value"), "test-key", nil
}

func (s *sealer) input() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return string(s.plaintext)
}

func TestPublisherRegistrationAppendsWithoutMutatingProjection(t *testing.T) {
	cfg := testConfig()
	cfg.Protocol.PublisherExtensionEnabled = true
	events := &eventAppender{events: make(chan state.Event, 4)}
	view := &projection{snapshot: state.EmptySnapshot()}
	handler := newTestHandler(t, cfg, events, view, &sealer{})

	topic := "https://publisher.example.test/orders?region=west"
	response := serveForm(handler, url.Values{
		"hub.mode":         {"register"},
		"hub.topic":        {topic},
		"hub.content_type": {"application/json; charset=utf-8"},
	})
	if response.Code != http.StatusOK {
		t.Fatalf("registration status = %d body=%q", response.Code, response.Body.String())
	}
	event := receiveEvent(t, events.events)
	registered, ok := event.(state.TopicRegistered)
	if !ok {
		t.Fatalf("event type = %T", event)
	}
	topicID, _ := persistence.TopicID(topic)
	destination, _ := persistence.ContentDestination(topic)
	if registered.Topic.ID != topicID ||
		registered.Topic.CanonicalURL != topic ||
		registered.Topic.ContentDestination != string(destination) ||
		registered.Topic.ContentType != "application/json; charset=utf-8" ||
		registered.Meta.Actor.Type != "publisher" || registered.Meta.Actor.ID != "subject-1" {
		t.Fatalf("registration = %#v", registered)
	}
	if len(view.Snapshot().Topics) != 0 {
		t.Fatal("callback mutated projection before state consumption")
	}

	applied, _, err := (state.Reducer{}).Apply(state.EmptySnapshot(), registered)
	if err != nil {
		t.Fatal(err)
	}
	view.set(applied)
	response = serveForm(handler, url.Values{
		"hub.mode": {"register"}, "hub.topic": {topic},
		"hub.content_type": {"application/json; charset=utf-8"},
	})
	if response.Code != http.StatusConflict {
		t.Fatalf("duplicate registration status = %d", response.Code)
	}
	assertNoEvent(t, events.events)

	response = serveForm(handler, url.Values{"hub.mode": {"deregister"}, "hub.topic": {topic}})
	if response.Code != http.StatusOK {
		t.Fatalf("deregistration status = %d body=%q", response.Code, response.Body.String())
	}
	event = receiveEvent(t, events.events)
	deregistered, ok := event.(state.TopicDeregistered)
	if !ok || deregistered.TopicID != topicID {
		t.Fatalf("deregistration = %#v", event)
	}
	if view.Snapshot().Topics[topicID].Status != state.TopicActive {
		t.Fatal("deregistration callback mutated projection before state consumption")
	}
}

func TestTopicContentTypeDefaultsAndCannotChangeAfterDeregistration(t *testing.T) {
	cfg := testConfig()
	cfg.Protocol.PublisherExtensionEnabled = true
	events := &eventAppender{events: make(chan state.Event, 4)}
	view := &projection{snapshot: state.EmptySnapshot()}
	handler := newTestHandler(t, cfg, events, view, &sealer{})
	topic := "https://publisher.example.test/default-content"

	response := serveForm(handler, url.Values{"hub.mode": {"register"}, "hub.topic": {topic}})
	if response.Code != http.StatusOK {
		t.Fatalf("registration status = %d body=%q", response.Code, response.Body.String())
	}
	registered := receiveEvent(t, events.events).(state.TopicRegistered)
	if registered.Topic.ContentType != state.DefaultTopicContentType {
		t.Fatalf("default content type = %q", registered.Topic.ContentType)
	}
	snapshot, _, err := (state.Reducer{}).Apply(state.EmptySnapshot(), registered)
	if err != nil {
		t.Fatal(err)
	}
	deregistered := state.TopicDeregistered{Meta: state.EventMetadata{SchemaVersion: state.SchemaVersion, EventID: "deregister", OccurredAt: adapterTime, Actor: state.Actor{Type: "publisher"}}, TopicID: registered.Topic.ID}
	snapshot, _, err = (state.Reducer{}).Apply(snapshot, deregistered)
	if err != nil {
		t.Fatal(err)
	}
	view.set(snapshot)

	response = serveForm(handler, url.Values{
		"hub.mode": {"register"}, "hub.topic": {topic}, "hub.content_type": {"text/plain"},
	})
	if response.Code >= 200 && response.Code < 300 {
		t.Fatalf("content type change status = %d", response.Code)
	}
	assertNoEvent(t, events.events)
}

func TestVerifiedSubscriptionSealsAndAppendsOwnedState(t *testing.T) {
	cfg := testConfig()
	events := &eventAppender{events: make(chan state.Event, 4)}
	topic := "https://publisher.example.test/orders"
	view := &projection{snapshot: activeTopicSnapshot(t, topic)}
	secrets := &sealer{}
	handler := newTestHandler(t, cfg, events, view, secrets)

	callback := verificationServer(t)
	values := url.Values{
		"hub.mode":             {"subscribe"},
		"hub.topic":            {topic},
		"hub.callback":         {callback.URL + "/capability?token=opaque"},
		"hub.lease_seconds":    {"300"},
		"hub.secret":           {"subscriber-secret"},
		"kafka.consumer_group": {"workers"},
	}
	response := serveForm(handler, values)
	if response.Code != http.StatusAccepted {
		t.Fatalf("subscription status = %d body=%q", response.Code, response.Body.String())
	}
	event := receiveEvent(t, events.events)
	verified, ok := event.(state.SubscriptionVerified)
	if !ok {
		t.Fatalf("event type = %T", event)
	}
	subscription := verified.Subscription
	if !strings.HasPrefix(subscription.ID, "subscription-") ||
		!strings.HasPrefix(subscription.ConsumerID, "delivery-") ||
		subscription.ID == subscription.ConsumerID ||
		subscription.ServerID != cfg.Server.ID ||
		string(subscription.SecretCiphertext) != "sealed-value" ||
		subscription.SecretKeyID != "test-key" ||
		secrets.input() != "subscriber-secret" || subscription.Parameters["kafka.consumer_group"][0] != "workers" {
		t.Fatalf("subscription = %#v sealed input=%q", subscription, secrets.input())
	}
	encoded, err := state.EncodeEvent(verified)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "subscriber-secret") {
		t.Fatal("plaintext secret entered durable state event")
	}
	if _, ok := view.Snapshot().Subscriptions[subscription.ID]; ok {
		t.Fatal("verified callback mutated projection before state consumption")
	}

	snapshot := view.Snapshot()
	subscription.Status = state.SubscriptionActive
	snapshot.Subscriptions[subscription.ID] = subscription
	view.set(snapshot)
	response = serveForm(handler, values)
	if response.Code != http.StatusConflict {
		t.Fatalf("active renewal status = %d", response.Code)
	}
	assertNoEvent(t, events.events)
}

func TestVerifiedUnsubscriptionAppendsCurrentProductIdentity(t *testing.T) {
	cfg := testConfig()
	events := &eventAppender{events: make(chan state.Event, 4)}
	topic := "https://publisher.example.test/orders"
	callback := verificationServer(t)
	snapshot := activeTopicSnapshot(t, topic)
	subscription := state.Subscription{
		ID: "subscription-current", TopicID: mustTopicID(t, topic),
		TopicURL: topic, CallbackURL: callback.URL, LeaseStartedAt: adapterTime,
		ServerID: cfg.Server.ID, ConsumerID: "delivery-current",
		Status: state.SubscriptionActive,
	}
	snapshot.Subscriptions[subscription.ID] = subscription
	view := &projection{snapshot: snapshot}
	handler := newTestHandler(t, cfg, events, view, &sealer{})

	response := serveForm(handler, url.Values{
		"hub.mode": {"unsubscribe"}, "hub.topic": {topic}, "hub.callback": {callback.URL},
	})
	if response.Code != http.StatusAccepted {
		t.Fatalf("unsubscription status = %d body=%q", response.Code, response.Body.String())
	}
	event := receiveEvent(t, events.events)
	unsubscribed, ok := event.(state.SubscriptionUnsubscribed)
	if !ok || unsubscribed.SubscriptionID != subscription.ID {
		t.Fatalf("unsubscription = %#v", event)
	}
	if view.Snapshot().Subscriptions[subscription.ID].Status != state.SubscriptionActive {
		t.Fatal("unsubscription callback mutated projection before state consumption")
	}
}

func TestPreviewDoesNotRenewOrExpireActiveSubscription(t *testing.T) {
	cfg := testConfig()
	events := &eventAppender{events: make(chan state.Event, 1)}
	topic := "https://publisher.example.test/orders"
	callback := "https://subscriber.example.test/callback"
	snapshot := activeTopicSnapshot(t, topic)
	snapshot.Subscriptions["subscription-expired"] = state.Subscription{
		ID: "subscription-expired", TopicID: mustTopicID(t, topic),
		TopicURL: topic, CallbackURL: callback,
		LeaseStartedAt: adapterTime.Add(-time.Hour), EffectiveLeaseSeconds: "1",
		ServerID: cfg.Server.ID, ConsumerID: "delivery-expired",
		Status: state.SubscriptionActive,
	}
	handler := newTestHandler(t, cfg, events, &projection{snapshot: snapshot}, &sealer{})
	response := serveForm(handler, url.Values{
		"hub.mode": {"subscribe"}, "hub.topic": {topic}, "hub.callback": {callback},
	})
	if response.Code != http.StatusConflict {
		t.Fatalf("expired active subscription status = %d", response.Code)
	}
	assertNoEvent(t, events.events)
}

func TestPublisherExtensionAndContentIngestionAreExplicit(t *testing.T) {
	cfg := testConfig()
	events := &eventAppender{events: make(chan state.Event, 1)}
	view := &projection{snapshot: state.EmptySnapshot()}
	handler := newTestHandler(t, cfg, events, view, &sealer{})
	response := serveForm(handler, url.Values{
		"hub.mode": {"register"}, "hub.topic": {"https://publisher.example.test/orders"},
	})
	if response.Code != http.StatusBadRequest {
		t.Fatalf("disabled publisher extension status = %d", response.Code)
	}

	cfg.Protocol.PublisherExtensionEnabled = true
	handler = newTestHandler(t, cfg, events, view, &sealer{})
	request := httptest.NewRequest(http.MethodPost,
		"/websub?hub.mode=publish&hub.topic=https%3A%2F%2Fpublisher.example.test%2Forders",
		strings.NewReader(`{"order":"A-42"}`))
	request.Header.Set("Content-Type", "application/json; charset=utf-8")
	request.Header.Set("X-Go-Publisher", "publish")
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code >= 200 && response.Code < 300 {
		t.Fatalf("content was acknowledged without a durable sink: %d", response.Code)
	}
}

func TestPublicationContentTypeMismatchIsDenied(t *testing.T) {
	cfg := testConfig()
	cfg.Protocol.PublisherExtensionEnabled = true
	events := &eventAppender{events: make(chan state.Event, 1)}
	view := &projection{snapshot: state.EmptySnapshot()}
	called := false
	handler := newTestHandlerWithContent(t, cfg, events, view, &sealer{}, contentSinkFunc(func(_ context.Context, update ContentUpdate) error {
		called = true
		if update.ContentType != "text/plain" {
			t.Fatalf("publication content type = %q", update.ContentType)
		}
		return ErrContentTypeMismatch
	}))
	request := httptest.NewRequest(http.MethodPost,
		"/websub?hub.mode=publish&hub.topic=https%3A%2F%2Fpublisher.example.test%2Forders",
		strings.NewReader("not-json"))
	request.Header.Set("Content-Type", "text/plain")
	request.Header.Set("X-Go-Publisher", "publish")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if !called || response.Code >= 200 && response.Code < 300 {
		t.Fatalf("called=%v status=%d body=%q", called, response.Code, response.Body.String())
	}
}

func TestNewRequiresDurableDependencies(t *testing.T) {
	cfg := testConfig()
	if _, err := New(cfg, Dependencies{}); err == nil {
		t.Fatal("missing durable dependencies accepted")
	}
}

func newTestHandler(t *testing.T, cfg config.HubConfig, events EventAppender, view Projection, secrets SecretSealer) *Handler {
	return newTestHandlerWithDependencies(t, cfg, events, view, secrets, allowSubscriptions{}, nil)
}

func newTestHandlerWithValidator(t *testing.T, cfg config.HubConfig, events EventAppender, view Projection, secrets SecretSealer, subscriptions SubscriptionValidator) *Handler {
	return newTestHandlerWithDependencies(t, cfg, events, view, secrets, subscriptions, nil)
}

func newTestHandlerWithContent(t *testing.T, cfg config.HubConfig, events EventAppender, view Projection, secrets SecretSealer, content ContentSink) *Handler {
	return newTestHandlerWithDependencies(t, cfg, events, view, secrets, allowSubscriptions{}, content)
}

func newTestHandlerWithDependencies(t *testing.T, cfg config.HubConfig, events EventAppender, view Projection, secrets SecretSealer, subscriptions SubscriptionValidator, content ContentSink) *Handler {
	t.Helper()
	sequence := 0
	handler, err := New(cfg, Dependencies{
		Events: events, Projection: view, Secrets: secrets, Callbacks: allowCallbacks{}, Authorization: allowAuthorization{},
		Subscriptions: subscriptions, Content: content, VerificationClient: http.DefaultClient,
		Now: func() time.Time { return adapterTime },
		NewEventID: func() (string, error) {
			sequence++
			return "event-test-" + string(rune('0'+sequence)), nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := handler.Close(context.Background()); err != nil {
			t.Errorf("close handler: %v", err)
		}
	})
	return handler
}

func testConfig() config.HubConfig {
	cfg := config.HubDefaults()
	cfg.Server.ID = "hub-a"
	cfg.Server.PublicURL = "https://hub.example.test/websub"
	return cfg
}

func activeTopicSnapshot(t *testing.T, topic string) state.Snapshot {
	t.Helper()
	topicID := mustTopicID(t, topic)
	snapshot := state.EmptySnapshot()
	snapshot.Topics[topicID] = state.Topic{
		ID: topicID, CanonicalURL: topic, ContentDestination: "content",
		ContentType: "application/json", Status: state.TopicActive, RegisteredAt: adapterTime,
	}
	return snapshot
}

func mustTopicID(t *testing.T, topic string) string {
	t.Helper()
	topicID, err := persistence.TopicID(topic)
	if err != nil {
		t.Fatal(err)
	}
	return topicID
}

func verificationServer(t *testing.T) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		challenge := request.URL.Query().Get("hub.challenge")
		if challenge == "" {
			http.Error(response, "missing challenge", http.StatusBadRequest)
			return
		}
		response.Header().Set("Content-Type", "text/plain")
		_, _ = response.Write([]byte(challenge))
	}))
	t.Cleanup(server.Close)
	return server
}

func serveForm(handler http.Handler, values url.Values) *httptest.ResponseRecorder {
	request := httptest.NewRequest(http.MethodPost, "/websub", strings.NewReader(values.Encode()))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func receiveEvent(t *testing.T, events <-chan state.Event) state.Event {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	select {
	case event := <-events:
		return event
	case <-ctx.Done():
		t.Fatal("timed out waiting for durable state event")
		return nil
	}
}

func assertNoEvent(t *testing.T, events <-chan state.Event) {
	t.Helper()
	select {
	case event := <-events:
		t.Fatalf("unexpected state event %T", event)
	default:
	}
}
