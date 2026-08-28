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

package management

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/ayeshLK/websubhub/internal/state"
)

func TestQueriesCanonicalSnapshotWithStableFilteringAndBounds(t *testing.T) {
	snapshot := managementSnapshot()
	service, err := New(testSnapshots{snapshot: snapshot})
	if err != nil {
		t.Fatal(err)
	}
	topics, err := service.ListTopics(t.Context(), TopicQuery{Limit: 1, Status: "active"})
	if err != nil {
		t.Fatal(err)
	}
	if topics.Revision != 9 || len(topics.Topics) != 1 || topics.Topics[0].ID != "topic-a" {
		t.Fatalf("topics = %#v", topics)
	}
	subscriptions, err := service.ListSubscriptions(t.Context(), SubscriptionQuery{Limit: 1, TopicID: "topic-a", Status: "stale"})
	if err != nil {
		t.Fatal(err)
	}
	if subscriptions.Revision != 9 || len(subscriptions.Subscriptions) != 1 || subscriptions.Subscriptions[0].ID != "subscription-b" {
		t.Fatalf("subscriptions = %#v", subscriptions)
	}
}

func TestSubscriptionViewsRedactSensitiveState(t *testing.T) {
	service, err := New(testSnapshots{snapshot: managementSnapshot()})
	if err != nil {
		t.Fatal(err)
	}
	result, err := service.GetSubscription(t.Context(), "subscription-b")
	if err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	text := string(body)
	if !strings.Contains(text, `"callback":"https://subscriber.example/callback"`) {
		t.Fatalf("callback was not safely represented: %s", text)
	}
	for _, forbidden := range []string{"capability-token", "ciphertext-value", "key-1", "consumer-internal"} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("response disclosed %q: %s", forbidden, text)
		}
	}
}

func TestDetailDistinguishesRetainedAndUnknownResources(t *testing.T) {
	service, err := New(testSnapshots{snapshot: managementSnapshot()})
	if err != nil {
		t.Fatal(err)
	}
	topic, err := service.GetTopic(t.Context(), "topic-z")
	if err != nil || topic.Topic.Status != state.TopicInactive {
		t.Fatalf("inactive topic = %#v, %v", topic, err)
	}
	subscription, err := service.GetSubscription(t.Context(), "subscription-z")
	if err != nil || subscription.Subscription.Status != state.SubscriptionRemoved {
		t.Fatalf("removed subscription = %#v, %v", subscription, err)
	}
	if _, err := service.GetTopic(t.Context(), "unknown"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unknown topic error = %v", err)
	}
}

func TestQueryValidationAndSnapshotFailure(t *testing.T) {
	service, err := New(testSnapshots{snapshot: managementSnapshot()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.ListTopics(t.Context(), TopicQuery{Limit: 101}); !errors.Is(err, ErrInvalidQuery) {
		t.Fatalf("limit error = %v", err)
	}
	if _, err := service.ListSubscriptions(t.Context(), SubscriptionQuery{Status: "unknown"}); !errors.Is(err, ErrInvalidQuery) {
		t.Fatalf("status error = %v", err)
	}
	want := errors.New("consolidator unavailable")
	failing, err := New(testSnapshots{err: want})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := failing.ListTopics(t.Context(), TopicQuery{}); !errors.Is(err, want) {
		t.Fatalf("snapshot error = %v", err)
	}
}

type testSnapshots struct {
	snapshot state.Snapshot
	err      error
}

func (s testSnapshots) Snapshot(context.Context) (state.Snapshot, error) {
	return s.snapshot, s.err
}

func managementSnapshot() state.Snapshot {
	now := time.Date(2026, 8, 27, 0, 0, 0, 0, time.UTC)
	snapshot := state.EmptySnapshot()
	snapshot.Revision = 9
	snapshot.Topics["topic-a"] = state.Topic{ID: "topic-a", CanonicalURL: "https://publisher.example/a", ContentDestination: "internal-a", Status: state.TopicActive, RegisteredAt: now, Revision: 1}
	snapshot.Topics["topic-z"] = state.Topic{ID: "topic-z", CanonicalURL: "https://publisher.example/z", ContentDestination: "internal-z", Status: state.TopicInactive, RegisteredAt: now, Revision: 8}
	snapshot.Subscriptions["subscription-b"] = state.Subscription{
		ID: "subscription-b", TopicID: "topic-a", TopicURL: "https://publisher.example/a",
		CallbackURL:      "https://subscriber.example/callback?token=capability-token#fragment",
		SecretCiphertext: []byte("ciphertext-value"), SecretKeyID: "key-1", LeaseStartedAt: now,
		ServerID: "hub-a", ConsumerID: "consumer-internal", Status: state.SubscriptionStale, StaleReason: "delivery_attempts_exhausted", Revision: 7,
	}
	snapshot.Subscriptions["subscription-z"] = state.Subscription{
		ID: "subscription-z", TopicID: "topic-z", TopicURL: "https://publisher.example/z",
		CallbackURL: "https://subscriber.example/removed", LeaseStartedAt: now,
		ServerID: "hub-a", ConsumerID: "consumer-removed", Status: state.SubscriptionRemoved, Revision: 9,
	}
	return snapshot
}
