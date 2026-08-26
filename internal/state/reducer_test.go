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

package state

import (
	"bytes"
	"errors"
	"testing"
	"time"
)

var testTime = time.Date(2026, 8, 25, 10, 0, 0, 0, time.UTC)

func meta(id string) EventMetadata {
	return EventMetadata{SchemaVersion: SchemaVersion, EventID: id, OccurredAt: testTime, Actor: Actor{Type: "test", ID: "actor"}}
}
func topicEvent() TopicRegistered {
	return TopicRegistered{Meta: meta("event-topic"), Topic: Topic{ID: "topic-1", CanonicalURL: "https://publisher.example/resource", ContentDestination: "content-1", RegisteredAt: testTime, RegisteredBy: "actor"}}
}
func subscriptionEvent() SubscriptionVerified {
	return SubscriptionVerified{Meta: meta("event-sub"), Subscription: Subscription{ID: "sub-1", TopicID: "topic-1", TopicURL: "https://publisher.example/resource", CallbackURL: "https://subscriber.example/callback?cap=redacted", SecretCiphertext: []byte("ciphertext"), SecretKeyID: "key-1", LeaseStartedAt: testTime, EffectiveLeaseSeconds: "86400", ServerID: "hub-1", ConsumerID: "consumer-1"}}
}

func TestReducerLifecycleAndIdempotence(t *testing.T) {
	t.Parallel()
	reducer := Reducer{}
	state, changed, err := reducer.Apply(EmptySnapshot(), topicEvent())
	if err != nil || !changed || state.Revision != 1 {
		t.Fatalf("register = (%v, %v, %v)", state.Revision, changed, err)
	}

	unchanged, changed, err := reducer.Apply(state, topicEvent())
	if err != nil || changed || unchanged.Revision != state.Revision {
		t.Fatalf("duplicate changed state: changed=%v err=%v", changed, err)
	}

	state, changed, err = reducer.Apply(state, subscriptionEvent())
	if err != nil || !changed || state.Revision != 2 {
		t.Fatalf("subscribe: changed=%v err=%v", changed, err)
	}
	state, changed, err = reducer.Apply(state, subscriptionEvent())
	if err != nil || changed || state.Revision != 2 {
		t.Fatalf("duplicate subscription changed state: changed=%v err=%v", changed, err)
	}
	originalConsumer := state.Subscriptions["sub-1"].ConsumerID

	state, _, err = reducer.Apply(state, SubscriptionStaleEvent{Meta: meta("event-stale"), SubscriptionID: "sub-1", Reason: "retry_exhausted"})
	if err != nil || state.Subscriptions["sub-1"].Status != SubscriptionStale {
		t.Fatalf("stale: %v", err)
	}
	state, _, err = reducer.Apply(state, SubscriptionReactivated{Meta: meta("event-reactivate"), SubscriptionID: "sub-1"})
	if err != nil || state.Subscriptions["sub-1"].Status != SubscriptionActive || state.Subscriptions["sub-1"].ConsumerID != originalConsumer {
		t.Fatalf("reactivate: %#v, %v", state.Subscriptions["sub-1"], err)
	}
	state, _, err = reducer.Apply(state, SubscriptionRemovedEvent{Meta: meta("event-410"), SubscriptionID: "sub-1", Cause: "http_410"})
	if err != nil || state.Subscriptions["sub-1"].Status != SubscriptionRemoved {
		t.Fatalf("remove: %v", err)
	}
	_, _, err = reducer.Apply(state, SubscriptionReactivated{Meta: meta("event-invalid"), SubscriptionID: "sub-1"})
	if !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("reactivate removed error = %v", err)
	}
}

func TestReducerDoesNotMutateInput(t *testing.T) {
	t.Parallel()
	input := EmptySnapshot()
	output, _, err := (Reducer{}).Apply(input, topicEvent())
	if err != nil {
		t.Fatal(err)
	}
	if len(input.Topics) != 0 || len(output.Topics) != 1 {
		t.Fatalf("input/output sizes = %d/%d", len(input.Topics), len(output.Topics))
	}
}

func TestReducerCoalescesConcurrentActiveRenewal(t *testing.T) {
	t.Parallel()
	reducer := Reducer{}
	snapshot, _, err := reducer.Apply(EmptySnapshot(), topicEvent())
	if err != nil {
		t.Fatal(err)
	}
	snapshot, _, err = reducer.Apply(snapshot, subscriptionEvent())
	if err != nil {
		t.Fatal(err)
	}
	renewal := subscriptionEvent()
	renewal.Meta = meta("event-concurrent-renewal")
	renewal.Subscription.ID = "sub-2"
	renewal.Subscription.ConsumerID = "consumer-2"
	renewal.Subscription.LeaseStartedAt = testTime.Add(time.Second)
	unchanged, changed, err := reducer.Apply(snapshot, renewal)
	if err != nil || changed || unchanged.Revision != snapshot.Revision {
		t.Fatalf("concurrent renewal: changed=%v revision=%d err=%v", changed, unchanged.Revision, err)
	}
	snapshot, _, err = reducer.Apply(snapshot, SubscriptionUnsubscribed{
		Meta: meta("event-unsubscribe"), SubscriptionID: "sub-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, changed, err := reducer.Apply(snapshot, renewal); err != nil || !changed {
		t.Fatalf("resubscribe after removal: changed=%v err=%v", changed, err)
	}
}

func TestSnapshotEncodingIsDeterministicAndStrict(t *testing.T) {
	t.Parallel()
	reducer := Reducer{}
	one, _, _ := reducer.Apply(EmptySnapshot(), topicEvent())
	one, _, _ = reducer.Apply(one, subscriptionEvent())
	two := EmptySnapshot()
	two.Revision = one.Revision
	two.Subscriptions["sub-1"] = one.Subscriptions["sub-1"]
	two.Topics["topic-1"] = one.Topics["topic-1"]

	encodedOne, err := EncodeSnapshot(one)
	if err != nil {
		t.Fatal(err)
	}
	encodedTwo, err := EncodeSnapshot(two)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(encodedOne, encodedTwo) {
		t.Fatalf("encoding differs:\n%s\n%s", encodedOne, encodedTwo)
	}
	roundTrip, err := DecodeSnapshot(encodedOne)
	if err != nil {
		t.Fatal(err)
	}
	if roundTrip.Revision != 2 || roundTrip.Subscriptions["sub-1"].ConsumerID != "consumer-1" {
		t.Fatalf("round trip = %#v", roundTrip)
	}
	if _, err := DecodeSnapshot([]byte(`{"schema_version":2,"revision":0,"topics":[],"subscriptions":[]}`)); err == nil {
		t.Fatal("unknown version accepted")
	}
	if _, err := DecodeSnapshot([]byte(`{"schema_version":1,"revision":0,"topics":[],"subscriptions":[],"provider_offset":1}`)); err == nil {
		t.Fatal("unknown field accepted")
	}
}
