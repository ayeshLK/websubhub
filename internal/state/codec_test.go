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
	"reflect"
	"strings"
	"testing"
)

func TestEventCodecRoundTrip(t *testing.T) {
	t.Parallel()
	events := []Event{
		topicEvent(),
		TopicDeregistered{Meta: meta("deregister"), TopicID: "topic-1"},
		subscriptionEvent(),
		SubscriptionUnsubscribed{Meta: meta("unsubscribe"), SubscriptionID: "sub-1"},
		SubscriptionStaleEvent{Meta: meta("stale"), SubscriptionID: "sub-1", Reason: "retry_exhausted"},
		SubscriptionReactivated{Meta: meta("reactivate"), SubscriptionID: "sub-1"},
		SubscriptionRemovedEvent{Meta: meta("remove"), SubscriptionID: "sub-1", Cause: "http_410"},
	}
	for _, event := range events {
		event := event
		t.Run(event.eventType(), func(t *testing.T) {
			encoded, err := EncodeEvent(event)
			if err != nil {
				t.Fatal(err)
			}
			decoded, err := DecodeEvent(encoded)
			if err != nil {
				t.Fatal(err)
			}
			if reflect.Indirect(reflect.ValueOf(decoded)).Interface() != nil && !reflect.DeepEqual(reflect.Indirect(reflect.ValueOf(decoded)).Interface(), event) {
				t.Fatalf("decoded = %#v, want %#v", decoded, event)
			}
		})
	}
}

func TestEventCodecRejectsUnknownAndTrailingData(t *testing.T) {
	t.Parallel()
	if _, err := DecodeEvent([]byte(`{"type":"invented"}`)); err == nil || !strings.Contains(err.Error(), "unknown") {
		t.Fatalf("unknown type error = %v", err)
	}
	encoded, err := EncodeEvent(topicEvent())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeEvent(append(encoded, []byte(` {}`)...)); err == nil || !strings.Contains(err.Error(), "trailing") {
		t.Fatalf("trailing value error = %v", err)
	}
}
