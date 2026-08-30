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
	"testing"
	"time"

	"github.com/ayeshLK/websubhub/internal/persistence/messagestore"
	"github.com/ayeshLK/websubhub/internal/state"
)

func TestPermanentMessageStoreValidationDeniesBeforeVerification(t *testing.T) {
	notifications := make(chan url.Values, 1)
	callback := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		notifications <- request.URL.Query()
		response.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(callback.Close)

	events := &eventAppender{events: make(chan state.Event, 1)}
	topic := "https://publisher.example.test/orders"
	view := &projection{snapshot: activeTopicSnapshot(t, topic)}
	validator := subscriptionValidatorFunc(func(_ context.Context, destination messagestore.Destination, options messagestore.SubscriptionOptions) error {
		if destination != "content" || options.Parameters["kafka.topic_partitions"][0] != "4" {
			t.Fatalf("validation destination=%q options=%#v", destination, options)
		}
		return &messagestore.PermanentSubscriptionError{Reason: "Kafka topic partition does not exist"}
	})
	handler := newTestHandlerWithValidator(t, testConfig(), events, view, &sealer{}, validator)
	response := serveForm(handler, url.Values{
		"hub.mode": {"subscribe"}, "hub.topic": {topic}, "hub.callback": {callback.URL},
		"kafka.topic_partitions": {"4"},
	})
	if response.Code != http.StatusAccepted {
		t.Fatalf("status=%d body=%q", response.Code, response.Body.String())
	}

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	select {
	case query := <-notifications:
		if query.Get("hub.mode") != "denied" || query.Get("hub.topic") != topic ||
			query.Get("hub.reason") != "Kafka topic partition does not exist" || query.Get("hub.challenge") != "" {
			t.Fatalf("denial query=%#v", query)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for denial callback")
	}
	assertNoEvent(t, events.events)
}
