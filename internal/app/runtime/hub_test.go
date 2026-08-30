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

package runtime

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ayeshLK/websubhub/internal/persistence/messagestore"
	"github.com/ayeshLK/websubhub/internal/state"
)

func TestPublicMuxExposesOnlyConfiguredHubPath(t *testing.T) {
	handler, err := publicMux("https://hub.example.test/websub", http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) { response.WriteHeader(http.StatusNoContent) }))
	if err != nil {
		t.Fatal(err)
	}
	for path, expected := range map[string]int{"/websub": http.StatusNoContent, "/": http.StatusNotFound, "/websub/extra": http.StatusNotFound} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
		if response.Code != expected {
			t.Errorf("path %s status=%d want=%d", path, response.Code, expected)
		}
	}
}

func TestEnsureHubDestinationsProvisionsOnlyActiveTopicsAndDLQ(t *testing.T) {
	administrator := &recordingAdministrator{}
	snapshot := state.EmptySnapshot()
	snapshot.Topics["active"] = state.Topic{ID: "active", ContentDestination: "content-active", Status: state.TopicActive}
	snapshot.Topics["removed"] = state.Topic{ID: "removed", ContentDestination: "content-removed", Status: state.TopicInactive}
	if err := ensureHubDestinations(t.Context(), administrator, snapshot, "delivery-dlq"); err != nil {
		t.Fatal(err)
	}
	if len(administrator.specs) != 2 || administrator.specs[0].Name != "delivery-dlq" || administrator.specs[1].Name != "content-active" {
		t.Fatalf("destination specs = %#v", administrator.specs)
	}
	for _, spec := range administrator.specs {
		if spec.Partitions != 1 {
			t.Fatalf("destination %s partitions=%d", spec.Name, spec.Partitions)
		}
	}
}

type recordingAdministrator struct {
	specs []messagestore.DestinationSpec
}

func (a *recordingAdministrator) EnsureDestination(_ context.Context, spec messagestore.DestinationSpec) error {
	a.specs = append(a.specs, spec)
	return nil
}
func (*recordingAdministrator) ValidateSubscription(context.Context, messagestore.Destination, messagestore.SubscriptionOptions) error {
	return nil
}
func (*recordingAdministrator) OpenConsumer(context.Context, messagestore.ConsumerSpec) (messagestore.Consumer, error) {
	panic("unexpected OpenConsumer")
}
func (*recordingAdministrator) Capabilities(context.Context) (messagestore.Capabilities, error) {
	panic("unexpected Capabilities")
}
func (*recordingAdministrator) Close(context.Context) error { return nil }
