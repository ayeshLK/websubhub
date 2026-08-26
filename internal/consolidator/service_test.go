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

package consolidator

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ayeshLK/websubhub/internal/persistence/messagestore"
	"github.com/ayeshLK/websubhub/internal/persistence/messagestore/messagestoretest"
	"github.com/ayeshLK/websubhub/internal/persistence/statestore"
	"github.com/ayeshLK/websubhub/internal/state"
)

func TestStartBuildsSnapshotAndServesBarrier(t *testing.T) {
	backing := messagestoretest.New(messagestore.Capabilities{})
	store := stateStore(t, backing)
	event := registered("event-1")
	if err := store.Append(t.Context(), event); err != nil {
		t.Fatal(err)
	}
	if err := store.Append(t.Context(), event); err != nil {
		t.Fatal(err)
	}
	service, err := New(store)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	if !service.Ready() || service.Snapshot().Revision != 1 {
		t.Fatalf("ready=%v snapshot=%#v", service.Ready(), service.Snapshot())
	}

	server := httptest.NewServer(service.Handler())
	defer server.Close()
	client, err := NewClient(server.URL, server.Client(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Ready(t.Context()); err != nil {
		t.Fatal(err)
	}
	snapshot, err := client.Snapshot(t.Context())
	if err != nil || snapshot.Revision != 1 {
		t.Fatalf("snapshot = %#v, %v", snapshot, err)
	}
}

func TestSnapshotUnavailableBeforeStart(t *testing.T) {
	backing := messagestoretest.New(messagestore.Capabilities{})
	service, err := New(stateStore(t, backing))
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodGet, SnapshotPath, nil)
	response := httptest.NewRecorder()
	service.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d", response.Code)
	}
}

func stateStore(t *testing.T, backing *messagestoretest.Store) *statestore.MessageStore {
	t.Helper()
	store, err := statestore.New(backing.Producer(), backing.Administrator(), time.Hour, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func registered(id string) state.TopicRegistered {
	return state.TopicRegistered{
		Meta:  state.EventMetadata{SchemaVersion: state.SchemaVersion, EventID: id, OccurredAt: time.Unix(1, 0).UTC(), Actor: state.Actor{Type: "test"}},
		Topic: state.Topic{ID: "topic-1", CanonicalURL: "https://example.test/topic", ContentDestination: "content-1", RegisteredAt: time.Unix(1, 0).UTC()},
	}
}
