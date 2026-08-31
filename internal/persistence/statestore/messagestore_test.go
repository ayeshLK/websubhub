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

package statestore

import (
	"testing"
	"time"

	"github.com/ayeshLK/websubhub/internal/persistence"
	"github.com/ayeshLK/websubhub/internal/persistence/messagestore"
	"github.com/ayeshLK/websubhub/internal/persistence/messagestore/messagestoretest"
	"github.com/ayeshLK/websubhub/internal/state"
)

func TestLoadSnapshotSelectsGreatestRevisionAndEmpty(t *testing.T) {
	backing := messagestoretest.New(messagestore.Capabilities{})
	store := newTestStore(t, backing)
	if err := store.Initialize(t.Context()); err != nil {
		t.Fatal(err)
	}
	empty, err := store.LoadSnapshot(t.Context())
	if err != nil || empty.Revision != 0 {
		t.Fatalf("empty snapshot = %#v, %v", empty, err)
	}
	high := snapshotAt(t, 2)
	low := snapshotAt(t, 1)
	if err := store.SaveSnapshot(t.Context(), high); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveSnapshot(t.Context(), low); err != nil {
		t.Fatal(err)
	}
	loaded, err := store.LoadSnapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Revision != high.Revision {
		t.Fatalf("loaded revision = %d", loaded.Revision)
	}
}

func TestStateEventsRemainTyped(t *testing.T) {
	backing := messagestoretest.New(messagestore.Capabilities{})
	store := newTestStore(t, backing)
	if err := store.Initialize(t.Context()); err != nil {
		t.Fatal(err)
	}
	event := topicEvent("event-1", "topic-1")
	if err := store.Append(t.Context(), event); err != nil {
		t.Fatal(err)
	}
	consumer, err := store.OpenEvents(t.Context(), "typed-reader", messagestore.StartEarliest)
	if err != nil {
		t.Fatal(err)
	}
	batch, err := consumer.Receive(t.Context(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(batch.Records) != 1 || batch.Records[0].Event.Metadata().EventID != event.Meta.EventID || !batch.CaughtUp {
		t.Fatalf("batch = %#v", batch)
	}
}

func TestLoadSnapshotRejectsMalformedRecord(t *testing.T) {
	backing := messagestoretest.New(messagestore.Capabilities{})
	store := newTestStore(t, backing)
	if err := store.Initialize(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := backing.Producer().Send(t.Context(), persistence.StateSnapshotsDestination, messagestore.Message{
		ID: "bad", Body: []byte("{"), ContentType: SnapshotContentType,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.LoadSnapshot(t.Context()); err == nil {
		t.Fatal("malformed snapshot accepted")
	}
}

func TestConfiguredDestinationsAreUsed(t *testing.T) {
	backing := messagestoretest.New(messagestore.Capabilities{})
	options := DefaultOptions()
	options.EventsDestination = "custom-state-events"
	options.SnapshotsDestination = "custom-state-snapshots"
	options.SnapshotLoadBatch = 1
	store, err := New(backing.Producer(), backing.Administrator(), options)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Initialize(t.Context()); err != nil {
		t.Fatal(err)
	}
	event := topicEvent("event-1", "topic-1")
	if err := store.Append(t.Context(), event); err != nil {
		t.Fatal(err)
	}
	consumer, err := store.OpenEvents(t.Context(), "custom-reader", messagestore.StartEarliest)
	if err != nil {
		t.Fatal(err)
	}
	if metadata := consumer.(*eventConsumer).consumer.Metadata(); metadata.Destination != options.EventsDestination {
		t.Fatalf("event destination = %q", metadata.Destination)
	}
	if err := store.SaveSnapshot(t.Context(), snapshotAt(t, 1)); err != nil {
		t.Fatal(err)
	}
	if snapshot, err := store.LoadSnapshot(t.Context()); err != nil || snapshot.Revision != 1 {
		t.Fatalf("snapshot = %#v, %v", snapshot, err)
	}
}

func TestOptionsRejectInvalidTopology(t *testing.T) {
	backing := messagestoretest.New(messagestore.Capabilities{})
	options := DefaultOptions()
	options.SnapshotsDestination = options.EventsDestination
	if _, err := New(backing.Producer(), backing.Administrator(), options); err == nil {
		t.Fatal("identical state destinations accepted")
	}
	options = DefaultOptions()
	options.SnapshotLoadBatch = 0
	if _, err := New(backing.Producer(), backing.Administrator(), options); err == nil {
		t.Fatal("zero snapshot load batch accepted")
	}
}

func newTestStore(t *testing.T, backing *messagestoretest.Store) *MessageStore {
	t.Helper()
	options := DefaultOptions()
	options.EventsRetention = time.Hour
	options.SnapshotsRetention = time.Hour
	store, err := New(backing.Producer(), backing.Administrator(), options)
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func topicEvent(eventID, topicID string) state.TopicRegistered {
	return state.TopicRegistered{
		Meta:  state.EventMetadata{SchemaVersion: state.SchemaVersion, EventID: eventID, OccurredAt: time.Unix(1, 0).UTC(), Actor: state.Actor{Type: "test"}},
		Topic: state.Topic{ID: topicID, CanonicalURL: "https://example.test/" + topicID, ContentDestination: "content-" + topicID, ContentType: "application/json", RegisteredAt: time.Unix(1, 0).UTC()},
	}
}

func snapshotAt(t *testing.T, revision uint64) state.Snapshot {
	t.Helper()
	snapshot := state.EmptySnapshot()
	reducer := state.Reducer{}
	for i := uint64(0); i < revision; i++ {
		event := topicEvent("event-"+string(rune('a'+i)), "topic-"+string(rune('a'+i)))
		next, changed, err := reducer.Apply(snapshot, event)
		if err != nil || !changed {
			t.Fatalf("reduce snapshot: changed=%v err=%v", changed, err)
		}
		snapshot = next
	}
	return snapshot
}
