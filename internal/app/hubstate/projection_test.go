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

package hubstate

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ayeshLK/websubhub/internal/persistence/messagestore"
	"github.com/ayeshLK/websubhub/internal/persistence/messagestore/messagestoretest"
	"github.com/ayeshLK/websubhub/internal/persistence/statestore"
	"github.com/ayeshLK/websubhub/internal/state"
)

type source struct {
	readyErr    error
	snapshot    state.Snapshot
	snapshotErr error
}

func (s *source) Ready(context.Context) error { return s.readyErr }
func (s *source) Snapshot(context.Context) (state.Snapshot, error) {
	return s.snapshot, s.snapshotErr
}

func TestStartInstallsSnapshotAndAppliesBufferedDuplicate(t *testing.T) {
	backing := messagestoretest.New(messagestore.Capabilities{})
	store := testStore(t, backing)
	if err := store.Initialize(t.Context()); err != nil {
		t.Fatal(err)
	}
	event := testEvent("event-1")
	snapshot, changed, err := (state.Reducer{}).Apply(state.EmptySnapshot(), event)
	if err != nil || !changed {
		t.Fatal(err)
	}

	hooked := &openHookStore{Store: store, hook: func(ctx context.Context) error {
		return store.Append(ctx, event)
	}}
	projection, err := New(hooked, &source{snapshot: snapshot}, "hub-1", 10)
	if err != nil {
		t.Fatal(err)
	}
	if err := projection.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	if !projection.Ready() || projection.Snapshot().Revision != 1 {
		t.Fatalf("ready=%v snapshot=%#v", projection.Ready(), projection.Snapshot())
	}
}

func TestStartRejectsUnavailableAndMalformedSnapshot(t *testing.T) {
	store := testStore(t, messagestoretest.New(messagestore.Capabilities{}))
	projection, _ := New(store, &source{readyErr: errors.New("offline")}, "hub-1", 10)
	if err := projection.Start(t.Context()); err == nil {
		t.Fatal("unavailable consolidator accepted")
	}
	projection, _ = New(store, &source{snapshot: state.Snapshot{SchemaVersion: state.SchemaVersion}}, "hub-1", 10)
	if err := projection.Start(t.Context()); err == nil {
		t.Fatal("malformed snapshot accepted")
	}
}

func TestRestartUsesStableConsumerProgress(t *testing.T) {
	backing := messagestoretest.New(messagestore.Capabilities{})
	store := testStore(t, backing)
	if err := store.Initialize(t.Context()); err != nil {
		t.Fatal(err)
	}
	src := &source{snapshot: state.EmptySnapshot()}
	first, _ := New(store, src, "stable-hub", 10)
	if err := first.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	event := testEvent("event-1")
	if err := store.Append(t.Context(), event); err != nil {
		t.Fatal(err)
	}
	if _, err := first.Consume(t.Context(), 10); err != nil {
		t.Fatal(err)
	}
	src.snapshot = first.Snapshot()
	if err := first.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	second, _ := New(store, src, "stable-hub", 10)
	if err := second.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	if second.Snapshot().Revision != 1 {
		t.Fatalf("restart revision = %d", second.Snapshot().Revision)
	}
}

type openHookStore struct {
	statestore.Store
	hook func(context.Context) error
}

func (s *openHookStore) OpenEvents(ctx context.Context, id messagestore.ConsumerID, start messagestore.StartPosition) (statestore.EventConsumer, error) {
	consumer, err := s.Store.OpenEvents(ctx, id, start)
	if err != nil {
		return nil, err
	}
	if err := s.hook(ctx); err != nil {
		return nil, err
	}
	return consumer, nil
}

func testStore(t *testing.T, backing *messagestoretest.Store) *statestore.MessageStore {
	t.Helper()
	store, err := statestore.New(backing.Producer(), backing.Administrator(), time.Hour, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func testEvent(id string) state.TopicRegistered {
	return state.TopicRegistered{
		Meta:  state.EventMetadata{SchemaVersion: state.SchemaVersion, EventID: id, OccurredAt: time.Unix(1, 0).UTC(), Actor: state.Actor{Type: "test"}},
		Topic: state.Topic{ID: "topic-1", CanonicalURL: "https://example.test/topic", ContentDestination: "content-1", RegisteredAt: time.Unix(1, 0).UTC()},
	}
}
