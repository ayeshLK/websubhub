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

package delivery

import (
	"context"
	"sync"
	"testing"

	"github.com/ayeshLK/websubhub/internal/persistence/messagestore"
	"github.com/ayeshLK/websubhub/internal/state"
)

func TestManagerRunsOneOwnedActiveWorkerAndPermanentlyStopsRemoval(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	factory := &runnerFactory{}
	manager, err := NewManager(ctx, "hub-1", factory)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := managerSnapshot(state.SubscriptionActive, "hub-1")
	if err := manager.Reconcile(snapshot); err != nil {
		t.Fatal(err)
	}
	if err := manager.Reconcile(snapshot); err != nil {
		t.Fatal(err)
	}
	if len(factory.runners) != 1 || factory.consumerIDs[0] != "consumer-stable" {
		t.Fatalf("workers=%d consumer IDs=%#v", len(factory.runners), factory.consumerIDs)
	}
	snapshot.Subscriptions["subscription-1"] = state.Subscription{ID: "subscription-1", TopicID: "topic-1", Status: state.SubscriptionRemoved, ServerID: "hub-1", ConsumerID: "consumer-stable"}
	if err := manager.Reconcile(snapshot); err != nil {
		t.Fatal(err)
	}
	if got := factory.runners[0].intent(); got != messagestore.ClosePermanent {
		t.Fatalf("stop intent = %q", got)
	}
}

func TestManagerUsesTemporaryStopForStaleAndWrongOwner(t *testing.T) {
	for _, test := range []struct {
		name   string
		status state.SubscriptionStatus
		owner  string
	}{{"stale", state.SubscriptionStale, "hub-1"}, {"ownership", state.SubscriptionActive, "hub-2"}} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			factory := &runnerFactory{}
			manager, _ := NewManager(ctx, "hub-1", factory)
			if err := manager.Reconcile(managerSnapshot(state.SubscriptionActive, "hub-1")); err != nil {
				t.Fatal(err)
			}
			if err := manager.Reconcile(managerSnapshot(test.status, test.owner)); err != nil {
				t.Fatal(err)
			}
			if got := factory.runners[0].intent(); got != messagestore.CloseTemporary {
				t.Fatalf("stop intent = %q", got)
			}
		})
	}
}

func managerSnapshot(status state.SubscriptionStatus, owner string) state.Snapshot {
	return state.Snapshot{Topics: map[string]state.Topic{"topic-1": {ID: "topic-1", Status: state.TopicActive, ContentDestination: "content"}}, Subscriptions: map[string]state.Subscription{"subscription-1": {ID: "subscription-1", TopicID: "topic-1", Status: status, ServerID: owner, ConsumerID: "consumer-stable"}}}
}

type runnerFactory struct {
	runners     []*blockingRunner
	consumerIDs []string
}

func (f *runnerFactory) New(_ state.Topic, subscription state.Subscription) (Runner, error) {
	runner := &blockingRunner{done: make(chan struct{})}
	f.runners = append(f.runners, runner)
	f.consumerIDs = append(f.consumerIDs, subscription.ConsumerID)
	return runner, nil
}

type blockingRunner struct {
	mu         sync.Mutex
	stopIntent messagestore.ClosureIntent
	done       chan struct{}
	once       sync.Once
}

func (r *blockingRunner) Run(context.Context) error { <-r.done; return nil }
func (r *blockingRunner) Stop(intent messagestore.ClosureIntent) {
	r.mu.Lock()
	r.stopIntent = intent
	r.mu.Unlock()
	r.once.Do(func() { close(r.done) })
}
func (r *blockingRunner) intent() messagestore.ClosureIntent {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.stopIntent
}
