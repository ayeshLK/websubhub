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
	"errors"
	"sync"

	"github.com/ayeshLK/websubhub/internal/config"
	"github.com/ayeshLK/websubhub/internal/persistence/messagestore"
	"github.com/ayeshLK/websubhub/internal/state"
)

type Runner interface {
	Run(context.Context) error
	Stop(messagestore.ClosureIntent)
}

type WorkerFactory interface {
	New(state.Topic, state.Subscription) (Runner, error)
}

type Factory struct {
	Config       config.Delivery
	Dependencies Dependencies
}

func (f Factory) New(topic state.Topic, subscription state.Subscription) (Runner, error) {
	return NewWorker(f.Config, topic, subscription, f.Dependencies)
}

type Manager struct {
	serverID string
	factory  WorkerFactory

	mu      sync.Mutex
	ctx     context.Context
	workers map[string]*managedWorker
}

type managedWorker struct {
	runner Runner
	done   chan struct{}
}

func NewManager(ctx context.Context, serverID string, factory WorkerFactory) (*Manager, error) {
	if ctx == nil || serverID == "" || factory == nil {
		return nil, errors.New("context, server ID, and worker factory are required")
	}
	return &Manager{ctx: ctx, serverID: serverID, factory: factory, workers: make(map[string]*managedWorker)}, nil
}

// Reconcile maintains exactly one worker for every active subscription owned
// by this server. Reactivation reuses the persisted consumer identity.
func (m *Manager) Reconcile(snapshot state.Snapshot) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	desired := make(map[string]struct{})
	for id, subscription := range snapshot.Subscriptions {
		topic, ok := snapshot.Topics[subscription.TopicID]
		if !ok || topic.Status != state.TopicActive || subscription.Status != state.SubscriptionActive || subscription.ServerID != m.serverID {
			continue
		}
		desired[id] = struct{}{}
		if _, running := m.workers[id]; running {
			continue
		}
		runner, err := m.factory.New(topic, subscription)
		if err != nil {
			return err
		}
		managed := &managedWorker{runner: runner, done: make(chan struct{})}
		m.workers[id] = managed
		go m.run(managed)
	}
	for id, managed := range m.workers {
		if _, ok := desired[id]; ok {
			continue
		}
		intent := messagestore.CloseTemporary
		subscription, exists := snapshot.Subscriptions[id]
		if !exists || subscription.Status == state.SubscriptionRemoved {
			intent = messagestore.ClosePermanent
		} else if topic, ok := snapshot.Topics[subscription.TopicID]; !ok || topic.Status != state.TopicActive {
			intent = messagestore.ClosePermanent
		}
		managed.runner.Stop(intent)
		select {
		case <-managed.done:
			delete(m.workers, id)
		default:
		}
	}
	return nil
}

func (m *Manager) run(managed *managedWorker) {
	_ = managed.runner.Run(m.ctx)
	close(managed.done)
	// Keep terminal workers registered until reconciliation observes the state
	// transition they emitted. This prevents an active-but-lagging projection
	// from immediately starting the same consumer again.
}

func (m *Manager) Close(ctx context.Context) error {
	m.mu.Lock()
	workers := make([]*managedWorker, 0, len(m.workers))
	for _, worker := range m.workers {
		workers = append(workers, worker)
		worker.runner.Stop(messagestore.CloseTemporary)
	}
	m.mu.Unlock()
	for _, worker := range workers {
		select {
		case <-worker.done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}
