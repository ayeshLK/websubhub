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

// Package hubstate implements gap-free local state projection startup.
package hubstate

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/ayeshLK/websubhub/internal/persistence"
	"github.com/ayeshLK/websubhub/internal/persistence/messagestore"
	"github.com/ayeshLK/websubhub/internal/persistence/statestore"
	"github.com/ayeshLK/websubhub/internal/state"
)

type SnapshotSource interface {
	Ready(context.Context) error
	Snapshot(context.Context) (state.Snapshot, error)
}

type Projection struct {
	store     statestore.Store
	source    SnapshotSource
	serverID  string
	bufferMax int
	reducer   state.Reducer

	mu       sync.RWMutex
	snapshot state.Snapshot
	consumer statestore.EventConsumer
	ready    bool
}

func New(store statestore.Store, source SnapshotSource, serverID string, bufferMax int) (*Projection, error) {
	if store == nil || source == nil || serverID == "" {
		return nil, errors.New("state store, snapshot source, and server ID are required")
	}
	if bufferMax < 1 {
		return nil, errors.New("startup buffer maximum must be positive")
	}
	return &Projection{store: store, source: source, serverID: serverID, bufferMax: bufferMax, snapshot: state.EmptySnapshot()}, nil
}

func (p *Projection) Start(ctx context.Context) error {
	if err := p.source.Ready(ctx); err != nil {
		return fmt.Errorf("consolidator is not ready: %w", err)
	}
	id, err := persistence.HubStateConsumerID(p.serverID)
	if err != nil {
		return err
	}
	consumer, err := p.store.OpenEvents(ctx, id, messagestore.StartLatest)
	if err != nil {
		return fmt.Errorf("open hub state consumer: %w", err)
	}
	fail := func(cause error) error {
		_ = consumer.Close(context.WithoutCancel(ctx), messagestore.CloseTemporary)
		return cause
	}

	// Reaching an observed end establishes the new group's latest boundary
	// before the barrier snapshot is requested.
	caughtUp, err := consumer.CaughtUp(ctx)
	if err != nil {
		return fail(fmt.Errorf("establish hub state consumer: %w", err))
	}
	buffered := statestore.EventBatch{CaughtUp: caughtUp}
	if !caughtUp {
		buffered, err = consumer.Receive(ctx, p.bufferMax)
		if err != nil {
			return fail(fmt.Errorf("buffer hub state events: %w", err))
		}
		caughtUp, err = consumer.CaughtUp(ctx)
		if err != nil {
			return fail(fmt.Errorf("check hub startup buffer boundary: %w", err))
		}
		if !caughtUp {
			return fail(errors.New("hub startup state buffer exceeded its configured maximum"))
		}
	}
	snapshot, err := p.source.Snapshot(ctx)
	if err != nil {
		return fail(fmt.Errorf("retrieve barrier snapshot: %w", err))
	}
	if _, err := state.EncodeSnapshot(snapshot); err != nil {
		return fail(fmt.Errorf("validate barrier snapshot: %w", err))
	}
	current, err := reduce(snapshot, buffered.Records, p.reducer)
	if err != nil {
		return fail(err)
	}
	if err := acknowledge(ctx, consumer, buffered.Records); err != nil {
		return fail(err)
	}

	p.mu.Lock()
	p.snapshot = current
	p.consumer = consumer
	p.ready = true
	p.mu.Unlock()
	return nil
}

func (p *Projection) Consume(ctx context.Context, max int) (bool, error) {
	p.mu.RLock()
	consumer, current, ready := p.consumer, p.snapshot, p.ready
	p.mu.RUnlock()
	if !ready || consumer == nil {
		return false, errors.New("hub projection is not ready")
	}
	caughtUp, err := consumer.CaughtUp(ctx)
	if err != nil {
		p.markUnready()
		return false, err
	}
	if caughtUp {
		return true, nil
	}
	batch, err := consumer.Receive(ctx, max)
	if err != nil {
		p.markUnready()
		return false, err
	}
	next, err := reduce(current, batch.Records, p.reducer)
	if err != nil {
		p.markUnready()
		return false, err
	}
	if err := acknowledge(ctx, consumer, batch.Records); err != nil {
		p.markUnready()
		return false, err
	}
	p.mu.Lock()
	p.snapshot = next
	p.mu.Unlock()
	return batch.CaughtUp, nil
}

func reduce(current state.Snapshot, records []statestore.EventRecord, reducer state.Reducer) (state.Snapshot, error) {
	for _, record := range records {
		next, _, err := reducer.Apply(current, record.Event)
		if err != nil {
			return state.Snapshot{}, fmt.Errorf("reduce buffered state event %q: %w", record.Event.Metadata().EventID, err)
		}
		current = next
	}
	return current, nil
}

func acknowledge(ctx context.Context, consumer statestore.EventConsumer, records []statestore.EventRecord) error {
	for _, record := range records {
		if err := consumer.Ack(ctx, record.Receipt); err != nil {
			return fmt.Errorf("acknowledge state event %q: %w", record.Event.Metadata().EventID, err)
		}
	}
	return nil
}

func (p *Projection) Ready() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.ready
}

func (p *Projection) Snapshot() state.Snapshot {
	p.mu.RLock()
	defer p.mu.RUnlock()
	body, err := state.EncodeSnapshot(p.snapshot)
	if err != nil {
		return state.Snapshot{}
	}
	snapshot, err := state.DecodeSnapshot(body)
	if err != nil {
		return state.Snapshot{}
	}
	return snapshot
}

func (p *Projection) Close(ctx context.Context) error {
	p.mu.Lock()
	consumer := p.consumer
	p.ready = false
	p.mu.Unlock()
	if consumer == nil {
		return nil
	}
	return consumer.Close(ctx, messagestore.CloseTemporary)
}

func (p *Projection) markUnready() {
	p.mu.Lock()
	p.ready = false
	p.mu.Unlock()
}
