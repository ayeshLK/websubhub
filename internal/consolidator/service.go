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

// Package consolidator reduces durable state events into complete revisioned
// snapshots and serves the internal snapshot API.
package consolidator

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"sync"

	"github.com/ayeshLK/websubhub/internal/persistence"
	"github.com/ayeshLK/websubhub/internal/persistence/messagestore"
	"github.com/ayeshLK/websubhub/internal/persistence/statestore"
	"github.com/ayeshLK/websubhub/internal/state"
)

const SnapshotPath = "/internal/v1/snapshot"

type Service struct {
	store   statestore.Store
	reducer state.Reducer

	consumeMu      sync.Mutex
	mu             sync.RWMutex
	snapshot       state.Snapshot
	consumer       statestore.EventConsumer
	initialized    bool
	consumerActive bool
	validSnapshot  bool
	caughtUp       bool
}

func New(store statestore.Store) (*Service, error) {
	if store == nil {
		return nil, errors.New("state store is required")
	}
	return &Service{store: store, snapshot: state.EmptySnapshot()}, nil
}

func (s *Service) Start(ctx context.Context) error {
	if err := s.store.Initialize(ctx); err != nil {
		return fmt.Errorf("initialize state store: %w", err)
	}
	snapshot, err := s.store.LoadSnapshot(ctx)
	if err != nil {
		return fmt.Errorf("load latest snapshot: %w", err)
	}
	if _, err := state.EncodeSnapshot(snapshot); err != nil {
		return fmt.Errorf("validate initial snapshot: %w", err)
	}
	consumer, err := s.store.OpenEvents(ctx, persistence.ConsolidatorConsumerID, messagestore.StartEarliest)
	if err != nil {
		return fmt.Errorf("open state consumer: %w", err)
	}

	s.mu.Lock()
	s.snapshot = snapshot
	s.consumer = consumer
	s.initialized = true
	s.consumerActive = true
	s.validSnapshot = true
	s.caughtUp = false
	s.mu.Unlock()

	// Persisting revision zero makes an empty installation explicitly available,
	// and overwriting an existing revision key is byte-idempotent.
	if err := s.store.SaveSnapshot(ctx, snapshot); err != nil {
		s.fail()
		return fmt.Errorf("publish initial snapshot: %w", err)
	}
	for {
		caughtUp, err := s.Consume(ctx, 100)
		if err != nil {
			return err
		}
		if caughtUp {
			return nil
		}
	}
}

func (s *Service) Consume(ctx context.Context, max int) (bool, error) {
	s.consumeMu.Lock()
	defer s.consumeMu.Unlock()
	return s.consume(ctx, max)
}

func (s *Service) consume(ctx context.Context, max int) (bool, error) {
	s.mu.RLock()
	consumer := s.consumer
	current := s.snapshot
	active := s.consumerActive
	s.mu.RUnlock()
	if !active || consumer == nil {
		return false, errors.New("state consumer is not active")
	}
	caughtUp, err := consumer.CaughtUp(ctx)
	if err != nil {
		s.fail()
		return false, fmt.Errorf("check state replay boundary: %w", err)
	}
	if caughtUp {
		s.mu.Lock()
		s.caughtUp = true
		s.mu.Unlock()
		return true, nil
	}
	batch, err := consumer.Receive(ctx, max)
	if err != nil {
		s.fail()
		return false, fmt.Errorf("receive state events: %w", err)
	}
	for _, record := range batch.Records {
		next, changed, err := s.reducer.Apply(current, record.Event)
		if err != nil {
			s.fail()
			return false, fmt.Errorf("reduce state event %q: %w", record.Event.Metadata().EventID, err)
		}
		if changed {
			if err := s.store.SaveSnapshot(ctx, next); err != nil {
				s.fail()
				return false, fmt.Errorf("publish snapshot revision %d: %w", next.Revision, err)
			}
			current = next
			s.mu.Lock()
			s.snapshot = current
			s.validSnapshot = true
			s.mu.Unlock()
		}
		if err := consumer.Ack(ctx, record.Receipt); err != nil {
			s.fail()
			return false, fmt.Errorf("acknowledge state event %q: %w", record.Event.Metadata().EventID, err)
		}
	}
	if batch.CaughtUp {
		s.mu.Lock()
		s.caughtUp = true
		s.mu.Unlock()
	}
	return batch.CaughtUp, nil
}

func (s *Service) Refresh(ctx context.Context) error {
	s.consumeMu.Lock()
	defer s.consumeMu.Unlock()
	for {
		caughtUp, err := s.consume(ctx, 100)
		if err != nil {
			return err
		}
		if caughtUp {
			return nil
		}
	}
}

func (s *Service) Ready() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.initialized && s.consumerActive && s.validSnapshot && s.caughtUp
}

func (s *Service) Snapshot() state.Snapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	body, err := state.EncodeSnapshot(s.snapshot)
	if err != nil {
		return state.Snapshot{}
	}
	clone, err := state.DecodeSnapshot(body)
	if err != nil {
		return state.Snapshot{}
	}
	return clone
}

func (s *Service) Close(ctx context.Context) error {
	s.mu.Lock()
	consumer := s.consumer
	s.consumerActive = false
	s.caughtUp = false
	s.mu.Unlock()
	if consumer == nil {
		return nil
	}
	return consumer.Close(ctx, messagestore.CloseTemporary)
}

func (s *Service) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc(SnapshotPath, s.serveSnapshot)
	mux.HandleFunc("/health/live", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("/health/ready", func(w http.ResponseWriter, _ *http.Request) {
		if !s.Ready() {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	return mux
}

func (s *Service) serveSnapshot(w http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !s.Ready() {
		http.Error(w, "not ready", http.StatusServiceUnavailable)
		return
	}
	if err := s.Refresh(request.Context()); err != nil {
		http.Error(w, "snapshot unavailable", http.StatusServiceUnavailable)
		return
	}
	snapshot := s.Snapshot()
	body, err := state.EncodeSnapshot(snapshot)
	if err != nil {
		http.Error(w, "snapshot unavailable", http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", statestore.SnapshotContentType)
	w.Header().Set("X-WebSubHub-State-Revision", strconv.FormatUint(snapshot.Revision, 10))
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}

func (s *Service) fail() {
	s.mu.Lock()
	s.consumerActive = false
	s.caughtUp = false
	s.mu.Unlock()
}
