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
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ayeshLK/websubhub/internal/persistence"
	"github.com/ayeshLK/websubhub/internal/persistence/messagestore"
	"github.com/ayeshLK/websubhub/internal/state"
)

const (
	EventContentType    = "application/vnd.websubhub.state-event+json; version=1"
	SnapshotContentType = "application/vnd.websubhub.state-snapshot+json; version=1"
	snapshotConsumerID  = messagestore.ConsumerID("state-snapshot-loader")
)

type MessageStore struct {
	producer      messagestore.Producer
	administrator messagestore.Administrator
	options       Options
}

type Options struct {
	EventsDestination    messagestore.Destination
	SnapshotsDestination messagestore.Destination
	EventsRetention      time.Duration
	SnapshotsRetention   time.Duration
	SnapshotLoadBatch    int
}

func DefaultOptions() Options {
	return Options{
		EventsDestination:    persistence.StateEventsDestination,
		SnapshotsDestination: persistence.StateSnapshotsDestination,
		EventsRetention:      7 * 24 * time.Hour,
		SnapshotsRetention:   30 * 24 * time.Hour,
		SnapshotLoadBatch:    100,
	}
}

func New(producer messagestore.Producer, administrator messagestore.Administrator, options Options) (*MessageStore, error) {
	if producer == nil || administrator == nil {
		return nil, errors.New("producer and administrator are required")
	}
	if options.EventsDestination == "" || options.SnapshotsDestination == "" {
		return nil, errors.New("state event and snapshot destinations are required")
	}
	if options.EventsDestination == options.SnapshotsDestination {
		return nil, errors.New("state event and snapshot destinations must be different")
	}
	if options.EventsRetention <= 0 || options.SnapshotsRetention <= 0 {
		return nil, errors.New("state event and snapshot retention must be positive")
	}
	if options.SnapshotLoadBatch < 1 {
		return nil, errors.New("snapshot load batch must be positive")
	}
	return &MessageStore{producer: producer, administrator: administrator, options: options}, nil
}

func (s *MessageStore) Initialize(ctx context.Context) error {
	if err := s.administrator.EnsureDestination(ctx, messagestore.DestinationSpec{
		Name: s.options.EventsDestination, Retention: s.options.EventsRetention, Partitions: 1,
	}); err != nil {
		return fmt.Errorf("ensure state events destination: %w", err)
	}
	if err := s.administrator.EnsureDestination(ctx, messagestore.DestinationSpec{
		Name: s.options.SnapshotsDestination, Compacted: true, Retention: s.options.SnapshotsRetention, Partitions: 1,
	}); err != nil {
		return fmt.Errorf("ensure state snapshots destination: %w", err)
	}
	return nil
}

func (s *MessageStore) Append(ctx context.Context, event state.Event) error {
	body, err := state.EncodeEvent(event)
	if err != nil {
		return err
	}
	return s.producer.Send(ctx, s.options.EventsDestination, messagestore.Message{
		ID: event.Metadata().EventID, Body: body, ContentType: EventContentType,
	})
}

func (s *MessageStore) OpenEvents(ctx context.Context, id messagestore.ConsumerID, start messagestore.StartPosition) (EventConsumer, error) {
	consumer, err := s.administrator.OpenConsumer(ctx, messagestore.ConsumerSpec{
		ID: id, Destination: s.options.EventsDestination, StartPosition: start,
	})
	if err != nil {
		return nil, err
	}
	return &eventConsumer{consumer: consumer}, nil
}

func (s *MessageStore) SaveSnapshot(ctx context.Context, snapshot state.Snapshot) error {
	body, err := state.EncodeSnapshot(snapshot)
	if err != nil {
		return err
	}
	return s.producer.Send(ctx, s.options.SnapshotsDestination, messagestore.Message{
		ID: fmt.Sprintf("snapshot-%020d", snapshot.Revision), Body: body, ContentType: SnapshotContentType,
	})
}

func (s *MessageStore) LoadSnapshot(ctx context.Context) (result state.Snapshot, resultErr error) {
	consumer, err := s.administrator.OpenConsumer(ctx, messagestore.ConsumerSpec{
		ID: snapshotConsumerID, Destination: s.options.SnapshotsDestination, StartPosition: messagestore.StartEarliest,
	})
	if err != nil {
		return state.Snapshot{}, err
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		if closeErr := consumer.Close(closeCtx, messagestore.ClosePermanent); resultErr == nil && closeErr != nil {
			resultErr = fmt.Errorf("close snapshot reader: %w", closeErr)
		}
	}()

	result = state.EmptySnapshot()
	for {
		caughtUp, err := consumer.CaughtUp(ctx)
		if err != nil {
			return state.Snapshot{}, fmt.Errorf("check snapshot replay boundary: %w", err)
		}
		if caughtUp {
			return result, nil
		}
		batch, err := consumer.Receive(ctx, s.options.SnapshotLoadBatch)
		if err != nil {
			return state.Snapshot{}, fmt.Errorf("receive snapshots: %w", err)
		}
		for _, record := range batch.Messages {
			if record.Message.StorageError != "" {
				return state.Snapshot{}, fmt.Errorf("stored snapshot %q is malformed: %s", record.Message.ID, record.Message.StorageError)
			}
			if record.Message.ContentType != SnapshotContentType {
				return state.Snapshot{}, fmt.Errorf("stored snapshot %q has content type %q", record.Message.ID, record.Message.ContentType)
			}
			snapshot, err := state.DecodeSnapshot(record.Message.Body)
			if err != nil {
				return state.Snapshot{}, fmt.Errorf("decode stored snapshot %q: %w", record.Message.ID, err)
			}
			if snapshot.Revision > result.Revision {
				result = snapshot
			}
			if err := consumer.Ack(ctx, record.Receipt); err != nil {
				return state.Snapshot{}, fmt.Errorf("acknowledge snapshot %q: %w", record.Message.ID, err)
			}
		}
	}
}

type eventConsumer struct {
	consumer messagestore.Consumer
}

func (c *eventConsumer) Receive(ctx context.Context, max int) (EventBatch, error) {
	batch, err := c.consumer.Receive(ctx, max)
	if err != nil {
		return EventBatch{}, err
	}
	result := EventBatch{Records: make([]EventRecord, 0, len(batch.Messages)), CaughtUp: batch.CaughtUp}
	for _, record := range batch.Messages {
		if record.Message.StorageError != "" {
			return EventBatch{}, fmt.Errorf("stored state event %q is malformed: %s", record.Message.ID, record.Message.StorageError)
		}
		if record.Message.ContentType != EventContentType {
			return EventBatch{}, fmt.Errorf("stored state event %q has content type %q", record.Message.ID, record.Message.ContentType)
		}
		event, err := state.DecodeEvent(record.Message.Body)
		if err != nil {
			return EventBatch{}, fmt.Errorf("decode stored state event %q: %w", record.Message.ID, err)
		}
		if event.Metadata().EventID != record.Message.ID {
			return EventBatch{}, fmt.Errorf("stored state event ID mismatch for %q", record.Message.ID)
		}
		result.Records = append(result.Records, EventRecord{Event: event, Receipt: record.Receipt})
	}
	return result, nil
}

func (c *eventConsumer) CaughtUp(ctx context.Context) (bool, error) {
	return c.consumer.CaughtUp(ctx)
}

func (c *eventConsumer) Ack(ctx context.Context, receipt messagestore.Receipt) error {
	return c.consumer.Ack(ctx, receipt)
}

func (c *eventConsumer) Close(ctx context.Context, intent messagestore.ClosureIntent) error {
	return c.consumer.Close(ctx, intent)
}

var _ Store = (*MessageStore)(nil)
