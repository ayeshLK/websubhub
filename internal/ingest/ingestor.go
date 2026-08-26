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

// Package ingest persists exact resource representations for asynchronous delivery.
package ingest

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"mime"

	"github.com/ayeshLK/websubhub/internal/app/resourcehub"
	"github.com/ayeshLK/websubhub/internal/persistence"
	"github.com/ayeshLK/websubhub/internal/persistence/messagestore"
	"github.com/ayeshLK/websubhub/internal/state"
)

var (
	ErrEventFetchUnavailable = errors.New("event-only resource updates are unavailable until safe fetching is configured")
	ErrTopicUnavailable      = errors.New("resource topic is not active")
)

type Projection interface{ Snapshot() state.Snapshot }
type MessageIDGenerator func() (string, error)

type Ingestor struct {
	producer     messagestore.Producer
	projection   Projection
	newMessageID MessageIDGenerator
}

func New(producer messagestore.Producer, projection Projection, newMessageID MessageIDGenerator) (*Ingestor, error) {
	if producer == nil || projection == nil {
		return nil, errors.New("message producer and state projection are required")
	}
	if newMessageID == nil {
		newMessageID = randomMessageID
	}
	return &Ingestor{producer: producer, projection: projection, newMessageID: newMessageID}, nil
}

func (i *Ingestor) Persist(ctx context.Context, update resourcehub.ContentUpdate) error {
	if update.Kind == resourcehub.ContentUpdateEvent {
		return ErrEventFetchUnavailable
	}
	if update.Kind != resourcehub.ContentUpdateExact {
		return fmt.Errorf("unsupported content update kind %q", update.Kind)
	}
	if update.Topic == "" {
		return errors.New("content topic is required")
	}
	if update.ContentType == "" {
		return errors.New("content type is required")
	}
	if _, _, err := mime.ParseMediaType(update.ContentType); err != nil {
		return fmt.Errorf("invalid content type: %w", err)
	}
	topicID, err := persistence.TopicID(update.Topic)
	if err != nil {
		return err
	}
	topic, ok := i.projection.Snapshot().Topics[topicID]
	if !ok || topic.Status != state.TopicActive || topic.ContentDestination == "" {
		return ErrTopicUnavailable
	}
	messageID, err := i.newMessageID()
	if err != nil {
		return fmt.Errorf("generate content message ID: %w", err)
	}
	if messageID == "" {
		return errors.New("content message ID is empty")
	}
	message := messagestore.Message{ID: messageID, Body: bytes.Clone(update.Body), ContentType: update.ContentType, Metadata: map[string]string{"topic-id": topic.ID}}
	if err := i.producer.Send(ctx, messagestore.Destination(topic.ContentDestination), message); err != nil {
		return fmt.Errorf("persist content message: %w", err)
	}
	return nil
}

func randomMessageID() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	return "message-" + hex.EncodeToString(value[:]), nil
}
