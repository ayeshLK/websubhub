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

package admin

import (
	"context"
	"errors"
	"strconv"
	"sync"

	"github.com/ayeshLK/websubhub/internal/persistence/messagestore"
)

const dlqInspectionConsumerID messagestore.ConsumerID = "ops-dlq-inspection"

type MessageStoreDLQInspector struct {
	administrator messagestore.Administrator
	destination   messagestore.Destination
	mu            sync.Mutex
}

func NewMessageStoreDLQInspector(administrator messagestore.Administrator, destination messagestore.Destination) (*MessageStoreDLQInspector, error) {
	if administrator == nil || destination == "" {
		return nil, errors.New("message store administrator and DLQ destination are required")
	}
	return &MessageStoreDLQInspector{administrator: administrator, destination: destination}, nil
}

// List intentionally leaves inspection progress unacknowledged and closes the
// consumer temporarily. Repeated inspection therefore cannot remove or skip a
// DLQ record; v0.5 does not expose replay or pagination mutations.
func (i *MessageStoreDLQInspector) List(ctx context.Context, limit int) ([]DLQEntry, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	consumer, err := i.administrator.OpenConsumer(ctx, messagestore.ConsumerSpec{ID: dlqInspectionConsumerID, Destination: i.destination, StartPosition: messagestore.StartEarliest})
	if err != nil {
		return nil, errors.New("open DLQ inspection consumer")
	}
	defer func() { _ = consumer.Close(context.WithoutCancel(ctx), messagestore.CloseTemporary) }()
	batch, err := consumer.Receive(ctx, limit)
	if err != nil {
		return nil, errors.New("inspect DLQ")
	}
	entries := make([]DLQEntry, 0, len(batch.Messages))
	for _, received := range batch.Messages {
		attempt, _ := strconv.ParseUint(received.Message.Metadata["attempt"], 10, 32)
		entries = append(entries, DLQEntry{
			MessageID: received.Message.ID, TopicID: received.Message.Metadata["topic-id"],
			SubscriptionID: received.Message.Metadata["subscription-id"], FailureClass: received.Message.Metadata["failure-class"],
			Attempt: uint32(attempt), ContentType: received.Message.ContentType, BodyBytes: int64(len(received.Message.Body)),
		})
	}
	return entries, nil
}
