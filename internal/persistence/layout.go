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

// Package persistence defines provider-neutral destination and consumer
// identities used by product composition roots.
package persistence

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"hash"
	"time"

	"github.com/ayeshLK/websubhub/internal/persistence/messagestore"
)

const (
	StateEventsDestination    messagestore.Destination = "websub-events"
	StateSnapshotsDestination messagestore.Destination = "websub-events-snapshots"
	DeliveryDLQDestination    messagestore.Destination = "websub-delivery-dlq"
	ConsolidatorConsumerID    messagestore.ConsumerID  = "state-consolidator"
)

func ContentDestination(exactTopicURL string) (messagestore.Destination, error) {
	if exactTopicURL == "" {
		return "", errors.New("topic URL is required")
	}
	return messagestore.Destination("websub-topic-" + digest(exactTopicURL)), nil
}

func HubStateConsumerID(serverID string) (messagestore.ConsumerID, error) {
	if serverID == "" {
		return "", errors.New("server ID is required")
	}
	return messagestore.ConsumerID("state-hub-" + digest(serverID)), nil
}

func SubscriptionConsumerID(exactTopicURL, exactCallbackURL string, originalSubscriptionTime time.Time) (messagestore.ConsumerID, error) {
	if exactTopicURL == "" || exactCallbackURL == "" || originalSubscriptionTime.IsZero() {
		return "", errors.New("topic, callback, and original subscription time are required")
	}
	hasher := sha256.New()
	writeField(hasher, exactTopicURL)
	writeField(hasher, exactCallbackURL)
	writeField(hasher, originalSubscriptionTime.UTC().Format(time.RFC3339Nano))
	return messagestore.ConsumerID("subscription-" + hex.EncodeToString(hasher.Sum(nil))), nil
}

func digest(value string) string {
	result := sha256.Sum256([]byte(value))
	return hex.EncodeToString(result[:])
}
func writeField(destination hash.Hash, value string) {
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(value)))
	_, _ = destination.Write(length[:])
	_, _ = destination.Write([]byte(value))
}
