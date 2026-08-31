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

package state

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"github.com/ayeshLK/websubhub/internal/persistence/messagestore"
)

type snapshotWire struct {
	SchemaVersion uint16         `json:"schema_version"`
	Revision      uint64         `json:"revision"`
	Topics        []Topic        `json:"topics"`
	Subscriptions []Subscription `json:"subscriptions"`
}

func EncodeSnapshot(snapshot Snapshot) ([]byte, error) {
	if err := validateSnapshot(snapshot); err != nil {
		return nil, err
	}
	wire := snapshotWire{SchemaVersion: SchemaVersion, Revision: snapshot.Revision}
	for _, topic := range snapshot.Topics {
		wire.Topics = append(wire.Topics, topic)
	}
	for _, subscription := range snapshot.Subscriptions {
		wire.Subscriptions = append(wire.Subscriptions, cloneSubscription(subscription))
	}
	sort.Slice(wire.Topics, func(i, j int) bool { return wire.Topics[i].ID < wire.Topics[j].ID })
	sort.Slice(wire.Subscriptions, func(i, j int) bool { return wire.Subscriptions[i].ID < wire.Subscriptions[j].ID })
	return json.Marshal(wire)
}

func DecodeSnapshot(data []byte) (Snapshot, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var wire snapshotWire
	if err := decoder.Decode(&wire); err != nil {
		return Snapshot{}, fmt.Errorf("decode snapshot: %w", err)
	}
	if err := requireEOF(decoder); err != nil {
		return Snapshot{}, fmt.Errorf("decode snapshot: %w", err)
	}
	snapshot := EmptySnapshot()
	snapshot.SchemaVersion = wire.SchemaVersion
	snapshot.Revision = wire.Revision
	for _, topic := range wire.Topics {
		if _, exists := snapshot.Topics[topic.ID]; exists {
			return Snapshot{}, fmt.Errorf("duplicate topic %q", topic.ID)
		}
		snapshot.Topics[topic.ID] = topic
	}
	for _, subscription := range wire.Subscriptions {
		if _, exists := snapshot.Subscriptions[subscription.ID]; exists {
			return Snapshot{}, fmt.Errorf("duplicate subscription %q", subscription.ID)
		}
		snapshot.Subscriptions[subscription.ID] = cloneSubscription(subscription)
	}
	if err := validateSnapshot(snapshot); err != nil {
		return Snapshot{}, err
	}
	return snapshot, nil
}

func validateSnapshot(snapshot Snapshot) error {
	if snapshot.SchemaVersion != SchemaVersion {
		return fmt.Errorf("unsupported snapshot schema version %d", snapshot.SchemaVersion)
	}
	if snapshot.Topics == nil || snapshot.Subscriptions == nil {
		return errors.New("snapshot maps must be initialized")
	}
	for id, topic := range snapshot.Topics {
		if id == "" || topic.ID != id || topic.CanonicalURL == "" || topic.ContentDestination == "" || topic.ContentType == "" {
			return fmt.Errorf("invalid topic %q", id)
		}
		normalized, err := NormalizeContentType(topic.ContentType)
		if err != nil || normalized != topic.ContentType {
			return fmt.Errorf("topic %q has invalid content type", id)
		}
		if topic.Status != TopicActive && topic.Status != TopicInactive {
			return fmt.Errorf("topic %q has invalid status", id)
		}
		if topic.Revision > snapshot.Revision {
			return fmt.Errorf("topic %q revision exceeds snapshot", id)
		}
	}
	for id, subscription := range snapshot.Subscriptions {
		if id == "" || subscription.ID != id || subscription.TopicID == "" || subscription.TopicURL == "" || subscription.CallbackURL == "" || subscription.ServerID == "" || subscription.ConsumerID == "" {
			return fmt.Errorf("invalid subscription %q", id)
		}
		if subscription.Status != SubscriptionActive && subscription.Status != SubscriptionStale && subscription.Status != SubscriptionRemoved {
			return fmt.Errorf("subscription %q has invalid status", id)
		}
		if _, err := messagestore.NewSubscriptionOptions(subscription.Parameters); err != nil {
			return fmt.Errorf("subscription %q has invalid options", id)
		}
		if subscription.Revision > snapshot.Revision {
			return fmt.Errorf("subscription %q revision exceeds snapshot", id)
		}
	}
	return nil
}
