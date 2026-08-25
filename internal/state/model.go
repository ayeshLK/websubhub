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

// Package state defines WebSubHub's concrete, versioned product state and
// deterministic reduction behavior.
package state

import "time"

const SchemaVersion uint16 = 1

type TopicStatus string

const (
	TopicActive   TopicStatus = "active"
	TopicInactive TopicStatus = "inactive"
)

type SubscriptionStatus string

const (
	SubscriptionActive  SubscriptionStatus = "active"
	SubscriptionStale   SubscriptionStatus = "stale"
	SubscriptionRemoved SubscriptionStatus = "removed"
)

type Topic struct {
	ID                 string      `json:"id"`
	CanonicalURL       string      `json:"canonical_url"`
	ContentDestination string      `json:"content_destination"`
	Status             TopicStatus `json:"status"`
	RegisteredAt       time.Time   `json:"registered_at"`
	RegisteredBy       string      `json:"registered_by,omitempty"`
	Revision           uint64      `json:"revision"`
}

type Subscription struct {
	ID                    string             `json:"id"`
	TopicID               string             `json:"topic_id"`
	TopicURL              string             `json:"topic_url"`
	CallbackURL           string             `json:"callback_url"`
	SecretCiphertext      []byte             `json:"secret_ciphertext,omitempty"`
	SecretKeyID           string             `json:"secret_key_id,omitempty"`
	LeaseStartedAt        time.Time          `json:"lease_started_at"`
	EffectiveLeaseSeconds string             `json:"effective_lease_seconds,omitempty"`
	ServerID              string             `json:"server_id"`
	ConsumerID            string             `json:"consumer_id"`
	Status                SubscriptionStatus `json:"status"`
	StaleReason           string             `json:"stale_reason,omitempty"`
	Revision              uint64             `json:"revision"`
}

type Snapshot struct {
	SchemaVersion uint16
	Revision      uint64
	Topics        map[string]Topic
	Subscriptions map[string]Subscription
}

func EmptySnapshot() Snapshot {
	return Snapshot{
		SchemaVersion: SchemaVersion,
		Topics:        make(map[string]Topic),
		Subscriptions: make(map[string]Subscription),
	}
}
