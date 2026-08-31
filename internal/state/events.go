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
	"errors"
	"fmt"
	"time"
)

type Actor struct {
	Type string `json:"type"`
	ID   string `json:"id,omitempty"`
}

type EventMetadata struct {
	SchemaVersion uint16    `json:"schema_version"`
	EventID       string    `json:"event_id"`
	OccurredAt    time.Time `json:"occurred_at"`
	Actor         Actor     `json:"actor"`
}

type Event interface {
	Metadata() EventMetadata
	eventType() string
}

type TopicRegistered struct {
	Meta  EventMetadata `json:"meta"`
	Topic Topic         `json:"topic"`
}

func (e TopicRegistered) Metadata() EventMetadata { return e.Meta }
func (TopicRegistered) eventType() string         { return "topic_registered" }

type TopicDeregistered struct {
	Meta    EventMetadata `json:"meta"`
	TopicID string        `json:"topic_id"`
}

func (e TopicDeregistered) Metadata() EventMetadata { return e.Meta }
func (TopicDeregistered) eventType() string         { return "topic_deregistered" }

type SubscriptionVerified struct {
	Meta         EventMetadata `json:"meta"`
	Subscription Subscription  `json:"subscription"`
}

func (e SubscriptionVerified) Metadata() EventMetadata { return e.Meta }
func (SubscriptionVerified) eventType() string         { return "subscription_verified" }

type SubscriptionUnsubscribed struct {
	Meta           EventMetadata `json:"meta"`
	SubscriptionID string        `json:"subscription_id"`
}

func (e SubscriptionUnsubscribed) Metadata() EventMetadata { return e.Meta }
func (SubscriptionUnsubscribed) eventType() string         { return "subscription_unsubscribed" }

type SubscriptionStaleEvent struct {
	Meta           EventMetadata `json:"meta"`
	SubscriptionID string        `json:"subscription_id"`
	Reason         string        `json:"reason"`
}

func (e SubscriptionStaleEvent) Metadata() EventMetadata { return e.Meta }
func (SubscriptionStaleEvent) eventType() string         { return "subscription_stale" }

type SubscriptionReactivated struct {
	Meta           EventMetadata `json:"meta"`
	SubscriptionID string        `json:"subscription_id"`
}

func (e SubscriptionReactivated) Metadata() EventMetadata { return e.Meta }
func (SubscriptionReactivated) eventType() string         { return "subscription_reactivated" }

type SubscriptionRemovedEvent struct {
	Meta           EventMetadata `json:"meta"`
	SubscriptionID string        `json:"subscription_id"`
	Cause          string        `json:"cause"`
}

func (e SubscriptionRemovedEvent) Metadata() EventMetadata { return e.Meta }
func (SubscriptionRemovedEvent) eventType() string         { return "subscription_removed" }

func validateEvent(event Event) error {
	if event == nil {
		return errors.New("event is required")
	}
	meta := event.Metadata()
	if meta.SchemaVersion != SchemaVersion {
		return fmt.Errorf("unsupported state event schema version %d", meta.SchemaVersion)
	}
	if meta.EventID == "" {
		return errors.New("event ID is required")
	}
	if meta.OccurredAt.IsZero() || meta.OccurredAt.Location() != time.UTC {
		return errors.New("event occurrence time must be non-zero UTC")
	}
	if meta.Actor.Type == "" {
		return errors.New("actor type is required")
	}
	if registered, ok := event.(TopicRegistered); ok {
		normalized, err := NormalizeContentType(registered.Topic.ContentType)
		if err != nil || normalized != registered.Topic.ContentType {
			return errors.New("topic registration content type is invalid")
		}
	}
	return nil
}
