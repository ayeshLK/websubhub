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
	"io"
)

type eventHeader struct {
	Type string `json:"type"`
}

func EncodeEvent(event Event) ([]byte, error) {
	if err := validateEvent(event); err != nil {
		return nil, err
	}
	switch e := event.(type) {
	case TopicRegistered:
		return json.Marshal(struct {
			Type string `json:"type"`
			TopicRegistered
		}{e.eventType(), e})
	case TopicDeregistered:
		return json.Marshal(struct {
			Type string `json:"type"`
			TopicDeregistered
		}{e.eventType(), e})
	case SubscriptionVerified:
		return json.Marshal(struct {
			Type string `json:"type"`
			SubscriptionVerified
		}{e.eventType(), e})
	case SubscriptionUnsubscribed:
		return json.Marshal(struct {
			Type string `json:"type"`
			SubscriptionUnsubscribed
		}{e.eventType(), e})
	case SubscriptionStaleEvent:
		return json.Marshal(struct {
			Type string `json:"type"`
			SubscriptionStaleEvent
		}{e.eventType(), e})
	case SubscriptionReactivated:
		return json.Marshal(struct {
			Type string `json:"type"`
			SubscriptionReactivated
		}{e.eventType(), e})
	case SubscriptionRemovedEvent:
		return json.Marshal(struct {
			Type string `json:"type"`
			SubscriptionRemovedEvent
		}{e.eventType(), e})
	default:
		return nil, fmt.Errorf("unsupported state event %T", event)
	}
}

func DecodeEvent(data []byte) (Event, error) {
	var header eventHeader
	headerDecoder := json.NewDecoder(bytes.NewReader(data))
	if err := headerDecoder.Decode(&header); err != nil {
		return nil, fmt.Errorf("decode event header: %w", err)
	}
	var event Event
	switch header.Type {
	case "topic_registered":
		var wire struct {
			Type string `json:"type"`
			TopicRegistered
		}
		if err := decodeStrict(data, &wire); err != nil {
			return nil, fmt.Errorf("decode %s event: %w", header.Type, err)
		}
		event = wire.TopicRegistered
	case "topic_deregistered":
		var wire struct {
			Type string `json:"type"`
			TopicDeregistered
		}
		if err := decodeStrict(data, &wire); err != nil {
			return nil, fmt.Errorf("decode %s event: %w", header.Type, err)
		}
		event = wire.TopicDeregistered
	case "subscription_verified":
		var wire struct {
			Type string `json:"type"`
			SubscriptionVerified
		}
		if err := decodeStrict(data, &wire); err != nil {
			return nil, fmt.Errorf("decode %s event: %w", header.Type, err)
		}
		event = wire.SubscriptionVerified
	case "subscription_unsubscribed":
		var wire struct {
			Type string `json:"type"`
			SubscriptionUnsubscribed
		}
		if err := decodeStrict(data, &wire); err != nil {
			return nil, fmt.Errorf("decode %s event: %w", header.Type, err)
		}
		event = wire.SubscriptionUnsubscribed
	case "subscription_stale":
		var wire struct {
			Type string `json:"type"`
			SubscriptionStaleEvent
		}
		if err := decodeStrict(data, &wire); err != nil {
			return nil, fmt.Errorf("decode %s event: %w", header.Type, err)
		}
		event = wire.SubscriptionStaleEvent
	case "subscription_reactivated":
		var wire struct {
			Type string `json:"type"`
			SubscriptionReactivated
		}
		if err := decodeStrict(data, &wire); err != nil {
			return nil, fmt.Errorf("decode %s event: %w", header.Type, err)
		}
		event = wire.SubscriptionReactivated
	case "subscription_removed":
		var wire struct {
			Type string `json:"type"`
			SubscriptionRemovedEvent
		}
		if err := decodeStrict(data, &wire); err != nil {
			return nil, fmt.Errorf("decode %s event: %w", header.Type, err)
		}
		event = wire.SubscriptionRemovedEvent
	default:
		return nil, fmt.Errorf("unknown state event type %q", header.Type)
	}
	if err := validateEvent(event); err != nil {
		return nil, err
	}
	return event, nil
}

func decodeStrict(data []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	return requireEOF(decoder)
}

func requireEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); errors.Is(err, io.EOF) {
		return nil
	} else if err != nil {
		return err
	}
	return errors.New("trailing JSON value")
}
