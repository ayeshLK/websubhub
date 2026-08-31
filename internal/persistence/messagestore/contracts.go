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

// Package messagestore defines WebSubHub's internal provider-neutral message
// persistence boundary. Provider concepts must not escape this package.
package messagestore

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"
)

var (
	ErrClosed       = errors.New("message store consumer is closed")
	ErrNotSupported = errors.New("message store operation is not supported")
	ErrOutOfOrder   = errors.New("acknowledgement is not contiguous")
)

type Destination string
type ConsumerID string

// Message preserves application bytes without reserialization. ContentType and
// Metadata are optional typed-envelope fields; ordinary resource content omits
// both because its destination and topic state define the channel contract.
type Message struct {
	ID           string
	Body         []byte
	ContentType  string
	Metadata     map[string]string
	StorageError string
}

// Receipt is an opaque, consumer-scoped acknowledgement token. Value has no
// product meaning and must never cross a public or persisted product contract.
type Receipt struct {
	Consumer ConsumerID
	Value    string
}

type ReceivedMessage struct {
	Message Message
	Receipt Receipt
}

// ReceiveBatch reports messages and whether this poll delivered through the
// provider end boundary observed for every assigned destination shard.
type ReceiveBatch struct {
	Messages []ReceivedMessage
	CaughtUp bool
}

type Producer interface {
	Send(context.Context, Destination, Message) error
	Close(context.Context) error
}

type StartPosition string

const (
	StartEarliest StartPosition = "earliest"
	StartLatest   StartPosition = "latest"
)

type ConsumerMetadata struct {
	ID            ConsumerID
	Destination   Destination
	StartPosition StartPosition
}

type ClosureIntent string

const (
	CloseTemporary ClosureIntent = "temporary"
	ClosePermanent ClosureIntent = "permanent"
)

type NackOptions struct {
	Delay time.Duration
}

type DeadLetter struct {
	Destination    Destination
	Message        Message
	TopicID        string
	SubscriptionID string
	FailureClass   string
	Attempt        uint32
	StorageError   string
}

type Consumer interface {
	Metadata() ConsumerMetadata
	Receive(context.Context, int) (ReceiveBatch, error)
	CaughtUp(context.Context) (bool, error)
	Ack(context.Context, Receipt) error
	Nack(context.Context, Receipt, NackOptions) error
	DeadLetter(context.Context, Receipt, DeadLetter) error
	Reconnect(context.Context) error
	Close(context.Context, ClosureIntent) error
}

type DestinationSpec struct {
	Name       Destination
	Compacted  bool
	Retention  time.Duration
	Partitions int
}

type ConsumerSpec struct {
	ID            ConsumerID
	Destination   Destination
	StartPosition StartPosition
	Subscription  *SubscriptionOptions
}

const (
	MaxSubscriptionOptionKeys       = 32
	MaxSubscriptionOptionKeyBytes   = 128
	MaxSubscriptionOptionValues     = 8
	MaxSubscriptionOptionValueBytes = 1024
	MaxSubscriptionOptionsBytes     = 8192
)

// SubscriptionOptions is bounded product context for a WebSub delivery
// consumer. It contains no callback, secret, authorization, or provider
// credential fields.
type SubscriptionOptions struct {
	Parameters map[string][]string
}

// PermanentSubscriptionError reports subscriber-correctable options without
// placing the safe public reason in generic error strings or provider logs.
type PermanentSubscriptionError struct {
	Reason string
}

func (*PermanentSubscriptionError) Error() string { return "invalid subscription options" }

func NewSubscriptionOptions(parameters map[string][]string) (SubscriptionOptions, error) {
	if len(parameters) > MaxSubscriptionOptionKeys {
		return SubscriptionOptions{}, &PermanentSubscriptionError{Reason: "too many subscription options"}
	}
	cloned := make(map[string][]string, len(parameters))
	total := 0
	for key, values := range parameters {
		if key == "" || len(key) > MaxSubscriptionOptionKeyBytes {
			return SubscriptionOptions{}, &PermanentSubscriptionError{Reason: "invalid subscription option name"}
		}
		if len(values) > MaxSubscriptionOptionValues {
			return SubscriptionOptions{}, &PermanentSubscriptionError{Reason: "too many subscription option values"}
		}
		total += len(key)
		cloned[key] = slices.Clone(values)
		for _, value := range values {
			if len(value) > MaxSubscriptionOptionValueBytes {
				return SubscriptionOptions{}, &PermanentSubscriptionError{Reason: "subscription option value is too long"}
			}
			total += len(value)
		}
	}
	if total > MaxSubscriptionOptionsBytes {
		return SubscriptionOptions{}, &PermanentSubscriptionError{Reason: "subscription options are too large"}
	}
	if len(cloned) == 0 {
		cloned = nil
	}
	return SubscriptionOptions{Parameters: cloned}, nil
}

func (o SubscriptionOptions) Clone() SubscriptionOptions {
	clone := SubscriptionOptions{Parameters: maps.Clone(o.Parameters)}
	for key, values := range clone.Parameters {
		clone.Parameters[key] = slices.Clone(values)
	}
	return clone
}

func PermanentSubscriptionReason(err error) (string, bool) {
	var permanent *PermanentSubscriptionError
	if !errors.As(err, &permanent) {
		return "", false
	}
	if permanent.Reason == "" {
		return "", true
	}
	if len(permanent.Reason) > 256 {
		return "", true
	}
	return permanent.Reason, true
}

type Administrator interface {
	EnsureDestination(context.Context, DestinationSpec) error
	ValidateSubscription(context.Context, Destination, SubscriptionOptions) error
	OpenConsumer(context.Context, ConsumerSpec) (Consumer, error)
	Capabilities(context.Context) (Capabilities, error)
	Close(context.Context) error
}

func ValidateConsumerSpec(spec ConsumerSpec) error {
	if spec.ID == "" || spec.Destination == "" {
		return errors.New("consumer ID and destination are required")
	}
	if spec.StartPosition != StartEarliest && spec.StartPosition != StartLatest {
		return fmt.Errorf("unsupported consumer start position %q", spec.StartPosition)
	}
	return nil
}
