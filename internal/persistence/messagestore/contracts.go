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
	"time"
)

var (
	ErrClosed       = errors.New("message store consumer is closed")
	ErrNotSupported = errors.New("message store operation is not supported")
	ErrOutOfOrder   = errors.New("acknowledgement is not contiguous")
)

type Destination string
type ConsumerID string

// Message preserves application bytes and media type without reserialization.
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
}

type Administrator interface {
	EnsureDestination(context.Context, DestinationSpec) error
	OpenConsumer(context.Context, ConsumerSpec) (Consumer, error)
	Capabilities(context.Context) (Capabilities, error)
	Close(context.Context) error
}
