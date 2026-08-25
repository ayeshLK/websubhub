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
	ID          string
	Body        []byte
	ContentType string
	Metadata    map[string]string
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
	Receive(context.Context, int) ([]ReceivedMessage, error)
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
