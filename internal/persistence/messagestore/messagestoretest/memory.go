// Package messagestoretest contains a deterministic in-process MessageStore
// double for the portable provider conformance suite.
package messagestoretest

import (
	"context"
	"fmt"
	"maps"
	"strconv"
	"sync"

	"github.com/ayeshLK/websubhub/internal/persistence/messagestore"
)

type Store struct {
	mu           sync.Mutex
	messages     map[messagestore.Destination][]messagestore.Message
	progress     map[string]int
	capabilities messagestore.Capabilities
}

func New(capabilities messagestore.Capabilities) *Store {
	return &Store{messages: make(map[messagestore.Destination][]messagestore.Message), progress: make(map[string]int), capabilities: capabilities}
}
func (s *Store) Producer() messagestore.Producer           { return producer{s} }
func (s *Store) Administrator() messagestore.Administrator { return administrator{s} }

type producer struct{ store *Store }

func (p producer) Send(_ context.Context, destination messagestore.Destination, message messagestore.Message) error {
	p.store.mu.Lock()
	defer p.store.mu.Unlock()
	message.Body = append([]byte(nil), message.Body...)
	message.Metadata = maps.Clone(message.Metadata)
	p.store.messages[destination] = append(p.store.messages[destination], message)
	return nil
}
func (producer) Close(context.Context) error { return nil }

type administrator struct{ store *Store }

func (a administrator) EnsureDestination(_ context.Context, spec messagestore.DestinationSpec) error {
	if spec.Name == "" {
		return fmt.Errorf("destination is required")
	}
	a.store.mu.Lock()
	defer a.store.mu.Unlock()
	if _, ok := a.store.messages[spec.Name]; !ok {
		a.store.messages[spec.Name] = nil
	}
	return nil
}
func (a administrator) OpenConsumer(_ context.Context, spec messagestore.ConsumerSpec) (messagestore.Consumer, error) {
	if spec.ID == "" || spec.Destination == "" {
		return nil, fmt.Errorf("consumer ID and destination are required")
	}
	a.store.mu.Lock()
	defer a.store.mu.Unlock()
	key := string(spec.Destination) + "\x00" + string(spec.ID)
	if _, ok := a.store.progress[key]; !ok && spec.StartPosition == messagestore.StartLatest {
		a.store.progress[key] = len(a.store.messages[spec.Destination])
	}
	return &consumer{store: a.store, metadata: messagestore.ConsumerMetadata{ID: spec.ID, Destination: spec.Destination, StartPosition: spec.StartPosition}, key: key}, nil
}
func (a administrator) Capabilities(context.Context) (messagestore.Capabilities, error) {
	return a.store.capabilities, nil
}
func (administrator) Close(context.Context) error { return nil }

type consumer struct {
	store    *Store
	metadata messagestore.ConsumerMetadata
	key      string
	closed   bool
}

func (c *consumer) Metadata() messagestore.ConsumerMetadata { return c.metadata }
func (c *consumer) Receive(_ context.Context, max int) ([]messagestore.ReceivedMessage, error) {
	c.store.mu.Lock()
	defer c.store.mu.Unlock()
	if c.closed {
		return nil, messagestore.ErrClosed
	}
	position := c.store.progress[c.key]
	available := c.store.messages[c.metadata.Destination]
	if max < 1 {
		return nil, fmt.Errorf("max must be positive")
	}
	end := min(position+max, len(available))
	result := make([]messagestore.ReceivedMessage, 0, end-position)
	for i := position; i < end; i++ {
		message := available[i]
		message.Body = append([]byte(nil), message.Body...)
		message.Metadata = maps.Clone(message.Metadata)
		result = append(result, messagestore.ReceivedMessage{Message: message, Receipt: messagestore.Receipt{Consumer: c.metadata.ID, Value: strconv.Itoa(i)}})
	}
	return result, nil
}
func (c *consumer) Ack(_ context.Context, receipt messagestore.Receipt) error {
	c.store.mu.Lock()
	defer c.store.mu.Unlock()
	if c.closed {
		return messagestore.ErrClosed
	}
	position, err := strconv.Atoi(receipt.Value)
	if err != nil || receipt.Consumer != c.metadata.ID {
		return fmt.Errorf("invalid receipt")
	}
	if position != c.store.progress[c.key] {
		return messagestore.ErrOutOfOrder
	}
	c.store.progress[c.key]++
	return nil
}
func (c *consumer) Nack(_ context.Context, receipt messagestore.Receipt, _ messagestore.NackOptions) error {
	c.store.mu.Lock()
	defer c.store.mu.Unlock()
	if c.closed {
		return messagestore.ErrClosed
	}
	position, err := strconv.Atoi(receipt.Value)
	if err != nil || position != c.store.progress[c.key] {
		return messagestore.ErrOutOfOrder
	}
	return nil
}
func (c *consumer) DeadLetter(ctx context.Context, receipt messagestore.Receipt, record messagestore.DeadLetter) error {
	if err := c.store.Producer().Send(ctx, record.Destination, record.Message); err != nil {
		return err
	}
	return c.Ack(ctx, receipt)
}
func (c *consumer) Reconnect(context.Context) error {
	c.store.mu.Lock()
	defer c.store.mu.Unlock()
	c.closed = false
	return nil
}
func (c *consumer) Close(_ context.Context, intent messagestore.ClosureIntent) error {
	c.store.mu.Lock()
	defer c.store.mu.Unlock()
	c.closed = true
	if intent == messagestore.ClosePermanent {
		delete(c.store.progress, c.key)
	}
	return nil
}
