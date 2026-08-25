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

package kafka

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/ayeshLK/websubhub/internal/persistence/messagestore"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

type consumerClient interface {
	PollRecords(context.Context, int) kgo.Fetches
	CommitRecords(context.Context, ...*kgo.Record) error
	AllowRebalance()
	Close()
}

type pendingRecord struct {
	record  *kgo.Record
	receipt messagestore.Receipt
	message messagestore.Message
}

type Consumer struct {
	mu          sync.Mutex
	config      Config
	spec        messagestore.ConsumerSpec
	metadata    messagestore.ConsumerMetadata
	client      consumerClient
	producer    messagestore.Producer
	admin       adminClient
	pending     []pendingRecord
	nextReceipt uint64
	notBefore   time.Time
	closed      bool
	permanent   bool
}

func newConsumer(config Config, spec messagestore.ConsumerSpec, producer messagestore.Producer, admin adminClient) (*Consumer, error) {
	if spec.ID == "" || spec.Destination == "" {
		return nil, errors.New("consumer ID and destination are required")
	}
	if spec.StartPosition != messagestore.StartEarliest && spec.StartPosition != messagestore.StartLatest {
		return nil, fmt.Errorf("unsupported consumer start position %q", spec.StartPosition)
	}
	consumer := &Consumer{config: config, spec: spec, metadata: messagestore.ConsumerMetadata{ID: spec.ID, Destination: spec.Destination, StartPosition: spec.StartPosition}, producer: producer, admin: admin}
	client, err := consumer.openClient()
	if err != nil {
		return nil, err
	}
	consumer.client = client
	return consumer, nil
}

func (c *Consumer) openClient() (consumerClient, error) {
	reset := kgo.NewOffset().AtEnd()
	if c.spec.StartPosition == messagestore.StartEarliest {
		reset = kgo.NewOffset().AtStart()
	}
	return kgo.NewClient(consumerOptions(c.config, c.spec, reset)...)
}

func consumerOptions(config Config, spec messagestore.ConsumerSpec, reset kgo.Offset) []kgo.Opt {
	return append(config.commonOptions(), kgo.ConsumerGroup(groupName(spec.ID)), kgo.ConsumeTopics(string(spec.Destination)), kgo.ConsumeResetOffset(reset), kgo.DisableAutoCommit(), kgo.BlockRebalanceOnPoll())
}

func (c *Consumer) Metadata() messagestore.ConsumerMetadata { return c.metadata }

func (c *Consumer) Receive(ctx context.Context, max int) ([]messagestore.ReceivedMessage, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, messagestore.ErrClosed
	}
	if max < 1 {
		return nil, errors.New("receive maximum must be positive")
	}
	if wait := time.Until(c.notBefore); wait > 0 {
		timer := time.NewTimer(wait)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
	if len(c.pending) == 0 {
		fetches := c.client.PollRecords(ctx, max)
		if errs := fetches.Errors(); len(errs) != 0 {
			joined := make([]error, 0, len(errs))
			for _, fetchErr := range errs {
				joined = append(joined, fetchErr.Err)
			}
			return nil, errors.Join(joined...)
		}
		for _, record := range fetches.Records() {
			message, err := decodeRecord(record)
			if err != nil {
				c.client.AllowRebalance()
				return nil, err
			}
			c.nextReceipt++
			receipt := messagestore.Receipt{Consumer: c.spec.ID, Value: strconv.FormatUint(c.nextReceipt, 10)}
			c.pending = append(c.pending, pendingRecord{record: record, receipt: receipt, message: message})
		}
	}
	result := make([]messagestore.ReceivedMessage, 0, min(max, len(c.pending)))
	for _, pending := range c.pending[:min(max, len(c.pending))] {
		result = append(result, messagestore.ReceivedMessage{Message: pending.message, Receipt: pending.receipt})
	}
	return result, nil
}

func (c *Consumer) Ack(ctx context.Context, receipt messagestore.Receipt) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return messagestore.ErrClosed
	}
	if len(c.pending) == 0 || receipt != c.pending[0].receipt {
		return messagestore.ErrOutOfOrder
	}
	if err := c.client.CommitRecords(ctx, c.pending[0].record); err != nil {
		return err
	}
	c.pending = c.pending[1:]
	if len(c.pending) == 0 {
		c.client.AllowRebalance()
	}
	return nil
}

func (c *Consumer) Nack(_ context.Context, receipt messagestore.Receipt, options messagestore.NackOptions) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return messagestore.ErrClosed
	}
	if options.Delay < 0 {
		return errors.New("negative redelivery delay")
	}
	if len(c.pending) == 0 || receipt != c.pending[0].receipt {
		return messagestore.ErrOutOfOrder
	}
	c.notBefore = time.Now().Add(options.Delay)
	return nil
}

func (c *Consumer) DeadLetter(ctx context.Context, receipt messagestore.Receipt, record messagestore.DeadLetter) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return messagestore.ErrClosed
	}
	if len(c.pending) == 0 || receipt != c.pending[0].receipt {
		return messagestore.ErrOutOfOrder
	}
	if record.Message.ID != c.pending[0].message.ID {
		return errors.New("dead-letter message does not match pending record")
	}
	record.Message.Metadata = cloneMetadata(record.Message.Metadata)
	record.Message.Metadata["failure-class"] = record.FailureClass
	record.Message.Metadata["topic-id"] = record.TopicID
	record.Message.Metadata["subscription-id"] = record.SubscriptionID
	record.Message.Metadata["attempt"] = strconv.FormatUint(uint64(record.Attempt), 10)
	if record.Message.StorageError != "" {
		record.Message.Metadata["storage-error"] = record.Message.StorageError
		record.Message.StorageError = ""
	}
	if err := c.producer.Send(ctx, record.Destination, record.Message); err != nil {
		return err
	}
	if err := c.client.CommitRecords(ctx, c.pending[0].record); err != nil {
		return err
	}
	c.pending = c.pending[1:]
	if len(c.pending) == 0 {
		c.client.AllowRebalance()
	}
	return nil
}

func (c *Consumer) Reconnect(context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.permanent {
		return messagestore.ErrClosed
	}
	if c.client != nil {
		if len(c.pending) != 0 {
			c.client.AllowRebalance()
		}
		c.client.Close()
	}
	client, err := c.openClient()
	if err != nil {
		c.closed = true
		return err
	}
	c.client, c.pending, c.closed = client, nil, false
	return nil
}

func (c *Consumer) Close(ctx context.Context, intent messagestore.ClosureIntent) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if intent != messagestore.CloseTemporary && intent != messagestore.ClosePermanent {
		return fmt.Errorf("unknown closure intent %q", intent)
	}
	if intent == messagestore.ClosePermanent && c.permanent {
		return nil
	}
	if !c.closed {
		if len(c.pending) != 0 {
			c.client.AllowRebalance()
		}
		c.client.Close()
		c.closed = true
	}
	if intent == messagestore.ClosePermanent {
		_, err := c.admin.DeleteGroup(ctx, groupName(c.spec.ID))
		if err == nil {
			c.permanent = true
		}
		return err
	}
	return nil
}

func groupName(id messagestore.ConsumerID) string {
	digest := sha256.Sum256([]byte(id))
	return "websubhub-" + hex.EncodeToString(digest[:])
}

func cloneMetadata(source map[string]string) map[string]string {
	clone := make(map[string]string, len(source)+4)
	for key, value := range source {
		clone[key] = value
	}
	return clone
}

var _ adminClient = (*kadm.Client)(nil)
