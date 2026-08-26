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
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

type consumerClient interface {
	PollRecords(context.Context, int) kgo.Fetches
	UncommittedOffsets() map[string]map[int32]kgo.EpochOffset
	CommittedOffsets() map[string]map[int32]kgo.EpochOffset
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
	mu           sync.Mutex
	assignmentMu sync.RWMutex
	config       Config
	spec         messagestore.ConsumerSpec
	metadata     messagestore.ConsumerMetadata
	client       consumerClient
	producer     messagestore.Producer
	admin        adminClient
	pending      []pendingRecord
	nextReceipt  uint64
	delivered    int
	notBefore    time.Time
	closed       bool
	permanent    bool
	assigned     map[int32]struct{}
	assignedOnce chan struct{}
}

func newConsumer(config Config, spec messagestore.ConsumerSpec, producer messagestore.Producer, admin adminClient) (*Consumer, error) {
	if spec.ID == "" || spec.Destination == "" {
		return nil, errors.New("consumer ID and destination are required")
	}
	if spec.StartPosition != messagestore.StartEarliest && spec.StartPosition != messagestore.StartLatest {
		return nil, fmt.Errorf("unsupported consumer start position %q", spec.StartPosition)
	}
	consumer := &Consumer{config: config, spec: spec, assignedOnce: make(chan struct{}), metadata: messagestore.ConsumerMetadata{ID: spec.ID, Destination: spec.Destination, StartPosition: spec.StartPosition}, producer: producer, admin: admin}
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
	opts := consumerOptions(c.config, c.spec, reset)
	opts = append(opts, kgo.OnPartitionsAssigned(func(_ context.Context, _ *kgo.Client, assignment map[string][]int32) {
		c.assignmentMu.Lock()
		defer c.assignmentMu.Unlock()
		c.assigned = make(map[int32]struct{})
		for _, partition := range assignment[string(c.spec.Destination)] {
			c.assigned[partition] = struct{}{}
		}
		if c.assignedOnce != nil {
			close(c.assignedOnce)
			c.assignedOnce = nil
		}
	}))
	return kgo.NewClient(opts...)
}

func consumerOptions(config Config, spec messagestore.ConsumerSpec, reset kgo.Offset) []kgo.Opt {
	return append(config.commonOptions(), kgo.ConsumerGroup(groupName(spec.ID)), kgo.ConsumeTopics(string(spec.Destination)), kgo.ConsumeResetOffset(reset), kgo.DisableAutoCommit(), kgo.BlockRebalanceOnPoll())
}

func (c *Consumer) Metadata() messagestore.ConsumerMetadata { return c.metadata }

func (c *Consumer) Receive(ctx context.Context, max int) (messagestore.ReceiveBatch, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return messagestore.ReceiveBatch{}, messagestore.ErrClosed
	}
	if max < 1 {
		return messagestore.ReceiveBatch{}, errors.New("receive maximum must be positive")
	}
	if wait := time.Until(c.notBefore); wait > 0 {
		timer := time.NewTimer(wait)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return messagestore.ReceiveBatch{}, ctx.Err()
		case <-timer.C:
		}
	}
	caughtUp := false
	if c.delivered == len(c.pending) {
		fetches := c.client.PollRecords(ctx, max)
		if errs := fetches.Errors(); len(errs) != 0 {
			joined := make([]error, 0, len(errs))
			for _, fetchErr := range errs {
				joined = append(joined, fetchErr.Err)
			}
			return messagestore.ReceiveBatch{}, errors.Join(joined...)
		}
		caughtUp = kafkaCaughtUp(fetches, c.client.UncommittedOffsets())
		if err := c.appendFetched(fetches); err != nil {
			return messagestore.ReceiveBatch{}, err
		}
	}
	end := min(c.delivered+max, len(c.pending))
	result := make([]messagestore.ReceivedMessage, 0, end-c.delivered)
	for _, pending := range c.pending[c.delivered:end] {
		result = append(result, messagestore.ReceivedMessage{Message: pending.message, Receipt: pending.receipt})
	}
	c.delivered = end
	return messagestore.ReceiveBatch{Messages: result, CaughtUp: caughtUp && c.delivered == len(c.pending)}, nil
}

func kafkaCaughtUp(fetches kgo.Fetches, positions map[string]map[int32]kgo.EpochOffset) bool {
	partitions := 0
	for _, fetch := range fetches {
		for _, topic := range fetch.Topics {
			for _, partition := range topic.Partitions {
				partitions++
				position, ok := positions[topic.Topic][partition.Partition]
				if !ok || position.Offset < partition.HighWatermark {
					return false
				}
			}
		}
	}
	return partitions > 0
}

func (c *Consumer) assignmentNotification() <-chan struct{} {
	c.assignmentMu.RLock()
	defer c.assignmentMu.RUnlock()
	return c.assignedOnce
}

func (c *Consumer) assignedPartitions() map[int32]struct{} {
	c.assignmentMu.RLock()
	defer c.assignmentMu.RUnlock()
	result := make(map[int32]struct{}, len(c.assigned))
	for partition := range c.assigned {
		result[partition] = struct{}{}
	}
	return result
}

func (c *Consumer) CaughtUp(ctx context.Context) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return false, messagestore.ErrClosed
	}
	if c.delivered < len(c.pending) {
		return false, nil
	}
	positions := c.client.UncommittedOffsets()
	if len(positions[string(c.spec.Destination)]) == 0 && len(c.assignedPartitions()) == 0 {
		pollCtx, stopPoll := context.WithTimeout(ctx, 10*time.Second)
		assigned := c.assignmentNotification()
		cancelled := make(chan struct{})
		go func() {
			select {
			case <-assigned:
				stopPoll()
			case <-cancelled:
			}
		}()
		fetches := c.client.PollRecords(pollCtx, 1)
		close(cancelled)
		stopPoll()
		if errs := fetches.Errors(); len(errs) != 0 {
			joined := make([]error, 0, len(errs))
			for _, fetchErr := range errs {
				if errors.Is(fetchErr.Err, context.Canceled) || errors.Is(fetchErr.Err, context.DeadlineExceeded) {
					continue
				}
				joined = append(joined, fetchErr.Err)
			}
			if len(joined) != 0 {
				return false, errors.Join(joined...)
			}
		}
		if err := ctx.Err(); err != nil {
			return false, err
		}
		if err := c.appendFetched(fetches); err != nil {
			return false, err
		}
		if c.delivered < len(c.pending) {
			return false, nil
		}
		positions = c.client.UncommittedOffsets()
	}
	ends, err := c.listEndOffsets(ctx)
	if err != nil {
		return false, err
	}
	partitions := ends[string(c.spec.Destination)]
	if len(partitions) == 0 {
		return false, errors.New("Kafka returned no end offsets for consumer destination")
	}
	assigned := c.assignedPartitions()
	if len(assigned) == 0 {
		return false, nil
	}
	committed := c.client.CommittedOffsets()
	for partition, end := range partitions {
		if _, ok := assigned[partition]; !ok {
			return false, nil
		}
		position, ok := positions[string(c.spec.Destination)][partition]
		if !ok {
			position, ok = committed[string(c.spec.Destination)][partition]
		}
		if !ok {
			if end.Offset == 0 || c.spec.StartPosition == messagestore.StartLatest {
				continue
			}
			return false, nil
		}
		if position.Offset < end.Offset {
			return false, nil
		}
	}
	return true, nil
}

func (c *Consumer) listEndOffsets(ctx context.Context) (kadm.ListedOffsets, error) {
	lookupCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	for {
		ends, err := c.admin.ListEndOffsets(lookupCtx, string(c.spec.Destination))
		if err == nil {
			err = ends.Error()
		}
		if err == nil {
			return ends, nil
		}
		if !errors.Is(err, kerr.UnknownTopicOrPartition) && !errors.Is(err, kerr.LeaderNotAvailable) {
			return nil, err
		}
		timer := time.NewTimer(10 * time.Millisecond)
		select {
		case <-lookupCtx.Done():
			timer.Stop()
			return nil, lookupCtx.Err()
		case <-timer.C:
		}
	}
}

func (c *Consumer) appendFetched(fetches kgo.Fetches) error {
	for _, record := range fetches.Records() {
		message, err := decodeRecord(record)
		if err != nil {
			c.client.AllowRebalance()
			return err
		}
		c.nextReceipt++
		receipt := messagestore.Receipt{Consumer: c.spec.ID, Value: strconv.FormatUint(c.nextReceipt, 10)}
		c.pending = append(c.pending, pendingRecord{record: record, receipt: receipt, message: message})
	}
	return nil
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
	if c.delivered > 0 {
		c.delivered--
	}
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
	c.delivered = 0
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
	if c.delivered > 0 {
		c.delivered--
	}
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
	c.assignmentMu.Lock()
	c.assigned = nil
	c.assignedOnce = make(chan struct{})
	c.assignmentMu.Unlock()
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
	c.client, c.pending, c.delivered, c.closed = client, nil, 0, false
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
