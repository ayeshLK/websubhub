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
	"errors"
	"testing"

	"github.com/ayeshLK/websubhub/internal/persistence/messagestore"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

type fakeConsumerClient struct {
	fetches, last                        kgo.Fetches
	positions                            map[string]map[int32]kgo.EpochOffset
	setOffsets                           map[string]map[int32]kgo.EpochOffset
	commits                              []*kgo.Record
	allows, closes, committedOffsetCalls int
}

func (f *fakeConsumerClient) PollRecords(context.Context, int) kgo.Fetches {
	result := f.fetches
	f.last = result
	f.fetches = nil
	return result
}
func (f *fakeConsumerClient) CommittedOffsets() map[string]map[int32]kgo.EpochOffset {
	f.committedOffsetCalls++
	return nil
}
func (f *fakeConsumerClient) UncommittedOffsets() map[string]map[int32]kgo.EpochOffset {
	if f.positions != nil {
		return f.positions
	}
	return fetchPositions(f.last)
}
func (f *fakeConsumerClient) SetOffsets(offsets map[string]map[int32]kgo.EpochOffset) {
	f.setOffsets = offsets
}
func (f *fakeConsumerClient) CommitRecords(_ context.Context, records ...*kgo.Record) error {
	f.commits = append(f.commits, records...)
	return nil
}
func (f *fakeConsumerClient) AllowRebalance() { f.allows++ }
func (f *fakeConsumerClient) Close()          { f.closes++ }

type sentMessage struct {
	destination messagestore.Destination
	message     messagestore.Message
}
type fakeMessageProducer struct {
	sent []sentMessage
	err  error
}

func (f *fakeMessageProducer) Send(_ context.Context, destination messagestore.Destination, message messagestore.Message) error {
	if f.err != nil {
		return f.err
	}
	f.sent = append(f.sent, sentMessage{destination, message})
	return nil
}
func (*fakeMessageProducer) Close(context.Context) error { return nil }

func TestConsumerEnforcesContiguousProgress(t *testing.T) {
	t.Parallel()
	records := []*kgo.Record{storedRecord(t, "message-1", 4), storedRecord(t, "message-2", 5)}
	client := &fakeConsumerClient{fetches: fetches(records...)}
	consumer := testConsumer(client, new(fakeMessageProducer), new(fakeAdminClient))
	batch, err := receiveMessages(consumer, t.Context(), 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(batch) != 2 {
		t.Fatalf("batch length = %d", len(batch))
	}
	if err := consumer.Ack(t.Context(), batch[1].Receipt); !errors.Is(err, messagestore.ErrOutOfOrder) {
		t.Fatalf("out-of-order ack = %v", err)
	}
	if err := consumer.Nack(t.Context(), batch[0].Receipt, messagestore.NackOptions{}); err != nil {
		t.Fatal(err)
	}
	redelivery, err := receiveMessages(consumer, t.Context(), 2)
	if err != nil || redelivery[0].Receipt != batch[0].Receipt {
		t.Fatalf("redelivery = %#v, %v", redelivery, err)
	}
	if err := consumer.Ack(t.Context(), batch[0].Receipt); err != nil {
		t.Fatal(err)
	}
	if err := consumer.Ack(t.Context(), batch[1].Receipt); err != nil {
		t.Fatal(err)
	}
	if len(client.commits) != 2 || client.commits[0].Offset != 4 || client.commits[1].Offset != 5 {
		t.Fatalf("commits = %#v", client.commits)
	}
	if client.allows != 1 {
		t.Fatalf("AllowRebalance calls = %d", client.allows)
	}
}

func TestConsumerDeadLetterValidatesBeforePublish(t *testing.T) {
	t.Parallel()
	client := &fakeConsumerClient{fetches: fetches(storedRecord(t, "message-1", 4))}
	producer := new(fakeMessageProducer)
	consumer := testConsumer(client, producer, new(fakeAdminClient))
	batch, err := receiveMessages(consumer, t.Context(), 1)
	if err != nil {
		t.Fatal(err)
	}
	record := messagestore.DeadLetter{Destination: "dlq", Message: batch[0].Message, TopicID: "topic-1", SubscriptionID: "sub-1", FailureClass: "malformed", Attempt: 2}
	wrong := messagestore.Receipt{Consumer: "consumer-1", Value: "wrong"}
	if err := consumer.DeadLetter(t.Context(), wrong, record); !errors.Is(err, messagestore.ErrOutOfOrder) {
		t.Fatalf("wrong receipt = %v", err)
	}
	if len(producer.sent) != 0 {
		t.Fatal("invalid receipt published DLQ record")
	}
	if err := consumer.DeadLetter(t.Context(), batch[0].Receipt, record); err != nil {
		t.Fatal(err)
	}
	if len(producer.sent) != 1 || producer.sent[0].destination != "dlq" || producer.sent[0].message.Metadata["failure-class"] != "malformed" {
		t.Fatalf("DLQ send = %#v", producer.sent)
	}
	if len(client.commits) != 1 {
		t.Fatalf("commits = %d", len(client.commits))
	}
}

func TestConsumerClosureIntent(t *testing.T) {
	t.Parallel()
	client := new(fakeConsumerClient)
	admin := new(fakeAdminClient)
	consumer := testConsumer(client, new(fakeMessageProducer), admin)
	if err := consumer.Close(t.Context(), messagestore.CloseTemporary); err != nil {
		t.Fatal(err)
	}
	if admin.deleted != 0 || client.closes != 1 || client.allows != 1 {
		t.Fatalf("temporary close deleted=%d closes=%d allows=%d", admin.deleted, client.closes, client.allows)
	}
	if err := consumer.Close(t.Context(), messagestore.ClosePermanent); err != nil {
		t.Fatal(err)
	}
	if err := consumer.Close(t.Context(), messagestore.ClosePermanent); err != nil {
		t.Fatal(err)
	}
	if admin.deleted != 1 {
		t.Fatalf("permanent deletes = %d", admin.deleted)
	}
}

func TestEmptyReceiveReleasesBlockedRebalance(t *testing.T) {
	client := &fakeConsumerClient{fetches: fetches()}
	consumer := testConsumer(client, new(fakeMessageProducer), new(fakeAdminClient))
	batch, err := consumer.Receive(t.Context(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(batch.Messages) != 0 || client.allows != 1 {
		t.Fatalf("batch=%#v allows=%d", batch, client.allows)
	}
}

func TestCommittedRecordRetainsCaughtUpBoundary(t *testing.T) {
	record := storedRecord(t, "message-1", 4)
	client := &fakeConsumerClient{fetches: fetches(record)}
	administrator := &fakeAdminClient{ends: kadm.ListedOffsets{"events": {0: {Topic: "events", Partition: 0, Offset: 5}}}}
	consumer := testConsumer(client, new(fakeMessageProducer), administrator)
	consumer.assigned = map[int32]struct{}{0: {}}
	batch, err := consumer.Receive(t.Context(), 1)
	if err != nil || len(batch.Messages) != 1 {
		t.Fatalf("receive=%#v err=%v", batch, err)
	}
	if err := consumer.Ack(t.Context(), batch.Messages[0].Receipt); err != nil {
		t.Fatal(err)
	}
	client.positions = map[string]map[int32]kgo.EpochOffset{}
	caughtUp, err := consumer.CaughtUp(t.Context())
	if err != nil || !caughtUp {
		t.Fatalf("caughtUp=%v err=%v established=%#v", caughtUp, err, consumer.established)
	}
}

func TestEmptyAssignedDestinationIsCaughtUpWithoutCommittedOffsetLookup(t *testing.T) {
	client := &fakeConsumerClient{positions: map[string]map[int32]kgo.EpochOffset{"events": {0: {Offset: -1}}}}
	administrator := &fakeAdminClient{ends: kadm.ListedOffsets{"events": {0: {Topic: "events", Partition: 0, Offset: 0}}}}
	consumer := testConsumer(client, new(fakeMessageProducer), administrator)
	consumer.assigned = map[int32]struct{}{0: {}}
	caughtUp, err := consumer.CaughtUp(t.Context())
	if err != nil || !caughtUp {
		t.Fatalf("caughtUp=%v err=%v", caughtUp, err)
	}
	if client.committedOffsetCalls != 0 {
		t.Fatalf("committed offset lookups=%d", client.committedOffsetCalls)
	}
	if client.setOffsets["events"][0].Offset != 0 {
		t.Fatalf("established offsets=%#v", client.setOffsets)
	}
}

func TestStartLatestEstablishesConcreteEndOffset(t *testing.T) {
	client := &fakeConsumerClient{positions: map[string]map[int32]kgo.EpochOffset{"events": {0: {Offset: -1}}}}
	administrator := &fakeAdminClient{ends: kadm.ListedOffsets{"events": {0: {Topic: "events", Partition: 0, Offset: 7}}}}
	consumer := testConsumer(client, new(fakeMessageProducer), administrator)
	consumer.spec.StartPosition = messagestore.StartLatest
	consumer.assigned = map[int32]struct{}{0: {}}
	caughtUp, err := consumer.CaughtUp(t.Context())
	if err != nil || !caughtUp {
		t.Fatalf("caughtUp=%v err=%v", caughtUp, err)
	}
	if client.setOffsets["events"][0].Offset != 7 {
		t.Fatalf("established offsets=%#v", client.setOffsets)
	}
	administrator.ends["events"][0] = kadm.ListedOffset{Topic: "events", Partition: 0, Offset: 8}
	caughtUp, err = consumer.CaughtUp(t.Context())
	if err != nil || caughtUp {
		t.Fatalf("advanced caughtUp=%v err=%v", caughtUp, err)
	}
	if client.setOffsets["events"][0].Offset != 7 {
		t.Fatalf("moving reset boundary=%#v", client.setOffsets)
	}
}

func receiveMessages(consumer messagestore.Consumer, ctx context.Context, max int) ([]messagestore.ReceivedMessage, error) {
	batch, err := consumer.Receive(ctx, max)
	return batch.Messages, err
}

func fetchPositions(fetches kgo.Fetches) map[string]map[int32]kgo.EpochOffset {
	positions := make(map[string]map[int32]kgo.EpochOffset)
	for _, fetch := range fetches {
		for _, topic := range fetch.Topics {
			if positions[topic.Topic] == nil {
				positions[topic.Topic] = make(map[int32]kgo.EpochOffset)
			}
			for _, partition := range topic.Partitions {
				position := partition.LogStartOffset
				if len(partition.Records) != 0 {
					position = partition.Records[len(partition.Records)-1].Offset + 1
				}
				positions[topic.Topic][partition.Partition] = kgo.EpochOffset{Offset: position}
			}
		}
	}
	return positions
}

func testConsumer(client consumerClient, producer messagestore.Producer, admin adminClient) *Consumer {
	spec := messagestore.ConsumerSpec{ID: "consumer-1", Destination: "events", StartPosition: messagestore.StartEarliest}
	return &Consumer{spec: spec, metadata: messagestore.ConsumerMetadata{ID: spec.ID, Destination: spec.Destination, StartPosition: spec.StartPosition}, client: client, producer: producer, admin: admin, group: groupName(spec.ID), established: make(map[int32]kgo.EpochOffset)}
}

func storedRecord(t *testing.T, id string, offset int64) *kgo.Record {
	t.Helper()
	record, err := encodeRecord("events", messagestore.Message{ID: id, Body: []byte(id), ContentType: "application/octet-stream"})
	if err != nil {
		t.Fatal(err)
	}
	record.Partition = 0
	record.Offset = offset
	return record
}
func fetches(records ...*kgo.Record) kgo.Fetches {
	highWatermark := int64(0)
	if len(records) != 0 {
		highWatermark = records[len(records)-1].Offset + 1
	}
	return kgo.Fetches{{Topics: []kgo.FetchTopic{{Topic: "events", Partitions: []kgo.FetchPartition{{Partition: 0, HighWatermark: highWatermark, Records: records}}}}}}
}

func TestConsumerRetainsMalformedRecordAtContiguousHead(t *testing.T) {
	t.Parallel()
	malformed := &kgo.Record{Topic: "events", Partition: 0, Offset: 4, Value: []byte("untrusted")}
	valid := storedRecord(t, "message-2", 5)
	client := &fakeConsumerClient{fetches: fetches(malformed, valid)}
	consumer := testConsumer(client, new(fakeMessageProducer), new(fakeAdminClient))
	batch, err := receiveMessages(consumer, t.Context(), 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(batch) != 2 || batch[0].Message.StorageError == "" {
		t.Fatalf("malformed batch = %#v", batch)
	}
	if err := consumer.Ack(t.Context(), batch[1].Receipt); !errors.Is(err, messagestore.ErrOutOfOrder) {
		t.Fatalf("later record bypassed malformed head: %v", err)
	}
	if len(client.commits) != 0 {
		t.Fatalf("committed past malformed record: %#v", client.commits)
	}
}
