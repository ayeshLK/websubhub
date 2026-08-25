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
	"github.com/twmb/franz-go/pkg/kgo"
)

type fakeConsumerClient struct {
	fetches        kgo.Fetches
	commits        []*kgo.Record
	allows, closes int
}

func (f *fakeConsumerClient) PollRecords(context.Context, int) kgo.Fetches {
	result := f.fetches
	f.fetches = nil
	return result
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
	batch, err := consumer.Receive(t.Context(), 2)
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
	redelivery, err := consumer.Receive(t.Context(), 2)
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
	batch, err := consumer.Receive(t.Context(), 1)
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
	if admin.deleted != 0 || client.closes != 1 {
		t.Fatalf("temporary close deleted=%d closes=%d", admin.deleted, client.closes)
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

func testConsumer(client consumerClient, producer messagestore.Producer, admin adminClient) *Consumer {
	spec := messagestore.ConsumerSpec{ID: "consumer-1", Destination: "events", StartPosition: messagestore.StartEarliest}
	return &Consumer{spec: spec, metadata: messagestore.ConsumerMetadata{ID: spec.ID, Destination: spec.Destination, StartPosition: spec.StartPosition}, client: client, producer: producer, admin: admin}
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
	return kgo.Fetches{{Topics: []kgo.FetchTopic{{Topic: "events", Partitions: []kgo.FetchPartition{{Partition: 0, Records: records}}}}}}
}

func TestConsumerRetainsMalformedRecordAtContiguousHead(t *testing.T) {
	t.Parallel()
	malformed := &kgo.Record{Topic: "events", Partition: 0, Offset: 4, Value: []byte("untrusted")}
	valid := storedRecord(t, "message-2", 5)
	client := &fakeConsumerClient{fetches: fetches(malformed, valid)}
	consumer := testConsumer(client, new(fakeMessageProducer), new(fakeAdminClient))
	batch, err := consumer.Receive(t.Context(), 2)
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
