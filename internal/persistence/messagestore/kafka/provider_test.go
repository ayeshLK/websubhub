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
	"reflect"
	"strings"
	"testing"

	"github.com/ayeshLK/websubhub/internal/persistence/messagestore"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

type fakeProduceClient struct {
	records []*kgo.Record
	err     error
	closes  int
}

func (f *fakeProduceClient) ProduceSync(_ context.Context, records ...*kgo.Record) kgo.ProduceResults {
	f.records = append(f.records, records...)
	results := make(kgo.ProduceResults, len(records))
	for i, record := range records {
		results[i] = kgo.ProduceResult{Record: record, Err: f.err}
	}
	return results
}
func (f *fakeProduceClient) Close() { f.closes++ }

func TestProducerSendAndClose(t *testing.T) {
	t.Parallel()
	client := new(fakeProduceClient)
	producer := &Producer{client: client}
	message := messagestore.Message{ID: "message-1", Body: []byte("exact"), ContentType: "text/plain"}
	if err := producer.Send(t.Context(), "topic", message); err != nil {
		t.Fatal(err)
	}
	if len(client.records) != 1 || string(client.records[0].Value) != "exact" {
		t.Fatalf("records = %#v", client.records)
	}
	client.err = errors.New("produce failed")
	if err := producer.Send(t.Context(), "topic", message); !errors.Is(err, client.err) {
		t.Fatalf("send error = %v", err)
	}
	if err := producer.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	_ = producer.Close(t.Context())
	if client.closes != 1 {
		t.Fatalf("close calls = %d", client.closes)
	}
	if err := producer.Send(t.Context(), "topic", message); !errors.Is(err, messagestore.ErrClosed) {
		t.Fatalf("closed send = %v", err)
	}
}

func TestConfigAndCapabilities(t *testing.T) {
	t.Parallel()
	if err := (Config{}).validate(); err == nil {
		t.Fatal("empty brokers accepted")
	}
	config := Config{Brokers: []string{"kafka:9092"}}.normalized()
	if config.ClientID != "websubhub" || config.DefaultReplicationFactor != -1 {
		t.Fatalf("normalized config = %#v", config)
	}
	got := capabilities()
	if err := got.Validate(); err != nil {
		t.Fatal(err)
	}
	if got.Statuses[messagestore.DeadLettering].Support != messagestore.Emulated {
		t.Fatalf("DLQ capability = %#v", got.Statuses[messagestore.DeadLettering])
	}
	if err := got.Require(messagestore.Transactions); err == nil {
		t.Fatal("restricted transactions satisfied requirement")
	}
	group := groupName("product-consumer")
	if !strings.HasPrefix(group, "websubhub-") || strings.Contains(group, "product-consumer") {
		t.Fatalf("unsafe Kafka group mapping %q", group)
	}
	client, err := kgo.NewClient(producerOptions(config)...)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if got := client.OptValue(kgo.DisableIdempotentWrite); got != false {
		t.Fatalf("idempotence disabled = %v", got)
	}
	if got := client.OptValue(kgo.RequiredAcks); !reflect.DeepEqual(got, kgo.AllISRAcks()) {
		t.Fatalf("required acks = %#v", got)
	}
	spec := messagestore.ConsumerSpec{ID: "consumer", Destination: "events", StartPosition: messagestore.StartEarliest}
	consumerClient, err := kgo.NewClient(consumerOptions(config, spec, kgo.NewOffset().AtStart())...)
	if err != nil {
		t.Fatal(err)
	}
	defer consumerClient.Close()
	if got := consumerClient.OptValue(kgo.DisableAutoCommit); got != true {
		t.Fatalf("auto commit disabled = %v", got)
	}
	if got := consumerClient.OptValue(kgo.BlockRebalanceOnPoll); got != true {
		t.Fatalf("block rebalance = %v", got)
	}
}

var _ produceClient = (*fakeProduceClient)(nil)
var _ adminClient = (*fakeAdminClient)(nil)

type fakeAdminClient struct {
	deleted     int
	details     kadm.TopicDetails
	resources   kadm.ResourceConfigs
	created     kadm.CreateTopicResponses
	createCalls int
}

func (f *fakeAdminClient) ListTopics(context.Context, ...string) (kadm.TopicDetails, error) {
	return f.details, nil
}
func (f *fakeAdminClient) CreateTopics(context.Context, int32, int16, map[string]*string, ...string) (kadm.CreateTopicResponses, error) {
	f.createCalls++
	return f.created, nil
}
func (f *fakeAdminClient) DescribeTopicConfigs(context.Context, ...string) (kadm.ResourceConfigs, error) {
	return f.resources, nil
}
func (f *fakeAdminClient) DeleteGroup(_ context.Context, group string) (kadm.DeleteGroupResponse, error) {
	f.deleted++
	return kadm.DeleteGroupResponse{Group: group}, nil
}
