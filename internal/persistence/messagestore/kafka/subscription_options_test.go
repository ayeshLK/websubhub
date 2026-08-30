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
	"errors"
	"testing"

	"github.com/ayeshLK/websubhub/internal/persistence/messagestore"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestSubscriptionOptionsValidation(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		parameters map[string][]string
		permanent  bool
	}{
		{name: "default"},
		{name: "unrelated", parameters: map[string][]string{"tenant": {"blue"}}},
		{name: "custom group", parameters: map[string][]string{consumerGroupParameter: {"workers"}}},
		{name: "partitions", parameters: map[string][]string{topicPartitionsParameter: {"0, 2"}}},
		{name: "mutually exclusive", parameters: map[string][]string{consumerGroupParameter: {"workers"}, topicPartitionsParameter: {"0"}}, permanent: true},
		{name: "duplicate group value", parameters: map[string][]string{consumerGroupParameter: {"one", "two"}}, permanent: true},
		{name: "empty group", parameters: map[string][]string{consumerGroupParameter: {""}}, permanent: true},
		{name: "negative partition", parameters: map[string][]string{topicPartitionsParameter: {"-1"}}, permanent: true},
		{name: "signed partition", parameters: map[string][]string{topicPartitionsParameter: {"+1"}}, permanent: true},
		{name: "duplicate partition", parameters: map[string][]string{topicPartitionsParameter: {"1,1"}}, permanent: true},
		{name: "overflow partition", parameters: map[string][]string{topicPartitionsParameter: {"2147483648"}}, permanent: true},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			options, err := messagestore.NewSubscriptionOptions(test.parameters)
			if err == nil {
				_, err = parseSubscriptionOptions(options)
			}
			_, permanent := messagestore.PermanentSubscriptionReason(err)
			if permanent != test.permanent {
				t.Fatalf("error=%v permanent=%v", err, permanent)
			}
		})
	}
}

func TestAdministratorValidatesPartitionMembership(t *testing.T) {
	t.Parallel()
	fake := &fakeAdminClient{details: kadm.TopicDetails{
		"events": {Topic: "events", Partitions: kadm.PartitionDetails{0: {Partition: 0}, 1: {Partition: 1}}},
	}}
	administrator := &Administrator{admin: fake}
	valid, _ := messagestore.NewSubscriptionOptions(map[string][]string{topicPartitionsParameter: {"1"}})
	if err := administrator.ValidateSubscription(t.Context(), "events", valid); err != nil {
		t.Fatal(err)
	}
	invalid, _ := messagestore.NewSubscriptionOptions(map[string][]string{topicPartitionsParameter: {"2"}})
	err := administrator.ValidateSubscription(t.Context(), "events", invalid)
	if reason, permanent := messagestore.PermanentSubscriptionReason(err); !permanent || reason == "" {
		t.Fatalf("missing partition error=%v reason=%q permanent=%v", err, reason, permanent)
	}

	fake.details["events"] = kadm.TopicDetail{Topic: "events", Err: kerr.TopicAuthorizationFailed}
	if err := administrator.ValidateSubscription(t.Context(), "events", valid); !errors.Is(err, kerr.TopicAuthorizationFailed) {
		t.Fatalf("authorization error=%v", err)
	}
}

func TestUnrelatedOptionsDoNotRequireKafkaMetadata(t *testing.T) {
	t.Parallel()
	administrator := &Administrator{admin: &fakeAdminClient{}}
	options, _ := messagestore.NewSubscriptionOptions(map[string][]string{"tenant": {"blue"}})
	if err := administrator.ValidateSubscription(t.Context(), "events", options); err != nil {
		t.Fatal(err)
	}
}

func TestCustomGroupIsSharedAndNotDeleted(t *testing.T) {
	t.Parallel()
	admin := new(fakeAdminClient)
	consumer := testConsumer(new(fakeConsumerClient), new(fakeMessageProducer), admin)
	consumer.group = "shared-workers"
	consumer.shared = true
	if err := consumer.Close(t.Context(), messagestore.ClosePermanent); err != nil {
		t.Fatal(err)
	}
	if admin.deleted != 0 || !consumer.permanent {
		t.Fatalf("deleted=%d permanent=%v", admin.deleted, consumer.permanent)
	}
}

func TestExplicitAssignmentCommitsProviderDerivedProgress(t *testing.T) {
	t.Parallel()
	client := &fakeConsumerClient{fetches: fetches(storedRecord(t, "message-1", 4))}
	admin := new(fakeAdminClient)
	consumer := testConsumer(client, new(fakeMessageProducer), admin)
	consumer.group = groupName(consumer.spec.ID)
	consumer.directPartitions = []int32{0}
	batch, err := consumer.Receive(t.Context(), 1)
	if err != nil || len(batch.Messages) != 1 {
		t.Fatalf("batch=%#v err=%v", batch, err)
	}
	if err := consumer.Ack(t.Context(), batch.Messages[0].Receipt); err != nil {
		t.Fatal(err)
	}
	if len(client.commits) != 0 || len(admin.commits) != 1 || admin.commitGroups[0] != consumer.group {
		t.Fatalf("client commits=%d admin commits=%#v groups=%#v", len(client.commits), admin.commits, admin.commitGroups)
	}
	offset, ok := admin.commits[0].Lookup("events", 0)
	if !ok || offset.At != 5 {
		t.Fatalf("committed offset=%#v exists=%v", offset, ok)
	}
}

func TestExplicitAssignmentRestoresCommittedOffset(t *testing.T) {
	t.Parallel()
	admin := &fakeAdminClient{offsets: kadm.OffsetResponses{
		"events": {2: {Offset: kadm.Offset{Topic: "events", Partition: 2, At: 9}}},
	}}
	consumer := testConsumer(new(fakeConsumerClient), new(fakeMessageProducer), admin)
	consumer.group = groupName(consumer.spec.ID)
	consumer.directPartitions = []int32{2}
	offsets, err := consumer.directOffsets(t.Context(), kgo.NewOffset().AtStart())
	if err != nil {
		t.Fatal(err)
	}
	if got := offsets[2]; got != kgo.NewOffset().At(9) {
		t.Fatalf("restored offset=%#v", got)
	}
}
