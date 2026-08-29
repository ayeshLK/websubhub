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
	"strings"
	"testing"
	"time"

	"github.com/ayeshLK/websubhub/internal/persistence/messagestore"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
)

func TestAdministratorValidatesExistingDestination(t *testing.T) {
	t.Parallel()
	cleanup, retention := "delete", "3600000"
	fake := &fakeAdminClient{details: kadm.TopicDetails{"events": {Topic: "events", Partitions: kadm.PartitionDetails{0: {Partition: 0}}}}, resources: kadm.ResourceConfigs{{Name: "events", Configs: []kadm.Config{{Key: "cleanup.policy", Value: &cleanup}, {Key: "retention.ms", Value: &retention}}}}}
	administrator := &Administrator{config: Config{DefaultReplicationFactor: 1}, admin: fake}
	spec := messagestore.DestinationSpec{Name: "events", Partitions: 1, Retention: time.Hour}
	if err := administrator.EnsureDestination(t.Context(), spec); err != nil {
		t.Fatal(err)
	}
	spec.Retention = 2 * time.Hour
	if err := administrator.EnsureDestination(t.Context(), spec); err == nil || !strings.Contains(err.Error(), "retention") {
		t.Fatalf("retention mismatch = %v", err)
	}
}

func TestAdministratorCreatesMissingDestination(t *testing.T) {
	t.Parallel()
	cleanup := "compact"
	fake := &fakeAdminClient{
		details:     kadm.TopicDetails{},
		created:     kadm.CreateTopicResponses{"snapshots": {Topic: "snapshots"}},
		afterCreate: kadm.TopicDetails{"snapshots": {Topic: "snapshots", Partitions: kadm.PartitionDetails{0: {Partition: 0}}}},
		resources:   kadm.ResourceConfigs{{Name: "snapshots", Configs: []kadm.Config{{Key: "cleanup.policy", Value: &cleanup}}}},
	}
	administrator := &Administrator{config: Config{DefaultReplicationFactor: 1}, admin: fake}
	if err := administrator.EnsureDestination(t.Context(), messagestore.DestinationSpec{Name: "snapshots", Partitions: 1, Compacted: true}); err != nil {
		t.Fatal(err)
	}
	if fake.createCalls != 1 {
		t.Fatalf("create calls = %d", fake.createCalls)
	}
}

func TestAdministratorAcceptsValidatedConcurrentCreation(t *testing.T) {
	t.Parallel()
	cleanup := "delete"
	fake := &fakeAdminClient{
		details:     kadm.TopicDetails{},
		created:     kadm.CreateTopicResponses{"content": {Topic: "content", Err: kerr.TopicAlreadyExists}},
		afterCreate: kadm.TopicDetails{"content": {Topic: "content", Partitions: kadm.PartitionDetails{0: {Partition: 0}}}},
		resources:   kadm.ResourceConfigs{{Name: "content", Configs: []kadm.Config{{Key: "cleanup.policy", Value: &cleanup}}}},
	}
	administrator := &Administrator{config: Config{DefaultReplicationFactor: 1}, admin: fake}
	if err := administrator.EnsureDestination(t.Context(), messagestore.DestinationSpec{Name: "content", Partitions: 1}); err != nil {
		t.Fatal(err)
	}
}
