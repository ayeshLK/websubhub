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
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ayeshLK/websubhub/internal/persistence/messagestore"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

type adminClient interface {
	ListTopics(context.Context, ...string) (kadm.TopicDetails, error)
	ListEndOffsets(context.Context, ...string) (kadm.ListedOffsets, error)
	CreateTopics(context.Context, int32, int16, map[string]*string, ...string) (kadm.CreateTopicResponses, error)
	DeleteGroup(context.Context, string) (kadm.DeleteGroupResponse, error)
	DescribeTopicConfigs(context.Context, ...string) (kadm.ResourceConfigs, error)
}

type Administrator struct {
	mu          sync.Mutex
	config      Config
	client      *kgo.Client
	admin       adminClient
	dlqProducer *Producer
	closed      bool
}

func NewAdministrator(config Config) (*Administrator, error) {
	if err := config.validate(); err != nil {
		return nil, err
	}
	config = config.normalized()
	client, err := kgo.NewClient(config.commonOptions()...)
	if err != nil {
		return nil, err
	}
	producer, err := NewProducer(config)
	if err != nil {
		client.Close()
		return nil, err
	}
	return &Administrator{config: config, client: client, admin: kadm.NewClient(client), dlqProducer: producer}, nil
}

func (a *Administrator) EnsureDestination(ctx context.Context, spec messagestore.DestinationSpec) error {
	if spec.Name == "" {
		return errors.New("destination name is required")
	}
	if spec.Partitions < 1 {
		return errors.New("destination partitions must be positive")
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return messagestore.ErrClosed
	}
	details, err := a.admin.ListTopics(ctx, string(spec.Name))
	if err != nil {
		return err
	}
	if detail, exists := details[string(spec.Name)]; exists && detail.Err == nil {
		if len(detail.Partitions) != spec.Partitions {
			return fmt.Errorf("Kafka destination %s has %d partitions, require %d", spec.Name, len(detail.Partitions), spec.Partitions)
		}
		return a.validateDestinationConfig(ctx, spec)
	}
	configs := map[string]*string{}
	if spec.Compacted {
		configs["cleanup.policy"] = kadm.StringPtr("compact")
	}
	if spec.Retention > 0 {
		configs["retention.ms"] = kadm.StringPtr(strconv.FormatInt(spec.Retention.Milliseconds(), 10))
	}
	responses, err := a.admin.CreateTopics(ctx, int32(spec.Partitions), a.config.DefaultReplicationFactor, configs, string(spec.Name))
	if err != nil {
		return err
	}
	response, ok := responses[string(spec.Name)]
	if !ok {
		return fmt.Errorf("Kafka did not return destination creation status for %s", spec.Name)
	}
	if response.Err == nil {
		return nil
	}
	if !errors.Is(response.Err, kerr.TopicAlreadyExists) {
		return response.Err
	}
	// Another node can win the create race after our initial lookup. Treat
	// that response as idempotent only after validating the winner created the
	// destination with the requested observable shape.
	visibilityCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var detail kadm.TopicDetail
	for {
		details, err = a.admin.ListTopics(visibilityCtx, string(spec.Name))
		if err != nil {
			return err
		}
		var exists bool
		detail, exists = details[string(spec.Name)]
		if exists && detail.Err == nil {
			break
		}
		timer := time.NewTimer(10 * time.Millisecond)
		select {
		case <-visibilityCtx.Done():
			timer.Stop()
			return fmt.Errorf("Kafka destination %s was created concurrently but did not become visible: %w", spec.Name, visibilityCtx.Err())
		case <-timer.C:
		}
	}
	if len(detail.Partitions) != spec.Partitions {
		return fmt.Errorf("Kafka destination %s has %d partitions, require %d", spec.Name, len(detail.Partitions), spec.Partitions)
	}
	return a.validateDestinationConfig(ctx, spec)
}

func (a *Administrator) validateDestinationConfig(ctx context.Context, spec messagestore.DestinationSpec) error {
	resources, err := a.admin.DescribeTopicConfigs(ctx, string(spec.Name))
	if err != nil {
		return err
	}
	resource, err := resources.On(string(spec.Name), nil)
	if err != nil {
		return err
	}
	if resource.Err != nil {
		return resource.Err
	}
	values := make(map[string]string, len(resource.Configs))
	for _, config := range resource.Configs {
		values[config.Key] = config.MaybeValue()
	}
	compacted := false
	for _, policy := range strings.Split(values["cleanup.policy"], ",") {
		if strings.TrimSpace(policy) == "compact" {
			compacted = true
		}
	}
	if compacted != spec.Compacted {
		return fmt.Errorf("Kafka destination %s cleanup.policy does not match compacted=%t", spec.Name, spec.Compacted)
	}
	if spec.Retention > 0 && values["retention.ms"] != strconv.FormatInt(spec.Retention.Milliseconds(), 10) {
		return fmt.Errorf("Kafka destination %s retention.ms=%q, require %d", spec.Name, values["retention.ms"], spec.Retention.Milliseconds())
	}
	return nil
}

func (a *Administrator) OpenConsumer(_ context.Context, spec messagestore.ConsumerSpec) (messagestore.Consumer, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return nil, messagestore.ErrClosed
	}
	return newConsumer(a.config, spec, a.dlqProducer, a.admin)
}

func (*Administrator) Capabilities(context.Context) (messagestore.Capabilities, error) {
	return capabilities(), nil
}

func (a *Administrator) Close(ctx context.Context) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return nil
	}
	a.client.Close()
	a.closed = true
	return a.dlqProducer.Close(ctx)
}

func capabilities() messagestore.Capabilities {
	return messagestore.Capabilities{Provider: "kafka", Statuses: map[messagestore.Capability]messagestore.CapabilityStatus{
		messagestore.DurablePublish:      {Support: messagestore.Native, Detail: "acks=all with idempotent production"},
		messagestore.Ordering:            {Support: messagestore.Native, Detail: "ordered within a Kafka partition"},
		messagestore.DurableSubscription: {Support: messagestore.Native, Detail: "Kafka consumer-group offsets"},
		messagestore.Acknowledgement:     {Support: messagestore.Native, Detail: "synchronous contiguous offset commits"},
		messagestore.Replay:              {Support: messagestore.Native, Detail: "retained Kafka records"},
		messagestore.Retention:           {Support: messagestore.Native, Detail: "Kafka topic retention and compaction"},
		messagestore.DeadLettering:       {Support: messagestore.Emulated, Detail: "publish then commit; crash may duplicate the DLQ record"},
		messagestore.DelayedDelivery:     {Support: messagestore.Emulated, Detail: "consumer remains uncommitted while WebSubHub schedules retry"},
		messagestore.Transactions:        {Support: messagestore.Restricted, Detail: "not exposed by the v0.5 MessageStore boundary"},
		messagestore.Provisioning:        {Support: messagestore.Native, Detail: "Kafka administration APIs"},
		messagestore.ConsumerScaling:     {Support: messagestore.Native, Detail: "Kafka consumer groups and partition ownership"},
	}}
}
