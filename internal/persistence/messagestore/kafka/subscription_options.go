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
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/ayeshLK/websubhub/internal/persistence/messagestore"
)

const (
	consumerGroupParameter   = "kafka.consumer_group"
	topicPartitionsParameter = "kafka.topic_partitions"
	maxConsumerGroupBytes    = 255
)

type subscriptionConfig struct {
	group      string
	partitions []int32
}

func parseSubscriptionOptions(options messagestore.SubscriptionOptions) (subscriptionConfig, error) {
	groupValues, hasGroup := options.Parameters[consumerGroupParameter]
	partitionValues, hasPartitions := options.Parameters[topicPartitionsParameter]
	if hasGroup && hasPartitions {
		return subscriptionConfig{}, permanent("Kafka consumer group and topic partitions cannot be combined")
	}
	config := subscriptionConfig{}
	if hasGroup {
		if len(groupValues) != 1 || groupValues[0] == "" || len(groupValues[0]) > maxConsumerGroupBytes || !utf8.ValidString(groupValues[0]) {
			return subscriptionConfig{}, permanent("invalid Kafka consumer group")
		}
		config.group = groupValues[0]
	}
	if hasPartitions {
		if len(partitionValues) != 1 || partitionValues[0] == "" {
			return subscriptionConfig{}, permanent("invalid Kafka topic partitions")
		}
		seen := make(map[int32]struct{})
		for _, item := range strings.Split(partitionValues[0], ",") {
			item = strings.TrimSpace(item)
			value, err := strconv.ParseInt(item, 10, 32)
			if err != nil || value < 0 || !decimalPartition(item) {
				return subscriptionConfig{}, permanent("invalid Kafka topic partitions")
			}
			partition := int32(value)
			if _, exists := seen[partition]; exists {
				return subscriptionConfig{}, permanent("duplicate Kafka topic partition")
			}
			seen[partition] = struct{}{}
			config.partitions = append(config.partitions, partition)
		}
		if len(config.partitions) == 0 {
			return subscriptionConfig{}, permanent("invalid Kafka topic partitions")
		}
	}
	return config, nil
}

func decimalPartition(value string) bool {
	if value == "" {
		return false
	}
	for _, character := range value {
		if character < '0' || character > '9' {
			return false
		}
	}
	return true
}

func permanent(reason string) error {
	if len(reason) > 256 {
		return &messagestore.PermanentSubscriptionError{}
	}
	return &messagestore.PermanentSubscriptionError{Reason: reason}
}

func validatePartitionMembership(config subscriptionConfig, destination messagestore.Destination, partitions map[int32]struct{}) error {
	for _, partition := range config.partitions {
		if _, exists := partitions[partition]; !exists {
			return permanent("Kafka topic partition does not exist")
		}
	}
	if destination == "" {
		return fmt.Errorf("destination is required")
	}
	return nil
}
