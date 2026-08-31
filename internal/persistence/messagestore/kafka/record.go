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
	"fmt"
	"maps"
	"sort"
	"strings"

	"github.com/ayeshLK/websubhub/internal/persistence/messagestore"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	headerMessageID      = "websubhub-message-id"
	headerContentType    = "content-type"
	headerMetadataPrefix = "websubhub-meta-"
)

func encodeRecord(destination messagestore.Destination, message messagestore.Message) (*kgo.Record, error) {
	if destination == "" {
		return nil, errors.New("destination is required")
	}
	if message.ID == "" {
		return nil, errors.New("message ID is required")
	}
	if message.StorageError != "" {
		return nil, errors.New("a storage-invalid message cannot be published as normal content")
	}
	record := &kgo.Record{Topic: string(destination), Key: []byte(message.ID), Value: append([]byte(nil), message.Body...)}
	record.Headers = append(record.Headers, kgo.RecordHeader{Key: headerMessageID, Value: []byte(message.ID)})
	if message.ContentType != "" {
		record.Headers = append(record.Headers, kgo.RecordHeader{Key: headerContentType, Value: []byte(message.ContentType)})
	}
	keys := make([]string, 0, len(message.Metadata))
	for key := range message.Metadata {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if key == "" || strings.ToLower(key) != key || strings.HasPrefix(key, headerMetadataPrefix) || key == headerMessageID || key == headerContentType {
			return nil, fmt.Errorf("unsafe message metadata key %q", key)
		}
		record.Headers = append(record.Headers, kgo.RecordHeader{Key: headerMetadataPrefix + key, Value: []byte(message.Metadata[key])})
	}
	return record, nil
}

func decodeRecord(record *kgo.Record) (messagestore.Message, error) {
	message := messagestore.Message{Body: append([]byte(nil), record.Value...), Metadata: make(map[string]string)}
	for _, header := range record.Headers {
		switch {
		case header.Key == headerMessageID:
			message.ID = string(header.Value)
		case header.Key == headerContentType:
			message.ContentType = string(header.Value)
		case strings.HasPrefix(header.Key, headerMetadataPrefix):
			message.Metadata[strings.TrimPrefix(header.Key, headerMetadataPrefix)] = string(header.Value)
		}
	}
	if message.ID == "" {
		message.StorageError = "missing_message_id"
	}
	message.Metadata = maps.Clone(message.Metadata)
	return message, nil
}
