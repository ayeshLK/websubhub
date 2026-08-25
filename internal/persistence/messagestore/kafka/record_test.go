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
	"github.com/twmb/franz-go/pkg/kgo"
	"reflect"
	"testing"

	"github.com/ayeshLK/websubhub/internal/persistence/messagestore"
)

func TestRecordRoundTripPreservesExactContent(t *testing.T) {
	t.Parallel()
	message := messagestore.Message{ID: "message-1", Body: []byte{0, 1, 2, 255}, ContentType: "application/json; charset=utf-8", Metadata: map[string]string{"z": "last", "a": "first"}}
	record, err := encodeRecord("destination", message)
	if err != nil {
		t.Fatal(err)
	}
	message.Body[0] = 9
	message.Metadata["a"] = "changed"
	decoded, err := decodeRecord(record)
	if err != nil {
		t.Fatal(err)
	}
	want := messagestore.Message{ID: "message-1", Body: []byte{0, 1, 2, 255}, ContentType: "application/json; charset=utf-8", Metadata: map[string]string{"z": "last", "a": "first"}}
	if !reflect.DeepEqual(decoded, want) {
		t.Fatalf("decoded = %#v, want %#v", decoded, want)
	}
	if record.Headers[2].Key != "websubhub-meta-a" || record.Headers[3].Key != "websubhub-meta-z" {
		t.Fatalf("metadata headers are not deterministic: %#v", record.Headers)
	}
}

func TestRecordValidation(t *testing.T) {
	t.Parallel()
	if _, err := encodeRecord("", messagestore.Message{ID: "id", ContentType: "text/plain"}); err == nil {
		t.Fatal("empty destination accepted")
	}
	if _, err := encodeRecord("topic", messagestore.Message{ID: "id", ContentType: "text/plain", Metadata: map[string]string{"Unsafe": "value"}}); err == nil {
		t.Fatal("unsafe metadata key accepted")
	}
	stored, err := decodeRecord(mustRecord(t, messagestore.Message{ID: "id", ContentType: "text/plain"}, true))
	if err != nil || stored.StorageError != "missing_message_id_and_content_type" {
		t.Fatalf("malformed stored record = %#v, %v", stored, err)
	}
}

func mustRecord(t *testing.T, message messagestore.Message, stripHeaders bool) *kgo.Record {
	t.Helper()
	record, err := encodeRecord("topic", message)
	if err != nil {
		t.Fatal(err)
	}
	if stripHeaders {
		record.Headers = nil
	}
	return record
}
