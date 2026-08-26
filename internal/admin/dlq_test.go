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

package admin

import (
	"context"
	"testing"

	"github.com/ayeshLK/websubhub/internal/persistence/messagestore"
	"github.com/ayeshLK/websubhub/internal/persistence/messagestore/messagestoretest"
)

func TestMessageStoreDLQInspectionIsSafeAndNonDestructive(t *testing.T) {
	store := messagestoretest.New(messagestore.Capabilities{})
	message := messagestore.Message{ID: "message-1", Body: []byte("customer-payload"), ContentType: "application/json", Metadata: map[string]string{"topic-id": "topic-1", "subscription-id": "subscription-1", "failure-class": "http_400", "attempt": "2", "unsafe": "must-not-escape"}}
	if err := store.Producer().Send(context.Background(), "dlq", message); err != nil {
		t.Fatal(err)
	}
	inspector, err := NewMessageStoreDLQInspector(store.Administrator(), "dlq")
	if err != nil {
		t.Fatal(err)
	}
	for call := 0; call < 2; call++ {
		entries, err := inspector.List(context.Background(), 10)
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) != 1 || entries[0].MessageID != "message-1" || entries[0].Attempt != 2 || entries[0].BodyBytes != int64(len(message.Body)) || entries[0].FailureClass != "http_400" {
			t.Fatalf("entries = %#v", entries)
		}
	}
}
