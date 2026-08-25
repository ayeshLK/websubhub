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

package conformance

import (
	"testing"

	"github.com/ayeshLK/websubhub/internal/persistence/messagestore"
	"github.com/ayeshLK/websubhub/internal/persistence/messagestore/messagestoretest"
)

type harness struct {
	store            *messagestoretest.Store
	destination, dlq messagestore.Destination
}

func (h harness) Producer() messagestore.Producer           { return h.store.Producer() }
func (h harness) Administrator() messagestore.Administrator { return h.store.Administrator() }
func (h harness) Destination() messagestore.Destination     { return h.destination }
func (h harness) DLQDestination() messagestore.Destination  { return h.dlq }

func TestInProcessDoublePassesConformance(t *testing.T) {
	Run(t, func(t *testing.T) Harness {
		t.Helper()
		store := messagestoretest.New(testCapabilities())
		h := harness{store: store, destination: "events", dlq: "dlq"}
		if err := h.Administrator().EnsureDestination(t.Context(), messagestore.DestinationSpec{Name: h.destination, Partitions: 1}); err != nil {
			t.Fatal(err)
		}
		if err := h.Administrator().EnsureDestination(t.Context(), messagestore.DestinationSpec{Name: h.dlq, Partitions: 1}); err != nil {
			t.Fatal(err)
		}
		return h
	})
}

func testCapabilities() messagestore.Capabilities {
	statuses := make(map[messagestore.Capability]messagestore.CapabilityStatus)
	for _, capability := range []messagestore.Capability{messagestore.DurablePublish, messagestore.Ordering, messagestore.DurableSubscription, messagestore.Acknowledgement, messagestore.Replay, messagestore.Retention, messagestore.DeadLettering, messagestore.DelayedDelivery, messagestore.Transactions, messagestore.Provisioning, messagestore.ConsumerScaling} {
		statuses[capability] = messagestore.CapabilityStatus{Support: messagestore.Native}
	}
	return messagestore.Capabilities{Provider: "memory-test", Statuses: statuses}
}
