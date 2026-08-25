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
	"crypto/rand"
	"encoding/hex"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ayeshLK/websubhub/internal/persistence/messagestore"
	"github.com/ayeshLK/websubhub/internal/persistence/messagestore/conformance"
)

type integrationHarness struct {
	producer         *Producer
	administrator    *Administrator
	destination, dlq messagestore.Destination
}

func (h integrationHarness) Producer() messagestore.Producer           { return h.producer }
func (h integrationHarness) Administrator() messagestore.Administrator { return h.administrator }
func (h integrationHarness) Destination() messagestore.Destination     { return h.destination }
func (h integrationHarness) DLQDestination() messagestore.Destination  { return h.dlq }

func TestKafkaConformance(t *testing.T) {
	brokers := os.Getenv("WEBSUBHUB_TEST_KAFKA_BROKERS")
	if brokers == "" {
		t.Skip("WEBSUBHUB_TEST_KAFKA_BROKERS is not set")
	}
	config := Config{Brokers: strings.Split(brokers, ","), ClientID: "websubhub-conformance", DefaultReplicationFactor: 1}
	prefix := "websubhub-conformance-" + randomSuffix(t)
	var sequence atomic.Uint64
	conformance.Run(t, func(t *testing.T) conformance.Harness {
		n := sequence.Add(1)
		destination := messagestore.Destination(prefix + "-events-" + strconv.FormatUint(n, 10))
		dlq := messagestore.Destination(prefix + "-dlq-" + strconv.FormatUint(n, 10))
		producer, err := NewProducer(config)
		if err != nil {
			t.Fatal(err)
		}
		administrator, err := NewAdministrator(config)
		if err != nil {
			_ = producer.Close(t.Context())
			t.Fatal(err)
		}
		for _, name := range []messagestore.Destination{destination, dlq} {
			if err := administrator.EnsureDestination(t.Context(), messagestore.DestinationSpec{Name: name, Partitions: 1, Retention: time.Hour}); err != nil {
				t.Fatal(err)
			}
		}
		t.Cleanup(func() { _ = producer.Close(t.Context()); _ = administrator.Close(t.Context()) })
		return integrationHarness{producer, administrator, destination, dlq}
	})
}

func randomSuffix(t *testing.T) string {
	t.Helper()
	var value [6]byte
	if _, err := rand.Read(value[:]); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(value[:])
}
