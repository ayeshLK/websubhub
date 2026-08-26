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

package provider

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/ayeshLK/websubhub/internal/config"
)

func TestKafkaMapsPlainSASLFromSecretFile(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "password")
	if err := os.WriteFile(path, []byte("secret\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	source := config.HubDefaults().MessageStore
	source.Kafka.Brokers = []string{"kafka:9092"}
	source.Kafka.SASL = config.KafkaSASL{Mechanism: "plain", Username: "hub", PasswordFile: path}
	result, err := Kafka(source)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.SASL) != 1 || result.SASL[0].Name() != "PLAIN" || result.Brokers[0] != "kafka:9092" {
		t.Fatalf("Kafka config = %#v", result)
	}
}

func TestKafkaRejectsUnsupportedProviderAndEmptySecret(t *testing.T) {
	source := config.HubDefaults().MessageStore
	source.Provider = "other"
	if _, err := Kafka(source); err == nil {
		t.Fatal("unsupported provider accepted")
	}
	path := filepath.Join(t.TempDir(), "empty")
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	source = config.HubDefaults().MessageStore
	source.Kafka.SASL = config.KafkaSASL{Mechanism: "plain", Username: "hub", PasswordFile: path}
	if _, err := Kafka(source); err == nil {
		t.Fatal("empty SASL secret accepted")
	}
}
