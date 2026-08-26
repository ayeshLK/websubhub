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

package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadTOMLAndEnvironmentOverrides(t *testing.T) {
	path := filepath.Join(t.TempDir(), "websubhub.toml")
	data := `[server]
id = "file-id"

[consolidator]
endpoint = "https://consolidator.internal:8443"

[internal_auth]
mode = "mtls"

[internal_auth.client]
certificate_file = "client.crt"
private_key_file = "client.key"
server_ca_file = "ca.crt"

[message_store]
provider = "kafka"

[message_store.kafka]
brokers = ["kafka:9092"]
`
	if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path, []string{"WEBSUBHUB__SERVER__ID=environment-id"}, Hub)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Server.ID != "environment-id" || cfg.MessageStore.Kafka.Brokers[0] != "kafka:9092" {
		t.Fatalf("config = %#v", cfg)
	}
}

func TestEnvironmentArrayOverride(t *testing.T) {
	cfg, err := Load("", []string{
		"WEBSUBHUB__SERVER__ID=hub-1",
		`WEBSUBHUB__MESSAGE_STORE__KAFKA__BROKERS=["one:9092", "two:9092"]`,
	}, Hub)
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.MessageStore.Kafka.Brokers) != 2 {
		t.Fatalf("brokers = %#v", cfg.MessageStore.Kafka.Brokers)
	}
}

func TestLoadRejectsUnknownFileAndEnvironmentKeys(t *testing.T) {
	path := filepath.Join(t.TempDir(), "unknown.toml")
	if err := os.WriteFile(path, []byte("[server]\nunknown = true\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path, nil, Hub); err == nil {
		t.Fatal("unknown TOML key accepted")
	}
	if _, err := Load("", []string{"WEBSUBHUB__SERVER__UNKNOWN=value"}, Hub); err == nil {
		t.Fatal("unknown environment override accepted")
	}
}

func TestMTLSValidationCannotSilentlyDowngrade(t *testing.T) {
	cfg := Defaults()
	cfg.Server.ID = "hub-1"
	cfg.MessageStore.Kafka.Brokers = []string{"kafka:9092"}
	cfg.InternalAuth.Mode = "mtls"
	cfg.Consolidator.Endpoint = "https://consolidator:8443"
	if err := cfg.Validate(Hub); err == nil || !strings.Contains(err.Error(), "all required") {
		t.Fatalf("partial hub mTLS error = %v", err)
	}
	cfg.InternalAuth.Mode = "none"
	cfg.Consolidator.Endpoint = "http://consolidator:8081"
	cfg.InternalAuth.Client.CertificateFile = "unexpected.crt"
	if err := cfg.Validate(Hub); err == nil {
		t.Fatal("client certificate accepted in none mode")
	}
}

func TestExampleConfiguration(t *testing.T) {
	path := filepath.Join("..", "..", "configs", "websubhub.example.toml")
	if _, err := Load(path, nil, Hub); err != nil {
		t.Fatalf("hub example: %v", err)
	}
	if _, err := Load(path, nil, Consolidator); err != nil {
		t.Fatalf("consolidator example: %v", err)
	}
}

func TestKafkaSecurityConfigurationIsStrict(t *testing.T) {
	cfg := Defaults()
	cfg.Server.ID = "hub-1"
	cfg.MessageStore.Kafka.Brokers = []string{"kafka:9092"}
	cfg.MessageStore.Kafka.TLS.Enabled = true
	if err := cfg.Validate(Hub); err == nil {
		t.Fatal("TLS without a CA accepted")
	}
	cfg.MessageStore.Kafka.TLS = KafkaTLS{}
	cfg.MessageStore.Kafka.SASL.Mechanism = "plain"
	if err := cfg.Validate(Hub); err == nil {
		t.Fatal("SASL without secret file accepted")
	}
}
