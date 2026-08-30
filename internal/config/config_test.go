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
	"time"
)

func TestLoadHubTOMLAndEnvironmentOverrides(t *testing.T) {
	path := writeConfig(t, "websubhub.toml", `[server]
id = "file-id"

[consolidator]
endpoint = "https://consolidator.internal:8443"

[consolidator.auth]
mode = "mtls"

[consolidator.auth.mtls]
certificate_file = "client.crt"
private_key_file = "client.key"
server_ca_file = "ca.crt"

[message_store]
provider = "kafka"

[message_store.kafka]
brokers = ["kafka:9092"]
`)
	cfg, err := LoadHub(path, noneAuthEnvironment(
		"WEBSUBHUB__SERVER__ID=environment-id",
		"WEBSUBHUB__STATE__EVENTS__DESTINATION=environment-state-events",
		"WEBSUBHUB__STATE__STARTUP__BUFFER_MAX=2048",
	))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Server.ID != "environment-id" ||
		cfg.State.Events.Destination != "environment-state-events" ||
		cfg.State.Startup.BufferMax != 2048 ||
		cfg.MessageStore.Kafka.Brokers[0] != "kafka:9092" {
		t.Fatalf("config = %#v", cfg)
	}
}

func TestLoadConsolidatorEnvironmentOverrides(t *testing.T) {
	path := writeConfig(t, "websubhub-consolidator.toml", `[message_store.kafka]
brokers = ["kafka:9092"]
`)
	loaded, err := LoadConsolidator(path, []string{
		"WEBSUBHUB__SERVER__LISTEN=:9081",
		"WEBSUBHUB__STATE__EVENTS__RETENTION=168h",
		"WEBSUBHUB__STATE__CONSUMER__BATCH_SIZE=25",
	})
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Server.Listen != ":9081" ||
		loaded.State.Events.Retention.Value() != 7*24*time.Hour ||
		loaded.State.Consumer.BatchSize != 25 ||
		loaded.MessageStore.Kafka.ClientID != "websubhub-consolidator" {
		t.Fatalf("config = %#v", loaded)
	}
}

func TestEnvironmentArrayOverride(t *testing.T) {
	cfg, err := LoadHub("", noneAuthEnvironment(
		"WEBSUBHUB__SERVER__ID=hub-1",
		`WEBSUBHUB__MESSAGE_STORE__KAFKA__BROKERS=["one:9092", "two:9092"]`,
	))
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.MessageStore.Kafka.Brokers) != 2 {
		t.Fatalf("brokers = %#v", cfg.MessageStore.Kafka.Brokers)
	}
}

func TestResourceProtocolEnvironmentOverrides(t *testing.T) {
	cfg, err := LoadHub("", noneAuthEnvironment(
		"WEBSUBHUB__SERVER__ID=hub-1",
		"WEBSUBHUB__SERVER__PUBLIC_URL=https://hub.example.test/websub",
		"WEBSUBHUB__PROTOCOL__PUBLISHER_EXTENSION_ENABLED=true",
		"WEBSUBHUB__PROTOCOL__VERIFICATION_WORKERS=8",
		`WEBSUBHUB__MESSAGE_STORE__KAFKA__BROKERS=["kafka:9092"]`,
	))
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.Protocol.PublisherExtensionEnabled || cfg.Protocol.VerificationWorkers != 8 {
		t.Fatalf("protocol config = %#v", cfg.Protocol)
	}
}

func TestDeliveryEnvironmentOverrides(t *testing.T) {
	cfg, err := LoadHub("", noneAuthEnvironment(
		"WEBSUBHUB__SERVER__ID=hub-1",
		"WEBSUBHUB__DELIVERY__RETRY__STRATEGY=message_store",
		"WEBSUBHUB__DELIVERY__RETRY__HTTP__BACKOFF_FACTOR=1.5",
		"WEBSUBHUB__DELIVERY__RETRY__HTTP__RETRY_STATUS_CODES=[429,503]",
		"WEBSUBHUB__DELIVERY__RETRY__MESSAGE_STORE__DEAD_LETTER_STATUS_CODES=[400,404]",
		`WEBSUBHUB__MESSAGE_STORE__KAFKA__BROKERS=["kafka:9092"]`,
	))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Delivery.Retry.Strategy != "message_store" || cfg.Delivery.Retry.HTTP.BackoffFactor != 1.5 || len(cfg.Delivery.Retry.HTTP.RetryStatusCodes) != 2 || len(cfg.Delivery.Retry.MessageStore.DeadLetterStatusCodes) != 2 {
		t.Fatalf("delivery config = %#v", cfg.Delivery)
	}
}

func TestSecurityEnvironmentOverrides(t *testing.T) {
	cfg, err := LoadHub("", noneAuthEnvironment(
		"WEBSUBHUB__SERVER__ID=hub-1",
		"WEBSUBHUB__OPERATIONS__LISTEN=127.0.0.1:9191",
		"WEBSUBHUB__SECURITY__JWT__ISSUER=https://issuer.example.test",
		"WEBSUBHUB__SECURITY__JWT__JWKS_URL=https://issuer.example.test/keys",
		`WEBSUBHUB__SECURITY__JWT__ALGORITHMS=["RS256","ES256"]`,
		"WEBSUBHUB__SECURITY__CALLBACKS__ALLOWED_PORTS=[443,8443]",
		`WEBSUBHUB__SECURITY__CALLBACKS__ALLOWED_HOSTS=["subscriber.internal"]`,
		`WEBSUBHUB__SECURITY__CALLBACKS__ALLOWED_CIDRS=["10.20.0.0/16"]`,
		`WEBSUBHUB__MESSAGE_STORE__KAFKA__BROKERS=["kafka:9092"]`,
	))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Operations.Listen != "127.0.0.1:9191" || len(cfg.Security.JWT.Algorithms) != 2 || len(cfg.Security.Callbacks.AllowedPorts) != 2 || cfg.Security.Callbacks.AllowedHosts[0] != "subscriber.internal" {
		t.Fatalf("security config = %#v operations = %#v", cfg.Security, cfg.Operations)
	}
}

func TestSecurityConfigurationCannotDowngradeJWTOrCallbackPolicy(t *testing.T) {
	cfg := validHubConfig()
	cfg.Server.ID = "hub-1"
	cfg.MessageStore.Kafka.Brokers = []string{"kafka:9092"}
	cfg.Security.JWT.Algorithms = []string{"HS256"}
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "asymmetric") {
		t.Fatalf("symmetric algorithm error = %v", err)
	}
	cfg = validHubConfig()
	cfg.Server.ID = "hub-1"
	cfg.MessageStore.Kafka.Brokers = []string{"kafka:9092"}
	cfg.Security.JWT.JWKSURL = "http://issuer.example.test/keys"
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "HTTPS") {
		t.Fatalf("insecure JWKS error = %v", err)
	}
	cfg = validHubConfig()
	cfg.Server.ID = "hub-1"
	cfg.MessageStore.Kafka.Brokers = []string{"kafka:9092"}
	cfg.Security.Callbacks.AllowedCIDRs = []string{"not-a-cidr"}
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "CIDR") {
		t.Fatalf("callback CIDR error = %v", err)
	}
}

func TestAPIAuthenticationModesAreExplicit(t *testing.T) {
	tests := []struct {
		name       string
		configure  func(*HubConfig)
		errorField string
	}{
		{name: "missing public", configure: func(cfg *HubConfig) { cfg.Server.Auth.Mode = "" }, errorField: "server.auth.mode"},
		{name: "missing operations", configure: func(cfg *HubConfig) { cfg.Operations.Auth.Mode = "" }, errorField: "operations.auth.mode"},
		{name: "unknown public", configure: func(cfg *HubConfig) { cfg.Server.Auth.Mode = "optional" }, errorField: "server.auth.mode"},
		{name: "unknown operations", configure: func(cfg *HubConfig) { cfg.Operations.Auth.Mode = "optional" }, errorField: "operations.auth.mode"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := validHubConfig()
			cfg.Server.ID = "hub-1"
			cfg.MessageStore.Kafka.Brokers = []string{"kafka:9092"}
			test.configure(&cfg)
			if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), test.errorField) {
				t.Fatalf("mode error = %v", err)
			}
		})
	}
}

func TestNoneAuthenticationDoesNotRequireJWTConfiguration(t *testing.T) {
	cfg := validHubConfig()
	cfg.Server.ID = "hub-1"
	cfg.MessageStore.Kafka.Brokers = []string{"kafka:9092"}
	cfg.Server.Auth.Mode = AuthModeNone
	cfg.Operations.Auth.Mode = AuthModeNone
	cfg.Security.JWT = JWT{}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestJWTModeRequiresCompleteJWTConfiguration(t *testing.T) {
	cfg := validHubConfig()
	cfg.Server.ID = "hub-1"
	cfg.MessageStore.Kafka.Brokers = []string{"kafka:9092"}
	cfg.Server.Auth.Mode = AuthModeNone
	cfg.Security.JWT.Issuer = ""
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "security.jwt.issuer") {
		t.Fatalf("missing JWT issuer error = %v", err)
	}
}

func TestDeliveryConfigurationRejectsAmbiguousMappings(t *testing.T) {
	cfg := validHubConfig()
	cfg.Server.ID = "hub-1"
	cfg.MessageStore.Kafka.Brokers = []string{"kafka:9092"}
	cfg.Delivery.Retry.Strategy = "message_store"
	cfg.Delivery.Retry.MessageStore.RedeliverStatusCodes = []int{410}
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "410") {
		t.Fatalf("410 mapping error = %v", err)
	}
	cfg = validHubConfig()
	cfg.Server.ID = "hub-1"
	cfg.MessageStore.Kafka.Brokers = []string{"kafka:9092"}
	cfg.Delivery.Retry.Strategy = "message_store"
	cfg.Delivery.Retry.MessageStore.FailStatusCodes = []int{503}
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "both") {
		t.Fatalf("duplicate mapping error = %v", err)
	}
}

func TestProcessRootsRejectEachOthersKeys(t *testing.T) {
	hubOnly := writeConfig(t, "hub-only.toml", `[server]
id = "hub-1"

[message_store.kafka]
brokers = ["kafka:9092"]
`)
	if _, err := LoadConsolidator(hubOnly, nil); err == nil {
		t.Fatal("consolidator accepted hub server ID")
	}
	consolidatorOnly := writeConfig(t, "consolidator-only.toml", `[server.auth]
mode = "none"

[server.auth.mtls]
client_ca_file = "consolidator-only-ca.crt"

[message_store.kafka]
brokers = ["kafka:9092"]
`)
	if _, err := LoadHub(consolidatorOnly, []string{"WEBSUBHUB__SERVER__ID=hub-1"}); err == nil {
		t.Fatal("hub accepted consolidator server authentication")
	}
	if _, err := LoadConsolidator("", []string{
		"WEBSUBHUB__SERVER__ID=invalid",
		`WEBSUBHUB__MESSAGE_STORE__KAFKA__BROKERS=["kafka:9092"]`,
	}); err == nil {
		t.Fatal("consolidator accepted hub-only environment override")
	}
	if _, err := LoadHub("", noneAuthEnvironment(
		"WEBSUBHUB__SERVER__ID=hub-1",
		"WEBSUBHUB__STATE__SNAPSHOTS__DESTINATION=invalid",
		`WEBSUBHUB__MESSAGE_STORE__KAFKA__BROKERS=["kafka:9092"]`,
	)); err == nil {
		t.Fatal("hub accepted consolidator-only state override")
	}
	if _, err := LoadConsolidator("", []string{
		"WEBSUBHUB__STATE__STARTUP__BUFFER_MAX=10",
		`WEBSUBHUB__MESSAGE_STORE__KAFKA__BROKERS=["kafka:9092"]`,
	}); err == nil {
		t.Fatal("consolidator accepted hub-only startup override")
	}
}

func TestLoadRejectsUnknownKeys(t *testing.T) {
	path := writeConfig(t, "unknown.toml", "[server]\nunknown = true\n")
	if _, err := LoadHub(path, nil); err == nil {
		t.Fatal("unknown TOML key accepted")
	}
	if _, err := LoadHub("", noneAuthEnvironment("WEBSUBHUB__SERVER__UNKNOWN=value")); err == nil {
		t.Fatal("unknown environment override accepted")
	}
}

func TestMTLSValidationCannotSilentlyDowngrade(t *testing.T) {
	hub := validHubConfig()
	hub.Server.ID = "hub-1"
	hub.MessageStore.Kafka.Brokers = []string{"kafka:9092"}
	hub.Consolidator.Auth.Mode = "mtls"
	hub.Consolidator.Endpoint = "https://consolidator:8443"
	if err := hub.Validate(); err == nil || !strings.Contains(err.Error(), "all required") {
		t.Fatalf("partial hub mTLS error = %v", err)
	}
	hub.Consolidator.Auth.Mode = "none"
	hub.Consolidator.Endpoint = "http://consolidator:8081"
	hub.Consolidator.Auth.MTLS.CertificateFile = "unexpected.crt"
	if err := hub.Validate(); err == nil {
		t.Fatal("hub client certificate accepted in none mode")
	}

	consolidator := ConsolidatorDefaults()
	consolidator.MessageStore.Kafka.Brokers = []string{"kafka:9092"}
	consolidator.Server.Auth.Mode = "mtls"
	if err := consolidator.Validate(); err == nil || !strings.Contains(err.Error(), "all required") {
		t.Fatalf("partial consolidator mTLS error = %v", err)
	}
}

func TestExampleConfigurations(t *testing.T) {
	root := filepath.Join("..", "..", "configs")
	if _, err := LoadHub(filepath.Join(root, "websubhub.example.toml"), nil); err != nil {
		t.Fatalf("hub example: %v", err)
	}
	if _, err := LoadConsolidator(filepath.Join(root, "websubhub-consolidator.example.toml"), nil); err != nil {
		t.Fatalf("consolidator example: %v", err)
	}
	packaged := filepath.Join("..", "..", "packaging", "websubhub", "config", "websubhub.toml")
	if _, err := LoadHub(packaged, nil); err != nil {
		t.Fatalf("packaged hub config: %v", err)
	}
}

func TestKafkaSecurityConfigurationIsStrict(t *testing.T) {
	cfg := validHubConfig()
	cfg.Server.ID = "hub-1"
	cfg.MessageStore.Kafka.Brokers = []string{"kafka:9092"}
	cfg.MessageStore.Kafka.TLS.Enabled = true
	if err := cfg.Validate(); err == nil {
		t.Fatal("TLS without a CA accepted")
	}
	cfg.MessageStore.Kafka.TLS = KafkaTLS{InsecureSkipVerify: true}
	if err := cfg.Validate(); err == nil {
		t.Fatal("TLS option accepted while TLS is disabled")
	}
	cfg.MessageStore.Kafka.TLS = KafkaTLS{}
	cfg.MessageStore.Kafka.SASL.Mechanism = "plain"
	if err := cfg.Validate(); err == nil {
		t.Fatal("SASL without secret file accepted")
	}
}

func TestStateConfigurationValidation(t *testing.T) {
	hub := validHubConfig()
	hub.Server.ID = "hub-1"
	hub.MessageStore.Kafka.Brokers = []string{"kafka:9092"}
	hub.State.Startup.BufferMax = 0
	if err := hub.Validate(); err == nil || !strings.Contains(err.Error(), "buffer_max") {
		t.Fatalf("hub startup buffer error = %v", err)
	}
	hub = validHubConfig()
	hub.Server.ID = "hub-1"
	hub.MessageStore.Kafka.Brokers = []string{"kafka:9092"}
	hub.State.Events.ConsumerBatchSize = 0
	if err := hub.Validate(); err == nil || !strings.Contains(err.Error(), "consumer_batch_size") {
		t.Fatalf("hub consumer batch error = %v", err)
	}

	consolidator := ConsolidatorDefaults()
	consolidator.MessageStore.Kafka.Brokers = []string{"kafka:9092"}
	consolidator.State.Snapshots.Destination = consolidator.State.Events.Destination
	if err := consolidator.Validate(); err == nil || !strings.Contains(err.Error(), "must be different") {
		t.Fatalf("consolidator destination error = %v", err)
	}
	consolidator = ConsolidatorDefaults()
	consolidator.MessageStore.Kafka.Brokers = []string{"kafka:9092"}
	consolidator.State.Events.Retention = 0
	if err := consolidator.Validate(); err == nil || !strings.Contains(err.Error(), "retention") {
		t.Fatalf("consolidator retention error = %v", err)
	}
}

func TestResourceProtocolValidation(t *testing.T) {
	cfg := validHubConfig()
	cfg.Server.ID = "hub-1"
	cfg.MessageStore.Kafka.Brokers = []string{"kafka:9092"}
	cfg.Server.PublicURL = "https://hub.example.test/websub?secret=query"
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "public_url") {
		t.Fatalf("public URL error = %v", err)
	}
	cfg = validHubConfig()
	cfg.Server.ID = "hub-1"
	cfg.MessageStore.Kafka.Brokers = []string{"kafka:9092"}
	cfg.Protocol.DefaultLease = Duration(11 * 24 * time.Hour)
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "default_lease") {
		t.Fatalf("lease error = %v", err)
	}
	cfg = validHubConfig()
	cfg.Server.ID = "hub-1"
	cfg.MessageStore.Kafka.Brokers = []string{"kafka:9092"}
	cfg.Protocol.VerificationQueue = 0
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "workers and queue") {
		t.Fatalf("verification bounds error = %v", err)
	}
}

func noneAuthEnvironment(values ...string) []string {
	return append([]string{
		"WEBSUBHUB__SERVER__AUTH__MODE=none",
		"WEBSUBHUB__OPERATIONS__AUTH__MODE=none",
	}, values...)
}

func validHubConfig() HubConfig {
	cfg := HubDefaults()
	cfg.Server.Auth.Mode = AuthModeJWT
	cfg.Operations.Auth.Mode = AuthModeJWT
	cfg.Security.JWT.Issuer = "https://issuer.example.test"
	cfg.Security.JWT.Audience = "websubhub"
	cfg.Security.JWT.JWKSURL = "https://issuer.example.test/keys"
	return cfg
}

func writeConfig(t *testing.T, name, contents string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}
