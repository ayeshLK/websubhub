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

// Package config owns WebSubHub's process-specific typed configuration roots
// and their shared value types.
package config

import (
	"bytes"
	"errors"
	"fmt"
	"net/url"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/pelletier/go-toml/v2"
)

const EnvPrefix = "WEBSUBHUB__"

type Duration time.Duration

func (d *Duration) UnmarshalText(text []byte) error {
	value, err := time.ParseDuration(string(text))
	if err != nil {
		return err
	}
	*d = Duration(value)
	return nil
}

func (d Duration) Value() time.Duration { return time.Duration(d) }

type HubConfig struct {
	Server       HubServer          `toml:"server"`
	Consolidator ConsolidatorClient `toml:"consolidator"`
	MessageStore MessageStore       `toml:"message_store"`
}

type HubServer struct {
	ID     string `toml:"id"`
	Listen string `toml:"listen"`
}

type ConsolidatorClient struct {
	Endpoint string     `toml:"endpoint"`
	Timeout  Duration   `toml:"timeout"`
	Auth     ClientAuth `toml:"auth"`
}

type ClientAuth struct {
	Mode string          `toml:"mode"`
	MTLS MTLSClientFiles `toml:"mtls"`
}

type ConsolidatorConfig struct {
	Server       ConsolidatorServer `toml:"server"`
	MessageStore MessageStore       `toml:"message_store"`
}

type ConsolidatorServer struct {
	Listen string     `toml:"listen"`
	Auth   ServerAuth `toml:"auth"`
}

type ServerAuth struct {
	Mode string          `toml:"mode"`
	MTLS MTLSServerFiles `toml:"mtls"`
}

type MTLSServerFiles struct {
	CertificateFile string `toml:"certificate_file"`
	PrivateKeyFile  string `toml:"private_key_file"`
	ClientCAFile    string `toml:"client_ca_file"`
}

type MTLSClientFiles struct {
	CertificateFile string `toml:"certificate_file"`
	PrivateKeyFile  string `toml:"private_key_file"`
	ServerCAFile    string `toml:"server_ca_file"`
	ServerName      string `toml:"server_name"`
}

type MessageStore struct {
	Provider string      `toml:"provider"`
	Kafka    KafkaConfig `toml:"kafka"`
}

type KafkaConfig struct {
	Brokers                  []string  `toml:"brokers"`
	ClientID                 string    `toml:"client_id"`
	DialTimeout              Duration  `toml:"dial_timeout"`
	RequestTimeoutOverhead   Duration  `toml:"request_timeout_overhead"`
	DefaultReplicationFactor int16     `toml:"default_replication_factor"`
	TLS                      KafkaTLS  `toml:"tls"`
	SASL                     KafkaSASL `toml:"sasl"`
}

type KafkaTLS struct {
	Enabled            bool   `toml:"enabled"`
	CAFile             string `toml:"ca_file"`
	CertificateFile    string `toml:"certificate_file"`
	PrivateKeyFile     string `toml:"private_key_file"`
	ServerName         string `toml:"server_name"`
	InsecureSkipVerify bool   `toml:"insecure_skip_verify"`
}

type KafkaSASL struct {
	Mechanism    string `toml:"mechanism"`
	Username     string `toml:"username"`
	PasswordFile string `toml:"password_file"`
}

func HubDefaults() HubConfig {
	return HubConfig{
		Server:       HubServer{Listen: ":8080"},
		Consolidator: ConsolidatorClient{Endpoint: "http://127.0.0.1:8081", Timeout: Duration(10 * time.Second), Auth: ClientAuth{Mode: "none"}},
		MessageStore: messageStoreDefaults("websubhub"),
	}
}

func ConsolidatorDefaults() ConsolidatorConfig {
	return ConsolidatorConfig{
		Server:       ConsolidatorServer{Listen: ":8081", Auth: ServerAuth{Mode: "none"}},
		MessageStore: messageStoreDefaults("websubhub-consolidator"),
	}
}

func messageStoreDefaults(clientID string) MessageStore {
	return MessageStore{Provider: "kafka", Kafka: KafkaConfig{
		ClientID: clientID, DialTimeout: Duration(10 * time.Second),
		RequestTimeoutOverhead: Duration(30 * time.Second), DefaultReplicationFactor: -1,
	}}
}

func LoadHub(path string, environ []string) (HubConfig, error) {
	return load(path, environ, HubDefaults(), HubConfig.Validate)
}

func LoadConsolidator(path string, environ []string) (ConsolidatorConfig, error) {
	return load(path, environ, ConsolidatorDefaults(), ConsolidatorConfig.Validate)
}

func load[T any](path string, environ []string, cfg T, validate func(T) error) (T, error) {
	if path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			var zero T
			return zero, fmt.Errorf("read configuration: %w", err)
		}
		decoder := toml.NewDecoder(bytes.NewReader(data))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&cfg); err != nil {
			var zero T
			return zero, fmt.Errorf("decode configuration: %w", err)
		}
	}
	if err := applyEnvironment(&cfg, environ); err != nil {
		var zero T
		return zero, err
	}
	if err := validate(cfg); err != nil {
		var zero T
		return zero, err
	}
	return cfg, nil
}

func (c HubConfig) Validate() error {
	if c.Server.ID == "" {
		return errors.New("server.id is required")
	}
	if c.Server.Listen == "" {
		return errors.New("server.listen is required")
	}
	if c.Consolidator.Timeout <= 0 {
		return errors.New("consolidator.timeout must be positive")
	}
	endpoint, err := url.Parse(c.Consolidator.Endpoint)
	if err != nil || endpoint.Host == "" {
		return errors.New("consolidator.endpoint must be an absolute HTTP URL")
	}
	if err := validateClientAuth(c.Consolidator.Auth); err != nil {
		return err
	}
	expectedScheme := "http"
	if c.Consolidator.Auth.Mode == "mtls" {
		expectedScheme = "https"
	}
	if endpoint.Scheme != expectedScheme {
		return fmt.Errorf("consolidator.endpoint must use %s when consolidator.auth.mode = %q", expectedScheme, c.Consolidator.Auth.Mode)
	}
	return c.MessageStore.validate()
}

func (c ConsolidatorConfig) Validate() error {
	if c.Server.Listen == "" {
		return errors.New("server.listen is required")
	}
	if err := validateServerAuth(c.Server.Auth); err != nil {
		return err
	}
	return c.MessageStore.validate()
}

func validateClientAuth(auth ClientAuth) error {
	switch auth.Mode {
	case "none":
		if anySet(auth.MTLS.CertificateFile, auth.MTLS.PrivateKeyFile, auth.MTLS.ServerCAFile, auth.MTLS.ServerName) {
			return errors.New("consolidator.auth.mtls settings require mode = \"mtls\"")
		}
	case "mtls":
		if err := requireAll("consolidator.auth.mtls", auth.MTLS.CertificateFile, auth.MTLS.PrivateKeyFile, auth.MTLS.ServerCAFile); err != nil {
			return err
		}
	default:
		return errors.New("consolidator.auth.mode must be \"none\" or \"mtls\"")
	}
	return nil
}

func validateServerAuth(auth ServerAuth) error {
	switch auth.Mode {
	case "none":
		if anySet(auth.MTLS.CertificateFile, auth.MTLS.PrivateKeyFile, auth.MTLS.ClientCAFile) {
			return errors.New("server.auth.mtls settings require mode = \"mtls\"")
		}
	case "mtls":
		if err := requireAll("server.auth.mtls", auth.MTLS.CertificateFile, auth.MTLS.PrivateKeyFile, auth.MTLS.ClientCAFile); err != nil {
			return err
		}
	default:
		return errors.New("server.auth.mode must be \"none\" or \"mtls\"")
	}
	return nil
}

func (c MessageStore) validate() error {
	if c.Provider != "kafka" {
		return fmt.Errorf("unsupported message_store.provider %q", c.Provider)
	}
	if len(c.Kafka.Brokers) == 0 {
		return errors.New("message_store.kafka.brokers requires at least one broker")
	}
	return c.Kafka.validate()
}

func (c KafkaConfig) validate() error {
	if c.DialTimeout <= 0 || c.RequestTimeoutOverhead <= 0 {
		return errors.New("message_store.kafka timeouts must be positive")
	}
	if c.DefaultReplicationFactor < -1 || c.DefaultReplicationFactor == 0 {
		return errors.New("message_store.kafka.default_replication_factor must be positive or -1")
	}
	for _, broker := range c.Brokers {
		if broker == "" {
			return errors.New("message_store.kafka.brokers cannot contain an empty address")
		}
	}
	if !c.TLS.Enabled && (c.TLS.InsecureSkipVerify || anySet(c.TLS.CAFile, c.TLS.CertificateFile, c.TLS.PrivateKeyFile, c.TLS.ServerName)) {
		return errors.New("message_store.kafka.tls settings require enabled = true")
	}
	if c.TLS.Enabled {
		if c.TLS.CAFile == "" {
			return errors.New("message_store.kafka.tls.ca_file is required when TLS is enabled")
		}
		if (c.TLS.CertificateFile == "") != (c.TLS.PrivateKeyFile == "") {
			return errors.New("message_store.kafka.tls certificate and private key must be configured together")
		}
	}
	switch c.SASL.Mechanism {
	case "":
		if anySet(c.SASL.Username, c.SASL.PasswordFile) {
			return errors.New("message_store.kafka.sasl credentials require a mechanism")
		}
	case "plain", "scram-sha-256", "scram-sha-512":
		if c.SASL.Username == "" || c.SASL.PasswordFile == "" {
			return errors.New("message_store.kafka.sasl username and password_file are required")
		}
	default:
		return fmt.Errorf("unsupported message_store.kafka.sasl.mechanism %q", c.SASL.Mechanism)
	}
	return nil
}

func requireAll(name string, values ...string) error {
	for _, value := range values {
		if value == "" {
			return fmt.Errorf("%s certificate, private key, and CA files are all required", name)
		}
	}
	return nil
}

func anySet(values ...string) bool {
	for _, value := range values {
		if value != "" {
			return true
		}
	}
	return false
}

func applyEnvironment[T any](cfg *T, environ []string) error {
	fields := make(map[string]reflect.Value)
	indexFields(reflect.ValueOf(cfg).Elem(), nil, fields)
	for _, entry := range environ {
		name, value, found := strings.Cut(entry, "=")
		if !found || !strings.HasPrefix(strings.ToUpper(name), EnvPrefix) {
			continue
		}
		path := strings.ToLower(strings.ReplaceAll(name[len(EnvPrefix):], "__", "."))
		field, ok := fields[path]
		if !ok {
			return fmt.Errorf("unknown environment override %s", name)
		}
		if err := setValue(field, value); err != nil {
			return fmt.Errorf("invalid environment override %s: %w", name, err)
		}
	}
	return nil
}

func indexFields(value reflect.Value, path []string, result map[string]reflect.Value) {
	typ := value.Type()
	for i := 0; i < value.NumField(); i++ {
		name := typ.Field(i).Tag.Get("toml")
		if name == "" || name == "-" {
			continue
		}
		field, next := value.Field(i), append(append([]string(nil), path...), name)
		if field.Kind() == reflect.Struct {
			indexFields(field, next, result)
		} else {
			result[strings.Join(next, ".")] = field
		}
	}
}

func setValue(field reflect.Value, raw string) error {
	if field.Type() == reflect.TypeFor[Duration]() {
		value, err := time.ParseDuration(raw)
		if err != nil {
			return err
		}
		field.SetInt(int64(value))
		return nil
	}
	switch field.Kind() {
	case reflect.String:
		field.SetString(raw)
	case reflect.Bool:
		value, err := strconv.ParseBool(raw)
		if err != nil {
			return err
		}
		field.SetBool(value)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		value, err := strconv.ParseInt(raw, 10, field.Type().Bits())
		if err != nil {
			return err
		}
		field.SetInt(value)
	case reflect.Slice:
		var wrapper struct {
			Value []string `toml:"value"`
		}
		if field.Type() != reflect.TypeFor[[]string]() {
			return fmt.Errorf("unsupported slice type %s", field.Type())
		}
		if err := toml.Unmarshal([]byte("value = "+raw), &wrapper); err != nil {
			return err
		}
		field.Set(reflect.ValueOf(wrapper.Value))
	default:
		return fmt.Errorf("unsupported field type %s", field.Type())
	}
	return nil
}
