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

// Package config owns WebSubHub's canonical typed configuration schema.
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

type Component string

const (
	Hub          Component = "websubhub"
	Consolidator Component = "websubhub-consolidator"
)

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

type Config struct {
	Server       Server          `toml:"server"`
	Consolidator ConsolidatorAPI `toml:"consolidator"`
	InternalAuth InternalAuth    `toml:"internal_auth"`
	MessageStore MessageStore    `toml:"message_store"`
}

type Server struct {
	ID     string `toml:"id"`
	Listen string `toml:"listen"`
}

type ConsolidatorAPI struct {
	Listen   string   `toml:"listen"`
	Endpoint string   `toml:"endpoint"`
	Timeout  Duration `toml:"timeout"`
}

type InternalAuth struct {
	Mode   string          `toml:"mode"`
	Server MTLSServerFiles `toml:"server"`
	Client MTLSClientFiles `toml:"client"`
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

func Defaults() Config {
	return Config{
		Server:       Server{Listen: ":8080"},
		Consolidator: ConsolidatorAPI{Listen: ":8081", Endpoint: "http://127.0.0.1:8081", Timeout: Duration(10 * time.Second)},
		InternalAuth: InternalAuth{Mode: "none"},
		MessageStore: MessageStore{Provider: "kafka", Kafka: KafkaConfig{
			ClientID: "websubhub", DialTimeout: Duration(10 * time.Second),
			RequestTimeoutOverhead: Duration(30 * time.Second), DefaultReplicationFactor: -1,
		}},
	}
}

func Load(path string, environ []string, component Component) (Config, error) {
	cfg := Defaults()
	if path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			return Config{}, fmt.Errorf("read configuration: %w", err)
		}
		decoder := toml.NewDecoder(bytes.NewReader(data))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&cfg); err != nil {
			return Config{}, fmt.Errorf("decode configuration: %w", err)
		}
	}
	if err := applyEnvironment(&cfg, environ); err != nil {
		return Config{}, err
	}
	if err := cfg.Validate(component); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func (c Config) Validate(component Component) error {
	if component != Hub && component != Consolidator {
		return fmt.Errorf("unknown component %q", component)
	}
	if c.MessageStore.Provider != "kafka" {
		return fmt.Errorf("unsupported message_store.provider %q", c.MessageStore.Provider)
	}
	if len(c.MessageStore.Kafka.Brokers) == 0 {
		return errors.New("message_store.kafka.brokers requires at least one broker")
	}
	if c.InternalAuth.Mode != "none" && c.InternalAuth.Mode != "mtls" {
		return errors.New("internal_auth.mode must be \"none\" or \"mtls\"")
	}
	if c.Consolidator.Timeout <= 0 {
		return errors.New("consolidator.timeout must be positive")
	}
	if err := c.MessageStore.Kafka.validate(); err != nil {
		return err
	}
	if c.InternalAuth.Mode == "none" && any(
		c.InternalAuth.Server.CertificateFile, c.InternalAuth.Server.PrivateKeyFile, c.InternalAuth.Server.ClientCAFile,
		c.InternalAuth.Client.CertificateFile, c.InternalAuth.Client.PrivateKeyFile, c.InternalAuth.Client.ServerCAFile, c.InternalAuth.Client.ServerName,
	) {
		return errors.New("internal_auth certificate settings require mode = \"mtls\"")
	}
	if component == Hub {
		if c.Server.ID == "" {
			return errors.New("server.id is required")
		}
		if c.Server.Listen == "" || c.Consolidator.Endpoint == "" {
			return errors.New("server.listen and consolidator.endpoint are required")
		}
		endpoint, err := url.Parse(c.Consolidator.Endpoint)
		if err != nil || endpoint.Host == "" {
			return errors.New("consolidator.endpoint must be an absolute HTTP URL")
		}
		expectedScheme := "http"
		if c.InternalAuth.Mode == "mtls" {
			expectedScheme = "https"
			if err := requireAll("internal_auth.client", c.InternalAuth.Client.CertificateFile, c.InternalAuth.Client.PrivateKeyFile, c.InternalAuth.Client.ServerCAFile); err != nil {
				return err
			}
		} else if any(c.InternalAuth.Client.CertificateFile, c.InternalAuth.Client.PrivateKeyFile, c.InternalAuth.Client.ServerCAFile, c.InternalAuth.Client.ServerName) {
			return errors.New("internal_auth.client settings require mode = \"mtls\"")
		}
		if endpoint.Scheme != expectedScheme {
			return fmt.Errorf("consolidator.endpoint must use %s when internal_auth.mode = %q", expectedScheme, c.InternalAuth.Mode)
		}
	}
	if component == Consolidator {
		if c.Consolidator.Listen == "" {
			return errors.New("consolidator.listen is required")
		}
		if c.InternalAuth.Mode == "mtls" {
			if err := requireAll("internal_auth.server", c.InternalAuth.Server.CertificateFile, c.InternalAuth.Server.PrivateKeyFile, c.InternalAuth.Server.ClientCAFile); err != nil {
				return err
			}
		} else if any(c.InternalAuth.Server.CertificateFile, c.InternalAuth.Server.PrivateKeyFile, c.InternalAuth.Server.ClientCAFile) {
			return errors.New("internal_auth.server settings require mode = \"mtls\"")
		}
	}
	return nil
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
	if !c.TLS.Enabled && any(c.TLS.CAFile, c.TLS.CertificateFile, c.TLS.PrivateKeyFile, c.TLS.ServerName) {
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
		if any(c.SASL.Username, c.SASL.PasswordFile) {
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

func any(values ...string) bool {
	for _, value := range values {
		if value != "" {
			return true
		}
	}
	return false
}

func applyEnvironment(cfg *Config, environ []string) error {
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
