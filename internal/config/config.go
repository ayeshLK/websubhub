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

const (
	defaultStateEventsDestination    = "websub-events"
	defaultStateSnapshotsDestination = "websub-events-snapshots"
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

type HubConfig struct {
	Server       HubServer          `toml:"server"`
	Protocol     ResourceProtocol   `toml:"protocol"`
	Consolidator ConsolidatorClient `toml:"consolidator"`
	State        HubState           `toml:"state"`
	Delivery     Delivery           `toml:"delivery"`
	MessageStore MessageStore       `toml:"message_store"`
}

type HubServer struct {
	ID        string `toml:"id"`
	Listen    string `toml:"listen"`
	PublicURL string `toml:"public_url"`
}

type ResourceProtocol struct {
	PublisherExtensionEnabled bool     `toml:"publisher_extension_enabled"`
	DefaultLease              Duration `toml:"default_lease"`
	MaxLease                  Duration `toml:"max_lease"`
	MaxRequestBody            int64    `toml:"max_request_body"`
	MaxCallbackBody           int64    `toml:"max_callback_body"`
	VerificationTimeout       Duration `toml:"verification_timeout"`
	VerificationWorkers       int      `toml:"verification_workers"`
	VerificationQueue         int      `toml:"verification_queue"`
	HubErrorCallbackEnabled   bool     `toml:"hub_error_callback_enabled"`
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
	State        ConsolidatorState  `toml:"state"`
	MessageStore MessageStore       `toml:"message_store"`
}

type HubState struct {
	Events  HubStateEvents  `toml:"events"`
	Startup HubStateStartup `toml:"startup"`
}

type HubStateEvents struct {
	Destination       string `toml:"destination"`
	ConsumerBatchSize int    `toml:"consumer_batch_size"`
}

type HubStateStartup struct {
	BufferMax int `toml:"buffer_max"`
}

type Delivery struct {
	ConsumerBatchSize int           `toml:"consumer_batch_size"`
	ReconnectInterval Duration      `toml:"reconnect_interval"`
	RequestTimeout    Duration      `toml:"request_timeout"`
	MaxResponseBody   int64         `toml:"max_response_body"`
	DLQDestination    string        `toml:"dlq_destination"`
	Retry             DeliveryRetry `toml:"retry"`
}

type DeliveryRetry struct {
	Strategy     string            `toml:"strategy"`
	HTTP         HTTPRetry         `toml:"http"`
	MessageStore MessageStoreRetry `toml:"message_store"`
}

type HTTPRetry struct {
	MaxAttempts      int      `toml:"max_attempts"`
	InitialInterval  Duration `toml:"initial_interval"`
	BackoffFactor    float64  `toml:"backoff_factor"`
	MaxInterval      Duration `toml:"max_interval"`
	JitterFactor     float64  `toml:"jitter_factor"`
	RetryStatusCodes []int    `toml:"retry_status_codes"`
	ResetOnExhaust   bool     `toml:"reset_on_exhaust"`
}

type MessageStoreRetry struct {
	Delay                 Duration `toml:"delay"`
	RedeliverStatusCodes  []int    `toml:"redeliver_status_codes"`
	DeadLetterStatusCodes []int    `toml:"dead_letter_status_codes"`
	FailStatusCodes       []int    `toml:"fail_status_codes"`
	DefaultAction         string   `toml:"default_action"`
	NetworkFailureAction  string   `toml:"network_failure_action"`
}

type ConsolidatorState struct {
	Events    StateDestination `toml:"events"`
	Snapshots StateDestination `toml:"snapshots"`
	Consumer  StateConsumer    `toml:"consumer"`
}

type StateDestination struct {
	Destination string   `toml:"destination"`
	Retention   Duration `toml:"retention"`
}

type StateConsumer struct {
	BatchSize int `toml:"batch_size"`
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
		Server: HubServer{Listen: ":8080", PublicURL: "http://127.0.0.1:8080/websub"},
		Protocol: ResourceProtocol{
			DefaultLease:        Duration(10 * 24 * time.Hour),
			MaxLease:            Duration(10 * 24 * time.Hour),
			MaxRequestBody:      64 << 10,
			MaxCallbackBody:     4 << 10,
			VerificationTimeout: Duration(10 * time.Second),
			VerificationWorkers: 4,
			VerificationQueue:   1024,
		},
		Consolidator: ConsolidatorClient{Endpoint: "http://127.0.0.1:8081", Timeout: Duration(10 * time.Second), Auth: ClientAuth{Mode: "none"}},
		State: HubState{
			Events:  HubStateEvents{Destination: defaultStateEventsDestination, ConsumerBatchSize: 100},
			Startup: HubStateStartup{BufferMax: 1000},
		},
		Delivery: Delivery{
			ConsumerBatchSize: 100,
			ReconnectInterval: Duration(5 * time.Second),
			RequestTimeout:    Duration(30 * time.Second),
			MaxResponseBody:   64 << 10,
			DLQDestination:    "websub-delivery-dlq",
			Retry: DeliveryRetry{
				Strategy: "http",
				HTTP: HTTPRetry{
					MaxAttempts: 5, InitialInterval: Duration(time.Second),
					BackoffFactor: 2, MaxInterval: Duration(30 * time.Second),
					JitterFactor:     .2,
					RetryStatusCodes: []int{408, 429, 500, 502, 503, 504},
				},
				MessageStore: MessageStoreRetry{
					Delay:                 Duration(5 * time.Second),
					RedeliverStatusCodes:  []int{408, 429, 500, 502, 503, 504},
					DeadLetterStatusCodes: []int{400, 404},
					DefaultAction:         "fail", NetworkFailureAction: "redeliver",
				},
			},
		},
		MessageStore: messageStoreDefaults("websubhub"),
	}
}

func ConsolidatorDefaults() ConsolidatorConfig {
	return ConsolidatorConfig{
		Server: ConsolidatorServer{Listen: ":8081", Auth: ServerAuth{Mode: "none"}},
		State: ConsolidatorState{
			Events:    StateDestination{Destination: defaultStateEventsDestination, Retention: Duration(7 * 24 * time.Hour)},
			Snapshots: StateDestination{Destination: defaultStateSnapshotsDestination, Retention: Duration(30 * 24 * time.Hour)},
			Consumer:  StateConsumer{BatchSize: 100},
		},
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
	publicURL, err := url.Parse(c.Server.PublicURL)
	if err != nil || publicURL.Host == "" || (publicURL.Scheme != "http" && publicURL.Scheme != "https") ||
		publicURL.User != nil || publicURL.RawQuery != "" || publicURL.Fragment != "" {
		return errors.New("server.public_url must be an absolute HTTP URL without userinfo, query, or fragment")
	}
	if err := c.Protocol.validate(); err != nil {
		return err
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
	if err := c.State.validate(); err != nil {
		return err
	}
	if err := c.Delivery.validate(); err != nil {
		return err
	}
	return c.MessageStore.validate()
}

func (c Delivery) validate() error {
	if c.ConsumerBatchSize < 1 {
		return errors.New("delivery.consumer_batch_size must be positive")
	}
	if c.ReconnectInterval <= 0 || c.RequestTimeout <= 0 {
		return errors.New("delivery intervals and timeouts must be positive")
	}
	if c.MaxResponseBody < 1 {
		return errors.New("delivery.max_response_body must be positive")
	}
	if strings.TrimSpace(c.DLQDestination) == "" {
		return errors.New("delivery.dlq_destination is required")
	}
	switch c.Retry.Strategy {
	case "http":
		if err := c.Retry.HTTP.validate(); err != nil {
			return err
		}
	case "message_store":
		if err := c.Retry.MessageStore.validate(); err != nil {
			return err
		}
	default:
		return errors.New("delivery.retry.strategy must be \"http\" or \"message_store\"")
	}
	return nil
}

func (c HTTPRetry) validate() error {
	if c.MaxAttempts < 1 || c.InitialInterval <= 0 || c.MaxInterval <= 0 {
		return errors.New("delivery.retry.http attempts and intervals must be positive")
	}
	if c.InitialInterval > c.MaxInterval {
		return errors.New("delivery.retry.http.initial_interval must not exceed max_interval")
	}
	if c.BackoffFactor < 1 {
		return errors.New("delivery.retry.http.backoff_factor must be at least 1")
	}
	if c.JitterFactor < 0 || c.JitterFactor > 1 {
		return errors.New("delivery.retry.http.jitter_factor must be between 0 and 1")
	}
	return validateStatusCodes("delivery.retry.http.retry_status_codes", c.RetryStatusCodes)
}

func (c MessageStoreRetry) validate() error {
	if c.Delay < 0 {
		return errors.New("delivery.retry.message_store.delay must not be negative")
	}
	seen := make(map[int]string)
	groups := []struct {
		name  string
		codes []int
	}{
		{"redeliver_status_codes", c.RedeliverStatusCodes},
		{"dead_letter_status_codes", c.DeadLetterStatusCodes},
		{"fail_status_codes", c.FailStatusCodes},
	}
	for _, group := range groups {
		if err := validateStatusCodes("delivery.retry.message_store."+group.name, group.codes); err != nil {
			return err
		}
		for _, code := range group.codes {
			if previous, ok := seen[code]; ok {
				return fmt.Errorf("delivery retry status %d is configured for both %s and %s", code, previous, group.name)
			}
			seen[code] = group.name
		}
	}
	if err := validateDeliveryAction("default_action", c.DefaultAction); err != nil {
		return err
	}
	return validateDeliveryAction("network_failure_action", c.NetworkFailureAction)
}

func validateStatusCodes(name string, codes []int) error {
	for _, code := range codes {
		if code < 300 || code > 599 || code == 410 {
			return fmt.Errorf("%s contains invalid status %d", name, code)
		}
	}
	return nil
}

func validateDeliveryAction(name, action string) error {
	switch action {
	case "redeliver", "dead_letter", "fail":
		return nil
	default:
		return fmt.Errorf("delivery.retry.message_store.%s must be redeliver, dead_letter, or fail", name)
	}
}

func (c ResourceProtocol) validate() error {
	if c.DefaultLease < Duration(time.Second) || c.MaxLease < Duration(time.Second) ||
		c.DefaultLease.Value()%time.Second != 0 || c.MaxLease.Value()%time.Second != 0 {
		return errors.New("protocol lease values must be positive whole seconds")
	}
	if c.DefaultLease > c.MaxLease {
		return errors.New("protocol.default_lease must not exceed protocol.max_lease")
	}
	if c.MaxRequestBody < 1 || c.MaxCallbackBody < 1 {
		return errors.New("protocol body limits must be positive")
	}
	if c.VerificationTimeout <= 0 {
		return errors.New("protocol.verification_timeout must be positive")
	}
	if c.VerificationWorkers < 1 || c.VerificationQueue < 1 {
		return errors.New("protocol verification workers and queue must be positive")
	}
	return nil
}

func (c ConsolidatorConfig) Validate() error {
	if c.Server.Listen == "" {
		return errors.New("server.listen is required")
	}
	if err := validateServerAuth(c.Server.Auth); err != nil {
		return err
	}
	if err := c.State.validate(); err != nil {
		return err
	}
	return c.MessageStore.validate()
}

func (c HubState) validate() error {
	if strings.TrimSpace(c.Events.Destination) == "" {
		return errors.New("state.events.destination is required")
	}
	if c.Events.ConsumerBatchSize < 1 {
		return errors.New("state.events.consumer_batch_size must be positive")
	}
	if c.Startup.BufferMax < 1 {
		return errors.New("state.startup.buffer_max must be positive")
	}
	return nil
}

func (c ConsolidatorState) validate() error {
	if strings.TrimSpace(c.Events.Destination) == "" {
		return errors.New("state.events.destination is required")
	}
	if strings.TrimSpace(c.Snapshots.Destination) == "" {
		return errors.New("state.snapshots.destination is required")
	}
	if c.Events.Destination == c.Snapshots.Destination {
		return errors.New("state event and snapshot destinations must be different")
	}
	if c.Events.Retention <= 0 {
		return errors.New("state.events.retention must be positive")
	}
	if c.Snapshots.Retention <= 0 {
		return errors.New("state.snapshots.retention must be positive")
	}
	if c.Consumer.BatchSize < 1 {
		return errors.New("state.consumer.batch_size must be positive")
	}
	return nil
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
	case reflect.Float32, reflect.Float64:
		value, err := strconv.ParseFloat(raw, field.Type().Bits())
		if err != nil {
			return err
		}
		field.SetFloat(value)
	case reflect.Slice:
		switch field.Type() {
		case reflect.TypeFor[[]string]():
			var wrapper struct {
				Value []string `toml:"value"`
			}
			if err := toml.Unmarshal([]byte("value = "+raw), &wrapper); err != nil {
				return err
			}
			field.Set(reflect.ValueOf(wrapper.Value))
		case reflect.TypeFor[[]int]():
			var wrapper struct {
				Value []int `toml:"value"`
			}
			if err := toml.Unmarshal([]byte("value = "+raw), &wrapper); err != nil {
				return err
			}
			field.Set(reflect.ValueOf(wrapper.Value))
		default:
			return fmt.Errorf("unsupported slice type %s", field.Type())
		}
	default:
		return fmt.Errorf("unsupported field type %s", field.Type())
	}
	return nil
}
