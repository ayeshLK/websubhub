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

// Package provider converts product configuration into concrete provider
// clients without exposing provider values to the rest of the application.
package provider

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"

	"github.com/ayeshLK/websubhub/internal/config"
	storekafka "github.com/ayeshLK/websubhub/internal/persistence/messagestore/kafka"
)

func Kafka(source config.MessageStore) (storekafka.Config, error) {
	if source.Provider != "kafka" {
		return storekafka.Config{}, fmt.Errorf("unsupported MessageStore provider %q", source.Provider)
	}
	result := storekafka.Config{
		Brokers: append([]string(nil), source.Kafka.Brokers...), ClientID: source.Kafka.ClientID,
		DialTimeout: source.Kafka.DialTimeout.Value(), RequestTimeoutOverhead: source.Kafka.RequestTimeoutOverhead.Value(),
		DefaultReplicationFactor: source.Kafka.DefaultReplicationFactor,
	}
	var err error
	result.TLS, err = kafkaTLS(source.Kafka.TLS)
	if err != nil {
		return storekafka.Config{}, err
	}
	if source.Kafka.SASL.Mechanism == "" {
		return result, nil
	}
	body, err := os.ReadFile(source.Kafka.SASL.PasswordFile)
	if err != nil {
		return storekafka.Config{}, fmt.Errorf("read Kafka SASL password: %w", err)
	}
	password := strings.TrimSpace(string(body))
	clear(body)
	if password == "" {
		return storekafka.Config{}, errors.New("Kafka SASL password file is empty")
	}
	switch source.Kafka.SASL.Mechanism {
	case "plain":
		result.SASL = append(result.SASL, plain.Auth{User: source.Kafka.SASL.Username, Pass: password}.AsMechanism())
	case "scram-sha-256":
		result.SASL = append(result.SASL, scram.Auth{User: source.Kafka.SASL.Username, Pass: password}.AsSha256Mechanism())
	case "scram-sha-512":
		result.SASL = append(result.SASL, scram.Auth{User: source.Kafka.SASL.Username, Pass: password}.AsSha512Mechanism())
	default:
		return storekafka.Config{}, fmt.Errorf("unsupported Kafka SASL mechanism %q", source.Kafka.SASL.Mechanism)
	}
	return result, nil
}

func kafkaTLS(source config.KafkaTLS) (*tls.Config, error) {
	if !source.Enabled {
		return nil, nil
	}
	body, err := os.ReadFile(source.CAFile)
	if err != nil {
		return nil, fmt.Errorf("read Kafka CA: %w", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(body) {
		return nil, errors.New("Kafka CA file contains no certificates")
	}
	result := &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots, ServerName: source.ServerName, InsecureSkipVerify: source.InsecureSkipVerify} // #nosec G402 -- explicit operator configuration.
	if source.CertificateFile != "" {
		certificate, err := tls.LoadX509KeyPair(source.CertificateFile, source.PrivateKeyFile)
		if err != nil {
			return nil, fmt.Errorf("load Kafka client identity: %w", err)
		}
		result.Certificates = []tls.Certificate{certificate}
	}
	return result, nil
}
