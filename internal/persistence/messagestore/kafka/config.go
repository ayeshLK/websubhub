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
	"crypto/tls"
	"errors"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
)

type Config struct {
	Brokers                  []string
	ClientID                 string
	TLS                      *tls.Config
	SASL                     []sasl.Mechanism
	DialTimeout              time.Duration
	RequestTimeoutOverhead   time.Duration
	DefaultReplicationFactor int16
}

func (c Config) validate() error {
	if len(c.Brokers) == 0 {
		return errors.New("at least one Kafka broker is required")
	}
	for _, broker := range c.Brokers {
		if broker == "" {
			return errors.New("Kafka broker cannot be empty")
		}
	}
	if c.DefaultReplicationFactor < -1 {
		return errors.New("Kafka replication factor must be positive or -1 for the broker default")
	}
	if c.DialTimeout < 0 || c.RequestTimeoutOverhead < 0 {
		return errors.New("Kafka timeouts cannot be negative")
	}
	return nil
}

func (c Config) normalized() Config {
	if c.ClientID == "" {
		c.ClientID = "websubhub"
	}
	if c.DialTimeout == 0 {
		c.DialTimeout = 10 * time.Second
	}
	if c.RequestTimeoutOverhead == 0 {
		c.RequestTimeoutOverhead = 30 * time.Second
	}
	if c.DefaultReplicationFactor == 0 {
		c.DefaultReplicationFactor = -1
	}
	return c
}

func (c Config) commonOptions() []kgo.Opt {
	opts := []kgo.Opt{kgo.SeedBrokers(c.Brokers...), kgo.ClientID(c.ClientID), kgo.DialTimeout(c.DialTimeout), kgo.RequestTimeoutOverhead(c.RequestTimeoutOverhead)}
	if c.TLS != nil {
		opts = append(opts, kgo.DialTLSConfig(c.TLS.Clone()))
	}
	if len(c.SASL) != 0 {
		opts = append(opts, kgo.SASL(c.SASL...))
	}
	return opts
}
