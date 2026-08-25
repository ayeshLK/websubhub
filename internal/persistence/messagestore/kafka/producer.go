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
	"context"
	"sync"

	"github.com/ayeshLK/websubhub/internal/persistence/messagestore"
	"github.com/twmb/franz-go/pkg/kgo"
)

type produceClient interface {
	ProduceSync(context.Context, ...*kgo.Record) kgo.ProduceResults
	Close()
}

type Producer struct {
	mu     sync.Mutex
	client produceClient
	closed bool
}

func NewProducer(config Config) (*Producer, error) {
	if err := config.validate(); err != nil {
		return nil, err
	}
	config = config.normalized()
	client, err := kgo.NewClient(producerOptions(config)...)
	if err != nil {
		return nil, err
	}
	return &Producer{client: client}, nil
}

func (p *Producer) Send(ctx context.Context, destination messagestore.Destination, message messagestore.Message) error {
	record, err := encodeRecord(destination, message)
	if err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return messagestore.ErrClosed
	}
	return p.client.ProduceSync(ctx, record).FirstErr()
}

func (p *Producer) Close(context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.closed {
		p.client.Close()
		p.closed = true
	}
	return nil
}

func producerOptions(config Config) []kgo.Opt {
	return append(config.commonOptions(), kgo.RequiredAcks(kgo.AllISRAcks()))
}
