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

package ingest

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/ayeshLK/websubhub/internal/app/resourcehub"
	"github.com/ayeshLK/websubhub/internal/persistence"
	"github.com/ayeshLK/websubhub/internal/persistence/messagestore"
	"github.com/ayeshLK/websubhub/internal/state"
)

func TestPersistPreservesExactRepresentation(t *testing.T) {
	topicURL := "https://publisher.example.test/resource"
	topicID, _ := persistence.TopicID(topicURL)
	projection := fixedProjection{snapshot: state.Snapshot{Topics: map[string]state.Topic{
		topicID: {ID: topicID, Status: state.TopicActive, ContentDestination: "content-destination"},
	}}}
	producer := &recordingProducer{}
	ingestor, err := New(producer, projection, func() (string, error) { return "message-1", nil })
	if err != nil {
		t.Fatal(err)
	}
	body := []byte{0x00, 0xff, '\n', '{', '}'}
	contentType := `application/octet-stream; profile="https://example.test/p"`
	if err := ingestor.Persist(context.Background(), resourcehub.ContentUpdate{Kind: resourcehub.ContentUpdateExact, Topic: topicURL, ContentType: contentType, Body: body}); err != nil {
		t.Fatal(err)
	}
	body[0] = 0x7f
	if producer.destination != "content-destination" || producer.message.ID != "message-1" || producer.message.ContentType != contentType || !bytes.Equal(producer.message.Body, []byte{0x00, 0xff, '\n', '{', '}'}) {
		t.Fatalf("stored message = %#v at %q", producer.message, producer.destination)
	}
	if len(producer.message.Metadata) != 1 || producer.message.Metadata["topic-id"] != topicID {
		t.Fatalf("metadata = %#v", producer.message.Metadata)
	}
}

func TestPersistRejectsUnavailableOrMalformedUpdates(t *testing.T) {
	ingestor, err := New(&recordingProducer{}, fixedProjection{snapshot: state.EmptySnapshot()}, func() (string, error) { return "id", nil })
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		update resourcehub.ContentUpdate
		want   error
	}{
		{"event fetch", resourcehub.ContentUpdate{Kind: resourcehub.ContentUpdateEvent}, ErrEventFetchUnavailable},
		{"inactive topic", resourcehub.ContentUpdate{Kind: resourcehub.ContentUpdateExact, Topic: "https://example.test", ContentType: "text/plain"}, ErrTopicUnavailable},
		{"invalid media type", resourcehub.ContentUpdate{Kind: resourcehub.ContentUpdateExact, Topic: "https://example.test", ContentType: "not a media type"}, nil},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := ingestor.Persist(context.Background(), test.update)
			if err == nil || test.want != nil && !errors.Is(err, test.want) {
				t.Fatalf("error = %v", err)
			}
		})
	}
}

func TestPersistReturnsDurableSendFailure(t *testing.T) {
	topicURL := "https://publisher.example.test/resource"
	topicID, _ := persistence.TopicID(topicURL)
	producer := &recordingProducer{err: errors.New("broker unavailable")}
	ingestor, err := New(producer, fixedProjection{snapshot: state.Snapshot{Topics: map[string]state.Topic{topicID: {ID: topicID, Status: state.TopicActive, ContentDestination: "content"}}}}, func() (string, error) { return "message-1", nil })
	if err != nil {
		t.Fatal(err)
	}
	if err := ingestor.Persist(context.Background(), resourcehub.ContentUpdate{Kind: resourcehub.ContentUpdateExact, Topic: topicURL, ContentType: "text/plain", Body: []byte("body")}); err == nil || !strings.Contains(err.Error(), "persist content message") {
		t.Fatalf("send error = %v", err)
	}
}

type fixedProjection struct{ snapshot state.Snapshot }

func (p fixedProjection) Snapshot() state.Snapshot { return p.snapshot }

type recordingProducer struct {
	destination messagestore.Destination
	message     messagestore.Message
	err         error
}

func (p *recordingProducer) Send(_ context.Context, destination messagestore.Destination, message messagestore.Message) error {
	p.destination, p.message = destination, message
	return p.err
}
func (*recordingProducer) Close(context.Context) error { return nil }
