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

package delivery

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ayeshLK/websubhub/internal/config"
	"github.com/ayeshLK/websubhub/internal/persistence/messagestore"
	"github.com/ayeshLK/websubhub/internal/state"
)

func TestLibraryAttemptPreservesBytesTypeMessageIDAndSignature(t *testing.T) {
	body := []byte{0x00, 0xff, '\n'}
	contentType := `application/octet-stream; profile="https://example.test/p"`
	var gotBody []byte
	var gotType, gotID, gotSignature string
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		gotBody, _ = io.ReadAll(request.Body)
		gotType = request.Header.Get("Content-Type")
		gotID = request.Header.Get(HeaderMessageID)
		gotSignature = request.Header.Get("X-Hub-Signature")
		response.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	factory := LibraryAttemptFactory{HubURL: "https://hub.example.test/websub", HTTPClient: server.Client()}
	attempt, err := factory.New(state.Subscription{TopicURL: "https://publisher.example.test/resource", CallbackURL: server.URL}, []byte("secret"))
	if err != nil {
		t.Fatal(err)
	}
	status, err := attempt.Deliver(context.Background(), messagestore.Message{ID: "message-1", Body: body, ContentType: contentType})
	if err != nil || status != http.StatusNoContent {
		t.Fatalf("delivery status %d: %v", status, err)
	}
	if !bytes.Equal(gotBody, body) || gotType != contentType || gotID != "message-1" || gotSignature != "sha256=db69bd84bd3f0fe6a5cdea5d16e5480baea1375676c04e04185fab082d4d88a5" {
		t.Fatalf("body=%x type=%q id=%q signature=%q", gotBody, gotType, gotID, gotSignature)
	}
}

func TestHTTPRetryAcknowledgesOnlyAfterSuccess(t *testing.T) {
	worker := testWorker(t, "http")
	attempt := &sequenceAttempt{results: []attemptResult{{status: 503, err: errors.New("unavailable")}, {status: 204}}}
	consumer := &fakeConsumer{}
	waits := 0
	worker.deps.Wait = func(context.Context, time.Duration) error { waits++; return nil }
	result, err := worker.deliverHTTP(context.Background(), consumer, attempt, receivedMessage())
	if err != nil || result != resultContinue || consumer.acks != 1 || waits != 1 || attempt.calls != 2 {
		t.Fatalf("result=%d err=%v consumer=%#v waits=%d calls=%d", result, err, consumer, waits, attempt.calls)
	}
}

func TestHTTPNonRetryableMarksStaleWithoutAcknowledging(t *testing.T) {
	worker := testWorker(t, "http")
	consumer := &fakeConsumer{}
	result, err := worker.deliverHTTP(context.Background(), consumer, &sequenceAttempt{results: []attemptResult{{status: 400, err: errors.New("bad request")}}}, receivedMessage())
	if err != nil || result != resultStale || consumer.acks != 0 {
		t.Fatalf("result=%d err=%v acks=%d", result, err, consumer.acks)
	}
	events := worker.deps.Events.(*recordingEvents).events
	stale, ok := events[0].(state.SubscriptionStaleEvent)
	if !ok || stale.Reason != "http_400" {
		t.Fatalf("events = %#v", events)
	}
}

func TestMessageStoreStrategyMapsNackAndDeadLetter(t *testing.T) {
	worker := testWorker(t, "message_store")
	consumer := &fakeConsumer{}
	result, err := worker.deliverMessageStore(context.Background(), consumer, &sequenceAttempt{results: []attemptResult{{status: 503, err: errors.New("unavailable")}}}, receivedMessage())
	if err != nil || result != resultRedeliver || consumer.nacks != 1 {
		t.Fatalf("result=%d err=%v nacks=%d", result, err, consumer.nacks)
	}
	result, err = worker.deliverMessageStore(context.Background(), consumer, &sequenceAttempt{results: []attemptResult{{status: 400, err: errors.New("bad request")}}}, receivedMessage())
	if err != nil || result != resultContinue || len(consumer.deadLetters) != 1 || consumer.deadLetters[0].FailureClass != "http_400" {
		t.Fatalf("result=%d err=%v dead letters=%#v", result, err, consumer.deadLetters)
	}
}

func TestMalformedContentGoesDirectlyToDLQ(t *testing.T) {
	worker := testWorker(t, "http")
	consumer := &fakeConsumer{}
	result, err := worker.deliver(context.Background(), consumer, &sequenceAttempt{}, messagestore.ReceivedMessage{Message: messagestore.Message{StorageError: "bad record"}, Receipt: messagestore.Receipt{Value: "1"}})
	if err != nil || result != resultContinue || len(consumer.deadLetters) != 1 || consumer.deadLetters[0].FailureClass != "storage_decode" {
		t.Fatalf("result=%d err=%v dead letters=%#v", result, err, consumer.deadLetters)
	}
	message := consumer.deadLetters[0].Message
	if message.ID != "dlq-event-1" || message.ContentType != "application/octet-stream" || message.StorageError != "" || len(message.Metadata) != 1 || message.Metadata["topic-id"] != "topic-1" {
		t.Fatalf("sanitized DLQ message = %#v", message)
	}
}

func TestGoneRemovesSubscriptionWithoutAcknowledging(t *testing.T) {
	worker := testWorker(t, "http")
	consumer := &fakeConsumer{}
	result, err := worker.deliverHTTP(context.Background(), consumer, &sequenceAttempt{results: []attemptResult{{status: http.StatusGone, err: errors.New("gone")}}}, receivedMessage())
	if err != nil || result != resultPermanent || consumer.acks != 0 {
		t.Fatalf("result=%d err=%v acks=%d", result, err, consumer.acks)
	}
	removed, ok := worker.deps.Events.(*recordingEvents).events[0].(state.SubscriptionRemovedEvent)
	if !ok || removed.Cause != "http_410" {
		t.Fatalf("event = %#v", worker.deps.Events.(*recordingEvents).events)
	}
}

func testWorker(t *testing.T, strategy string) *Worker {
	t.Helper()
	cfg := config.HubDefaults().Delivery
	cfg.Retry.Strategy = strategy
	worker, err := NewWorker(cfg, state.Topic{ID: "topic-1", ContentDestination: "content"}, state.Subscription{ID: "subscription-1", TopicID: "topic-1", TopicURL: "https://publisher.example.test/resource", CallbackURL: "https://subscriber.example.test/callback", ConsumerID: "consumer-1", ServerID: "hub-1"}, Dependencies{Administrator: fakeAdministrator{}, Events: &recordingEvents{}, Secrets: fakeSecrets{}, Attempts: fakeFactory{}, Now: func() time.Time { return time.Unix(1, 0).UTC() }, NewEventID: func() (string, error) { return "event-1", nil }, Jitter: func() float64 { return .5 }})
	if err != nil {
		t.Fatal(err)
	}
	return worker
}

func receivedMessage() messagestore.ReceivedMessage {
	return messagestore.ReceivedMessage{Message: messagestore.Message{ID: "message-1", Body: []byte("body"), ContentType: "text/plain"}, Receipt: messagestore.Receipt{Value: "receipt-1"}}
}

type attemptResult struct {
	status int
	err    error
}
type sequenceAttempt struct {
	results []attemptResult
	calls   int
}

func (a *sequenceAttempt) Deliver(context.Context, messagestore.Message) (int, error) {
	result := a.results[a.calls]
	a.calls++
	return result.status, result.err
}

type fakeFactory struct{}

func (fakeFactory) New(state.Subscription, []byte) (Attempt, error) { return &sequenceAttempt{}, nil }

type fakeSecrets struct{}

func (fakeSecrets) Open(context.Context, []byte, string) ([]byte, error) { return nil, nil }

type recordingEvents struct{ events []state.Event }

func (e *recordingEvents) Append(_ context.Context, event state.Event) error {
	e.events = append(e.events, event)
	return nil
}

type fakeAdministrator struct{}

func (fakeAdministrator) EnsureDestination(context.Context, messagestore.DestinationSpec) error {
	return nil
}
func (fakeAdministrator) ValidateSubscription(context.Context, messagestore.Destination, messagestore.SubscriptionOptions) error {
	return nil
}
func (fakeAdministrator) OpenConsumer(context.Context, messagestore.ConsumerSpec) (messagestore.Consumer, error) {
	return &fakeConsumer{}, nil
}
func (fakeAdministrator) Capabilities(context.Context) (messagestore.Capabilities, error) {
	return messagestore.Capabilities{}, nil
}
func (fakeAdministrator) Close(context.Context) error { return nil }

type fakeConsumer struct {
	acks, nacks int
	deadLetters []messagestore.DeadLetter
}

func (*fakeConsumer) Metadata() messagestore.ConsumerMetadata { return messagestore.ConsumerMetadata{} }
func (*fakeConsumer) Receive(ctx context.Context, _ int) (messagestore.ReceiveBatch, error) {
	<-ctx.Done()
	return messagestore.ReceiveBatch{}, ctx.Err()
}
func (*fakeConsumer) CaughtUp(context.Context) (bool, error)            { return false, nil }
func (c *fakeConsumer) Ack(context.Context, messagestore.Receipt) error { c.acks++; return nil }
func (c *fakeConsumer) Nack(context.Context, messagestore.Receipt, messagestore.NackOptions) error {
	c.nacks++
	return nil
}
func (c *fakeConsumer) DeadLetter(_ context.Context, _ messagestore.Receipt, deadLetter messagestore.DeadLetter) error {
	c.deadLetters = append(c.deadLetters, deadLetter)
	return nil
}
func (*fakeConsumer) Reconnect(context.Context) error                         { return nil }
func (*fakeConsumer) Close(context.Context, messagestore.ClosureIntent) error { return nil }
