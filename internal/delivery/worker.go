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

// Package delivery consumes persisted content and delivers it to verified subscribers.
package delivery

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	mathrand "math/rand/v2"
	"mime"
	"net/http"
	"sync"
	"time"

	websubhub "github.com/ayeshLK/lib-websubhub"

	"github.com/ayeshLK/websubhub/internal/config"
	"github.com/ayeshLK/websubhub/internal/persistence/messagestore"
	"github.com/ayeshLK/websubhub/internal/state"
)

var ErrSubscriptionStale = errors.New("subscription became stale")

const HeaderMessageID = "X-Hub-MessageId"

type SecretOpener interface {
	Open(context.Context, []byte, string) ([]byte, error)
}
type EventAppender interface {
	Append(context.Context, state.Event) error
}

type Attempt interface {
	Deliver(context.Context, messagestore.Message) (int, error)
}
type AttemptFactory interface {
	New(state.Subscription, []byte) (Attempt, error)
}
type WaitFunc func(context.Context, time.Duration) error

type Dependencies struct {
	Administrator messagestore.Administrator
	Events        EventAppender
	Secrets       SecretOpener
	Attempts      AttemptFactory
	Wait          WaitFunc
	Now           func() time.Time
	NewEventID    func() (string, error)
	Jitter        func() float64
}

type Worker struct {
	cfg           config.Delivery
	topic         state.Topic
	subscription  state.Subscription
	deps          Dependencies
	mu            sync.Mutex
	cancel        context.CancelFunc
	stopRequested bool
	stopIntent    messagestore.ClosureIntent
}

func NewWorker(cfg config.Delivery, topic state.Topic, subscription state.Subscription, deps Dependencies) (*Worker, error) {
	if deps.Administrator == nil || deps.Events == nil || deps.Secrets == nil || deps.Attempts == nil {
		return nil, errors.New("administrator, state events, secret opener, and attempt factory are required")
	}
	if topic.ID == "" || topic.ContentDestination == "" || subscription.ID == "" || subscription.ConsumerID == "" {
		return nil, errors.New("complete topic and subscription identities are required")
	}
	if subscription.TopicID != topic.ID {
		return nil, errors.New("subscription topic does not match delivery topic")
	}
	if deps.Wait == nil {
		deps.Wait = waitContext
	}
	if deps.Now == nil {
		deps.Now = time.Now
	}
	if deps.NewEventID == nil {
		deps.NewEventID = randomEventID
	}
	if deps.Jitter == nil {
		deps.Jitter = mathrand.Float64
	}
	return &Worker{cfg: cfg, topic: topic, subscription: subscription, deps: deps, stopIntent: messagestore.CloseTemporary}, nil
}

func (w *Worker) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	w.mu.Lock()
	w.cancel = cancel
	stopRequested := w.stopRequested
	w.mu.Unlock()
	if stopRequested {
		cancel()
	}
	defer cancel()
	var consumer messagestore.Consumer
	for {
		var err error
		consumer, err = w.deps.Administrator.OpenConsumer(ctx, messagestore.ConsumerSpec{ID: messagestore.ConsumerID(w.subscription.ConsumerID), Destination: messagestore.Destination(w.topic.ContentDestination), StartPosition: messagestore.StartLatest})
		if err == nil {
			break
		}
		if err := w.deps.Wait(ctx, w.cfg.ReconnectInterval.Value()); err != nil {
			return err
		}
	}
	defer func() {
		w.mu.Lock()
		intent := w.stopIntent
		w.cancel = nil
		w.mu.Unlock()
		_ = consumer.Close(context.WithoutCancel(ctx), intent)
	}()

	secret, err := w.openSecret(ctx)
	if err != nil {
		return w.markStale(ctx, "secret_unavailable")
	}
	attempt, err := w.deps.Attempts.New(w.subscription, secret)
	clear(secret)
	if err != nil {
		return w.markStale(ctx, "delivery_client_invalid")
	}

	for {
		batch, receiveErr := consumer.Receive(ctx, w.cfg.ConsumerBatchSize)
		if receiveErr != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if err := w.reconnect(ctx, consumer); err != nil {
				return err
			}
			continue
		}
	messages:
		for _, received := range batch.Messages {
			result, err := w.deliver(ctx, consumer, attempt, received)
			if err != nil {
				return err
			}
			switch result {
			case resultContinue:
			case resultRedeliver:
				break messages
			case resultPermanent:
				w.setStopIntent(messagestore.ClosePermanent)
				return nil
			case resultStale:
				return ErrSubscriptionStale
			}
		}
	}
}

// Stop interrupts the worker and selects whether its durable consumer progress
// is retained or removed. Permanent intent wins concurrent stop requests.
func (w *Worker) Stop(intent messagestore.ClosureIntent) {
	w.mu.Lock()
	w.stopRequested = true
	if intent == messagestore.ClosePermanent {
		w.stopIntent = intent
	}
	cancel := w.cancel
	w.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (w *Worker) setStopIntent(intent messagestore.ClosureIntent) {
	w.mu.Lock()
	w.stopIntent = intent
	w.mu.Unlock()
}

type deliveryResult uint8

const (
	resultContinue deliveryResult = iota
	resultRedeliver
	resultPermanent
	resultStale
)

func (w *Worker) deliver(ctx context.Context, consumer messagestore.Consumer, attempt Attempt, received messagestore.ReceivedMessage) (deliveryResult, error) {
	if failure := malformed(received.Message); failure != "" {
		deadLetter, err := w.deadLetter(received.Message, failure, 0)
		if err != nil {
			return resultContinue, err
		}
		if err := consumer.DeadLetter(ctx, received.Receipt, deadLetter); err != nil {
			return resultContinue, fmt.Errorf("dead-letter malformed content: %w", err)
		}
		return resultContinue, nil
	}
	if w.cfg.Retry.Strategy == "message_store" {
		return w.deliverMessageStore(ctx, consumer, attempt, received)
	}
	return w.deliverHTTP(ctx, consumer, attempt, received)
}

func (w *Worker) deliverHTTP(ctx context.Context, consumer messagestore.Consumer, attempt Attempt, received messagestore.ReceivedMessage) (deliveryResult, error) {
	var attemptNumber uint32
	for {
		attemptNumber++
		status, err := attempt.Deliver(ctx, received.Message)
		if err == nil {
			return resultContinue, consumer.Ack(ctx, received.Receipt)
		}
		if status == http.StatusGone || errors.Is(err, websubhub.ErrSubscriptionGone) {
			return w.remove(ctx, "http_410")
		}
		if !w.httpRetryable(status) {
			return w.staleResult(ctx, failureClass(status, err))
		}
		if attemptNumber >= uint32(w.cfg.Retry.HTTP.MaxAttempts) {
			if !w.cfg.Retry.HTTP.ResetOnExhaust {
				return w.staleResult(ctx, "retry_exhausted")
			}
			attemptNumber = 0
		}
		if err := w.deps.Wait(ctx, w.retryDelay(attemptNumber)); err != nil {
			return resultContinue, err
		}
	}
}

func (w *Worker) deliverMessageStore(ctx context.Context, consumer messagestore.Consumer, attempt Attempt, received messagestore.ReceivedMessage) (deliveryResult, error) {
	status, err := attempt.Deliver(ctx, received.Message)
	if err == nil {
		return resultContinue, consumer.Ack(ctx, received.Receipt)
	}
	if status == http.StatusGone || errors.Is(err, websubhub.ErrSubscriptionGone) {
		return w.remove(ctx, "http_410")
	}
	action := w.messageStoreAction(status)
	if status == 0 {
		action = w.cfg.Retry.MessageStore.NetworkFailureAction
	}
	switch action {
	case "redeliver":
		return resultRedeliver, consumer.Nack(ctx, received.Receipt, messagestore.NackOptions{Delay: w.cfg.Retry.MessageStore.Delay.Value()})
	case "dead_letter":
		deadLetter, buildErr := w.deadLetter(received.Message, failureClass(status, err), 1)
		if buildErr != nil {
			return resultContinue, buildErr
		}
		return resultContinue, consumer.DeadLetter(ctx, received.Receipt, deadLetter)
	default:
		return w.staleResult(ctx, failureClass(status, err))
	}
}

func (w *Worker) reconnect(ctx context.Context, consumer messagestore.Consumer) error {
	for {
		if err := w.deps.Wait(ctx, w.cfg.ReconnectInterval.Value()); err != nil {
			return err
		}
		if err := consumer.Reconnect(ctx); err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
}

func (w *Worker) openSecret(ctx context.Context) ([]byte, error) {
	if len(w.subscription.SecretCiphertext) == 0 && w.subscription.SecretKeyID == "" {
		return nil, nil
	}
	if len(w.subscription.SecretCiphertext) == 0 || w.subscription.SecretKeyID == "" {
		return nil, errors.New("incomplete protected secret")
	}
	return w.deps.Secrets.Open(ctx, w.subscription.SecretCiphertext, w.subscription.SecretKeyID)
}

func (w *Worker) markStale(ctx context.Context, reason string) error {
	_, err := w.staleResult(ctx, reason)
	if err != nil {
		return err
	}
	return ErrSubscriptionStale
}

func (w *Worker) staleResult(ctx context.Context, reason string) (deliveryResult, error) {
	meta, err := w.metadata()
	if err != nil {
		return resultContinue, err
	}
	if err := w.deps.Events.Append(ctx, state.SubscriptionStaleEvent{Meta: meta, SubscriptionID: w.subscription.ID, Reason: reason}); err != nil {
		return resultContinue, fmt.Errorf("persist stale subscription: %w", err)
	}
	return resultStale, nil
}

func (w *Worker) remove(ctx context.Context, cause string) (deliveryResult, error) {
	meta, err := w.metadata()
	if err != nil {
		return resultContinue, err
	}
	if err := w.deps.Events.Append(ctx, state.SubscriptionRemovedEvent{Meta: meta, SubscriptionID: w.subscription.ID, Cause: cause}); err != nil {
		return resultContinue, fmt.Errorf("persist removed subscription: %w", err)
	}
	return resultPermanent, nil
}

func (w *Worker) metadata() (state.EventMetadata, error) {
	id, err := w.deps.NewEventID()
	if err != nil {
		return state.EventMetadata{}, fmt.Errorf("generate delivery state event ID: %w", err)
	}
	return state.EventMetadata{SchemaVersion: state.SchemaVersion, EventID: id, OccurredAt: w.deps.Now().UTC(), Actor: state.Actor{Type: "delivery", ID: w.subscription.ServerID}}, nil
}

func (w *Worker) deadLetter(message messagestore.Message, failure string, attempt uint32) (messagestore.DeadLetter, error) {
	message.StorageError = ""
	message.Metadata = map[string]string{"topic-id": w.topic.ID}
	if message.ID == "" {
		id, err := w.deps.NewEventID()
		if err != nil {
			return messagestore.DeadLetter{}, fmt.Errorf("generate malformed DLQ message ID: %w", err)
		}
		message.ID = "dlq-" + id
	}
	if message.ContentType == "" {
		message.ContentType = "application/octet-stream"
	} else if _, _, err := mime.ParseMediaType(message.ContentType); err != nil {
		message.ContentType = "application/octet-stream"
	}
	return messagestore.DeadLetter{Destination: messagestore.Destination(w.cfg.DLQDestination), Message: message, TopicID: w.topic.ID, SubscriptionID: w.subscription.ID, FailureClass: failure, Attempt: attempt}, nil
}

func malformed(message messagestore.Message) string {
	if message.StorageError != "" {
		return "storage_decode"
	}
	if message.ID == "" {
		return "missing_message_id"
	}
	if message.ContentType == "" {
		return "missing_content_type"
	}
	if _, _, err := mime.ParseMediaType(message.ContentType); err != nil {
		return "invalid_content_type"
	}
	return ""
}

func (w *Worker) httpRetryable(status int) bool {
	if status == 0 {
		return true
	}
	return contains(w.cfg.Retry.HTTP.RetryStatusCodes, status)
}

func (w *Worker) messageStoreAction(status int) string {
	if contains(w.cfg.Retry.MessageStore.RedeliverStatusCodes, status) {
		return "redeliver"
	}
	if contains(w.cfg.Retry.MessageStore.DeadLetterStatusCodes, status) {
		return "dead_letter"
	}
	if contains(w.cfg.Retry.MessageStore.FailStatusCodes, status) {
		return "fail"
	}
	return w.cfg.Retry.MessageStore.DefaultAction
}

func (w *Worker) retryDelay(attempt uint32) time.Duration {
	if attempt == 0 {
		attempt = 1
	}
	base := float64(w.cfg.Retry.HTTP.InitialInterval.Value()) * math.Pow(w.cfg.Retry.HTTP.BackoffFactor, float64(attempt-1))
	maximum := float64(w.cfg.Retry.HTTP.MaxInterval.Value())
	if base > maximum {
		base = maximum
	}
	factor := 1 + (w.deps.Jitter()*2-1)*w.cfg.Retry.HTTP.JitterFactor
	return time.Duration(base * factor)
}

func failureClass(status int, err error) string {
	if status > 0 {
		return fmt.Sprintf("http_%d", status)
	}
	if err != nil {
		return "network_failure"
	}
	return "delivery_failure"
}

func contains(values []int, target int) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func waitContext(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func randomEventID() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	return "event-" + hex.EncodeToString(value[:]), nil
}

type LibraryAttemptFactory struct {
	HubURL          string
	HTTPClient      *http.Client
	Timeout         time.Duration
	MaxResponseBody int64
}

func (f LibraryAttemptFactory) New(subscription state.Subscription, secret []byte) (Attempt, error) {
	if f.HTTPClient == nil {
		return nil, errors.New("policy-controlled callback HTTP client is required")
	}
	client, err := websubhub.NewDeliveryClient(websubhub.Subscription{Hub: f.HubURL, Mode: websubhub.ModeSubscribe, Topic: subscription.TopicURL, Callback: subscription.CallbackURL, Secret: string(secret)}, websubhub.DeliveryConfig{HTTPClient: f.HTTPClient, Timeout: f.Timeout, MaxResponseBody: f.MaxResponseBody})
	if err != nil {
		return nil, err
	}
	return libraryAttempt{client: client}, nil
}

type libraryAttempt struct{ client *websubhub.DeliveryClient }

func (a libraryAttempt) Deliver(ctx context.Context, message messagestore.Message) (int, error) {
	response, err := a.client.Deliver(ctx, websubhub.ContentDistribution{ContentType: message.ContentType, Body: message.Body, Header: http.Header{HeaderMessageID: []string{message.ID}}})
	return response.StatusCode, err
}
