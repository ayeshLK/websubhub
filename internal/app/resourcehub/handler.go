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

// Package resourcehub composes lib-websubhub with product-owned durable
// resource-topic state.
package resourcehub

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"net/http"
	"time"

	websubhub "github.com/ayeshLK/lib-websubhub"

	"github.com/ayeshLK/websubhub/internal/config"
	"github.com/ayeshLK/websubhub/internal/persistence"
	"github.com/ayeshLK/websubhub/internal/security/auth"
	"github.com/ayeshLK/websubhub/internal/state"
)

type EventAppender interface {
	Append(context.Context, state.Event) error
}

type Projection interface {
	Snapshot() state.Snapshot
}

type SecretSealer interface {
	Seal(context.Context, []byte) (ciphertext []byte, keyID string, err error)
}

type ContentUpdateKind string

const (
	ContentUpdateEvent ContentUpdateKind = "event"
	ContentUpdateExact ContentUpdateKind = "content"
)

type ContentUpdate struct {
	Kind        ContentUpdateKind
	Topic       string
	ContentType string
	Body        []byte
}

type ContentSink interface {
	Persist(context.Context, ContentUpdate) error
}

type CallbackPolicy interface {
	ValidateURL(context.Context, string) error
}

type Authorizer interface {
	Middleware(http.Handler) http.Handler
	Authorize(context.Context, string) (string, error)
}

type Dependencies struct {
	Events             EventAppender
	Projection         Projection
	Secrets            SecretSealer
	Callbacks          CallbackPolicy
	Authorization      Authorizer
	Content            ContentSink
	VerificationClient *http.Client
	Now                func() time.Time
	NewEventID         func() (string, error)
}

type Handler struct {
	protocol *websubhub.Handler
	handler  http.Handler
}

func New(cfg config.HubConfig, dependencies Dependencies) (*Handler, error) {
	if dependencies.Events == nil || dependencies.Projection == nil || dependencies.Secrets == nil || dependencies.Callbacks == nil || dependencies.Authorization == nil || dependencies.VerificationClient == nil {
		return nil, errors.New("state events, projection, secret sealer, callback policy, authorization, and verification HTTP client are required")
	}
	if cfg.Server.ID == "" {
		return nil, errors.New("server ID is required")
	}
	if dependencies.Now == nil {
		dependencies.Now = time.Now
	}
	if dependencies.NewEventID == nil {
		dependencies.NewEventID = randomEventID
	}
	application := &adapter{
		serverID:      cfg.Server.ID,
		events:        dependencies.Events,
		projection:    dependencies.Projection,
		secrets:       dependencies.Secrets,
		callbacks:     dependencies.Callbacks,
		authorization: dependencies.Authorization,
		content:       dependencies.Content,
		now:           dependencies.Now,
		newEventID:    dependencies.NewEventID,
	}
	protocol, err := websubhub.NewHandler(websubhub.Config{
		HubURL:                   cfg.Server.PublicURL,
		DefaultLease:             cfg.Protocol.DefaultLease.Value(),
		MaxLease:                 cfg.Protocol.MaxLease.Value(),
		MaxRequestBody:           cfg.Protocol.MaxRequestBody,
		MaxCallbackBody:          cfg.Protocol.MaxCallbackBody,
		VerificationTimeout:      cfg.Protocol.VerificationTimeout.Value(),
		VerificationWorkers:      cfg.Protocol.VerificationWorkers,
		VerificationQueue:        cfg.Protocol.VerificationQueue,
		HTTPClient:               dependencies.VerificationClient,
		EnablePublisherExtension: cfg.Protocol.PublisherExtensionEnabled,
		EnableHubErrorCallback:   cfg.Protocol.HubErrorCallbackEnabled,
	}, application.service())
	if err != nil {
		return nil, err
	}
	return &Handler{protocol: protocol, handler: dependencies.Authorization.Middleware(protocol)}, nil
}

func (h *Handler) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	h.handler.ServeHTTP(response, request)
}

func (h *Handler) Close(ctx context.Context) error {
	return h.protocol.Close(ctx)
}

type adapter struct {
	serverID      string
	events        EventAppender
	projection    Projection
	secrets       SecretSealer
	callbacks     CallbackPolicy
	authorization Authorizer
	content       ContentSink
	now           func() time.Time
	newEventID    func() (string, error)
}

func (a *adapter) service() websubhub.Service {
	return websubhub.Service{
		OnRegisterTopic:            a.registerTopic,
		OnDeregisterTopic:          a.deregisterTopic,
		OnUpdateMessage:            a.updateMessage,
		OnSubscription:             a.admitSubscription,
		OnSubscriptionValidation:   a.validateSubscription,
		OnSubscriptionVerified:     a.subscriptionVerified,
		OnUnsubscription:           a.admitUnsubscription,
		OnUnsubscriptionValidation: a.validateUnsubscription,
		OnUnsubscriptionVerified:   a.unsubscriptionVerified,
	}
}

func (a *adapter) registerTopic(ctx context.Context, registration websubhub.TopicRegistration, _ websubhub.RequestMetadata) (websubhub.Result, error) {
	actorID, err := a.authorize(ctx, auth.ScopeTopicRegister)
	if err != nil {
		return websubhub.Result{}, err
	}
	topicID, err := persistence.TopicID(registration.Topic)
	if err != nil {
		return websubhub.Result{}, err
	}
	if topic, ok := a.projection.Snapshot().Topics[topicID]; ok && topic.Status == state.TopicActive {
		return conflict(), nil
	}
	meta, err := a.metadata("publisher", actorID)
	if err != nil {
		return websubhub.Result{}, err
	}
	contentDestination, err := persistence.ContentDestination(registration.Topic)
	if err != nil {
		return websubhub.Result{}, err
	}
	now := meta.OccurredAt
	event := state.TopicRegistered{Meta: meta, Topic: state.Topic{
		ID: topicID, CanonicalURL: registration.Topic,
		ContentDestination: string(contentDestination), RegisteredAt: now,
	}}
	return websubhub.Result{}, a.events.Append(ctx, event)
}

func (a *adapter) deregisterTopic(ctx context.Context, deregistration websubhub.TopicDeregistration, _ websubhub.RequestMetadata) (websubhub.Result, error) {
	actorID, err := a.authorize(ctx, auth.ScopeTopicDeregister)
	if err != nil {
		return websubhub.Result{}, err
	}
	topicID, err := persistence.TopicID(deregistration.Topic)
	if err != nil {
		return websubhub.Result{}, err
	}
	topic, ok := a.projection.Snapshot().Topics[topicID]
	if !ok || topic.Status != state.TopicActive {
		return conflict(), nil
	}
	meta, err := a.metadata("publisher", actorID)
	if err != nil {
		return websubhub.Result{}, err
	}
	return websubhub.Result{}, a.events.Append(ctx, state.TopicDeregistered{Meta: meta, TopicID: topicID})
}

func (a *adapter) updateMessage(ctx context.Context, message websubhub.UpdateMessage, _ websubhub.RequestMetadata) (websubhub.Result, error) {
	if _, err := a.authorize(ctx, auth.ScopeContentPublish); err != nil {
		return websubhub.Result{}, err
	}
	if a.content == nil {
		return websubhub.Result{}, &websubhub.DeniedError{Reason: "content ingestion is unavailable"}
	}
	kind := ContentUpdateEvent
	if message.Kind == websubhub.UpdateContent {
		kind = ContentUpdateExact
	}
	update := ContentUpdate{
		Kind: kind, Topic: message.Topic, ContentType: message.ContentType,
		Body: bytes.Clone(message.Body),
	}
	return websubhub.Result{}, a.content.Persist(ctx, update)
}

func (a *adapter) admitSubscription(ctx context.Context, subscription websubhub.Subscription, _ websubhub.RequestMetadata, _ *websubhub.Controller) (websubhub.Result, error) {
	if _, err := a.authorize(ctx, auth.ScopeSubscriptionCreate); err != nil {
		return websubhub.Result{}, err
	}
	if err := a.callbacks.ValidateURL(ctx, subscription.Callback); err != nil {
		return websubhub.Result{}, &websubhub.DeniedError{Reason: "callback destination is not allowed"}
	}
	snapshot := a.projection.Snapshot()
	if !topicIsActive(snapshot, subscription.Topic) {
		return conflict(), nil
	}
	if _, ok := currentSubscription(snapshot, subscription.Topic, subscription.Callback); ok {
		return conflict(), nil
	}
	return websubhub.Result{}, nil
}

func (a *adapter) validateSubscription(ctx context.Context, subscription websubhub.Subscription, _ websubhub.RequestMetadata) error {
	if err := a.callbacks.ValidateURL(ctx, subscription.Callback); err != nil {
		return &websubhub.DeniedError{Reason: "callback destination is not allowed"}
	}
	snapshot := a.projection.Snapshot()
	if !topicIsActive(snapshot, subscription.Topic) {
		return &websubhub.DeniedError{Reason: "topic is not registered"}
	}
	if _, ok := currentSubscription(snapshot, subscription.Topic, subscription.Callback); ok {
		return &websubhub.DeniedError{Reason: "subscription already exists"}
	}
	return nil
}

func (a *adapter) subscriptionVerified(ctx context.Context, verified websubhub.VerifiedSubscription, _ websubhub.RequestMetadata) error {
	actorID, err := a.authorize(ctx, auth.ScopeSubscriptionCreate)
	if err != nil {
		return err
	}
	topicID, err := persistence.TopicID(verified.Topic)
	if err != nil {
		return err
	}
	subscriptionID, err := persistence.SubscriptionID(verified.Topic, verified.Callback, verified.LeaseStartedAt)
	if err != nil {
		return err
	}
	consumerID, err := persistence.SubscriptionConsumerID(verified.Topic, verified.Callback, verified.LeaseStartedAt)
	if err != nil {
		return err
	}
	var ciphertext []byte
	keyID := ""
	if verified.Secret != "" {
		ciphertext, keyID, err = a.secrets.Seal(ctx, []byte(verified.Secret))
		if err != nil {
			return errors.New("seal subscription secret")
		}
		if len(ciphertext) == 0 || keyID == "" {
			return errors.New("secret sealer returned incomplete protected material")
		}
	}
	meta, err := a.metadata("subscriber", actorID)
	if err != nil {
		return err
	}
	return a.events.Append(ctx, state.SubscriptionVerified{Meta: meta, Subscription: state.Subscription{
		ID: subscriptionID, TopicID: topicID, TopicURL: verified.Topic,
		CallbackURL: verified.Callback, SecretCiphertext: ciphertext,
		SecretKeyID: keyID, LeaseStartedAt: verified.LeaseStartedAt.UTC(),
		EffectiveLeaseSeconds: verified.EffectiveLeaseSeconds,
		ServerID:              a.serverID, ConsumerID: string(consumerID),
	}})
}

func (a *adapter) admitUnsubscription(ctx context.Context, unsubscription websubhub.Unsubscription, _ websubhub.RequestMetadata, _ *websubhub.Controller) (websubhub.Result, error) {
	if _, err := a.authorize(ctx, auth.ScopeSubscriptionDelete); err != nil {
		return websubhub.Result{}, err
	}
	if err := a.callbacks.ValidateURL(ctx, unsubscription.Callback); err != nil {
		return websubhub.Result{}, &websubhub.DeniedError{Reason: "callback destination is not allowed"}
	}
	if _, ok := currentSubscription(a.projection.Snapshot(), unsubscription.Topic, unsubscription.Callback); !ok {
		return conflict(), nil
	}
	return websubhub.Result{}, nil
}

func (a *adapter) validateUnsubscription(ctx context.Context, unsubscription websubhub.Unsubscription, _ websubhub.RequestMetadata) error {
	if err := a.callbacks.ValidateURL(ctx, unsubscription.Callback); err != nil {
		return &websubhub.DeniedError{Reason: "callback destination is not allowed"}
	}
	if _, ok := currentSubscription(a.projection.Snapshot(), unsubscription.Topic, unsubscription.Callback); !ok {
		return &websubhub.DeniedError{Reason: "subscription does not exist"}
	}
	return nil
}

func (a *adapter) unsubscriptionVerified(ctx context.Context, verified websubhub.VerifiedUnsubscription, _ websubhub.RequestMetadata) error {
	actorID, err := a.authorize(ctx, auth.ScopeSubscriptionDelete)
	if err != nil {
		return err
	}
	subscription, ok := currentSubscription(a.projection.Snapshot(), verified.Topic, verified.Callback)
	if !ok {
		return &websubhub.DeniedError{Reason: "subscription does not exist"}
	}
	meta, err := a.metadata("subscriber", actorID)
	if err != nil {
		return err
	}
	return a.events.Append(ctx, state.SubscriptionUnsubscribed{Meta: meta, SubscriptionID: subscription.ID})
}

func (a *adapter) authorize(ctx context.Context, scope string) (string, error) {
	actorID, err := a.authorization.Authorize(ctx, scope)
	if err != nil || actorID == "" {
		return "", &websubhub.DeniedError{Reason: "operation is not authorized"}
	}
	return actorID, nil
}

func (a *adapter) metadata(actorType, actorID string) (state.EventMetadata, error) {
	id, err := a.newEventID()
	if err != nil {
		return state.EventMetadata{}, errors.New("generate state event ID")
	}
	now := a.now().UTC()
	if now.IsZero() {
		return state.EventMetadata{}, errors.New("state event time is required")
	}
	return state.EventMetadata{
		SchemaVersion: state.SchemaVersion,
		EventID:       id,
		OccurredAt:    now,
		Actor:         state.Actor{Type: actorType, ID: actorID},
	}, nil
}

func topicIsActive(snapshot state.Snapshot, exactTopicURL string) bool {
	topicID, err := persistence.TopicID(exactTopicURL)
	if err != nil {
		return false
	}
	topic, ok := snapshot.Topics[topicID]
	return ok && topic.Status == state.TopicActive && topic.CanonicalURL == exactTopicURL
}

func currentSubscription(snapshot state.Snapshot, exactTopicURL, exactCallbackURL string) (state.Subscription, bool) {
	var current state.Subscription
	for _, subscription := range snapshot.Subscriptions {
		if subscription.Status != state.SubscriptionRemoved &&
			subscription.TopicURL == exactTopicURL &&
			subscription.CallbackURL == exactCallbackURL &&
			(current.LeaseStartedAt.IsZero() || subscription.LeaseStartedAt.After(current.LeaseStartedAt)) {
			current = subscription
		}
	}
	return current, !current.LeaseStartedAt.IsZero()
}

func conflict() websubhub.Result {
	return websubhub.Result{StatusCode: http.StatusConflict}
}

func randomEventID() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	return "event-" + hex.EncodeToString(value[:]), nil
}

var _ http.Handler = (*Handler)(nil)
