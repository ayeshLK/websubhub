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

// Package management exposes bounded, provider-neutral query views over the
// consolidator's canonical materialized state.
package management

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/ayeshLK/websubhub/internal/state"
)

const MaximumPageSize = 100

var (
	ErrInvalidQuery = errors.New("invalid management query")
	ErrNotFound     = errors.New("management resource not found")
)

type SnapshotSource interface {
	Snapshot(context.Context) (state.Snapshot, error)
}

type TopicQuery struct {
	Limit  int
	Status string
}

type SubscriptionQuery struct {
	Limit   int
	Status  string
	TopicID string
}

type TopicView struct {
	ID           string            `json:"id"`
	URL          string            `json:"url"`
	ContentType  string            `json:"content_type"`
	Status       state.TopicStatus `json:"status"`
	RegisteredAt time.Time         `json:"registered_at"`
	Revision     uint64            `json:"revision"`
}

type SubscriptionView struct {
	ID                    string                   `json:"id"`
	TopicID               string                   `json:"topic_id"`
	Callback              string                   `json:"callback"`
	LeaseStartedAt        time.Time                `json:"lease_started_at"`
	EffectiveLeaseSeconds string                   `json:"effective_lease_seconds,omitempty"`
	ServerID              string                   `json:"server_id"`
	Status                state.SubscriptionStatus `json:"status"`
	StaleReason           string                   `json:"stale_reason,omitempty"`
	Revision              uint64                   `json:"revision"`
}

type TopicPage struct {
	Revision uint64      `json:"revision"`
	Topics   []TopicView `json:"topics"`
}

type SubscriptionPage struct {
	Revision      uint64             `json:"revision"`
	Subscriptions []SubscriptionView `json:"subscriptions"`
}

type TopicResult struct {
	Revision uint64    `json:"revision"`
	Topic    TopicView `json:"topic"`
}

type SubscriptionResult struct {
	Revision     uint64           `json:"revision"`
	Subscription SubscriptionView `json:"subscription"`
}

type QueryClient interface {
	ListTopics(context.Context, TopicQuery) (TopicPage, error)
	GetTopic(context.Context, string) (TopicResult, error)
	ListSubscriptions(context.Context, SubscriptionQuery) (SubscriptionPage, error)
	GetSubscription(context.Context, string) (SubscriptionResult, error)
}

type Service struct {
	snapshots SnapshotSource
}

func New(snapshots SnapshotSource) (*Service, error) {
	if snapshots == nil {
		return nil, errors.New("canonical snapshot source is required")
	}
	return &Service{snapshots: snapshots}, nil
}

func (s *Service) ListTopics(ctx context.Context, query TopicQuery) (TopicPage, error) {
	limit, err := validatedLimit(query.Limit)
	if err != nil {
		return TopicPage{}, err
	}
	if query.Status != "" && query.Status != string(state.TopicActive) && query.Status != string(state.TopicInactive) {
		return TopicPage{}, fmt.Errorf("%w: unsupported topic status", ErrInvalidQuery)
	}
	snapshot, err := s.snapshot(ctx)
	if err != nil {
		return TopicPage{}, err
	}
	topics := make([]TopicView, 0, min(len(snapshot.Topics), limit))
	ids := sortedKeys(snapshot.Topics)
	for _, id := range ids {
		topic := snapshot.Topics[id]
		if query.Status != "" && string(topic.Status) != query.Status {
			continue
		}
		topics = append(topics, topicView(topic))
		if len(topics) == limit {
			break
		}
	}
	return TopicPage{Revision: snapshot.Revision, Topics: topics}, nil
}

func (s *Service) GetTopic(ctx context.Context, id string) (TopicResult, error) {
	if strings.TrimSpace(id) == "" {
		return TopicResult{}, fmt.Errorf("%w: topic ID is required", ErrInvalidQuery)
	}
	snapshot, err := s.snapshot(ctx)
	if err != nil {
		return TopicResult{}, err
	}
	topic, ok := snapshot.Topics[id]
	if !ok {
		return TopicResult{}, fmt.Errorf("%w: topic", ErrNotFound)
	}
	return TopicResult{Revision: snapshot.Revision, Topic: topicView(topic)}, nil
}

func (s *Service) ListSubscriptions(ctx context.Context, query SubscriptionQuery) (SubscriptionPage, error) {
	limit, err := validatedLimit(query.Limit)
	if err != nil {
		return SubscriptionPage{}, err
	}
	if query.Status != "" && query.Status != string(state.SubscriptionActive) && query.Status != string(state.SubscriptionStale) && query.Status != string(state.SubscriptionRemoved) {
		return SubscriptionPage{}, fmt.Errorf("%w: unsupported subscription status", ErrInvalidQuery)
	}
	snapshot, err := s.snapshot(ctx)
	if err != nil {
		return SubscriptionPage{}, err
	}
	subscriptions := make([]SubscriptionView, 0, min(len(snapshot.Subscriptions), limit))
	ids := sortedKeys(snapshot.Subscriptions)
	for _, id := range ids {
		subscription := snapshot.Subscriptions[id]
		if query.Status != "" && string(subscription.Status) != query.Status {
			continue
		}
		if query.TopicID != "" && subscription.TopicID != query.TopicID {
			continue
		}
		subscriptions = append(subscriptions, subscriptionView(subscription))
		if len(subscriptions) == limit {
			break
		}
	}
	return SubscriptionPage{Revision: snapshot.Revision, Subscriptions: subscriptions}, nil
}

func (s *Service) GetSubscription(ctx context.Context, id string) (SubscriptionResult, error) {
	if strings.TrimSpace(id) == "" {
		return SubscriptionResult{}, fmt.Errorf("%w: subscription ID is required", ErrInvalidQuery)
	}
	snapshot, err := s.snapshot(ctx)
	if err != nil {
		return SubscriptionResult{}, err
	}
	subscription, ok := snapshot.Subscriptions[id]
	if !ok {
		return SubscriptionResult{}, fmt.Errorf("%w: subscription", ErrNotFound)
	}
	return SubscriptionResult{Revision: snapshot.Revision, Subscription: subscriptionView(subscription)}, nil
}

func (s *Service) snapshot(ctx context.Context) (state.Snapshot, error) {
	snapshot, err := s.snapshots.Snapshot(ctx)
	if err != nil {
		return state.Snapshot{}, fmt.Errorf("load canonical management snapshot: %w", err)
	}
	return snapshot, nil
}

func validatedLimit(limit int) (int, error) {
	if limit == 0 {
		return MaximumPageSize, nil
	}
	if limit < 1 || limit > MaximumPageSize {
		return 0, fmt.Errorf("%w: limit must be between 1 and %d", ErrInvalidQuery, MaximumPageSize)
	}
	return limit, nil
}

func topicView(topic state.Topic) TopicView {
	return TopicView{ID: topic.ID, URL: topic.CanonicalURL, ContentType: topic.ContentType, Status: topic.Status, RegisteredAt: topic.RegisteredAt, Revision: topic.Revision}
}

func subscriptionView(subscription state.Subscription) SubscriptionView {
	return SubscriptionView{
		ID: subscription.ID, TopicID: subscription.TopicID, Callback: redactURL(subscription.CallbackURL),
		LeaseStartedAt: subscription.LeaseStartedAt, EffectiveLeaseSeconds: subscription.EffectiveLeaseSeconds,
		ServerID: subscription.ServerID, Status: subscription.Status, StaleReason: subscription.StaleReason, Revision: subscription.Revision,
	}
}

func redactURL(raw string) string {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return "redacted"
	}
	parsed.RawQuery, parsed.ForceQuery, parsed.Fragment, parsed.User = "", false, "", nil
	return parsed.String()
}

func sortedKeys[T any](values map[string]T) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

var _ QueryClient = (*Service)(nil)
