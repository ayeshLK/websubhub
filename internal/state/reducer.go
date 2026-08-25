package state

import (
	"errors"
	"fmt"
	"maps"
	"slices"
)

var ErrInvalidTransition = errors.New("invalid state transition")

type Reducer struct{}

func (Reducer) Apply(current Snapshot, event Event) (Snapshot, bool, error) {
	if err := validateSnapshot(current); err != nil {
		return Snapshot{}, false, err
	}
	if err := validateEvent(event); err != nil {
		return Snapshot{}, false, err
	}

	next := cloneSnapshot(current)
	changed, err := apply(&next, event)
	if err != nil {
		return Snapshot{}, false, err
	}
	if !changed {
		return current, false, nil
	}
	next.Revision++
	stampRevision(&next, event, next.Revision)
	return next, true, nil
}

func apply(next *Snapshot, event Event) (bool, error) {
	switch e := event.(type) {
	case TopicRegistered:
		if e.Topic.ID == "" || e.Topic.CanonicalURL == "" || e.Topic.ContentDestination == "" || e.Topic.RegisteredAt.IsZero() {
			return false, fmt.Errorf("%w: incomplete topic registration", ErrInvalidTransition)
		}
		desired := e.Topic
		desired.Status = TopicActive
		desired.Revision = 0
		if existing, ok := next.Topics[desired.ID]; ok {
			if existing.Status == TopicActive && sameTopic(existing, desired) {
				return false, nil
			}
			if existing.CanonicalURL != desired.CanonicalURL {
				return false, fmt.Errorf("%w: topic ID collision", ErrInvalidTransition)
			}
		}
		next.Topics[desired.ID] = desired
		return true, nil
	case TopicDeregistered:
		topic, ok := next.Topics[e.TopicID]
		if !ok {
			return false, fmt.Errorf("%w: topic does not exist", ErrInvalidTransition)
		}
		if topic.Status == TopicInactive {
			return false, nil
		}
		topic.Status = TopicInactive
		next.Topics[e.TopicID] = topic
		return true, nil
	case SubscriptionVerified:
		desired := cloneSubscription(e.Subscription)
		if desired.ID == "" || desired.TopicID == "" || desired.TopicURL == "" || desired.CallbackURL == "" || desired.ServerID == "" || desired.ConsumerID == "" || desired.LeaseStartedAt.IsZero() {
			return false, fmt.Errorf("%w: incomplete verified subscription", ErrInvalidTransition)
		}
		topic, ok := next.Topics[desired.TopicID]
		if !ok || topic.Status != TopicActive || topic.CanonicalURL != desired.TopicURL {
			return false, fmt.Errorf("%w: subscription topic is not active", ErrInvalidTransition)
		}
		desired.Status = SubscriptionActive
		desired.StaleReason = ""
		desired.Revision = 0
		if existing, ok := next.Subscriptions[desired.ID]; ok {
			if existing.Status == SubscriptionActive && sameSubscription(existing, desired) {
				return false, nil
			}
			return false, fmt.Errorf("%w: subscription ID already exists", ErrInvalidTransition)
		}
		next.Subscriptions[desired.ID] = desired
		return true, nil
	case SubscriptionUnsubscribed:
		return removeSubscription(next, e.SubscriptionID)
	case SubscriptionRemovedEvent:
		return removeSubscription(next, e.SubscriptionID)
	case SubscriptionStaleEvent:
		subscription, ok := next.Subscriptions[e.SubscriptionID]
		if !ok || subscription.Status == SubscriptionRemoved {
			return false, fmt.Errorf("%w: subscription cannot become stale", ErrInvalidTransition)
		}
		if subscription.Status == SubscriptionStale && subscription.StaleReason == e.Reason {
			return false, nil
		}
		subscription.Status = SubscriptionStale
		subscription.StaleReason = e.Reason
		next.Subscriptions[e.SubscriptionID] = subscription
		return true, nil
	case SubscriptionReactivated:
		subscription, ok := next.Subscriptions[e.SubscriptionID]
		if !ok || subscription.Status == SubscriptionRemoved {
			return false, fmt.Errorf("%w: subscription cannot be reactivated", ErrInvalidTransition)
		}
		if subscription.Status == SubscriptionActive {
			return false, nil
		}
		subscription.Status = SubscriptionActive
		subscription.StaleReason = ""
		next.Subscriptions[e.SubscriptionID] = subscription
		return true, nil
	default:
		return false, fmt.Errorf("unsupported state event %T", event)
	}
}

func removeSubscription(next *Snapshot, id string) (bool, error) {
	subscription, ok := next.Subscriptions[id]
	if !ok {
		return false, fmt.Errorf("%w: subscription does not exist", ErrInvalidTransition)
	}
	if subscription.Status == SubscriptionRemoved {
		return false, nil
	}
	subscription.Status = SubscriptionRemoved
	subscription.StaleReason = ""
	next.Subscriptions[id] = subscription
	return true, nil
}

func stampRevision(snapshot *Snapshot, event Event, revision uint64) {
	switch e := event.(type) {
	case TopicRegistered:
		topic := snapshot.Topics[e.Topic.ID]
		topic.Revision = revision
		snapshot.Topics[e.Topic.ID] = topic
	case TopicDeregistered:
		topic := snapshot.Topics[e.TopicID]
		topic.Revision = revision
		snapshot.Topics[e.TopicID] = topic
	case SubscriptionVerified:
		sub := snapshot.Subscriptions[e.Subscription.ID]
		sub.Revision = revision
		snapshot.Subscriptions[e.Subscription.ID] = sub
	case SubscriptionUnsubscribed:
		sub := snapshot.Subscriptions[e.SubscriptionID]
		sub.Revision = revision
		snapshot.Subscriptions[e.SubscriptionID] = sub
	case SubscriptionStaleEvent:
		sub := snapshot.Subscriptions[e.SubscriptionID]
		sub.Revision = revision
		snapshot.Subscriptions[e.SubscriptionID] = sub
	case SubscriptionReactivated:
		sub := snapshot.Subscriptions[e.SubscriptionID]
		sub.Revision = revision
		snapshot.Subscriptions[e.SubscriptionID] = sub
	case SubscriptionRemovedEvent:
		sub := snapshot.Subscriptions[e.SubscriptionID]
		sub.Revision = revision
		snapshot.Subscriptions[e.SubscriptionID] = sub
	}
}

func cloneSnapshot(source Snapshot) Snapshot {
	clone := source
	clone.Topics = maps.Clone(source.Topics)
	clone.Subscriptions = make(map[string]Subscription, len(source.Subscriptions))
	for id, subscription := range source.Subscriptions {
		clone.Subscriptions[id] = cloneSubscription(subscription)
	}
	return clone
}

func cloneSubscription(source Subscription) Subscription {
	source.SecretCiphertext = slices.Clone(source.SecretCiphertext)
	return source
}

func sameTopic(a, b Topic) bool {
	a.Revision, b.Revision = 0, 0
	return a == b
}

func sameSubscription(a, b Subscription) bool {
	a.Revision, b.Revision = 0, 0
	return a.ID == b.ID && a.TopicID == b.TopicID && a.TopicURL == b.TopicURL && a.CallbackURL == b.CallbackURL && slices.Equal(a.SecretCiphertext, b.SecretCiphertext) && a.SecretKeyID == b.SecretKeyID && a.LeaseStartedAt.Equal(b.LeaseStartedAt) && a.EffectiveLeaseSeconds == b.EffectiveLeaseSeconds && a.ServerID == b.ServerID && a.ConsumerID == b.ConsumerID && a.Status == b.Status && a.StaleReason == b.StaleReason
}
