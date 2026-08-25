// Package state defines WebSubHub's concrete, versioned product state and
// deterministic reduction behavior.
package state

import "time"

const SchemaVersion uint16 = 1

type TopicStatus string

const (
	TopicActive   TopicStatus = "active"
	TopicInactive TopicStatus = "inactive"
)

type SubscriptionStatus string

const (
	SubscriptionActive  SubscriptionStatus = "active"
	SubscriptionStale   SubscriptionStatus = "stale"
	SubscriptionRemoved SubscriptionStatus = "removed"
)

type Topic struct {
	ID                 string      `json:"id"`
	CanonicalURL       string      `json:"canonical_url"`
	ContentDestination string      `json:"content_destination"`
	Status             TopicStatus `json:"status"`
	RegisteredAt       time.Time   `json:"registered_at"`
	RegisteredBy       string      `json:"registered_by,omitempty"`
	Revision           uint64      `json:"revision"`
}

type Subscription struct {
	ID                    string             `json:"id"`
	TopicID               string             `json:"topic_id"`
	TopicURL              string             `json:"topic_url"`
	CallbackURL           string             `json:"callback_url"`
	SecretCiphertext      []byte             `json:"secret_ciphertext,omitempty"`
	SecretKeyID           string             `json:"secret_key_id,omitempty"`
	LeaseStartedAt        time.Time          `json:"lease_started_at"`
	EffectiveLeaseSeconds string             `json:"effective_lease_seconds,omitempty"`
	ServerID              string             `json:"server_id"`
	ConsumerID            string             `json:"consumer_id"`
	Status                SubscriptionStatus `json:"status"`
	StaleReason           string             `json:"stale_reason,omitempty"`
	Revision              uint64             `json:"revision"`
}

type Snapshot struct {
	SchemaVersion uint16
	Revision      uint64
	Topics        map[string]Topic
	Subscriptions map[string]Subscription
}

func EmptySnapshot() Snapshot {
	return Snapshot{
		SchemaVersion: SchemaVersion,
		Topics:        make(map[string]Topic),
		Subscriptions: make(map[string]Subscription),
	}
}
