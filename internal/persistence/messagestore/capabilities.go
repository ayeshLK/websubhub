package messagestore

import (
	"errors"
	"fmt"
	"sort"
)

type Support string

const (
	Native      Support = "native"
	Emulated    Support = "product-emulated"
	Restricted  Support = "restricted"
	Unsupported Support = "unsupported"
)

type Capability string

const (
	DurablePublish      Capability = "durable_publish"
	Ordering            Capability = "ordering"
	DurableSubscription Capability = "durable_subscription"
	Acknowledgement     Capability = "acknowledgement"
	Replay              Capability = "replay"
	Retention           Capability = "retention"
	DeadLettering       Capability = "dead_lettering"
	DelayedDelivery     Capability = "delayed_delivery"
	Transactions        Capability = "transactions"
	Provisioning        Capability = "provisioning"
	ConsumerScaling     Capability = "consumer_scaling"
)

var allCapabilities = []Capability{
	DurablePublish, Ordering, DurableSubscription, Acknowledgement, Replay,
	Retention, DeadLettering, DelayedDelivery, Transactions, Provisioning,
	ConsumerScaling,
}

type CapabilityStatus struct {
	Support Support
	Detail  string
}

type Capabilities struct {
	Provider string
	Statuses map[Capability]CapabilityStatus
}

func (c Capabilities) Validate() error {
	if c.Provider == "" {
		return errors.New("provider is required")
	}
	for _, capability := range allCapabilities {
		status, ok := c.Statuses[capability]
		if !ok {
			return fmt.Errorf("capability %s is not declared", capability)
		}
		if !validSupport(status.Support) {
			return fmt.Errorf("capability %s has invalid support %q", capability, status.Support)
		}
		if (status.Support == Restricted || status.Support == Unsupported) && status.Detail == "" {
			return fmt.Errorf("capability %s requires a restriction detail", capability)
		}
	}
	for capability := range c.Statuses {
		if !knownCapability(capability) {
			return fmt.Errorf("unknown capability %q", capability)
		}
	}
	return nil
}

func (c Capabilities) Require(required ...Capability) error {
	if err := c.Validate(); err != nil {
		return err
	}
	missing := make([]string, 0)
	for _, capability := range required {
		status, ok := c.Statuses[capability]
		if !ok || status.Support == Unsupported || status.Support == Restricted {
			missing = append(missing, string(capability))
		}
	}
	if len(missing) == 0 {
		return nil
	}
	sort.Strings(missing)
	return fmt.Errorf("provider %s cannot satisfy required capabilities: %v", c.Provider, missing)
}

func validSupport(s Support) bool {
	return s == Native || s == Emulated || s == Restricted || s == Unsupported
}

func knownCapability(candidate Capability) bool {
	for _, capability := range allCapabilities {
		if candidate == capability {
			return true
		}
	}
	return false
}
