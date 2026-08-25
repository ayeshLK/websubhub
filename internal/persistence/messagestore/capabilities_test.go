package messagestore

import (
	"strings"
	"testing"
)

func TestCapabilitiesRequireRejectsRestrictedAndUnsupported(t *testing.T) {
	t.Parallel()
	statuses := make(map[Capability]CapabilityStatus)
	for _, capability := range allCapabilities {
		statuses[capability] = CapabilityStatus{Support: Native}
	}
	statuses[Replay] = CapabilityStatus{Support: Restricted, Detail: "time-based replay is unavailable"}
	statuses[Transactions] = CapabilityStatus{Support: Unsupported, Detail: "no atomic cross-destination writes"}
	capabilities := Capabilities{Provider: "test", Statuses: statuses}
	if err := capabilities.Validate(); err != nil {
		t.Fatal(err)
	}
	if err := capabilities.Require(DurablePublish, Ordering); err != nil {
		t.Fatal(err)
	}
	err := capabilities.Require(Transactions, Replay)
	if err == nil || !strings.Contains(err.Error(), "replay transactions") {
		t.Fatalf("Require() error = %v", err)
	}
}

func TestCapabilitiesRequireCompleteVocabulary(t *testing.T) {
	t.Parallel()
	capabilities := Capabilities{Provider: "test", Statuses: map[Capability]CapabilityStatus{DurablePublish: {Support: Native}}}
	if err := capabilities.Validate(); err == nil || !strings.Contains(err.Error(), "ordering") {
		t.Fatalf("Validate() error = %v", err)
	}
}
