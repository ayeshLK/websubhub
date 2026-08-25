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
