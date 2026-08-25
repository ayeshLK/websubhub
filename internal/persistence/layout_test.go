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

package persistence

import (
	"strings"
	"testing"
	"time"
)

func TestStableProviderNeutralMappings(t *testing.T) {
	t.Parallel()
	topic := "https://publisher.example/resource?exact=a%2Fb"
	destination, err := ContentDestination(topic)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(string(destination), "websub-topic-") || len(destination) != len("websub-topic-")+64 {
		t.Fatalf("destination = %q", destination)
	}
	if destinationAgain, _ := ContentDestination(topic); destinationAgain != destination {
		t.Fatalf("mapping changed: %q != %q", destinationAgain, destination)
	}
	hubOne, _ := HubStateConsumerID("hub-1")
	hubTwo, _ := HubStateConsumerID("hub-2")
	if hubOne == hubTwo || strings.Contains(string(hubOne), "hub-1") {
		t.Fatalf("unsafe hub identities %q %q", hubOne, hubTwo)
	}
}

func TestSubscriptionIdentityUsesUnambiguousFields(t *testing.T) {
	t.Parallel()
	started := time.Date(2026, 8, 26, 1, 2, 3, 4, time.UTC)
	one, err := SubscriptionConsumerID("ab", "c", started)
	if err != nil {
		t.Fatal(err)
	}
	two, err := SubscriptionConsumerID("a", "bc", started)
	if err != nil {
		t.Fatal(err)
	}
	if one == two {
		t.Fatalf("ambiguous identities: %q", one)
	}
	later, _ := SubscriptionConsumerID("ab", "c", started.Add(time.Nanosecond))
	if later == one {
		t.Fatalf("subscription timestamp did not change identity")
	}
}
