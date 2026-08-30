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

func TestSubscriptionOptionsAreBoundedAndDetached(t *testing.T) {
	t.Parallel()
	source := map[string][]string{"custom": {"value"}}
	options, err := NewSubscriptionOptions(source)
	if err != nil {
		t.Fatal(err)
	}
	source["custom"][0] = "mutated"
	if options.Parameters["custom"][0] != "value" {
		t.Fatalf("options aliased input: %#v", options)
	}
	clone := options.Clone()
	clone.Parameters["custom"][0] = "clone"
	if options.Parameters["custom"][0] != "value" {
		t.Fatalf("clone aliased options: %#v", options)
	}

	_, err = NewSubscriptionOptions(map[string][]string{"custom": {strings.Repeat("x", MaxSubscriptionOptionValueBytes+1)}})
	reason, permanent := PermanentSubscriptionReason(err)
	if !permanent || reason == "" || strings.Contains(err.Error(), reason) {
		t.Fatalf("error=%v reason=%q permanent=%v", err, reason, permanent)
	}
}

func TestOversizedPermanentReasonRemainsClassifiedWithoutDisclosure(t *testing.T) {
	t.Parallel()
	private := strings.Repeat("x", 300)
	reason, permanent := PermanentSubscriptionReason(&PermanentSubscriptionError{Reason: private})
	if !permanent || reason != "" {
		t.Fatalf("reason=%q permanent=%v", reason, permanent)
	}
}
