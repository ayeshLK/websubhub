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

package delivery

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ayeshLK/websubhub/internal/persistence/messagestore"
	"github.com/ayeshLK/websubhub/internal/state"
)

func TestPermanentConsumerConfigurationMarksSubscriptionStaleOnce(t *testing.T) {
	worker := testWorker(t, "http")
	worker.subscription.Parameters = map[string][]string{"kafka.topic_partitions": {"4"}}
	administrator := &rejectingAdministrator{}
	worker.deps.Administrator = administrator
	worker.deps.Wait = func(context.Context, time.Duration) error {
		t.Fatal("permanent configuration entered reconnect wait")
		return nil
	}

	err := worker.Run(t.Context())
	if !errors.Is(err, ErrSubscriptionStale) {
		t.Fatalf("run error=%v", err)
	}
	if administrator.spec.Subscription == nil ||
		administrator.spec.Subscription.Parameters["kafka.topic_partitions"][0] != "4" {
		t.Fatalf("consumer spec=%#v", administrator.spec)
	}
	events := worker.deps.Events.(*recordingEvents).events
	if len(events) != 1 {
		t.Fatalf("events=%#v", events)
	}
	stale, ok := events[0].(state.SubscriptionStaleEvent)
	if !ok || stale.Reason != "message_store_subscription_invalid" {
		t.Fatalf("stale event=%#v", events[0])
	}
}

type rejectingAdministrator struct {
	fakeAdministrator
	spec messagestore.ConsumerSpec
}

func (a *rejectingAdministrator) OpenConsumer(_ context.Context, spec messagestore.ConsumerSpec) (messagestore.Consumer, error) {
	a.spec = spec
	return nil, &messagestore.PermanentSubscriptionError{Reason: "private provider detail must not be persisted"}
}
