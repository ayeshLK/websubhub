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

package state

import (
	"bytes"
	"testing"
)

func TestMigrateEventV1ToV2(t *testing.T) {
	t.Parallel()
	v2, err := EncodeEvent(subscriptionEvent())
	if err != nil {
		t.Fatal(err)
	}
	v1 := bytes.Replace(v2, []byte(`"schema_version":2`), []byte(`"schema_version":1`), 1)
	withParameters := bytes.Replace(v1, []byte(`"consumer_id":"consumer-1"`), []byte(`"consumer_id":"consumer-1","parameters":{}`), 1)
	if _, err := MigrateEventV1ToV2(withParameters); err == nil {
		t.Fatal("v1 event containing v2 parameters migrated")
	}
	if _, err := DecodeEvent(v1); err == nil {
		t.Fatal("runtime accepted v1 event")
	}
	migrated, err := MigrateEventV1ToV2(v1)
	if err != nil {
		t.Fatal(err)
	}
	event, err := DecodeEvent(migrated)
	if err != nil {
		t.Fatal(err)
	}
	subscription := event.(SubscriptionVerified).Subscription
	if len(subscription.Parameters) != 0 || event.Metadata().SchemaVersion != SchemaVersion {
		t.Fatalf("migrated event=%#v", event)
	}
	if _, err := MigrateEventV1ToV2(migrated); err == nil {
		t.Fatal("v2 event accepted as v1 migration input")
	}
}

func TestMigrateSnapshotV1ToV2(t *testing.T) {
	t.Parallel()
	reducer := Reducer{}
	snapshot, _, err := reducer.Apply(EmptySnapshot(), topicEvent())
	if err != nil {
		t.Fatal(err)
	}
	snapshot, _, err = reducer.Apply(snapshot, subscriptionEvent())
	if err != nil {
		t.Fatal(err)
	}
	v2, err := EncodeSnapshot(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	v1 := bytes.Replace(v2, []byte(`"schema_version":2`), []byte(`"schema_version":1`), 1)
	withParameters := bytes.Replace(v1, []byte(`"consumer_id":"consumer-1"`), []byte(`"consumer_id":"consumer-1","parameters":{}`), 1)
	if _, err := MigrateSnapshotV1ToV2(withParameters); err == nil {
		t.Fatal("v1 snapshot containing v2 parameters migrated")
	}
	if _, err := DecodeSnapshot(v1); err == nil {
		t.Fatal("runtime accepted v1 snapshot")
	}
	migrated, err := MigrateSnapshotV1ToV2(v1)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeSnapshot(migrated)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.SchemaVersion != SchemaVersion || len(decoded.Subscriptions["sub-1"].Parameters) != 0 {
		t.Fatalf("migrated snapshot=%#v", decoded)
	}
	if _, err := MigrateSnapshotV1ToV2(migrated); err == nil {
		t.Fatal("v2 snapshot accepted as v1 migration input")
	}
}

func TestMigrationRejectsUnknownPersistedFields(t *testing.T) {
	t.Parallel()
	_, err := MigrateSnapshotV1ToV2([]byte(`{"schema_version":1,"revision":0,"topics":[],"subscriptions":[],"provider_offset":4}`))
	if err == nil {
		t.Fatal("unknown v1 snapshot field migrated")
	}
}
