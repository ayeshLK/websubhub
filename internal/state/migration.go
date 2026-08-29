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
	"encoding/json"
	"errors"
	"fmt"
)

const PreviousSchemaVersion uint16 = 1

// MigrateEventV1ToV2 transforms one exported v1 state event. Runtime decoders
// remain strict and do not invoke this function automatically.
func MigrateEventV1ToV2(data []byte) ([]byte, error) {
	var root map[string]json.RawMessage
	if err := decodeStrict(data, &root); err != nil {
		return nil, fmt.Errorf("decode v1 event: %w", err)
	}
	rawMeta, ok := root["meta"]
	if !ok {
		return nil, errors.New("v1 event metadata is required")
	}
	var meta EventMetadata
	if err := decodeStrict(rawMeta, &meta); err != nil {
		return nil, fmt.Errorf("decode v1 event metadata: %w", err)
	}
	if meta.SchemaVersion != PreviousSchemaVersion {
		return nil, fmt.Errorf("expected state event schema version %d, got %d", PreviousSchemaVersion, meta.SchemaVersion)
	}
	if err := rejectV2EventFields(root); err != nil {
		return nil, err
	}
	meta.SchemaVersion = SchemaVersion
	rawMeta, err := json.Marshal(meta)
	if err != nil {
		return nil, fmt.Errorf("encode v2 event metadata: %w", err)
	}
	root["meta"] = rawMeta
	migrated, err := json.Marshal(root)
	if err != nil {
		return nil, fmt.Errorf("encode v2 event: %w", err)
	}
	if _, err := DecodeEvent(migrated); err != nil {
		return nil, fmt.Errorf("validate migrated event: %w", err)
	}
	return migrated, nil
}

// MigrateSnapshotV1ToV2 transforms one exported v1 snapshot. Subscription
// parameters are initialized as absent. Runtime startup never calls this.
func MigrateSnapshotV1ToV2(data []byte) ([]byte, error) {
	var root map[string]json.RawMessage
	if err := decodeStrict(data, &root); err != nil {
		return nil, fmt.Errorf("decode v1 snapshot: %w", err)
	}
	rawVersion, ok := root["schema_version"]
	if !ok {
		return nil, errors.New("v1 snapshot schema version is required")
	}
	var version uint16
	if err := json.Unmarshal(rawVersion, &version); err != nil {
		return nil, fmt.Errorf("decode v1 snapshot schema version: %w", err)
	}
	if version != PreviousSchemaVersion {
		return nil, fmt.Errorf("expected snapshot schema version %d, got %d", PreviousSchemaVersion, version)
	}
	if err := rejectV2SnapshotFields(root); err != nil {
		return nil, err
	}
	root["schema_version"] = json.RawMessage("2")
	migrated, err := json.Marshal(root)
	if err != nil {
		return nil, fmt.Errorf("encode v2 snapshot: %w", err)
	}
	snapshot, err := DecodeSnapshot(migrated)
	if err != nil {
		return nil, fmt.Errorf("validate migrated snapshot: %w", err)
	}
	for id, subscription := range snapshot.Subscriptions {
		if len(subscription.Parameters) != 0 {
			return nil, fmt.Errorf("v1 subscription %q unexpectedly contains parameters", id)
		}
	}
	return migrated, nil
}

func rejectV2EventFields(root map[string]json.RawMessage) error {
	var eventType string
	if err := json.Unmarshal(root["type"], &eventType); err != nil || eventType != "subscription_verified" {
		return nil
	}
	var subscription map[string]json.RawMessage
	if err := json.Unmarshal(root["subscription"], &subscription); err != nil {
		return nil
	}
	if _, exists := subscription["parameters"]; exists {
		return errors.New("v1 subscription event contains version 2 parameters")
	}
	return nil
}

func rejectV2SnapshotFields(root map[string]json.RawMessage) error {
	var subscriptions []map[string]json.RawMessage
	if err := json.Unmarshal(root["subscriptions"], &subscriptions); err != nil {
		return nil
	}
	for _, subscription := range subscriptions {
		if _, exists := subscription["parameters"]; exists {
			return errors.New("v1 snapshot contains version 2 parameters")
		}
	}
	return nil
}
