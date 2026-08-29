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

package main

import (
	"bytes"
	"strings"
	"testing"

	"github.com/ayeshLK/websubhub/internal/state"
)

func TestRunMigratesSnapshot(t *testing.T) {
	t.Parallel()
	input := strings.NewReader(`{"schema_version":1,"revision":0,"topics":[],"subscriptions":[]}`)
	var output bytes.Buffer
	if err := run([]string{"snapshot"}, input, &output); err != nil {
		t.Fatal(err)
	}
	snapshot, err := state.DecodeSnapshot(output.Bytes())
	if err != nil || snapshot.SchemaVersion != state.SchemaVersion {
		t.Fatalf("snapshot=%#v err=%v", snapshot, err)
	}
}

func TestRunRejectsInvalidModeAndOversize(t *testing.T) {
	t.Parallel()
	if err := run(nil, strings.NewReader("{}"), new(bytes.Buffer)); err == nil {
		t.Fatal("missing mode accepted")
	}
	if err := run([]string{"event"}, bytes.NewReader(make([]byte, maxRecordBytes+1)), new(bytes.Buffer)); err == nil {
		t.Fatal("oversized record accepted")
	}
}
