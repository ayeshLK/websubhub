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

package observe

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
)

func TestLevelIsStrict(t *testing.T) {
	t.Parallel()

	tests := map[string]slog.Level{"debug": slog.LevelDebug, "info": slog.LevelInfo, "warn": slog.LevelWarn, "error": slog.LevelError}
	for value, want := range tests {
		got, err := Level(value)
		if err != nil || got != want {
			t.Fatalf("Level(%q) = %v, %v; want %v", value, got, err, want)
		}
	}
	for _, value := range []string{"", "INFO", "warning", "off"} {
		if _, err := Level(value); err == nil {
			t.Fatalf("Level(%q) accepted", value)
		}
	}
}

func TestLoggerHonorsConfiguredLevelAndFixedContext(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	logger := NewLogger(&output, slog.LevelWarn).With("component", "websubhub")
	logger.Info("not emitted", "operation", "startup")
	logger.Warn("authentication disabled", "operation", "authentication_disabled", "surface", "public")

	var record map[string]any
	if err := json.Unmarshal(output.Bytes(), &record); err != nil {
		t.Fatal(err)
	}
	if record["level"] != "WARN" || record["component"] != "websubhub" || record["operation"] != "authentication_disabled" {
		t.Fatalf("record = %#v", record)
	}
	if strings.Contains(output.String(), "not emitted") {
		t.Fatalf("info record passed warn filter: %s", output.String())
	}
}

func TestLoggerDropsSensitiveAndUnknownAttributes(t *testing.T) {
	var output bytes.Buffer
	logger := NewLogger(&output, slog.LevelDebug)
	logger.Info("delivery completed", "operation", "delivery", "status_class", "2xx", "authorization", "Bearer secret-token", "callback", "https://subscriber.example/?token=opaque", "topic_id", "https://topic.example/?capability=opaque", "payload", "customer-body", "provider_credentials", "password")
	text := output.String()
	if !strings.Contains(text, `"operation":"delivery"`) || !strings.Contains(text, `"status_class":"2xx"`) {
		t.Fatalf("safe attributes absent: %s", text)
	}
	for _, secret := range []string{"secret-token", "subscriber.example", "topic.example", "customer-body", "password", "authorization", "callback", "topic_id", "payload", "provider_credentials"} {
		if strings.Contains(text, secret) {
			t.Fatalf("log disclosed %q: %s", secret, text)
		}
	}
}

func TestLoggerBoundsAndFlattensStringValues(t *testing.T) {
	var output bytes.Buffer
	logger := NewLogger(&output, slog.LevelInfo)
	logger.Info("safe", "error_class", strings.Repeat("x", maximumLogValueBytes+100)+"\nsecond")
	if strings.Contains(output.String(), "second") || !strings.Contains(output.String(), "…") {
		t.Fatalf("unbounded log = %s", output.String())
	}
}
