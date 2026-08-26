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
	"log/slog"
	"strings"
	"testing"
)

func TestLoggerDropsSensitiveAndUnknownAttributes(t *testing.T) {
	var output bytes.Buffer
	logger := NewLogger(&output, slog.LevelDebug)
	logger.Info("delivery completed", "operation", "delivery", "status_class", "2xx", "authorization", "Bearer secret-token", "callback", "https://subscriber.example/?token=opaque", "payload", "customer-body", "provider_credentials", "password")
	text := output.String()
	if !strings.Contains(text, `"operation":"delivery"`) || !strings.Contains(text, `"status_class":"2xx"`) {
		t.Fatalf("safe attributes absent: %s", text)
	}
	for _, secret := range []string{"secret-token", "subscriber.example", "customer-body", "password", "authorization", "callback", "payload", "provider_credentials"} {
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
