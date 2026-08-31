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

package command

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"

	"github.com/ayeshLK/websubhub/internal/config"
)

func TestVersion(t *testing.T) {
	t.Parallel()

	var stdout, stderr bytes.Buffer
	if code := Run("websubhub", []string{"--version"}, &stdout, &stderr); code != 0 {
		t.Fatalf("Run() code = %d, stderr = %q", code, stderr.String())
	}
	if got := stdout.String(); !strings.Contains(got, "websubhub version=dev") {
		t.Fatalf("Run() stdout = %q", got)
	}
	if stderr.Len() != 0 {
		t.Fatalf("Run() stderr = %q", stderr.String())
	}
}

func TestRuntimeRequiresValidProcessConfiguration(t *testing.T) {
	t.Parallel()

	var stdout, stderr bytes.Buffer
	if code := RunContext(context.Background(), "websubhub", nil, nil, &stdout, &stderr); code != 1 {
		t.Fatalf("Run() code = %d, want 1", code)
	}
	if stdout.Len() != 0 {
		t.Fatalf("Run() stdout = %q", stdout.String())
	}
	record := decodeRecord(t, stderr.Bytes())
	if record["level"] != "ERROR" || record["operation"] != "configuration_load" || record["error_class"] != "configuration" {
		t.Fatalf("record = %#v", record)
	}
	if strings.Contains(stderr.String(), "server.id is required") {
		t.Fatalf("configuration detail leaked: %q", stderr.String())
	}
}

func TestAuthenticationWarnings(t *testing.T) {
	t.Parallel()

	cfg := config.HubDefaults()
	cfg.Server.Auth.Mode = config.AuthModeNone
	cfg.Operations.Auth.Mode = config.AuthModeJWT
	var output bytes.Buffer
	logger := configuredLogger(&output, "websubhub", config.LogLevelInfo)
	writeAuthenticationWarnings(logger, cfg)
	record := decodeRecord(t, output.Bytes())
	if record["level"] != "WARN" || record["operation"] != "authentication_disabled" || record["surface"] != "public" || record["auth_mode"] != "none" {
		t.Fatalf("record = %#v", record)
	}
}

func TestAuthenticationWarningsHonorConfiguredLevel(t *testing.T) {
	t.Parallel()

	cfg := config.HubDefaults()
	cfg.Server.Auth.Mode = config.AuthModeNone
	cfg.Operations.Auth.Mode = config.AuthModeNone
	var output bytes.Buffer
	writeAuthenticationWarnings(configuredLogger(&output, "websubhub", config.LogLevelError), cfg)
	if output.Len() != 0 {
		t.Fatalf("warnings passed error filter: %q", output.String())
	}
}

func TestUnknownComponentFailsClosed(t *testing.T) {
	t.Parallel()

	var stdout, stderr bytes.Buffer
	if code := RunContext(context.Background(), "unknown", nil, nil, &stdout, &stderr); code != 1 || decodeRecord(t, stderr.Bytes())["operation"] != "component_selection" {
		t.Fatalf("code=%d stderr=%q", code, stderr.String())
	}
}

func TestUnexpectedArgument(t *testing.T) {
	t.Parallel()

	var stdout, stderr bytes.Buffer
	if code := Run("websubhub", []string{"serve"}, &stdout, &stderr); code != 2 {
		t.Fatalf("Run() code = %d, want 2", code)
	}
}

func TestConfiguredLoggerUsesExactLevel(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	logger := configuredLogger(&output, "websubhub-consolidator", config.LogLevelWarn)
	logger.Info("not emitted", "operation", "test")
	logger.Warn("emitted", "operation", "test")
	record := decodeRecord(t, output.Bytes())
	if record["level"] != slog.LevelWarn.String() || record["component"] != "websubhub-consolidator" {
		t.Fatalf("record = %#v", record)
	}
}

func decodeRecord(t *testing.T, data []byte) map[string]any {
	t.Helper()
	var record map[string]any
	if err := json.Unmarshal(data, &record); err != nil {
		t.Fatalf("decode log %q: %v", data, err)
	}
	return record
}
