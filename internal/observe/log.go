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

// Package observe provides low-cardinality, secret-safe telemetry primitives.
package observe

import (
	"io"
	"log/slog"
	"strings"
)

const maximumLogValueBytes = 256

var allowedLogAttributes = map[string]struct{}{
	"component": {}, "operation": {}, "status_class": {}, "failure_class": {},
	"provider": {}, "event_id": {}, "message_id": {}, "subscription_id": {},
	"topic_id": {}, "revision": {}, "attempt": {}, "duration_ms": {}, "error_class": {},
}

func NewLogger(destination io.Writer, level slog.Leveler) *slog.Logger {
	handler := slog.NewJSONHandler(destination, &slog.HandlerOptions{Level: level, ReplaceAttr: replaceAttribute})
	return slog.New(handler)
}

func replaceAttribute(groups []string, attribute slog.Attr) slog.Attr {
	if len(groups) == 0 {
		switch attribute.Key {
		case slog.TimeKey, slog.LevelKey, slog.MessageKey, slog.SourceKey:
			return truncateAttribute(attribute)
		}
	}
	if _, ok := allowedLogAttributes[attribute.Key]; !ok {
		return slog.Attr{}
	}
	return truncateAttribute(attribute)
}

func truncateAttribute(attribute slog.Attr) slog.Attr {
	value := attribute.Value.Resolve()
	if value.Kind() != slog.KindString {
		return attribute
	}
	text := value.String()
	if len(text) > maximumLogValueBytes {
		text = text[:maximumLogValueBytes] + "…"
	}
	text = strings.ReplaceAll(text, "\n", " ")
	text = strings.ReplaceAll(text, "\r", " ")
	return slog.String(attribute.Key, text)
}
