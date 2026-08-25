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
	"strings"
	"testing"
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

func TestRuntimeIsExplicitlyUnavailable(t *testing.T) {
	t.Parallel()

	var stdout, stderr bytes.Buffer
	if code := Run("websubhub", nil, &stdout, &stderr); code != 1 {
		t.Fatalf("Run() code = %d, want 1", code)
	}
	if stdout.Len() != 0 {
		t.Fatalf("Run() stdout = %q", stdout.String())
	}
	if got := stderr.String(); !strings.Contains(got, "not implemented") {
		t.Fatalf("Run() stderr = %q", got)
	}
}

func TestUnexpectedArgument(t *testing.T) {
	t.Parallel()

	var stdout, stderr bytes.Buffer
	if code := Run("websubhub", []string{"serve"}, &stdout, &stderr); code != 2 {
		t.Fatalf("Run() code = %d, want 2", code)
	}
}
