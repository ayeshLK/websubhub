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
