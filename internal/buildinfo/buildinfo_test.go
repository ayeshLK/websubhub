package buildinfo

import "testing"

func TestDevelopmentBuildIdentity(t *testing.T) {
	t.Parallel()

	got := Current()
	if got.Version == "" || got.Commit == "" || got.Date == "" {
		t.Fatalf("Current() returned an empty field: %#v", got)
	}
	if got.String() != "version=dev commit=unknown date=unknown" {
		t.Fatalf("Current().String() = %q", got.String())
	}
}
