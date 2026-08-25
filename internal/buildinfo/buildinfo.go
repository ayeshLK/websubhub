// Package buildinfo exposes immutable build identity injected by the release
// build. Development builds use explicit sentinel values.
package buildinfo

import "fmt"

var (
	version = "dev"
	commit  = "unknown"
	date    = "unknown"
)

// Info describes one product build.
type Info struct {
	Version string
	Commit  string
	Date    string
}

// Current returns the build identity compiled into the current binary.
func Current() Info {
	return Info{
		Version: version,
		Commit:  commit,
		Date:    date,
	}
}

// String returns a stable, human-readable build identity.
func (i Info) String() string {
	return fmt.Sprintf("version=%s commit=%s date=%s", i.Version, i.Commit, i.Date)
}
