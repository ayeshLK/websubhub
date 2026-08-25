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
