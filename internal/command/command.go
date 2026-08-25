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

// Package command provides the small command surface shared by product
// binaries before their independent runtime composition roots are assembled.
package command

import (
	"flag"
	"fmt"
	"io"

	"github.com/ayeshLK/websubhub/internal/buildinfo"
)

// Run handles the common help and version flags for a component. Runtime
// startup is deliberately unavailable until its dependencies are implemented.
func Run(component string, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet(component, flag.ContinueOnError)
	flags.SetOutput(stderr)
	showVersion := flags.Bool("version", false, "print build identity and exit")
	flags.Usage = func() {
		fmt.Fprintf(flags.Output(), "Usage: %s [--version]\n", component)
		flags.PrintDefaults()
	}

	if err := flags.Parse(args); err != nil {
		return 2
	}
	if flags.NArg() != 0 {
		fmt.Fprintf(stderr, "%s: unexpected arguments: %v\n", component, flags.Args())
		return 2
	}
	if *showVersion {
		fmt.Fprintf(stdout, "%s %s\n", component, buildinfo.Current())
		return 0
	}

	fmt.Fprintf(stderr, "%s: runtime assembly is not implemented yet\n", component)
	return 1
}
