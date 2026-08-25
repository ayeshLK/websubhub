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
