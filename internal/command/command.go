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

// Package command provides the process command surface and runtime startup.
package command

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	appRuntime "github.com/ayeshLK/websubhub/internal/app/runtime"
	"github.com/ayeshLK/websubhub/internal/buildinfo"
	"github.com/ayeshLK/websubhub/internal/config"
)

func Run(component string, args []string, stdout, stderr io.Writer) int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return RunContext(ctx, component, args, os.Environ(), stdout, stderr)
}

func RunContext(ctx context.Context, component string, args, environ []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet(component, flag.ContinueOnError)
	flags.SetOutput(stderr)
	showVersion := flags.Bool("version", false, "print build identity and exit")
	configPath := flags.String("config", "", "path to the process-specific TOML configuration")
	flags.Usage = func() {
		fmt.Fprintf(flags.Output(), "Usage: %s [--config path] [--version]\n", component)
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
	var err error
	switch component {
	case "websubhub":
		var cfg config.HubConfig
		cfg, err = config.LoadHub(*configPath, environ)
		if err == nil {
			err = appRuntime.RunHub(ctx, cfg)
		}
	case "websubhub-consolidator":
		var cfg config.ConsolidatorConfig
		cfg, err = config.LoadConsolidator(*configPath, environ)
		if err == nil {
			err = appRuntime.RunConsolidator(ctx, cfg)
		}
	default:
		err = fmt.Errorf("unknown component %q", component)
	}
	if err != nil {
		fmt.Fprintf(stderr, "%s: %v\n", component, err)
		return 1
	}
	return 0
}
