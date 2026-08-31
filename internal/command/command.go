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
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	appRuntime "github.com/ayeshLK/websubhub/internal/app/runtime"
	"github.com/ayeshLK/websubhub/internal/buildinfo"
	"github.com/ayeshLK/websubhub/internal/config"
	"github.com/ayeshLK/websubhub/internal/observe"
)

func Run(component string, args []string, stdout, stderr io.Writer) int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return RunContext(ctx, component, args, os.Environ(), stdout, stderr)
}

func writeAuthenticationWarnings(logger *slog.Logger, cfg config.HubConfig) {
	if cfg.Server.Auth.Mode == config.AuthModeNone {
		logger.Warn("API authentication is disabled", "operation", "authentication_disabled", "surface", "public", "auth_mode", config.AuthModeNone)
	}
	if cfg.Operations.Auth.Mode == config.AuthModeNone {
		logger.Warn("API authentication is disabled", "operation", "authentication_disabled", "surface", "operations", "auth_mode", config.AuthModeNone)
	}
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
	bootstrapLogger := observe.NewLogger(stderr, slog.LevelInfo).With("component", component)
	switch component {
	case "websubhub":
		cfg, err := config.LoadHub(*configPath, environ)
		if err != nil {
			bootstrapLogger.Error("configuration rejected", "operation", "configuration_load", "error_class", "configuration")
			return 1
		}
		logger := configuredLogger(stderr, component, cfg.Logging.Level)
		writeAuthenticationWarnings(logger, cfg)
		logProcessStarting(logger, cfg.MessageStore.Provider)
		if err := appRuntime.RunHub(ctx, cfg, logger); err != nil {
			logger.Error("process failed", "operation", "process_failed", "error_class", "runtime")
			return 1
		}
		logger.Info("process stopped", "operation", "process_stopped")
	case "websubhub-consolidator":
		cfg, err := config.LoadConsolidator(*configPath, environ)
		if err != nil {
			bootstrapLogger.Error("configuration rejected", "operation", "configuration_load", "error_class", "configuration")
			return 1
		}
		logger := configuredLogger(stderr, component, cfg.Logging.Level)
		logProcessStarting(logger, cfg.MessageStore.Provider)
		if err := appRuntime.RunConsolidator(ctx, cfg, logger); err != nil {
			logger.Error("process failed", "operation", "process_failed", "error_class", "runtime")
			return 1
		}
		logger.Info("process stopped", "operation", "process_stopped")
	default:
		bootstrapLogger.Error("unknown component", "operation", "component_selection", "error_class", "configuration")
		return 1
	}
	return 0
}

func configuredLogger(destination io.Writer, component, levelName string) *slog.Logger {
	level, err := observe.Level(levelName)
	if err != nil {
		level = slog.LevelInfo
	}
	return observe.NewLogger(destination, level).With("component", component)
}

func logProcessStarting(logger *slog.Logger, provider string) {
	build := buildinfo.Current()
	logger.Info("process starting", "operation", "process_starting", "provider", provider, "version", build.Version, "commit", build.Commit, "build_date", build.Date)
}
