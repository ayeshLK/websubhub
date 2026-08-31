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

package runtime

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/ayeshLK/websubhub/internal/app/httpruntime"
	"github.com/ayeshLK/websubhub/internal/app/provider"
	"github.com/ayeshLK/websubhub/internal/config"
	"github.com/ayeshLK/websubhub/internal/consolidator"
	"github.com/ayeshLK/websubhub/internal/persistence/messagestore"
	storekafka "github.com/ayeshLK/websubhub/internal/persistence/messagestore/kafka"
	"github.com/ayeshLK/websubhub/internal/persistence/statestore"
	"github.com/ayeshLK/websubhub/internal/transport/mtls"
)

func RunConsolidator(ctx context.Context, cfg config.ConsolidatorConfig, logger *slog.Logger) (resultErr error) {
	if ctx == nil {
		return errors.New("context is required")
	}
	if logger == nil {
		return errors.New("logger is required")
	}
	if err := cfg.Validate(); err != nil {
		return err
	}
	logger.Info("runtime initialization started", "operation", "runtime_initializing", "provider", cfg.MessageStore.Provider)
	kafkaConfig, err := provider.Kafka(cfg.MessageStore)
	if err != nil {
		return err
	}
	producer, err := storekafka.NewProducer(kafkaConfig)
	if err != nil {
		return fmt.Errorf("open Kafka producer: %w", err)
	}
	administrator, err := storekafka.NewAdministrator(kafkaConfig)
	if err != nil {
		_ = producer.Close(context.Background())
		return fmt.Errorf("open Kafka administrator: %w", err)
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cfg.Server.ShutdownTimeout.Value())
		defer cancel()
		resultErr = errors.Join(resultErr, administrator.Close(closeCtx), producer.Close(closeCtx))
	}()

	store, err := statestore.New(producer, administrator, statestore.Options{
		EventsDestination: messagestore.Destination(cfg.State.Events.Destination), SnapshotsDestination: messagestore.Destination(cfg.State.Snapshots.Destination),
		EventsRetention: cfg.State.Events.Retention.Value(), SnapshotsRetention: cfg.State.Snapshots.Retention.Value(), SnapshotLoadBatch: cfg.State.Consumer.BatchSize,
	})
	if err != nil {
		return err
	}
	service, err := consolidator.New(store)
	if err != nil {
		return err
	}
	if err := service.Start(ctx); err != nil {
		return err
	}
	logger.Info("state service caught up", "operation", "state_service_ready", "revision", service.Snapshot().Revision)
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cfg.Server.ShutdownTimeout.Value())
		defer cancel()
		resultErr = errors.Join(resultErr, service.Close(closeCtx))
	}()

	server := httpruntime.NewServer(cfg.Server.Listen, service.Handler(), cfg.Server.ReadHeaderTimeout.Value(), cfg.Server.ReadTimeout.Value(), cfg.Server.WriteTimeout.Value(), cfg.Server.IdleTimeout.Value())
	serverTLS, err := mtls.Server(cfg.Server.Auth.Mode, cfg.Server.Auth.MTLS)
	if err != nil {
		return err
	}
	var listener httpruntime.Server = server
	if serverTLS != nil {
		listener, err = httpruntime.WithTLS(server, serverTLS)
		if err != nil {
			return err
		}
	}
	logger.Info("runtime initialized", "operation", "runtime_initialized", "provider", cfg.MessageStore.Provider)

	runCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(context.Canceled)
	go func() {
		if err := consumeConsolidator(runCtx, service, cfg.State.Consumer.BatchSize); err != nil && runCtx.Err() == nil {
			cancel(err)
		}
	}()
	serverErr := httpruntime.Run(runCtx, cfg.Server.ShutdownTimeout.Value(), listener)
	if runCtx.Err() == nil {
		cancel(serverErr)
	}
	return runtimeResult(ctx, runCtx, serverErr)
}

func consumeConsolidator(ctx context.Context, service *consolidator.Service, batchSize int) error {
	for {
		pollCtx, cancel := context.WithTimeout(ctx, idlePollInterval)
		caughtUp, err := service.Consume(pollCtx, batchSize)
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if errors.Is(err, context.DeadlineExceeded) {
				continue
			}
			return fmt.Errorf("consume state events: %w", err)
		}
		if caughtUp {
			if err := wait(ctx, idlePollInterval); err != nil {
				return err
			}
		}
	}
}
