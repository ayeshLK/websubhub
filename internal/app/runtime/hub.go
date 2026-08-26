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
	"net/http"
	"net/url"

	"github.com/ayeshLK/websubhub/internal/admin"
	"github.com/ayeshLK/websubhub/internal/app/httpruntime"
	"github.com/ayeshLK/websubhub/internal/app/hubstate"
	"github.com/ayeshLK/websubhub/internal/app/provider"
	"github.com/ayeshLK/websubhub/internal/app/resourcehub"
	"github.com/ayeshLK/websubhub/internal/config"
	"github.com/ayeshLK/websubhub/internal/consolidator"
	"github.com/ayeshLK/websubhub/internal/delivery"
	"github.com/ayeshLK/websubhub/internal/ingest"
	"github.com/ayeshLK/websubhub/internal/persistence/messagestore"
	storekafka "github.com/ayeshLK/websubhub/internal/persistence/messagestore/kafka"
	"github.com/ayeshLK/websubhub/internal/persistence/statestore"
	"github.com/ayeshLK/websubhub/internal/security/auth"
	"github.com/ayeshLK/websubhub/internal/security/callback"
	"github.com/ayeshLK/websubhub/internal/security/secrets"
	"github.com/ayeshLK/websubhub/internal/state"
	"github.com/ayeshLK/websubhub/internal/transport/mtls"
)

func RunHub(ctx context.Context, cfg config.HubConfig) (resultErr error) {
	if ctx == nil {
		return errors.New("context is required")
	}
	if err := cfg.Validate(); err != nil {
		return err
	}
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

	stateOptions := statestore.DefaultOptions()
	stateOptions.EventsDestination = messagestore.Destination(cfg.State.Events.Destination)
	store, err := statestore.New(producer, administrator, stateOptions)
	if err != nil {
		return err
	}
	internalTLS, err := mtls.Client(cfg.Consolidator.Auth.Mode, cfg.Consolidator.Auth.MTLS)
	if err != nil {
		return err
	}
	internalClient := &http.Client{
		Timeout:       cfg.Consolidator.Timeout.Value(),
		Transport:     &http.Transport{Proxy: nil, TLSClientConfig: internalTLS},
		CheckRedirect: func(*http.Request, []*http.Request) error { return errors.New("consolidator redirects are forbidden") },
	}
	snapshotSource, err := consolidator.NewClient(cfg.Consolidator.Endpoint, internalClient, 0)
	if err != nil {
		return err
	}
	projection, err := hubstate.New(store, snapshotSource, cfg.Server.ID, cfg.State.Startup.BufferMax)
	if err != nil {
		return err
	}
	if err := projection.Start(ctx); err != nil {
		return err
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cfg.Server.ShutdownTimeout.Value())
		defer cancel()
		resultErr = errors.Join(resultErr, projection.Close(closeCtx))
	}()

	callbackPolicy, callbackClient, err := callback.FromConfig(cfg.Security.Callbacks, nil)
	if err != nil {
		return err
	}
	secretProvider, err := secrets.OpenFile(cfg.Security.Secrets.KeyFile, cfg.Security.Secrets.KeyID)
	if err != nil {
		return err
	}
	keySource := auth.NewRemoteKeySource(cfg.Security.JWT, nil)
	verifier, err := auth.New(cfg.Security.JWT, keySource)
	if err != nil {
		return err
	}
	ingestor, err := ingest.New(producer, projection, nil)
	if err != nil {
		return err
	}
	protocol, err := resourcehub.New(cfg, resourcehub.Dependencies{
		Events: store, Projection: projection, Secrets: secretProvider, Callbacks: callbackPolicy,
		Authorization: verifier, Content: ingestor, VerificationClient: callbackClient,
	})
	if err != nil {
		return err
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cfg.Server.ShutdownTimeout.Value())
		defer cancel()
		resultErr = errors.Join(resultErr, protocol.Close(closeCtx))
	}()

	runCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(context.Canceled)
	attempts := delivery.LibraryAttemptFactory{HubURL: cfg.Server.PublicURL, HTTPClient: callbackClient, Timeout: cfg.Delivery.RequestTimeout.Value(), MaxResponseBody: cfg.Delivery.MaxResponseBody}
	factory := delivery.Factory{Config: cfg.Delivery, Dependencies: delivery.Dependencies{Administrator: administrator, Events: store, Secrets: secretProvider, Attempts: attempts}}
	manager, err := delivery.NewManager(runCtx, cfg.Server.ID, factory)
	if err != nil {
		return err
	}
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.WithoutCancel(ctx), cfg.Server.ShutdownTimeout.Value())
		defer closeCancel()
		resultErr = errors.Join(resultErr, manager.Close(closeCtx))
	}()
	if err := reconcileHub(ctx, administrator, manager, projection.Snapshot(), messagestore.Destination(cfg.Delivery.DLQDestination)); err != nil {
		return err
	}

	dlq, err := admin.NewMessageStoreDLQInspector(administrator, messagestore.Destination(cfg.Delivery.DLQDestination))
	if err != nil {
		return err
	}
	operations, err := admin.New(admin.Dependencies{Authentication: verifier, Readiness: projection, Projection: projection, Capabilities: administrator, DLQ: dlq})
	if err != nil {
		return err
	}
	publicHandler, err := publicMux(cfg.Server.PublicURL, protocol)
	if err != nil {
		return err
	}
	publicServer := httpruntime.NewServer(cfg.Server.Listen, publicHandler, cfg.Server.ReadHeaderTimeout.Value(), cfg.Server.ReadTimeout.Value(), cfg.Server.WriteTimeout.Value(), cfg.Server.IdleTimeout.Value())
	operationsServer := httpruntime.NewServer(cfg.Operations.Listen, operations, cfg.Operations.ReadHeaderTimeout.Value(), cfg.Operations.ReadTimeout.Value(), cfg.Operations.WriteTimeout.Value(), cfg.Operations.IdleTimeout.Value())

	go func() {
		if err := consumeHub(runCtx, projection, manager, administrator, cfg); err != nil && runCtx.Err() == nil {
			cancel(err)
		}
	}()
	shutdownTimeout := cfg.Server.ShutdownTimeout.Value()
	if cfg.Operations.ShutdownTimeout.Value() > shutdownTimeout {
		shutdownTimeout = cfg.Operations.ShutdownTimeout.Value()
	}
	serverErr := httpruntime.Run(runCtx, shutdownTimeout, publicServer, operationsServer)
	if runCtx.Err() == nil {
		cancel(serverErr)
	}
	return runtimeResult(ctx, runCtx, serverErr)
}

func publicMux(publicURL string, handler http.Handler) (http.Handler, error) {
	parsed, err := url.Parse(publicURL)
	if err != nil || parsed.Path == "" {
		return nil, errors.New("public hub URL requires a path")
	}
	mux := http.NewServeMux()
	mux.Handle(parsed.Path, handler)
	return mux, nil
}

func consumeHub(ctx context.Context, projection *hubstate.Projection, manager *delivery.Manager, administrator messagestore.Administrator, cfg config.HubConfig) error {
	lastRevision := projection.Snapshot().Revision
	for {
		caughtUp, err := projection.Consume(ctx, cfg.State.Events.ConsumerBatchSize)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("consume hub state events: %w", err)
		}
		snapshot := projection.Snapshot()
		if snapshot.Revision != lastRevision {
			if err := reconcileHub(ctx, administrator, manager, snapshot, messagestore.Destination(cfg.Delivery.DLQDestination)); err != nil {
				return err
			}
			lastRevision = snapshot.Revision
		}
		if caughtUp {
			if err := wait(ctx, idlePollInterval); err != nil {
				return err
			}
		}
	}
}

func reconcileHub(ctx context.Context, administrator messagestore.Administrator, manager *delivery.Manager, snapshot state.Snapshot, dlq messagestore.Destination) error {
	if err := ensureHubDestinations(ctx, administrator, snapshot, dlq); err != nil {
		return err
	}
	if err := manager.Reconcile(snapshot); err != nil {
		return fmt.Errorf("reconcile delivery workers: %w", err)
	}
	return nil
}

func ensureHubDestinations(ctx context.Context, administrator messagestore.Administrator, snapshot state.Snapshot, dlq messagestore.Destination) error {
	if err := administrator.EnsureDestination(ctx, messagestore.DestinationSpec{Name: dlq, Partitions: 1}); err != nil {
		return fmt.Errorf("ensure delivery DLQ destination: %w", err)
	}
	for _, topic := range snapshot.Topics {
		if topic.Status != state.TopicActive {
			continue
		}
		if err := administrator.EnsureDestination(ctx, messagestore.DestinationSpec{Name: messagestore.Destination(topic.ContentDestination), Partitions: 1}); err != nil {
			return fmt.Errorf("ensure content destination for topic %q: %w", topic.ID, err)
		}
	}
	return nil
}
