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

// Package httpruntime coordinates bounded HTTP listener shutdown.
package httpruntime

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"
)

type Server interface {
	ListenAndServe() error
	Shutdown(context.Context) error
}

func NewServer(address string, handler http.Handler, readHeaderTimeout, readTimeout, writeTimeout, idleTimeout time.Duration) *http.Server {
	return &http.Server{
		Addr: address, Handler: handler,
		ReadHeaderTimeout: readHeaderTimeout, ReadTimeout: readTimeout,
		WriteTimeout: writeTimeout, IdleTimeout: idleTimeout,
		MaxHeaderBytes: 64 << 10,
	}
}

// WithTLS adapts an HTTP server to perform its TLS handshake at the listener.
// Certificate paths are deliberately empty because the configured certificates
// are already loaded into TLSConfig.
func WithTLS(server *http.Server, tlsConfig *tls.Config) (Server, error) {
	if server == nil || tlsConfig == nil {
		return nil, errors.New("HTTP server and TLS configuration are required")
	}
	server.TLSConfig = tlsConfig.Clone()
	return tlsServer{Server: server}, nil
}

type tlsServer struct{ *http.Server }

func (s tlsServer) ListenAndServe() error { return s.Server.ListenAndServeTLS("", "") }

func Run(ctx context.Context, shutdownTimeout time.Duration, servers ...Server) error {
	if ctx == nil || shutdownTimeout <= 0 || len(servers) == 0 {
		return errors.New("context, positive shutdown timeout, and servers are required")
	}
	errorsChannel := make(chan error, len(servers))
	for _, server := range servers {
		if server == nil {
			return errors.New("HTTP server is required")
		}
		go func(server Server) {
			err := server.ListenAndServe()
			if err == nil {
				err = errors.New("HTTP server stopped unexpectedly")
			}
			if !errors.Is(err, http.ErrServerClosed) {
				errorsChannel <- err
				return
			}
			errorsChannel <- nil
		}(server)
	}
	var cause error
	select {
	case <-ctx.Done():
		cause = ctx.Err()
	case cause = <-errorsChannel:
		if cause == nil {
			cause = errors.New("HTTP server closed unexpectedly")
		}
	}
	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownTimeout)
	defer cancel()
	var wait sync.WaitGroup
	shutdownErrors := make(chan error, len(servers))
	for _, server := range servers {
		wait.Add(1)
		go func(server Server) {
			defer wait.Done()
			if err := server.Shutdown(shutdownCtx); err != nil {
				shutdownErrors <- err
			}
		}(server)
	}
	wait.Wait()
	close(shutdownErrors)
	for err := range shutdownErrors {
		cause = errors.Join(cause, fmt.Errorf("shutdown HTTP server: %w", err))
	}
	return cause
}
