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

package httpruntime

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"
)

func TestRunShutsEveryServerDownOnCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	first, second := newFakeServer(), newFakeServer()
	done := make(chan error, 1)
	go func() { done <- Run(ctx, time.Second, first, second) }()
	<-first.started
	<-second.started
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("run error = %v", err)
	}
	if first.shutdownCalls() != 1 || second.shutdownCalls() != 1 {
		t.Fatalf("shutdown calls = %d, %d", first.shutdownCalls(), second.shutdownCalls())
	}
}

func TestNewServerAppliesEveryBound(t *testing.T) {
	server := NewServer(":8080", http.NotFoundHandler(), time.Second, 2*time.Second, 3*time.Second, 4*time.Second)
	if server.Addr != ":8080" || server.ReadHeaderTimeout != time.Second || server.ReadTimeout != 2*time.Second || server.WriteTimeout != 3*time.Second || server.IdleTimeout != 4*time.Second || server.MaxHeaderBytes != 64<<10 {
		t.Fatalf("server = %#v", server)
	}
}

func TestRunShutsPeersDownOnListenerFailure(t *testing.T) {
	failing, peer := newFakeServer(), newFakeServer()
	done := make(chan error, 1)
	go func() { done <- Run(context.Background(), time.Second, failing, peer) }()
	<-failing.started
	<-peer.started
	failing.finish(errors.New("bind failed"))
	if err := <-done; err == nil || !errors.Is(err, failing.err) {
		t.Fatalf("run error = %v", err)
	}
	if peer.shutdownCalls() != 1 {
		t.Fatalf("peer shutdown calls = %d", peer.shutdownCalls())
	}
}

type fakeServer struct {
	started   chan struct{}
	result    chan error
	err       error
	mu        sync.Mutex
	shutdowns int
	once      sync.Once
}

func newFakeServer() *fakeServer {
	return &fakeServer{started: make(chan struct{}), result: make(chan error, 1)}
}
func (s *fakeServer) ListenAndServe() error { close(s.started); return <-s.result }
func (s *fakeServer) Shutdown(context.Context) error {
	s.mu.Lock()
	s.shutdowns++
	s.mu.Unlock()
	s.once.Do(func() { s.result <- http.ErrServerClosed })
	return nil
}
func (s *fakeServer) finish(err error)   { s.err = err; s.once.Do(func() { s.result <- err }) }
func (s *fakeServer) shutdownCalls() int { s.mu.Lock(); defer s.mu.Unlock(); return s.shutdowns }
