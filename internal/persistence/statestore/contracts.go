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

// Package statestore defines product state behavior implemented over a
// MessageStore. It must remain independent of provider packages.
package statestore

import (
	"context"

	"github.com/ayeshLK/websubhub/internal/persistence/messagestore"
	"github.com/ayeshLK/websubhub/internal/state"
)

type EventRecord struct {
	Event   state.Event
	Receipt messagestore.Receipt
}

type EventBatch struct {
	Records  []EventRecord
	CaughtUp bool
}

type EventConsumer interface {
	Receive(context.Context, int) (EventBatch, error)
	CaughtUp(context.Context) (bool, error)
	Ack(context.Context, messagestore.Receipt) error
	Close(context.Context, messagestore.ClosureIntent) error
}

type Store interface {
	Initialize(context.Context) error
	Append(context.Context, state.Event) error
	OpenEvents(context.Context, messagestore.ConsumerID, messagestore.StartPosition) (EventConsumer, error)
	LoadSnapshot(context.Context) (state.Snapshot, error)
	SaveSnapshot(context.Context, state.Snapshot) error
}
