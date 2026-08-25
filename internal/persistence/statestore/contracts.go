// Package statestore defines product state behavior implemented over a
// MessageStore. It must remain independent of provider packages.
package statestore

import (
	"context"

	"github.com/ayeshLK/websubhub/internal/state"
)

type Store interface {
	Append(context.Context, state.Event) error
	LoadSnapshot(context.Context) (state.Snapshot, error)
	SaveSnapshot(context.Context, state.Snapshot) error
}
