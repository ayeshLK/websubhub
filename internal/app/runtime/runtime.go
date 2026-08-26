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

// Package runtime composes product modules into the two v0.5 processes.
package runtime

import (
	"context"
	"errors"
	"time"
)

const idlePollInterval = 100 * time.Millisecond

func wait(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func runtimeResult(parent context.Context, run context.Context, serverErr error) error {
	if parent.Err() != nil {
		return nil
	}
	if cause := context.Cause(run); cause != nil && !errors.Is(cause, context.Canceled) {
		return cause
	}
	if errors.Is(serverErr, context.Canceled) {
		return nil
	}
	return serverErr
}
