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

package auth

import (
	"context"
	"net/http"
)

const UnauthenticatedActorID = "unauthenticated"

// None deliberately bypasses inbound authentication for a listener configured
// in none mode. Other security controls remain outside this boundary.
type None struct{}

func (None) Middleware(next http.Handler) http.Handler { return next }

func (None) Authorize(context.Context, string) (string, error) {
	return UnauthenticatedActorID, nil
}
