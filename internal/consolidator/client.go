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

package consolidator

import (
	"context"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"strings"

	"github.com/ayeshLK/websubhub/internal/state"
)

const defaultMaxSnapshotBytes int64 = 16 << 20

type Client struct {
	endpoint string
	client   *http.Client
	maxBytes int64
}

func NewClient(endpoint string, client *http.Client, maxBytes int64) (*Client, error) {
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return nil, errors.New("consolidator endpoint must be an absolute HTTP URL")
	}
	if client == nil {
		return nil, errors.New("HTTP client is required")
	}
	if maxBytes == 0 {
		maxBytes = defaultMaxSnapshotBytes
	}
	if maxBytes < 1 {
		return nil, errors.New("snapshot response limit must be positive")
	}
	return &Client{endpoint: strings.TrimRight(endpoint, "/"), client: client, maxBytes: maxBytes}, nil
}

func (c *Client) Ready(ctx context.Context) error {
	response, err := c.get(ctx, "/health/ready")
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusNoContent {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
		return fmt.Errorf("consolidator readiness returned HTTP %d", response.StatusCode)
	}
	return nil
}

func (c *Client) Snapshot(ctx context.Context) (state.Snapshot, error) {
	response, err := c.get(ctx, SnapshotPath)
	if err != nil {
		return state.Snapshot{}, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
		return state.Snapshot{}, fmt.Errorf("consolidator snapshot returned HTTP %d", response.StatusCode)
	}
	mediaType, _, err := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/vnd.websubhub.state-snapshot+json" {
		return state.Snapshot{}, fmt.Errorf("unexpected snapshot content type %q", response.Header.Get("Content-Type"))
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, c.maxBytes+1))
	if err != nil {
		return state.Snapshot{}, fmt.Errorf("read consolidator snapshot: %w", err)
	}
	if int64(len(body)) > c.maxBytes {
		return state.Snapshot{}, errors.New("consolidator snapshot exceeds configured limit")
	}
	snapshot, err := state.DecodeSnapshot(body)
	if err != nil {
		return state.Snapshot{}, fmt.Errorf("decode consolidator snapshot: %w", err)
	}
	return snapshot, nil
}

func (c *Client) get(ctx context.Context, path string) (*http.Response, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, c.endpoint+path, nil)
	if err != nil {
		return nil, err
	}
	response, err := c.client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("request consolidator: %w", err)
	}
	return response, nil
}

var _ interface {
	Ready(context.Context) error
	Snapshot(context.Context) (state.Snapshot, error)
} = (*Client)(nil)
