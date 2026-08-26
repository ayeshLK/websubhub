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

package acceptance

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

const (
	hubA        = "http://localhost:8080/websub"
	hubB        = "http://localhost:8083/websub"
	operationsA = "http://localhost:9090"
	operationsB = "http://localhost:9091"
	fixture     = "http://localhost:8082"
	topicURL    = "https://publisher.example.test/compose-orders"
)

type subscription struct {
	Callback string `json:"callback"`
	Status   string `json:"status"`
}

type subscriptions struct {
	Revision      uint64         `json:"revision"`
	Subscriptions []subscription `json:"subscriptions"`
}

type receipt struct {
	Path        string `json:"path"`
	Count       int    `json:"count"`
	BodyBase64  string `json:"body_base64"`
	ContentType string `json:"content_type"`
	MessageID   string `json:"message_id"`
	Signature   string `json:"signature"`
}

func TestComposeResourceLifecycle(t *testing.T) {
	if os.Getenv("WEBSUBHUB_ACCEPTANCE_COMPOSE") != "1" {
		t.Skip("set WEBSUBHUB_ACCEPTANCE_COMPOSE=1 against the prepared Compose topology")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	client := &http.Client{Timeout: 10 * time.Second}
	token := composeToken(t, ctx)
	waitReady(t, ctx, client, operationsA)
	waitReady(t, ctx, client, operationsB)

	response := request(t, ctx, client, http.MethodGet, operationsA+"/v1/subscriptions", "", nil, "")
	if response.StatusCode != http.StatusUnauthorized {
		closeResponse(response)
		t.Fatalf("unauthenticated operations status = %d", response.StatusCode)
	}
	closeResponse(response)

	formStatus(t, ctx, client, token, hubA, http.StatusOK, url.Values{
		"hub.mode": {"register"}, "hub.topic": {topicURL}, "hub.content_type": {"application/json"},
	})
	waitSubscriptions(t, ctx, client, token, func(a, b subscriptions) bool { return a.Revision >= 1 && b.Revision >= 1 })

	controls := map[string]int{
		"/callback-success": http.StatusNoContent,
		"/callback-retry":   http.StatusServiceUnavailable,
		"/callback-dlq":     http.StatusBadRequest,
		"/callback-stale":   http.StatusUnprocessableEntity,
		"/callback-gone":    http.StatusGone,
	}
	for path, status := range controls {
		control(t, ctx, client, path, status)
		formStatus(t, ctx, client, token, hubA, http.StatusAccepted, url.Values{
			"hub.mode": {"subscribe"}, "hub.topic": {topicURL}, "hub.callback": {"http://fixture:8082" + path},
			"hub.verify": {"sync"}, "hub.lease_seconds": {"300"}, "hub.secret": {"compose-secret"},
		})
	}
	waitSubscriptions(t, ctx, client, token, func(a, b subscriptions) bool {
		return a.Revision >= 6 && b.Revision >= 6 && statusCount(a, "active") == 5 && statusCount(b, "active") == 5
	})

	payload := []byte(`{"order":"compose-1"}`)
	publish(t, ctx, client, token, hubA, payload)
	first := waitReceipts(t, ctx, client, func(receipts map[string]receipt) bool {
		return receipts["/callback-success"].Count == 1 && receipts["/callback-retry"].Count >= 2 &&
			receipts["/callback-dlq"].Count == 1 && receipts["/callback-stale"].Count == 1 && receipts["/callback-gone"].Count == 1
	})
	success := first["/callback-success"]
	if success.BodyBase64 != base64.StdEncoding.EncodeToString(payload) || success.ContentType != "application/json" ||
		!strings.HasPrefix(success.MessageID, "message-") || !strings.HasPrefix(success.Signature, "sha256=") {
		t.Fatalf("signed success receipt = %#v", success)
	}

	control(t, ctx, client, "/callback-retry", http.StatusNoContent)
	waitSubscriptions(t, ctx, client, token, func(a, b subscriptions) bool {
		return statusFor(a, "/callback-stale") == "stale" && statusFor(a, "/callback-gone") == "removed" &&
			statusFor(b, "/callback-stale") == "stale" && statusFor(b, "/callback-gone") == "removed"
	})
	waitDLQCount(t, ctx, client, token, 1)
	settled := waitReceipts(t, ctx, client, func(receipts map[string]receipt) bool {
		return receipts["/callback-retry"].Count > first["/callback-retry"].Count
	})

	restartHubA(t, ctx)
	waitReady(t, ctx, client, operationsA)
	publish(t, ctx, client, token, hubB, []byte(`{"order":"compose-2"}`))
	waitReceipts(t, ctx, client, func(receipts map[string]receipt) bool {
		return receipts["/callback-success"].Count >= 2 && receipts["/callback-retry"].Count > settled["/callback-retry"].Count &&
			receipts["/callback-dlq"].Count == 2 && receipts["/callback-stale"].Count == 1 && receipts["/callback-gone"].Count == 1
	})
	waitDLQCount(t, ctx, client, token, 2)
	waitSubscriptions(t, ctx, client, token, func(a, b subscriptions) bool { return a.Revision == b.Revision && a.Revision >= 8 })
}

func composeToken(t *testing.T, ctx context.Context) string {
	t.Helper()
	certificate, err := os.ReadFile("../../deploy/compose/.generated/ca.crt")
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(certificate) {
		t.Fatal("compose CA certificate is invalid")
	}
	client := &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}}}
	response := request(t, ctx, client, http.MethodGet, "https://localhost:8443/token", "", nil, "")
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("token status = %d", response.StatusCode)
	}
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}

func formStatus(t *testing.T, ctx context.Context, client *http.Client, token, endpoint string, want int, values url.Values) {
	t.Helper()
	response := request(t, ctx, client, http.MethodPost, endpoint, "application/x-www-form-urlencoded", strings.NewReader(values.Encode()), token)
	defer response.Body.Close()
	if response.StatusCode != want {
		body, _ := io.ReadAll(response.Body)
		t.Fatalf("form status = %d, want %d: %s", response.StatusCode, want, body)
	}
}

func publish(t *testing.T, ctx context.Context, client *http.Client, token, endpoint string, body []byte) {
	t.Helper()
	endpoint += "?hub.mode=publish&hub.topic=" + url.QueryEscape(topicURL)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("X-Go-Publisher", "publish")
	response, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusAccepted {
		message, _ := io.ReadAll(response.Body)
		t.Fatalf("publish status = %d: %s", response.StatusCode, message)
	}
}

func request(t *testing.T, ctx context.Context, client *http.Client, method, endpoint, contentType string, body io.Reader, token string) *http.Response {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, method, endpoint, body)
	if err != nil {
		t.Fatal(err)
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	response, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return response
}

func control(t *testing.T, ctx context.Context, client *http.Client, path string, status int) {
	t.Helper()
	endpoint := fmt.Sprintf("%s/control?path=%s&status=%d", fixture, url.QueryEscape(path), status)
	response := request(t, ctx, client, http.MethodPost, endpoint, "", nil, "")
	defer response.Body.Close()
	if response.StatusCode != http.StatusNoContent {
		t.Fatalf("control status = %d", response.StatusCode)
	}
}

func waitSubscriptions(t *testing.T, ctx context.Context, client *http.Client, token string, condition func(subscriptions, subscriptions) bool) (subscriptions, subscriptions) {
	t.Helper()
	var a, b subscriptions
	wait(t, ctx, func() bool {
		a = getSubscriptions(t, ctx, client, token, operationsA)
		b = getSubscriptions(t, ctx, client, token, operationsB)
		return condition(a, b)
	})
	return a, b
}

func getSubscriptions(t *testing.T, ctx context.Context, client *http.Client, token, endpoint string) subscriptions {
	t.Helper()
	response := request(t, ctx, client, http.MethodGet, endpoint+"/v1/subscriptions", "", nil, token)
	defer response.Body.Close()
	var result subscriptions
	if response.StatusCode != http.StatusOK || json.NewDecoder(response.Body).Decode(&result) != nil {
		return subscriptions{}
	}
	return result
}

func waitReceipts(t *testing.T, ctx context.Context, client *http.Client, condition func(map[string]receipt) bool) map[string]receipt {
	t.Helper()
	result := make(map[string]receipt)
	for {
		response := request(t, ctx, client, http.MethodGet, fixture+"/received", "", nil, "")
		var values []receipt
		decoded := json.NewDecoder(response.Body).Decode(&values)
		response.Body.Close()
		if response.StatusCode == http.StatusOK && decoded == nil {
			result = make(map[string]receipt, len(values))
			for _, value := range values {
				result[value.Path] = value
			}
			if condition(result) {
				return result
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("%v; last receipts: %#v", ctx.Err(), result)
		case <-time.After(200 * time.Millisecond):
		}
	}
}

func waitDLQCount(t *testing.T, ctx context.Context, client *http.Client, token string, want int) {
	t.Helper()
	wait(t, ctx, func() bool {
		response := request(t, ctx, client, http.MethodGet, operationsA+"/v1/dlq", "", nil, token)
		defer response.Body.Close()
		var result struct {
			Entries []json.RawMessage `json:"entries"`
		}
		return response.StatusCode == http.StatusOK && json.NewDecoder(response.Body).Decode(&result) == nil && len(result.Entries) == want
	})
}

func restartHubA(t *testing.T, ctx context.Context) {
	t.Helper()
	command := exec.CommandContext(ctx, "docker", "compose", "-f", "../../deploy/compose/compose.yaml", "restart", "hub-a")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("restart hub-a: %v: %s", err, output)
	}
}

func waitReady(t *testing.T, ctx context.Context, client *http.Client, endpoint string) {
	t.Helper()
	wait(t, ctx, func() bool {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint+"/health/ready", nil)
		if err != nil {
			return false
		}
		response, err := client.Do(req)
		if err != nil {
			return false
		}
		defer response.Body.Close()
		return response.StatusCode == http.StatusOK
	})
}

func wait(t *testing.T, ctx context.Context, condition func() bool) {
	t.Helper()
	for {
		if condition() {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(200 * time.Millisecond):
		}
	}
}

func statusCount(result subscriptions, status string) int {
	count := 0
	for _, item := range result.Subscriptions {
		if item.Status == status {
			count++
		}
	}
	return count
}

func statusFor(result subscriptions, path string) string {
	for _, item := range result.Subscriptions {
		if strings.HasSuffix(item.Callback, path) {
			return item.Status
		}
	}
	return ""
}

func closeResponse(response *http.Response) {
	_, _ = io.Copy(io.Discard, response.Body)
	_ = response.Body.Close()
}
