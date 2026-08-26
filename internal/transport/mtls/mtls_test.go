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

package mtls

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/ayeshLK/websubhub/internal/config"
)

func TestNoneReturnsNoTLSConfiguration(t *testing.T) {
	server, err := Server("none", config.MTLSServerFiles{})
	if err != nil || server != nil {
		t.Fatalf("server = %#v, %v", server, err)
	}
	client, err := Client("none", config.MTLSClientFiles{})
	if err != nil || client != nil {
		t.Fatalf("client = %#v, %v", client, err)
	}
}

func TestMTLSRejectsInvalidMaterial(t *testing.T) {
	path := filepath.Join(t.TempDir(), "invalid.pem")
	if err := os.WriteFile(path, []byte("not a certificate"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Server("mtls", config.MTLSServerFiles{CertificateFile: path, PrivateKeyFile: path, ClientCAFile: path}); err == nil {
		t.Fatal("invalid server material accepted")
	}
	if _, err := Client("mtls", config.MTLSClientFiles{CertificateFile: path, PrivateKeyFile: path, ServerCAFile: path}); err == nil {
		t.Fatal("invalid client material accepted")
	}
}
