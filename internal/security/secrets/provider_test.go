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

package secrets

import (
	"bytes"
	"context"
	"encoding/base64"
	"os"
	"path/filepath"
	"testing"
)

func TestProviderRoundTripAndRandomNonce(t *testing.T) {
	provider := testProvider(t)
	plaintext := []byte("never persist this value")
	first, keyID, err := provider.Seal(context.Background(), plaintext)
	if err != nil {
		t.Fatal(err)
	}
	second, _, err := provider.Seal(context.Background(), plaintext)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(first, second) || bytes.Contains(first, plaintext) || keyID != "key-1" {
		t.Fatalf("ciphertexts equal=%v key ID=%q", bytes.Equal(first, second), keyID)
	}
	opened, err := provider.Open(context.Background(), first, keyID)
	if err != nil || !bytes.Equal(opened, plaintext) {
		t.Fatalf("opened=%q err=%v", opened, err)
	}
}

func TestProviderRejectsTamperingAndWrongKeyID(t *testing.T) {
	provider := testProvider(t)
	ciphertext, keyID, err := provider.Seal(context.Background(), []byte("secret"))
	if err != nil {
		t.Fatal(err)
	}
	ciphertext[len(ciphertext)-1] ^= 1
	if _, err := provider.Open(context.Background(), ciphertext, keyID); err == nil {
		t.Fatal("tampered ciphertext accepted")
	}
	if _, err := provider.Open(context.Background(), ciphertext, "wrong-key"); err == nil {
		t.Fatal("wrong key ID accepted")
	}
}

func TestOpenFileRejectsInvalidKey(t *testing.T) {
	path := filepath.Join(t.TempDir(), "key")
	if err := os.WriteFile(path, []byte("short"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenFile(path, "key-1"); err == nil {
		t.Fatal("short key accepted")
	}
}

func testProvider(t *testing.T) *Provider {
	t.Helper()
	path := filepath.Join(t.TempDir(), "key")
	key := bytes.Repeat([]byte{0x42}, keyBytes)
	if err := os.WriteFile(path, []byte(base64.StdEncoding.EncodeToString(key)+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	provider, err := OpenFile(path, "key-1")
	if err != nil {
		t.Fatal(err)
	}
	return provider
}
