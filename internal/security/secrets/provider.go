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

// Package secrets protects persisted subscription secrets.
package secrets

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"strings"
)

const keyBytes = 32

type Provider struct {
	keyID string
	aead  cipher.AEAD
}

// OpenFile loads one raw or base64-encoded 256-bit key. Key rotation can later
// replace this provider without changing the persisted ciphertext/key-ID shape.
func OpenFile(path, keyID string) (*Provider, error) {
	if path == "" || keyID == "" {
		return nil, errors.New("secret key file and key ID are required")
	}
	body, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read subscription secret key: %w", err)
	}
	key, err := decodeKey(body)
	clear(body)
	if err != nil {
		return nil, err
	}
	block, err := aes.NewCipher(key)
	clear(key)
	if err != nil {
		return nil, errors.New("initialize subscription secret encryption")
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, errors.New("initialize subscription secret authentication")
	}
	return &Provider{keyID: keyID, aead: aead}, nil
}

func decodeKey(body []byte) ([]byte, error) {
	if len(body) == keyBytes {
		return append([]byte(nil), body...), nil
	}
	decoded, err := base64.StdEncoding.DecodeString(strings.TrimSpace(string(body)))
	if err != nil || len(decoded) != keyBytes {
		clear(decoded)
		return nil, errors.New("subscription secret key must be exactly 32 raw bytes or their standard base64 encoding")
	}
	return decoded, nil
}

func (p *Provider) Seal(ctx context.Context, plaintext []byte) ([]byte, string, error) {
	if err := ctx.Err(); err != nil {
		return nil, "", err
	}
	if p == nil || p.aead == nil || p.keyID == "" {
		return nil, "", errors.New("subscription secret provider is unavailable")
	}
	nonce := make([]byte, p.aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, "", errors.New("generate subscription secret nonce")
	}
	ciphertext := p.aead.Seal(nonce, nonce, plaintext, []byte(p.keyID))
	return ciphertext, p.keyID, nil
}

func (p *Provider) Open(ctx context.Context, ciphertext []byte, keyID string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if p == nil || p.aead == nil || keyID == "" || keyID != p.keyID || len(ciphertext) < p.aead.NonceSize()+p.aead.Overhead() {
		return nil, errors.New("open subscription secret")
	}
	nonce, body := ciphertext[:p.aead.NonceSize()], ciphertext[p.aead.NonceSize():]
	plaintext, err := p.aead.Open(nil, nonce, body, []byte(keyID))
	if err != nil {
		return nil, errors.New("open subscription secret")
	}
	return plaintext, nil
}
