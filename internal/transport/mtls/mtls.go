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

// Package mtls loads strict mutual-TLS configurations for internal traffic.
package mtls

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"

	"github.com/ayeshLK/websubhub/internal/config"
)

func Server(mode string, files config.MTLSServerFiles) (*tls.Config, error) {
	if mode == "none" {
		return nil, nil
	}
	if mode != "mtls" {
		return nil, fmt.Errorf("unsupported internal authentication mode %q", mode)
	}
	certificate, err := tls.LoadX509KeyPair(files.CertificateFile, files.PrivateKeyFile)
	if err != nil {
		return nil, fmt.Errorf("load internal server identity: %w", err)
	}
	clientCAs, err := loadPool(files.ClientCAFile)
	if err != nil {
		return nil, fmt.Errorf("load internal client CA: %w", err)
	}
	return &tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{certificate},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    clientCAs,
	}, nil
}

func Client(mode string, files config.MTLSClientFiles) (*tls.Config, error) {
	if mode == "none" {
		return nil, nil
	}
	if mode != "mtls" {
		return nil, fmt.Errorf("unsupported internal authentication mode %q", mode)
	}
	certificate, err := tls.LoadX509KeyPair(files.CertificateFile, files.PrivateKeyFile)
	if err != nil {
		return nil, fmt.Errorf("load internal client identity: %w", err)
	}
	roots, err := loadPool(files.ServerCAFile)
	if err != nil {
		return nil, fmt.Errorf("load internal server CA: %w", err)
	}
	return &tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{certificate},
		RootCAs:      roots,
		ServerName:   files.ServerName,
	}, nil
}

func loadPool(path string) (*x509.CertPool, error) {
	if path == "" {
		return nil, errors.New("CA file is required")
	}
	pem, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, errors.New("CA file contains no certificates")
	}
	return pool, nil
}
