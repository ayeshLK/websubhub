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
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ayeshLK/websubhub/internal/config"
)

func TestMutualTLSVerifiesBothPeers(t *testing.T) {
	dir := t.TempDir()
	caCert, caKey, caPEM := certificateAuthority(t)
	caFile := writePEM(t, dir, "ca.pem", caPEM)
	serverCert, serverKey := issuedIdentity(t, caCert, caKey, "consolidator.internal", x509.ExtKeyUsageServerAuth)
	clientCert, clientKey := issuedIdentity(t, caCert, caKey, "hub.internal", x509.ExtKeyUsageClientAuth)

	serverTLS, err := Server("mtls", config.MTLSServerFiles{
		CertificateFile: writePEM(t, dir, "server.crt", serverCert),
		PrivateKeyFile:  writePEM(t, dir, "server.key", serverKey),
		ClientCAFile:    caFile,
	})
	if err != nil {
		t.Fatal(err)
	}
	clientTLS, err := Client("mtls", config.MTLSClientFiles{
		CertificateFile: writePEM(t, dir, "client.crt", clientCert),
		PrivateKeyFile:  writePEM(t, dir, "client.key", clientKey),
		ServerCAFile:    caFile,
		ServerName:      "consolidator.internal",
	})
	if err != nil {
		t.Fatal(err)
	}

	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if len(request.TLS.PeerCertificates) != 1 {
			t.Errorf("peer certificates = %d", len(request.TLS.PeerCertificates))
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	server.TLS = serverTLS
	server.StartTLS()
	defer server.Close()

	client := server.Client()
	client.Transport.(*http.Transport).TLSClientConfig = clientTLS
	response, err := client.Get(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusNoContent {
		t.Fatalf("status = %d", response.StatusCode)
	}

	anonymous := server.Client()
	anonymous.Transport = anonymous.Transport.(*http.Transport).Clone()
	anonymous.Transport.(*http.Transport).TLSClientConfig = clientTLS.Clone()
	anonymous.Transport.(*http.Transport).TLSClientConfig.Certificates = nil
	if _, err := anonymous.Get(server.URL); err == nil {
		t.Fatal("server accepted a client without a certificate")
	}
}

func certificateAuthority(t *testing.T) (*x509.Certificate, *ecdsa.PrivateKey, []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	certificate := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test CA"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, certificate, certificate, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return certificate, key, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

func issuedIdentity(t *testing.T, ca *x509.Certificate, caKey *ecdsa.PrivateKey, name string, usage x509.ExtKeyUsage) ([]byte, []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	certificate := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()), Subject: pkix.Name{CommonName: name},
		DNSNames: []string{name}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{usage},
	}
	der, err := x509.CreateCertificate(rand.Reader, certificate, ca, &key.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
}

func writePEM(t *testing.T, dir, name string, data []byte) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}
