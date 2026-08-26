#!/bin/sh
# Copyright 2026 Ayesh Almeida
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
generated_dir="$script_dir/.generated"
repository_dir=$(CDPATH= cd -- "$script_dir/../.." && pwd)
mkdir -p "$generated_dir/bin"

(
  cd "$repository_dir"
  CGO_ENABLED=0 go build -trimpath -o "$generated_dir/bin/websubhub" ./cmd/websubhub
  CGO_ENABLED=0 go build -trimpath -o "$generated_dir/bin/websubhub-consolidator" ./cmd/websubhub-consolidator
  CGO_ENABLED=0 go build -trimpath -o "$generated_dir/bin/websubhub-fixture" ./test/fixture
)

if [ ! -s "$generated_dir/ca.crt" ]; then
  openssl req -x509 -newkey rsa:2048 -nodes -days 2 -subj '/CN=WebSubHub Compose CA' \
    -keyout "$generated_dir/ca.key" -out "$generated_dir/ca.crt"
fi
if [ ! -s "$generated_dir/fixture.crt" ]; then
  openssl req -newkey rsa:2048 -nodes -subj '/CN=fixture' \
    -keyout "$generated_dir/fixture.key" -out "$generated_dir/fixture.csr"
  printf '%s\n' 'subjectAltName=DNS:fixture,DNS:localhost' 'extendedKeyUsage=serverAuth' > "$generated_dir/fixture.ext"
  openssl x509 -req -days 2 -in "$generated_dir/fixture.csr" \
    -CA "$generated_dir/ca.crt" -CAkey "$generated_dir/ca.key" -CAcreateserial \
    -extfile "$generated_dir/fixture.ext" -out "$generated_dir/fixture.crt"
fi
if [ ! -s "$generated_dir/jwt.key" ]; then
  openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 -out "$generated_dir/jwt.key"
fi
if [ ! -s "$generated_dir/subscription.key" ]; then
  openssl rand -base64 32 > "$generated_dir/subscription.key"
fi
# These are two-day, ignored, Compose-only credentials mounted read-only into
# an unprivileged container whose UID is intentionally unrelated to the host.
chmod 644 "$generated_dir/fixture.key" "$generated_dir/jwt.key" "$generated_dir/subscription.key"
chmod 600 "$generated_dir/ca.key"
