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

root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
compose="$root/deploy/compose/compose.yaml"

cleanup() {
  docker compose -f "$compose" down -v --remove-orphans
}
trap cleanup EXIT INT TERM

cleanup
sh "$root/deploy/compose/prepare.sh"
docker compose -f "$compose" up -d --build --wait
cd "$root"
status=0
WEBSUBHUB_ACCEPTANCE_COMPOSE=1 go test -v ./test/acceptance || status=$?
if [ "$status" -ne 0 ]; then
  docker compose -f "$compose" ps --all
  docker compose -f "$compose" exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --all-groups --describe || true
  docker compose -f "$compose" logs --no-color --tail=300
fi
exit "$status"
