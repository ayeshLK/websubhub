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

assert_version() {
	bump=$1
	previous=$2
	want=$3
	got=$(sh scripts/next-release-version.sh "$bump" "$previous")
	if [ "$got" != "$want" ]; then
		printf 'version calculation for %s from %s:\n%s\nwant:\n%s\n' "$bump" "$previous" "$got" "$want" >&2
		exit 1
	fi
}

assert_version patch - 'tag=v0.5.0
version=0.5.0
previous-tag='
assert_version patch v0.5.0 'tag=v0.5.1
version=0.5.1
previous-tag=v0.5.0'
assert_version minor v0.5.9 'tag=v0.6.0
version=0.6.0
previous-tag=v0.5.9'
assert_version major v0.9.8 'tag=v1.0.0
version=1.0.0
previous-tag=v0.9.8'

if sh scripts/next-release-version.sh automatic v0.5.0 >/dev/null 2>&1; then
	echo "invalid release bump was accepted" >&2
	exit 1
fi
if sh scripts/next-release-version.sh patch v0.5.0-rc.1 >/dev/null 2>&1; then
	echo "prerelease tag was accepted as the stable release base" >&2
	exit 1
fi

echo "release version calculation is valid"
