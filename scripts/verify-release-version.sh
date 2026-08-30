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

temporary=$(mktemp -d)
trap 'rm -rf "$temporary"' EXIT HUP INT TERM

write_properties() {
	printf 'version=%s\n' "$1" > "$temporary/release.properties"
}

assert_inspect() {
	property_version=$1
	previous=$2
	want=$3
	write_properties "$property_version"
	got=$(sh scripts/release-version.sh inspect "$temporary/release.properties" "$previous")
	if [ "$got" != "$want" ]; then
		printf 'inspection for %s after %s:\n%s\nwant:\n%s\n' "$property_version" "$previous" "$got" "$want" >&2
		exit 1
	fi
}

assert_rejected() {
	property_version=$1
	previous=$2
	write_properties "$property_version"
	if sh scripts/release-version.sh inspect "$temporary/release.properties" "$previous" >/dev/null 2>&1; then
		printf 'invalid release version was accepted: %s after %s\n' "$property_version" "$previous" >&2
		exit 1
	fi
}

assert_inspect 0.5.0-SNAPSHOT - 'state=snapshot
property-version=0.5.0-SNAPSHOT
version=0.5.0
tag=v0.5.0
previous-tag='
assert_inspect 0.6.0-SNAPSHOT v0.5.9 'state=snapshot
property-version=0.6.0-SNAPSHOT
version=0.6.0
tag=v0.6.0
previous-tag=v0.5.9'
assert_inspect 1.0.0 v0.9.8 'state=release
property-version=1.0.0
version=1.0.0
tag=v1.0.0
previous-tag=v0.9.8'

write_properties 0.5.0-SNAPSHOT
sh scripts/release-version.sh prepare "$temporary/release.properties" >/dev/null
test "$(sed -n 's/^version=//p' "$temporary/release.properties")" = 0.5.0
sh scripts/release-version.sh next-snapshot "$temporary/release.properties" >/dev/null
test "$(sed -n 's/^version=//p' "$temporary/release.properties")" = 0.5.1-SNAPSHOT

assert_rejected 0.5.0 v0.5.0
assert_rejected 0.4.9-SNAPSHOT v0.5.0
assert_rejected 0.5 v0.4.0
assert_rejected 00.5.0-SNAPSHOT -
assert_rejected 0.5.0-rc.1 -

printf 'version=0.5.0\nversion=0.5.1\n' > "$temporary/release.properties"
if sh scripts/release-version.sh inspect "$temporary/release.properties" - >/dev/null 2>&1; then
	echo "duplicate release property was accepted" >&2
	exit 1
fi

printf 'version=0.5.0\nchannel=preview\n' > "$temporary/release.properties"
if sh scripts/release-version.sh inspect "$temporary/release.properties" - >/dev/null 2>&1; then
	echo "unknown release property was accepted" >&2
	exit 1
fi

echo "release version lifecycle is valid"
