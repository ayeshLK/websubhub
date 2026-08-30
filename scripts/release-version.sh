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

mode=${1:-inspect}
properties=${2:-release.properties}
strict_version='^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$'
strict_tag='^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$'

read_property_version() {
	awk '
		/^[[:space:]]*#/ || /^[[:space:]]*$/ { next }
		/^version=/ {
			if (found) {
				print "release properties contains duplicate version entries" > "/dev/stderr"
				bad = 1
				exit
			}
			version = substr($0, length("version=") + 1)
			found = 1
			next
		}
		{
			print "release properties contains an unknown entry: " $0 > "/dev/stderr"
			bad = 1
			exit
		}
		END {
			if (bad) exit 1
			if (!found) {
				print "release properties does not define version" > "/dev/stderr"
				exit 1
			}
			print version
		}
	' "$properties"
}

property_version=$(read_property_version)
case "$property_version" in
	*-SNAPSHOT)
		state=snapshot
		version=${property_version%-SNAPSHOT}
		;;
	*)
		state=release
		version=$property_version
		;;
esac

if ! printf '%s\n' "$version" | grep -Eq "$strict_version"; then
	echo "release version must use strict X.Y.Z or X.Y.Z-SNAPSHOT syntax" >&2
	exit 1
fi

major=${version%%.*}
remainder=${version#*.}
minor=${remainder%%.*}
patch=${remainder#*.}

write_property_version() {
	replacement=$1
	temporary=${properties}.tmp.$$
	trap 'rm -f "$temporary"' EXIT HUP INT TERM
	awk -v replacement="$replacement" '
		/^version=/ { print "version=" replacement; next }
		{ print }
	' "$properties" > "$temporary"
	mv "$temporary" "$properties"
	trap - EXIT HUP INT TERM
}

case "$mode" in
	prepare)
		if [ "$state" != snapshot ]; then
			echo "only a snapshot version can be prepared for release" >&2
			exit 1
		fi
		write_property_version "$version"
		printf 'version=%s\n' "$version"
		printf 'tag=v%s\n' "$version"
		exit 0
		;;
	next-snapshot)
		if [ "$state" != release ]; then
			echo "only a released version can advance to the next snapshot" >&2
			exit 1
		fi
		next_version=${major}.${minor}.$((patch + 1))-SNAPSHOT
		write_property_version "$next_version"
		printf 'property-version=%s\n' "$next_version"
		printf 'version=%s\n' "${next_version%-SNAPSHOT}"
		exit 0
		;;
	inspect) ;;
	*)
		echo "release version mode must be inspect, prepare, or next-snapshot" >&2
		exit 1
		;;
esac

if [ "$#" -ge 3 ]; then
	if [ "$3" = "-" ]; then
		latest=
	else
		latest=$3
	fi
else
	latest=
	for candidate in $(git tag --list 'v*' --sort=-version:refname); do
		if printf '%s\n' "$candidate" | grep -Eq "$strict_tag"; then
			latest=$candidate
			break
		fi
	done
fi

if [ -n "$latest" ]; then
	if ! printf '%s\n' "$latest" | grep -Eq "$strict_tag"; then
		echo "previous release tag must use strict vX.Y.Z syntax" >&2
		exit 1
	fi
	previous_version=${latest#v}
	previous_major=${previous_version%%.*}
	previous_remainder=${previous_version#*.}
	previous_minor=${previous_remainder%%.*}
	previous_patch=${previous_remainder#*.}

	if [ "$major" -lt "$previous_major" ] ||
		{ [ "$major" -eq "$previous_major" ] && [ "$minor" -lt "$previous_minor" ]; } ||
		{ [ "$major" -eq "$previous_major" ] && [ "$minor" -eq "$previous_minor" ] && [ "$patch" -le "$previous_patch" ]; }; then
		echo "declared version $version must be newer than $previous_version" >&2
		exit 1
	fi
fi

printf 'state=%s\n' "$state"
printf 'property-version=%s\n' "$property_version"
printf 'version=%s\n' "$version"
printf 'tag=v%s\n' "$version"
printf 'previous-tag=%s\n' "$latest"
