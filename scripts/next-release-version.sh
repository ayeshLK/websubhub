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

bump=${1:-}
case "$bump" in
	patch | minor | major) ;;
	*)
		echo "release bump must be patch, minor, or major" >&2
		exit 1
		;;
esac

strict_tag='^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$'

if [ "$#" -ge 2 ]; then
	if [ "$2" = "-" ]; then
		latest=
	else
		latest=$2
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

if [ -z "$latest" ]; then
	tag=v0.5.0
	previous=
else
	if ! printf '%s\n' "$latest" | grep -Eq "$strict_tag"; then
		echo "previous release tag must use strict vX.Y.Z syntax" >&2
		exit 1
	fi

	version=${latest#v}
	major=${version%%.*}
	remainder=${version#*.}
	minor=${remainder%%.*}
	patch=${remainder#*.}

	case "$bump" in
		patch) patch=$((patch + 1)) ;;
		minor)
			minor=$((minor + 1))
			patch=0
			;;
		major)
			major=$((major + 1))
			minor=0
			patch=0
			;;
	esac
	tag=v${major}.${minor}.${patch}
	previous=$latest
fi

printf 'tag=%s\n' "$tag"
printf 'version=%s\n' "${tag#v}"
printf 'previous-tag=%s\n' "$previous"
