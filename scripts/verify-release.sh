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

dist_dir=${1:-dist}

fail() {
    echo "release verification: $*" >&2
    exit 1
}

command -v tar >/dev/null 2>&1 || fail "tar is required"
command -v unzip >/dev/null 2>&1 || fail "unzip is required"

cmp -s configs/websubhub-consolidator.example.toml \
    packaging/websubhub-consolidator/config/websubhub-consolidator.toml ||
    fail "packaged consolidator configuration differs from the repository template"

archive_count=$(find "$dist_dir" -maxdepth 1 -type f \
    \( -name 'websubhub_*.tar.gz' -o -name 'websubhub_*.zip' \
       -o -name 'websubhub-consolidator_*.tar.gz' \
       -o -name 'websubhub-consolidator_*.zip' \) | wc -l | tr -d ' ')
[ "$archive_count" = 12 ] || fail "expected 12 component archives, found $archive_count"

verify_archive() {
    component=$1
    os=$2
    arch=$3
    extension=tar.gz
    [ "$os" = windows ] && extension=zip

    set -- "$dist_dir"/"${component}"_*_"${os}"_"${arch}"."${extension}"
    [ "$#" = 1 ] && [ -f "$1" ] ||
        fail "expected one $component $os/$arch archive"
    archive=$1

    case "$archive" in
        *.zip) entries=$(unzip -Z1 "$archive") ;;
        *) entries=$(tar -tzf "$archive") ;;
    esac

    root=$(printf '%s\n' "$entries" | sed -n '1{s:/.*$::;p;}')
    [ -n "$root" ] || fail "$archive has no top-level directory"

    executable="$component"
    [ "$os" = windows ] && executable="${component}.exe"
    for required in \
        "$root/bin/$executable" \
        "$root/config/$component.toml" \
        "$root/README.md" \
        "$root/LICENSE" \
        "$root/NOTICE"
    do
        printf '%s\n' "$entries" | grep -Fx "$required" >/dev/null ||
            fail "$archive is missing $required"
    done

    packaged_config="packaging/$component/config/$component.toml"
    case "$archive" in
        *.zip)
            unzip -p "$archive" "$root/config/$component.toml" |
                cmp -s "$packaged_config" - ||
                fail "$archive contains an unexpected configuration"
            ;;
        *)
            tar -xOzf "$archive" "$root/config/$component.toml" |
                cmp -s "$packaged_config" - ||
                fail "$archive contains an unexpected configuration"
            ;;
    esac

    other=websubhub-consolidator
    [ "$component" = websubhub-consolidator ] && other=websubhub
    if printf '%s\n' "$entries" | grep -E "/bin/${other}(\.exe)?$" >/dev/null; then
        fail "$archive contains the other component binary"
    fi
}

for component in websubhub websubhub-consolidator; do
    for os in linux darwin windows; do
        for arch in amd64 arm64; do
            verify_archive "$component" "$os" "$arch"
        done
    done
done

echo "verified 12 independent component archives"
