#!/usr/bin/env bash
set -euo pipefail

if [[ $# != 1 ]]; then
  printf 'Usage: bash %s NEW_SOURCE_DIRECTORY\n' "$0" >&2
  exit 2
fi
bundle=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)
destination=$1
if [[ -e "$destination" || -L "$destination" ]]; then
  printf 'Refusing to modify an existing destination: %s\n' "$destination" >&2
  exit 2
fi
for tool in git python3 sha256sum; do
  command -v "$tool" >/dev/null || { printf 'Missing tool: %s\n' "$tool" >&2; exit 2; }
done
(
  cd "$bundle"
  sha256sum -c SHA256SUMS
)
source_commit=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["source_commit"])' "$bundle/manifest.json")
git init -q -- "$destination"
git -C "$destination" config core.autocrlf false
git -C "$destination" remote add origin https://github.com/bazelbuild/reclient.git
git -C "$destination" fetch --depth=1 origin "$source_commit"
git -C "$destination" checkout --detach FETCH_HEAD
[[ $(git -C "$destination" rev-parse HEAD) == "$source_commit" ]]
git -C "$destination" apply --check "$bundle/patches/rbe-scandeps-lifecycle-minimal.patch"
git -C "$destination" apply "$bundle/patches/rbe-scandeps-lifecycle-minimal.patch"
git -C "$destination" apply --check "$bundle/patches/rbe-scandeps-lifecycle-tests.patch"
git -C "$destination" apply "$bundle/patches/rbe-scandeps-lifecycle-tests.patch"
cp -- "$bundle/tests/actual_scanner_test.go.txt" "$destination/internal/pkg/cppdependencyscanner/depsscannerclient/actual_scanner_test.go"
git -C "$destination" diff --check
printf 'Prepared 0.172 source at %s\n' "$destination"
printf 'No build, deployment, existing-process operation, or binary version stamping was performed.\n'
