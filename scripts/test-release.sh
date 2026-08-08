#!/usr/bin/env bash
# Test driver for lib/release.sh. Plain bash assertions, no framework.
# Sourced by CI as `bash scripts/test-release.sh`.
set -u

SCRIPT_PATH="${BASH_SOURCE[0]}"
while [[ -L "${SCRIPT_PATH}" ]]; do
    SCRIPT_DIR="$(cd -P -- "$(dirname -- "${SCRIPT_PATH}")" && pwd)"
    SCRIPT_PATH="$(readlink -- "${SCRIPT_PATH}")"
    [[ "${SCRIPT_PATH}" == /* ]] || SCRIPT_PATH="${SCRIPT_DIR}/${SCRIPT_PATH}"
done
ROOT="$(cd -P -- "$(dirname -- "${SCRIPT_PATH}")/.." && pwd)"

# shellcheck source=../lib/release.sh
source "${ROOT}/lib/release.sh"

failures=0

assert_eq() {
    local actual="${1}" expected="${2}" label="${3}"
    if [[ "${actual}" == "${expected}" ]]; then
        printf 'ok   - %s\n' "${label}"
    else
        printf 'FAIL - %s: got %q, want %q\n' "${label}" "${actual}" "${expected}"
        failures=$((failures + 1))
    fi
}

assert_fails() {
    local label="${1}"
    shift
    if output="$("$@" 2>&1)"; then
        printf 'FAIL - %s: expected nonzero exit, got %q\n' "${label}" "${output}"
        failures=$((failures + 1))
    else
        printf 'ok   - %s\n' "${label}"
    fi
}

assert_eq "$(release_next_tag "" patch)" "v0.0.1" "no tags yet, patch bump"
assert_eq "$(release_next_tag v0.18.6 minor)" "v0.19.0" "mid-range minor bump"
assert_eq "$(release_next_tag v0.99.7 minor)" "v1.0.0" "minor carry into major (0.99 -> 1.0)"
assert_eq "$(release_next_tag v1.99.7 minor)" "v2.0.0" "minor carry into major (1.99 -> 2.0)"
assert_eq "$(release_next_tag v0.199.7 minor)" "v2.0.0" "mid-range carry within one major"
assert_eq "$(release_next_tag v1.2.9 patch)" "v1.2.10" "patch bump preserves major/minor"
assert_eq "$(release_next_tag v0.99.7 patch)" "v0.99.8" "patch bump does not trigger minor carry"
assert_eq "$(release_next_tag v0.99.7-beta minor)" "v1.0.0" "legacy -beta input parses and carries"

assert_fails "invalid tag rejected" release_next_tag "nope" minor
assert_fails "unsupported bump kind rejected" release_next_tag "v1.0.0" major

assert_eq "$(release_bump_kind_for_subject "feat(auto): add thing")" "minor" "feat classifies as minor"
assert_eq "$(release_bump_kind_for_subject "feat!: break api")" "minor" "feat! classifies as minor"
assert_eq "$(release_bump_kind_for_subject "fix(proxy): repair")" "patch" "fix classifies as patch"
assert_eq "$(release_bump_kind_for_subject "random text")" "patch" "non-conventional classifies as patch"

if (( failures > 0 )); then
    printf '%d assertion(s) failed\n' "${failures}" >&2
    exit 1
fi

printf 'all assertions passed\n'
