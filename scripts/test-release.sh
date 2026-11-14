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

publish_workflow="$(sed -n '/^  publish:/,/^  smoke:/p' \
    "${ROOT}/.github/workflows/release.yml")"
if grep -Eq 'registry-url:|NPM_TOKEN|NODE_AUTH_TOKEN|_authToken' <<<"${publish_workflow}"; then
    printf 'FAIL - npm OIDC publish job avoids token authentication configuration\n'
    failures=$((failures + 1))
else
    printf 'ok   - npm OIDC publish job avoids token authentication configuration\n'
fi

smoke_workflow="$(sed -n '/^  smoke:/,$p' "${ROOT}/.github/workflows/release.yml")"
smoke_checkout_line="$(grep -nF 'uses: actions/checkout@v4' <<<"${smoke_workflow}" | sed -n '1p' | cut -d: -f1)"
smoke_ref_line="$(grep -nF 'ref: ${{ needs.tag.outputs.tag }}' <<<"${smoke_workflow}" | sed -n '1p' | cut -d: -f1)"
smoke_source_line="$(grep -nF 'source lib/release.sh' <<<"${smoke_workflow}" | sed -n '1p' | cut -d: -f1)"
smoke_wait_line="$(grep -nF 'release_npm_wait_for_versions "${version}" 300 10' <<<"${smoke_workflow}" | sed -n '1p' | cut -d: -f1)"
smoke_metadata_line="$(grep -nF 'npm view "@shukelabs/agent-quota-gateway@${version}" os --json' <<<"${smoke_workflow}" | sed -n '1p' | cut -d: -f1)"
smoke_install_line="$(grep -nF 'npm install -g --prefix' <<<"${smoke_workflow}" | sed -n '1p' | cut -d: -f1)"
if [[ -n "${smoke_checkout_line}" && -n "${smoke_ref_line}" &&
    -n "${smoke_source_line}" && -n "${smoke_wait_line}" &&
    -n "${smoke_metadata_line}" && -n "${smoke_install_line}" ]] &&
    (( smoke_checkout_line < smoke_source_line &&
        smoke_ref_line < smoke_source_line &&
        smoke_source_line < smoke_wait_line &&
        smoke_wait_line < smoke_metadata_line &&
        smoke_wait_line < smoke_install_line )); then
    printf 'ok   - release smoke checks out source before waiting for npm versions\n'
else
    printf 'FAIL - release smoke checkout/wait ordering is incomplete\n'
    failures=$((failures + 1))
fi

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

assert_eq "$(release_npm_platform_rows)" $'linux-x64|linux-x64|linux|x64|tar.gz|agent-quota-gateway\nlinux-arm64|linux-arm64|linux|arm64|tar.gz|agent-quota-gateway\ndarwin-arm64|darwin-arm64|darwin|arm64|tar.gz|agent-quota-gateway\ndarwin-x64|darwin-x64|darwin|x64|tar.gz|agent-quota-gateway' \
    "npm platform matrix is complete and ordered"

npm_wait_dir="$(mktemp -d)"
npm_wait_bin="${npm_wait_dir}/npm"
npm_wait_count="${npm_wait_dir}/count"
cat >"${npm_wait_bin}" <<'STUB'
#!/usr/bin/env bash
set -u

count_file="${NPM_STUB_COUNT_FILE:?}"
count="$(cat "${count_file}" 2>/dev/null || printf '0')"
count=$((count + 1))
printf '%s\n' "${count}" >"${count_file}"

if [[ "${NPM_STUB_MODE:-transient}" == slow ]]; then
    while :; do :; done
fi
if [[ "${NPM_STUB_MODE:-transient}" == always-fail || "${count}" -le 2 ]]; then
    printf 'npm error code E404\nNo match found for version\n' >&2
    exit 1
fi
printf '%s\n' "${NPM_STUB_VERSION:?}"
STUB
chmod +x "${npm_wait_bin}"

: >"${npm_wait_count}"
if PATH="${npm_wait_dir}:${PATH}" NPM_STUB_COUNT_FILE="${npm_wait_count}" \
    NPM_STUB_VERSION=1.2.3 release_npm_wait_for_versions 1.2.3 5 0 \
    >"${npm_wait_dir}/transient.log" 2>&1; then
    printf 'ok   - npm version wait recovers from transient 404s\n'
else
    printf 'FAIL - npm version wait recovers from transient 404s\n'
    failures=$((failures + 1))
fi

: >"${npm_wait_count}"
if PATH="${npm_wait_dir}:${PATH}" NPM_STUB_COUNT_FILE="${npm_wait_count}" \
    NPM_STUB_MODE=always-fail NPM_STUB_VERSION=1.2.3 \
    release_npm_wait_for_versions 1.2.3 1 1 \
    >"${npm_wait_dir}/timeout.log" 2>&1; then
    printf 'FAIL - npm version wait reports an exhausted deadline\n'
    failures=$((failures + 1))
elif grep -Fq '@shukelabs/agent-quota-gateway@1.2.3' "${npm_wait_dir}/timeout.log" && \
    grep -Fq 'E404' "${npm_wait_dir}/timeout.log"; then
    printf 'ok   - npm version wait reports an exhausted deadline\n'
else
    printf 'FAIL - npm version wait reports package and last npm error\n'
    failures=$((failures + 1))
fi

wait_start="${SECONDS}"
: >"${npm_wait_count}"
if PATH="${npm_wait_dir}:${PATH}" NPM_STUB_COUNT_FILE="${npm_wait_count}" \
    NPM_STUB_MODE=slow NPM_STUB_VERSION=1.2.3 \
    release_npm_wait_for_versions 1.2.3 1 1 \
    >"${npm_wait_dir}/slow.log" 2>&1; then
    printf 'FAIL - npm version wait caps an in-flight query\n'
    failures=$((failures + 1))
elif (( SECONDS - wait_start <= 3 )) && \
    grep -Fq '@shukelabs/agent-quota-gateway@1.2.3' "${npm_wait_dir}/slow.log"; then
    printf 'ok   - npm version wait caps an in-flight query\n'
else
    printf 'FAIL - npm version wait exceeds the in-flight deadline\n'
    failures=$((failures + 1))
fi

manifest_dir="$(mktemp -d)"
release_npm_write_root_manifest 1.2.3 >"${manifest_dir}/package.json"
if release_npm_validate_manifest "${manifest_dir}/package.json" \
    '@shukelabs/agent-quota-gateway' 1.2.3 root; then
    printf 'ok   - root npm manifest validates\n'
else
    printf 'FAIL - root npm manifest validates\n'
    failures=$((failures + 1))
fi

invalid_repository_manifest="${manifest_dir}/invalid-repository.json"
node - "${manifest_dir}/package.json" "${invalid_repository_manifest}" <<'NODE'
const fs = require('node:fs');
const [sourcePath, destinationPath] = process.argv.slice(2);
const manifest = JSON.parse(fs.readFileSync(sourcePath, 'utf8'));
manifest.repository.url = 'https://github.com/SHUKE-LABS/agent-quota-gateway.git';
fs.writeFileSync(destinationPath, `${JSON.stringify(manifest)}\n`);
NODE
assert_fails "npm manifest rejects a mismatched repository URL" \
    release_npm_validate_manifest "${invalid_repository_manifest}" \
    '@shukelabs/agent-quota-gateway' 1.2.3 root

repository_url='git+https://github.com/SHUKE-LABS/agent-quota-gateway.git'
release_archive_dir="$(mktemp -d)"
release_archive_source_dir="$(mktemp -d)"
release_stage_parent="$(mktemp -d)"
release_stage_dir="${release_stage_parent}/npm-packages"
release_pack_dir="$(mktemp -d)"
release_pack_manifest_dir="$(mktemp -d)"

while IFS='|' read -r _package_key target _npm_os _npm_cpu archive binary; do
    printf 'binary for %s\n' "${target}" >"${release_archive_source_dir}/${binary}"
    tar -czf "${release_archive_dir}/agent-quota-gateway-1.2.3-${target}.${archive}" \
        -C "${release_archive_source_dir}" "${binary}"
done < <(release_npm_platform_rows)

staging_pack_status=0
if release_npm_stage_packages 1.2.3 "${release_archive_dir}" "${release_stage_dir}"; then
    printf 'ok   - all staged npm manifests validate\n'
else
    printf 'FAIL - all staged npm manifests validate\n'
    failures=$((failures + 1))
    staging_pack_status=1
fi

if (( staging_pack_status == 0 )); then
    while IFS= read -r package_dir; do
        if ! (
            cd "${release_stage_dir}/${package_dir}" &&
            npm pack --ignore-scripts --pack-destination "${release_pack_dir}" >/dev/null
        ); then
            printf 'FAIL - npm pack succeeds for %s\n' "${package_dir}"
            failures=$((failures + 1))
            staging_pack_status=1
        fi
    done < <(release_npm_package_directories)
fi

if (( staging_pack_status == 0 )); then
    if release_npm_verify_tarballs 1.2.3 "${release_pack_dir}"; then
        printf 'ok   - npm pack creates the complete staged package set\n'
    else
        printf 'FAIL - npm pack creates the complete staged package set\n'
        failures=$((failures + 1))
        staging_pack_status=1
    fi
fi

if (( staging_pack_status == 0 )); then
    while IFS= read -r package_dir; do
        if [[ "${package_dir}" == agent-quota-gateway ]]; then
            tarball="$(release_npm_tarball_name 1.2.3 root)"
        else
            tarball="$(release_npm_tarball_name 1.2.3 "${package_dir#agent-quota-gateway-}")"
        fi
        packed_manifest="${release_pack_manifest_dir}/${package_dir}.json"
        if tar -xOzf "${release_pack_dir}/${tarball}" package/package.json >"${packed_manifest}" && \
            node - "${packed_manifest}" "${repository_url}" <<'NODE'
const fs = require('node:fs');

const [, , manifestPath, expectedRepositoryUrl] = process.argv;
const manifest = JSON.parse(fs.readFileSync(manifestPath, 'utf8'));
if (manifest.repository?.url !== expectedRepositoryUrl) {
  console.error(`${manifestPath}: repository.url does not match the expected GitHub repository`);
  process.exit(1);
}
NODE
        then
            printf 'ok   - packed %s manifest has the repository URL\n' "${package_dir}"
        else
            printf 'FAIL - packed %s manifest has the repository URL\n' "${package_dir}"
            failures=$((failures + 1))
        fi
    done < <(release_npm_package_directories)
fi

assert_fails "npm staging rejects missing target archive" \
    release_npm_stage_packages 1.2.3 "${manifest_dir}" "${manifest_dir}/staged"

unsupported_package_dir="$(mktemp -d)"
unsupported_prefix="$(mktemp -d)"
release_npm_write_root_manifest 1.2.3 >"${unsupported_package_dir}/package.json"
node - "${unsupported_package_dir}/package.json" <<'NODE'
const fs = require('node:fs');
const manifestPath = process.argv[2];
const manifest = JSON.parse(fs.readFileSync(manifestPath, 'utf8'));
manifest.os = ['freebsd'];
fs.writeFileSync(manifestPath, `${JSON.stringify(manifest)}\n`);
NODE
unsupported_log="${unsupported_prefix}/npm.log"
if npm install --offline --ignore-scripts --prefix "${unsupported_prefix}" \
    "${unsupported_package_dir}" >"${unsupported_log}" 2>&1; then
    printf 'FAIL - npm rejects unsupported platform metadata\n'
    failures=$((failures + 1))
elif grep -Eiq 'freebsd|EBADPLATFORM|not compatible' "${unsupported_log}"; then
    printf 'ok   - npm rejects unsupported platform metadata\n'
else
    printf 'FAIL - npm rejects unsupported platform metadata without naming platform\n'
    failures=$((failures + 1))
fi

publish_dir="$(mktemp -d)"
publish_log="${publish_dir}/publish.log"
PUBLISH_STUB_LOG="${publish_log}"
PUBLISH_STUB_FAIL_ON=""

publish_stub() {
    local tarball="${5:-}"
    printf '%s\n' "$(basename -- "${tarball}")" >>"${PUBLISH_STUB_LOG}"
    [[ -n "${PUBLISH_STUB_FAIL_ON}" && "$(basename -- "${tarball}")" == "${PUBLISH_STUB_FAIL_ON}" ]] && return 1
    return 0
}

make_publish_tarballs() {
    local package_dir="" package_key="" tarball=""
    while IFS= read -r package_dir; do
        package_key="${package_dir#agent-quota-gateway-}"
        if [[ "${package_dir}" == agent-quota-gateway ]]; then
            tarball="$(release_npm_tarball_name 1.2.3 root)"
        else
            tarball="$(release_npm_tarball_name 1.2.3 "${package_key}")"
        fi
        mkdir -p "${publish_dir}/package"
        printf '{}\n' >"${publish_dir}/package/package.json"
        printf 'binary\n' >"${publish_dir}/package/LICENSE"
        if [[ "${package_dir}" == agent-quota-gateway ]]; then
            mkdir -p "${publish_dir}/package/bin" "${publish_dir}/package/deploy"
            printf '#!/usr/bin/env node\n' >"${publish_dir}/package/bin/agent-quota-gateway.js"
            printf 'unit\n' >"${publish_dir}/package/deploy/agent-quota-gateway.service"
            printf 'env\n' >"${publish_dir}/package/deploy/aqg.env.example"
        else
            mkdir -p "${publish_dir}/package/bin"
            printf 'binary\n' >"${publish_dir}/package/bin/agent-quota-gateway"
        fi
        tar -czf "${publish_dir}/${tarball}" -C "${publish_dir}" package
        rm -rf "${publish_dir}/package"
    done < <(release_npm_package_directories)
}

make_publish_tarballs

checksum_path="${publish_dir}/SHA256SUMS"
if release_npm_write_checksums "${publish_dir}" "${checksum_path}" 1.2.3 && \
    (cd "${publish_dir}" && sha256sum --check "${checksum_path}"); then
    printf 'ok   - npm tarball checksums validate\n'
else
    printf 'FAIL - npm tarball checksums validate\n'
    failures=$((failures + 1))
fi

rm -f "${publish_dir}/$(release_npm_tarball_name 1.2.3 darwin-x64)"
: >"${publish_log}"
assert_fails "npm publish preflight makes no calls with a missing tarball" \
    release_npm_publish 1.2.3 "${publish_dir}" publish_stub
assert_eq "$(wc -c <"${publish_log}" | tr -d ' ')" "0" \
    "npm publish preflight has zero calls"

make_publish_tarballs
PUBLISH_STUB_FAIL_ON="$(release_npm_tarball_name 1.2.3 linux-x64)"
: >"${publish_log}"
assert_fails "linux-x64 publish failure stops immediately" \
    release_npm_publish 1.2.3 "${publish_dir}" publish_stub
assert_eq "$(wc -l <"${publish_log}" | tr -d ' ')" "1" \
    "linux-x64 is the first publish call"

PUBLISH_STUB_FAIL_ON="$(release_npm_tarball_name 1.2.3 darwin-arm64)"
: >"${publish_log}"
assert_fails "later npm publish failure reports partial order" \
    release_npm_publish 1.2.3 "${publish_dir}" publish_stub
assert_eq "$(sed -n '1p' "${publish_log}")" \
    "$(release_npm_tarball_name 1.2.3 linux-x64)" "publish order starts with linux-x64"
assert_eq "$(sed -n '3p' "${publish_log}")" \
    "$(release_npm_tarball_name 1.2.3 darwin-arm64)" "later failure is the third publish call"

PUBLISH_STUB_FAIL_ON=""
: >"${publish_log}"
if release_npm_publish 1.2.3 "${publish_dir}" publish_stub; then
    printf 'ok   - all npm packages publish in matrix order\n'
else
    printf 'FAIL - all npm packages publish in matrix order\n'
    failures=$((failures + 1))
fi

# The release workflow passes a relative dir; npm must still see a file path.
publish_path_log="${publish_dir}/publish-paths.log"
path_stub() {
    printf '%s\n' "${5:-}" >>"${publish_path_log}"
}
mkdir -p "${publish_dir}/rel/npm-tarballs"
mv "${publish_dir}"/*.tgz "${publish_dir}/rel/npm-tarballs/"
(cd "${publish_dir}/rel" && release_npm_publish 1.2.3 npm-tarballs path_stub)
assert_eq "$(grep -cv '^\./npm-tarballs/' "${publish_path_log}")" "0" \
    "relative tarball dir reaches npm as ./npm-tarballs/<file>"

rm -rf "${npm_wait_dir}" "${manifest_dir}" "${unsupported_package_dir}" "${unsupported_prefix}" "${publish_dir}" \
    "${release_archive_dir}" "${release_archive_source_dir}" "${release_stage_parent}" \
    "${release_pack_dir}" "${release_pack_manifest_dir}"

if (( failures > 0 )); then
    printf '%d assertion(s) failed\n' "${failures}" >&2
    exit 1
fi

printf 'all assertions passed\n'
