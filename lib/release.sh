# Release helpers for tags and changelog generation.
#
# release_tag_regex intentionally matches both the legacy `-beta` suffix
# and the bare form: pre-existing v*-beta tags must still be found by
# "latest tag" lookups so version numbering continues unbroken, even
# though release_next_tag only ever produces the bare form going forward.
release_tag_regex() {
    printf '^v([0-9]+)\\.([0-9]+)\\.([0-9]+)(-beta)?$'
}


release_tags_desc() {
    local tag="" regex=""
    regex="$(release_tag_regex)"

    while IFS= read -r tag; do
        [[ "${tag}" =~ ${regex} ]] || continue
        printf '%s\n' "${tag}"
    done < <(git tag --list 'v*' --sort=-version:refname)
}


release_latest_tag() {
    local tag=""

    while IFS= read -r tag; do
        [[ -n "${tag}" ]] || continue
        printf '%s\n' "${tag}"
        return 0
    done < <(release_tags_desc)
}


release_head_tag() {
    local tag="" regex=""
    regex="$(release_tag_regex)"

    while IFS= read -r tag; do
        [[ "${tag}" =~ ${regex} ]] || continue
        printf '%s\n' "${tag}"
        return 0
    done < <(git tag --points-at HEAD --list 'v*' --sort=-version:refname)
}


release_head_subject() {
    git log -1 --format=%s HEAD
}


release_commit_type_for_subject() {
    local subject="${1:-}" type="" regex='^([[:alpha:]]+)(\([^)]+\))?(!)?:[[:space:]]*.+$'

    if [[ "${subject}" =~ ${regex} ]]; then
        type="${BASH_REMATCH[1],,}"
        printf '%s\n' "${type}"
        return 0
    fi

    printf 'other\n'
}


release_bump_kind_for_subject() {
    local subject="${1:-}" type=""
    type="$(release_commit_type_for_subject "${subject}")"

    case "${type}" in
        feat)
            printf 'minor\n'
            ;;
        *)
            printf 'patch\n'
            ;;
    esac
}


release_next_tag() {
    local latest_tag="${1:-}" bump_kind="${2:-patch}" major=0 minor=0 patch=0 regex=""
    regex="$(release_tag_regex)"

    if [[ -n "${latest_tag}" ]]; then
        [[ "${latest_tag}" =~ ${regex} ]] || {
            printf "release_next_tag: invalid tag '%s'\n" "${latest_tag}" >&2
            return 1
        }
        major="${BASH_REMATCH[1]}"
        minor="${BASH_REMATCH[2]}"
        patch="${BASH_REMATCH[3]}"
    fi

    case "${bump_kind}" in
        minor)
            # Feature-count versioning: one major covers 100 minor bumps, so
            # major * 100 + minor is the cumulative feature count. The carry
            # divides the *incremented* minor, so the 100th feat moves
            # v0.99.x -> v1.0.0 and v1.99.x -> v2.0.0. Major is a mechanical
            # counter, not a breaking-change declaration.
            minor=$((minor + 1))
            major=$((major + minor / 100))
            minor=$((minor % 100))
            patch=0
            ;;
        patch)
            patch=$((patch + 1))
            ;;
        *)
            printf "release_next_tag: unsupported bump kind '%s'\n" "${bump_kind}" >&2
            return 1
            ;;
    esac

    printf 'v%s.%s.%s\n' "${major}" "${minor}" "${patch}"
}


release_create_tag() {
    local current_tag="" latest_tag="" subject="" bump_kind="" next_tag=""

    current_tag="$(release_head_tag || true)"
    if [[ -n "${current_tag}" ]]; then
        printf '%s\n' "${current_tag}"
        return 0
    fi

    latest_tag="$(release_latest_tag || true)"
    subject="$(release_head_subject)"
    bump_kind="$(release_bump_kind_for_subject "${subject}")"
    next_tag="$(release_next_tag "${latest_tag}" "${bump_kind}")"

    git tag "${next_tag}" HEAD
    printf '%s\n' "${next_tag}"
}


release_changelog_bucket_for_subject() {
    local subject="${1:-}" type=""
    type="$(release_commit_type_for_subject "${subject}")"

    case "${type}" in
        feat) printf 'Features\n' ;;
        fix) printf 'Fixes\n' ;;
        refactor) printf 'Refactors\n' ;;
        perf) printf 'Performance\n' ;;
        docs) printf 'Docs\n' ;;
        *) printf 'Other Changes\n' ;;
    esac
}


release_render_changelog_section() {
    local title="${1:?section title required}" entry=""
    shift || true

    [[ $# -gt 0 ]] || return 0

    printf '### %s\n' "${title}"
    for entry in "$@"; do
        printf -- '- %s\n' "${entry}"
    done
    printf '\n'
}


# Render one day-group of tags.
# Args: previous_tag (may be empty) first_tag last_tag
# Output goes to stdout; caller redirects to the temp file.
_release_flush_group() {
    local previous_tag="${1}" first_tag="${2}" last_tag="${3}"
    local log_range="" release_date="" subject=""
    local -a features=() fixes=() refactors=() performance=() docs=() other_changes=()

    if [[ -n "${previous_tag}" ]]; then
        log_range="${previous_tag}..${first_tag}"
    elif [[ "${first_tag}" != "${last_tag}" ]]; then
        log_range="${last_tag}^..${first_tag}"
    else
        log_range="${first_tag}^!"
    fi

    while IFS= read -r subject; do
        [[ -n "${subject}" ]] || continue
        [[ "${subject}" == *"[skip ci]"* ]] && continue
        case "$(release_changelog_bucket_for_subject "${subject}")" in
            Features)     features+=("${subject}") ;;
            Fixes)        fixes+=("${subject}") ;;
            Refactors)    refactors+=("${subject}") ;;
            Performance)  performance+=("${subject}") ;;
            Docs)         docs+=("${subject}") ;;
            *)            other_changes+=("${subject}") ;;
        esac
    done < <(git log --reverse --format=%s "${log_range}")

    (( ${#features[@]} + ${#fixes[@]} + ${#refactors[@]} + ${#performance[@]} + ${#docs[@]} + ${#other_changes[@]} > 0 )) || return 0

    release_date="$(git log -1 --format=%cs "${first_tag}^{commit}")"
    if [[ "${first_tag}" == "${last_tag}" ]]; then
        printf '## %s (%s)\n\n' "${first_tag}" "${release_date}"
    else
        printf '## %s \xe2\x80\xa6 %s (%s)\n\n' "${first_tag}" "${last_tag}" "${release_date}"
    fi

    release_render_changelog_section "Features" "${features[@]}"
    release_render_changelog_section "Fixes" "${fixes[@]}"
    release_render_changelog_section "Refactors" "${refactors[@]}"
    release_render_changelog_section "Performance" "${performance[@]}"
    release_render_changelog_section "Docs" "${docs[@]}"
    release_render_changelog_section "Other Changes" "${other_changes[@]}"
}


release_generate_changelog() {
    local output_path="${1:-CHANGELOG.md}" tmp_output="" write_stdout=0
    local -a tags=()

    if [[ "${output_path}" == "-" ]]; then
        write_stdout=1
        tmp_output="$(mktemp)"
    else
        mkdir -p "$(dirname -- "${output_path}")"
        tmp_output="${output_path}.tmp"
    fi

    mapfile -t tags < <(release_tags_desc)

    {
        printf '# Changelog\n\n'
        printf '_Generated from release tags with `bash scripts/generate-changelog`._\n\n'

        if (( ${#tags[@]} == 0 )); then
            printf 'No release tags yet.\n'
        else
            local -a current_group=()
            local current_group_date="" tag="" tag_date=""

            for index in "${!tags[@]}"; do
                tag="${tags[${index}]}"
                tag_date="$(git log -1 --format=%cs "${tag}^{commit}")"

                if [[ "${tag_date}" == "${current_group_date}" ]]; then
                    current_group+=("${tag}")
                else
                    if (( ${#current_group[@]} > 0 )); then
                        _release_flush_group "${tag}" "${current_group[0]}" "${current_group[-1]}"
                    fi
                    current_group=("${tag}")
                    current_group_date="${tag_date}"
                fi
            done

            if (( ${#current_group[@]} > 0 )); then
                _release_flush_group "" "${current_group[0]}" "${current_group[-1]}"
            fi
        fi
    } > "${tmp_output}"

    if (( write_stdout == 1 )); then
        cat "${tmp_output}"
        rm -f "${tmp_output}"
        return 0
    fi

    mv "${tmp_output}" "${output_path}"
}


# The npm package matrix is deliberately kept in one place. The first field is
# the npm package suffix, followed by the archive target, npm os/cpu values,
# archive type, and binary filename. Release workflows and package staging
# consume the same rows so a new target cannot silently be built but omitted
# from the published package set (issue #364).
release_npm_platform_rows() {
    printf '%s\n' \
        'linux-x64|linux-x64|linux|x64|tar.gz|agent-quota-gateway' \
        'linux-arm64|linux-arm64|linux|arm64|tar.gz|agent-quota-gateway' \
        'darwin-arm64|darwin-arm64|darwin|arm64|tar.gz|agent-quota-gateway' \
        'darwin-x64|darwin-x64|darwin|x64|tar.gz|agent-quota-gateway'
}


release_npm_package_directories() {
    while IFS='|' read -r package_key _target _npm_os _npm_cpu _archive _binary; do
        printf 'agent-quota-gateway-%s\n' "${package_key}"
    done < <(release_npm_platform_rows)
    printf 'agent-quota-gateway\n'
}


release_npm_tarball_name() {
    local version="${1:-}" package_key="${2:-}"

    release_validate_version "${version}" || return 1
    if [[ "${package_key}" == 'root' ]]; then
        printf 'shukelabs-agent-quota-gateway-%s.tgz\n' "${version}"
    else
        printf 'shukelabs-agent-quota-gateway-%s-%s.tgz\n' "${package_key}" "${version}"
    fi
}


release_validate_version() {
    local version="${1:-}"

    [[ "${version}" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || {
        printf "release: invalid npm version '%s'\n" "${version}" >&2
        return 1
    }
}


release_npm_shim_path() {
    local repo_root="${1:-$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)}"

    printf '%s/packaging/npm/agent-quota-gateway.js\n' "${repo_root}"
}


release_npm_write_root_manifest() {
    local version="${1:-}"

    release_validate_version "${version}" || return 1
    cat <<EOF
{
  "name": "@shukelabs/agent-quota-gateway",
  "version": "${version}",
  "description": "Static Agent Quota Gateway launcher and systemd deployment templates.",
  "license": "SEE LICENSE IN LICENSE",
  "bin": {
    "agent-quota-gateway": "bin/agent-quota-gateway.js"
  },
  "files": [
    "bin",
    "deploy",
    "LICENSE"
  ],
  "os": [
    "darwin",
    "linux"
  ],
  "cpu": [
    "x64",
    "arm64"
  ],
  "publishConfig": {
    "access": "public"
  },
  "optionalDependencies": {
    "@shukelabs/agent-quota-gateway-linux-x64": "${version}",
    "@shukelabs/agent-quota-gateway-linux-arm64": "${version}",
    "@shukelabs/agent-quota-gateway-darwin-arm64": "${version}",
    "@shukelabs/agent-quota-gateway-darwin-x64": "${version}"
  }
}
EOF
}


release_npm_write_platform_manifest() {
    local version="${1:-}" package_key="${2:-}" npm_os="${3:-}" npm_cpu="${4:-}"

    release_validate_version "${version}" || return 1
    [[ -n "${package_key}" && -n "${npm_os}" && -n "${npm_cpu}" ]] || {
        printf 'release: incomplete npm platform metadata\n' >&2
        return 1
    }
    cat <<EOF
{
  "name": "@shukelabs/agent-quota-gateway-${package_key}",
  "version": "${version}",
  "description": "Native Agent Quota Gateway binary for ${npm_os}/${npm_cpu}.",
  "license": "SEE LICENSE IN LICENSE",
  "files": [
    "bin",
    "LICENSE"
  ],
  "os": [
    "${npm_os}"
  ],
  "cpu": [
    "${npm_cpu}"
  ],
  "publishConfig": {
    "access": "public"
  }
}
EOF
}


release_npm_validate_manifest() {
    local manifest_path="${1:-}" expected_name="${2:-}" version="${3:-}"
    local kind="${4:-}" npm_os="${5:-}" npm_cpu="${6:-}"

    [[ -f "${manifest_path}" ]] || {
        printf "release: npm manifest not found '%s'\n" "${manifest_path}" >&2
        return 1
    }
    command -v node >/dev/null 2>&1 || {
        printf 'release: node is required to validate npm manifests\n' >&2
        return 1
    }

    node - "${manifest_path}" "${expected_name}" "${version}" "${kind}" \
        "${npm_os}" "${npm_cpu}" <<'NODE'
const fs = require('node:fs');

const [, , manifestPath, expectedName, expectedVersion, kind, expectedOs, expectedCpu] = process.argv;
let manifest;
try {
  manifest = JSON.parse(fs.readFileSync(manifestPath, 'utf8'));
} catch (error) {
  console.error(`release: invalid npm manifest ${manifestPath}: ${error.message}`);
  process.exit(1);
}

function fail(message) {
  console.error(`release: ${manifestPath}: ${message}`);
  process.exit(1);
}

if (manifest.name !== expectedName) fail(`name '${manifest.name}' does not match '${expectedName}'`);
if (manifest.version !== expectedVersion) fail(`version '${manifest.version}' does not match '${expectedVersion}'`);
if (manifest.license !== 'SEE LICENSE IN LICENSE') fail('license must point to LICENSE');
if (manifest.publishConfig?.access !== 'public') fail('publishConfig.access must be public');
function sameObject(actual, expected) {
  if (!actual || typeof actual !== 'object' || Array.isArray(actual)) return false;
  const actualKeys = Object.keys(actual).sort();
  const expectedKeys = Object.keys(expected).sort();
  return actualKeys.length === expectedKeys.length &&
    actualKeys.every((key, index) => key === expectedKeys[index] && actual[key] === expected[key]);
}
function sameArray(actual, expected) {
  return JSON.stringify(actual) === JSON.stringify(expected);
}

if (kind === 'root') {
  if (!sameObject(manifest.bin, { 'agent-quota-gateway': 'bin/agent-quota-gateway.js' })) {
    fail('bin must expose exactly agent-quota-gateway');
  }
  if (!sameArray(manifest.files, ['bin', 'deploy', 'LICENSE'])) {
    fail('files must contain only bin, deploy, and LICENSE');
  }
  if (!sameArray(manifest.os, ['darwin', 'linux'])) fail('os matrix is incorrect');
  if (!sameArray(manifest.cpu, ['x64', 'arm64'])) fail('cpu matrix is incorrect');
  const expectedDependencies = {
    '@shukelabs/agent-quota-gateway-linux-x64': expectedVersion,
    '@shukelabs/agent-quota-gateway-linux-arm64': expectedVersion,
    '@shukelabs/agent-quota-gateway-darwin-arm64': expectedVersion,
    '@shukelabs/agent-quota-gateway-darwin-x64': expectedVersion,
  };
  if (!sameObject(manifest.optionalDependencies, expectedDependencies)) {
    fail('optionalDependencies must list all four platform packages at the release version');
  }
} else if (kind === 'platform') {
  if (manifest.bin || manifest.optionalDependencies) fail('platform packages must not expose commands or dependencies');
  if (!sameArray(manifest.files, ['bin', 'LICENSE'])) fail('platform files must contain only bin and LICENSE');
  if (!sameArray(manifest.os, [expectedOs])) fail(`os must be ${expectedOs}`);
  if (!sameArray(manifest.cpu, [expectedCpu])) fail(`cpu must be ${expectedCpu}`);
} else {
  fail(`unknown manifest kind '${kind}'`);
}
NODE
}


release_npm_validate_package_set() {
    local version="${1:-}" package_root="${2:-}"
    local package_key _target npm_os npm_cpu _archive binary package_dir file_count entry entry_name

    release_validate_version "${version}" || return 1
    [[ -d "${package_root}" ]] || {
        printf "release: npm package directory not found '%s'\n" "${package_root}" >&2
        return 1
    }

    package_dir="${package_root}/agent-quota-gateway"
    release_npm_validate_manifest "${package_dir}/package.json" \
        '@shukelabs/agent-quota-gateway' "${version}" root || return 1
    cmp -s "${package_dir}/bin/agent-quota-gateway.js" "$(release_npm_shim_path)" || {
        printf 'release: staged npm shim differs from packaging/npm/agent-quota-gateway.js\n' >&2
        return 1
    }
    cmp -s "${package_dir}/deploy/agent-quota-gateway.service" \
        "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)/deploy/agent-quota-gateway.service" || {
        printf 'release: staged systemd unit differs from deploy/agent-quota-gateway.service\n' >&2
        return 1
    }
    cmp -s "${package_dir}/deploy/aqg.env.example" \
        "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)/deploy/aqg.env.example" || {
        printf 'release: staged env template differs from deploy/aqg.env.example\n' >&2
        return 1
    }
    cmp -s "${package_dir}/LICENSE" "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)/LICENSE" || {
        printf 'release: staged LICENSE differs from LICENSE\n' >&2
        return 1
    }
    file_count="$(find "${package_dir}" -type f | wc -l | tr -d ' ')"
    [[ "${file_count}" == 5 ]] || {
        printf "release: root npm package must contain package.json, launcher, two deploy files, and LICENSE\n" >&2
        return 1
    }

    while IFS='|' read -r package_key _target npm_os npm_cpu _archive binary; do
        package_dir="${package_root}/agent-quota-gateway-${package_key}"
        release_npm_validate_manifest "${package_dir}/package.json" \
            "@shukelabs/agent-quota-gateway-${package_key}" "${version}" platform \
            "${npm_os}" "${npm_cpu}" || return 1
        [[ -f "${package_dir}/bin/${binary}" && -x "${package_dir}/bin/${binary}" ]] || {
            printf "release: executable '%s' missing from '%s'\n" "${binary}" "${package_dir}" >&2
            return 1
        }
        cmp -s "${package_dir}/LICENSE" "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)/LICENSE" || {
            printf "release: platform LICENSE differs from LICENSE\n" >&2
            return 1
        }
        file_count="$(find "${package_dir}" -type f | wc -l | tr -d ' ')"
        [[ "${file_count}" == 3 ]] || {
            printf "release: npm package '%s' contains unexpected files\n" "${package_dir}" >&2
            return 1
        }
    done < <(release_npm_platform_rows)

    while IFS= read -r package_dir; do
        [[ -d "${package_root}/${package_dir}" ]] || {
            printf "release: expected npm package directory missing '%s'\n" "${package_dir}" >&2
            return 1
        }
    done < <(release_npm_package_directories)

    for entry in "${package_root}"/*; do
        [[ -d "${entry}" ]] || {
            printf "release: unexpected file in npm package staging '%s'\n" "${entry}" >&2
            return 1
        }
        entry_name="${entry##*/}"
        case "${entry_name}" in
            agent-quota-gateway|agent-quota-gateway-linux-x64|agent-quota-gateway-linux-arm64|agent-quota-gateway-darwin-arm64|agent-quota-gateway-darwin-x64) ;;
            *)
                printf "release: unexpected npm package directory '%s'\n" "${entry_name}" >&2
                return 1
                ;;
        esac
    done
}


release_npm_stage_packages() {
    local version="${1:-}" archive_dir="${2:-}" output_dir="${3:-}"
    local repo_root="" staging="" package_key target npm_os npm_cpu archive binary
    local archive_path package_dir extract_dir extracted_binary

    release_validate_version "${version}" || return 1
    [[ -d "${archive_dir}" ]] || {
        printf "release: archive directory not found '%s'\n" "${archive_dir}" >&2
        return 1
    }
    [[ -n "${output_dir}" && ! -e "${output_dir}" ]] || {
        printf "release: npm staging output must be a new path '%s'\n" "${output_dir}" >&2
        return 1
    }
    repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
    [[ -f "${repo_root}/packaging/npm/agent-quota-gateway.js" &&
        -f "${repo_root}/deploy/agent-quota-gateway.service" &&
        -f "${repo_root}/deploy/aqg.env.example" &&
        -f "${repo_root}/LICENSE" ]] || {
        printf 'release: npm package source files are incomplete\n' >&2
        return 1
    }

    mkdir -p -- "$(dirname -- "${output_dir}")"
    staging="$(mktemp -d "${output_dir}.XXXXXX")" || return 1
    mkdir -p "${staging}/agent-quota-gateway/bin" "${staging}/agent-quota-gateway/deploy"
    cp -- "${repo_root}/packaging/npm/agent-quota-gateway.js" \
        "${staging}/agent-quota-gateway/bin/agent-quota-gateway.js"
    chmod +x "${staging}/agent-quota-gateway/bin/agent-quota-gateway.js"
    cp -- "${repo_root}/deploy/agent-quota-gateway.service" \
        "${repo_root}/deploy/aqg.env.example" "${staging}/agent-quota-gateway/deploy/"
    cp -- "${repo_root}/LICENSE" "${staging}/agent-quota-gateway/LICENSE"
    release_npm_write_root_manifest "${version}" >"${staging}/agent-quota-gateway/package.json"

    while IFS='|' read -r package_key target npm_os npm_cpu archive binary; do
        archive_path="${archive_dir}/agent-quota-gateway-${version}-${target}.${archive}"
        package_dir="${staging}/agent-quota-gateway-${package_key}"
        extract_dir="${staging}/.extract-${package_key}"
        [[ -f "${archive_path}" ]] || {
            printf "release: target archive not found '%s'\n" "${archive_path}" >&2
            rm -rf -- "${staging}"
            return 1
        }
        mkdir -p "${package_dir}/bin" "${extract_dir}"
        case "${archive}" in
            tar.gz)
                tar -xzf "${archive_path}" -C "${extract_dir}"
                ;;
            *)
                printf "release: unsupported npm archive type '%s'\n" "${archive}" >&2
                rm -rf -- "${staging}"
                return 1
                ;;
        esac
        extracted_binary="${extract_dir}/${binary}"
        [[ -f "${extracted_binary}" ]] || {
            printf "release: expected binary '%s' missing from '%s'\n" "${binary}" "${archive_path}" >&2
            rm -rf -- "${staging}"
            return 1
        }
        cp -- "${extracted_binary}" "${package_dir}/bin/${binary}"
        chmod +x "${package_dir}/bin/${binary}"
        cp -- "${repo_root}/LICENSE" "${package_dir}/LICENSE"
        release_npm_write_platform_manifest "${version}" "${package_key}" \
            "${npm_os}" "${npm_cpu}" >"${package_dir}/package.json"
        rm -rf -- "${extract_dir}"
    done < <(release_npm_platform_rows)

    if ! release_npm_validate_package_set "${version}" "${staging}"; then
        rm -rf -- "${staging}"
        return 1
    fi
    mv -- "${staging}" "${output_dir}"
}


release_npm_write_checksums() {
    local tarball_dir="${1:-}" checksum_path="${2:-}" version="${3:-}"
    local package_key tarball

    [[ -d "${tarball_dir}" && -n "${checksum_path}" ]] || {
        printf 'release: npm checksum inputs are incomplete\n' >&2
        return 1
    }
    release_validate_version "${version}" || return 1
    : >"${checksum_path}"
    while IFS= read -r package_key; do
        if [[ "${package_key}" == agent-quota-gateway ]]; then
            tarball="$(release_npm_tarball_name "${version}" root)"
        else
            tarball="$(release_npm_tarball_name "${version}" "${package_key#agent-quota-gateway-}")"
        fi
        [[ -f "${tarball_dir}/${tarball}" ]] || {
            printf "release: npm tarball not found '%s'\n" "${tarball_dir}/${tarball}" >&2
            return 1
        }
        if command -v sha256sum >/dev/null 2>&1; then
            (cd "${tarball_dir}" && sha256sum "${tarball}") >>"${checksum_path}"
        else
            (cd "${tarball_dir}" && shasum -a 256 "${tarball}") >>"${checksum_path}"
        fi
    done < <(release_npm_package_directories)
}


release_npm_verify_tarballs() {
    local version="${1:-}" tarball_dir="${2:-}" package_key tarball entries
    local expected_count actual_count

    release_validate_version "${version}" || return 1
    [[ -d "${tarball_dir}" ]] || {
        printf "release: npm tarball directory not found '%s'\n" "${tarball_dir}" >&2
        return 1
    }
    expected_count="$(release_npm_package_directories | wc -l | tr -d ' ')"
    actual_count="$(find "${tarball_dir}" -maxdepth 1 -type f -name '*.tgz' | wc -l | tr -d ' ')"
    [[ "${actual_count}" == "${expected_count}" ]] || {
        printf "release: expected %s npm tarballs in '%s', found %s\n" \
            "${expected_count}" "${tarball_dir}" "${actual_count}" >&2
        return 1
    }
    while IFS= read -r package_key; do
        if [[ "${package_key}" == agent-quota-gateway ]]; then
            tarball="$(release_npm_tarball_name "${version}" root)"
            required=(
                'package/package.json'
                'package/bin/agent-quota-gateway.js'
                'package/deploy/agent-quota-gateway.service'
                'package/deploy/aqg.env.example'
                'package/LICENSE'
            )
        else
            tarball="$(release_npm_tarball_name "${version}" "${package_key#agent-quota-gateway-}")"
            required=('package/package.json' 'package/bin/agent-quota-gateway' 'package/LICENSE')
        fi
        [[ -f "${tarball_dir}/${tarball}" ]] || {
            printf "release: npm tarball not found '%s'\n" "${tarball_dir}/${tarball}" >&2
            return 1
        }
        entries="$(tar -tzf "${tarball_dir}/${tarball}")" || return 1
        for entry in "${required[@]}"; do
            grep -Fqx "${entry}" <<<"${entries}" || {
                printf "release: '%s' lacks '%s'\n" "${tarball}" "${entry}" >&2
                return 1
            }
        done
    done < <(release_npm_package_directories)
}


release_npm_publish() {
    local version="${1:-}" tarball_dir="${2:-}" publisher="${3:-npm}"
    local package_key tarball
    local -a published=()

    release_validate_version "${version}" || return 1
    [[ -d "${tarball_dir}" ]] || {
        printf "release: npm tarball directory not found '%s'\n" "${tarball_dir}" >&2
        return 1
    }
    # Validate every input before the first registry call. This keeps a
    # missing package from turning a preflight mistake into a partial publish.
    release_npm_verify_tarballs "${version}" "${tarball_dir}" || return 1
    # npm reads a bare `dir/file.tgz` as a GitHub `owner/repo` spec and runs
    # `git ls-remote` over ssh; an explicit ./ or absolute path stays a tarball.
    case "${tarball_dir}" in
        /* | ./* | ../*) ;;
        *) tarball_dir="./${tarball_dir}" ;;
    esac

    while IFS= read -r package_key; do
        if [[ "${package_key}" == agent-quota-gateway ]]; then
            tarball="$(release_npm_tarball_name "${version}" root)"
        else
            tarball="$(release_npm_tarball_name "${version}" "${package_key#agent-quota-gateway-}")"
        fi
        if "${publisher}" publish --access public --ignore-scripts "${tarball_dir}/${tarball}"; then
            published+=("${tarball}")
        else
            printf '::error::npm publish failed after %d of %d packages\n' \
                "${#published[@]}" "$(release_npm_package_directories | wc -l | tr -d ' ')" >&2
            printf 'published before failure: %s\n' "${published[*]:-none}" >&2
            return 1
        fi
    done < <(release_npm_package_directories)
}
