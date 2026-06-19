# Release helpers for beta tags and changelog generation.

release_beta_tag_regex() {
    printf '^v([0-9]+)\\.([0-9]+)\\.([0-9]+)-beta$'
}


release_beta_tags_desc() {
    local tag="" regex=""
    regex="$(release_beta_tag_regex)"

    while IFS= read -r tag; do
        [[ "${tag}" =~ ${regex} ]] || continue
        printf '%s\n' "${tag}"
    done < <(git tag --list 'v*-beta' --sort=-version:refname)
}


release_latest_beta_tag() {
    local tag=""

    while IFS= read -r tag; do
        [[ -n "${tag}" ]] || continue
        printf '%s\n' "${tag}"
        return 0
    done < <(release_beta_tags_desc)
}


release_head_beta_tag() {
    local tag="" regex=""
    regex="$(release_beta_tag_regex)"

    while IFS= read -r tag; do
        [[ "${tag}" =~ ${regex} ]] || continue
        printf '%s\n' "${tag}"
        return 0
    done < <(git tag --points-at HEAD --list 'v*-beta' --sort=-version:refname)
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


release_next_beta_tag() {
    local latest_tag="${1:-}" bump_kind="${2:-patch}" major=0 minor=0 patch=0 regex=""
    regex="$(release_beta_tag_regex)"

    if [[ -n "${latest_tag}" ]]; then
        [[ "${latest_tag}" =~ ${regex} ]] || {
            printf "release_next_beta_tag: invalid beta tag '%s'\n" "${latest_tag}" >&2
            return 1
        }
        major="${BASH_REMATCH[1]}"
        minor="${BASH_REMATCH[2]}"
        patch="${BASH_REMATCH[3]}"
    fi

    case "${bump_kind}" in
        minor)
            minor=$((minor + 1))
            patch=0
            ;;
        patch)
            patch=$((patch + 1))
            ;;
        *)
            printf "release_next_beta_tag: unsupported bump kind '%s'\n" "${bump_kind}" >&2
            return 1
            ;;
    esac

    printf 'v%s.%s.%s-beta\n' "${major}" "${minor}" "${patch}"
}


release_create_beta_tag() {
    local current_tag="" latest_tag="" subject="" bump_kind="" next_tag=""

    current_tag="$(release_head_beta_tag || true)"
    if [[ -n "${current_tag}" ]]; then
        printf '%s\n' "${current_tag}"
        return 0
    fi

    latest_tag="$(release_latest_beta_tag || true)"
    subject="$(release_head_subject)"
    bump_kind="$(release_bump_kind_for_subject "${subject}")"
    next_tag="$(release_next_beta_tag "${latest_tag}" "${bump_kind}")"

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

    mapfile -t tags < <(release_beta_tags_desc)

    {
        printf '# Changelog\n\n'
        printf '_Generated from beta tags with `bash bin/generate-changelog`._\n\n'

        if (( ${#tags[@]} == 0 )); then
            printf 'No beta tags yet.\n'
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
