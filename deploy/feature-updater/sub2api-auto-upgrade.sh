#!/usr/bin/env bash
set -euo pipefail

# Canonical updater for the ticket292 build. The latest official release stays
# the build base while the feature branch remains the only patch source.
UPDATER_ID="sub2api-ticket292-updater-v1"

INSTALL_DIR="/opt/sub2api"
BINARY="${INSTALL_DIR}/sub2api"
FEATURE_DIR="${INSTALL_DIR}/feature"
PATCH_FILE="${FEATURE_DIR}/openai-codex-turn-state-ticket.patch"
STATE_FILE="${FEATURE_DIR}/upgrade-state"
LOG_FILE="${INSTALL_DIR}/data/logs/auto-upgrade.log"
LOCK_FILE="/run/sub2api-auto-upgrade.lock"
SELF_PATH="/usr/local/sbin/sub2api-auto-upgrade.sh"

UPSTREAM_REPO="Wei-Shaw/sub2api"
UPSTREAM_REF="main"
UPSTREAM_API="https://api.github.com/repos/${UPSTREAM_REPO}/releases/latest"
UPSTREAM_GIT="https://github.com/${UPSTREAM_REPO}.git"
UPSTREAM_SOURCE_BASE="https://github.com/${UPSTREAM_REPO}/archive/refs/tags"
FEATURE_REPO="alanbulan/sub2apioai"
FEATURE_REF="feat/openai-codex-turn-state-ticket"
FEATURE_GIT="https://github.com/${FEATURE_REPO}.git"
FEATURE_SCRIPT_PATH="deploy/feature-updater/sub2api-auto-upgrade.sh"
FEATURE_MARKER="ticket292"

SERVICE_NAME="sub2api"
SERVICE_USER="sub2api"
SERVICE_GROUP="sub2api"
BACKUP_RETENTION=3
BUILD_RETRIES=4
BUILD_RETRY_DELAY=20
PNPM_VERSION="9.15.9"
PNPM_SHA512="68046141893c66fad01c079231128e9afb89ef87e2691d69e4d40eee228988295fd4682181bae55b58418c3a253bde65a505ec7c5f9403ece5cc3cd37dcf2531"

# These files manage the feature repository itself and are not product code.
PATCH_EXCLUDE_ARGS=(
    "--exclude=.github/**"
    "--exclude=deploy/feature-updater/**"
)

BUILD_IMAGE=""
BUILD_CONTAINER=""
UPGRADE_TMPDIR=""
BUILT_ARTIFACT=""
PREPARED_PATCH=""
PREPARED_PATCH_SHA256=""

mkdir -p "$(dirname "$LOG_FILE")" "$FEATURE_DIR"

log() {
    printf '%s %s\n' "$(date -Is)" "$*" | tee -a "$LOG_FILE"
}

normalize_version() {
    printf '%s' "$1" | sed -E 's/^v//'
}

current_version() {
    "$BINARY" --version 2>/dev/null \
        | sed -nE 's/.*Sub2API[[:space:]]+([^[:space:]]+).*/\1/p' \
        | head -1 || true
}

latest_version() {
    curl -fsSL --connect-timeout 10 --max-time 30 "$UPSTREAM_API" \
        | sed -nE 's/.*"tag_name"[[:space:]]*:[[:space:]]*"([^"]+)".*/\1/p' \
        | head -1
}

feature_head() {
    git ls-remote "$FEATURE_GIT" "refs/heads/${FEATURE_REF}" 2>/dev/null \
        | awk -v ref="refs/heads/${FEATURE_REF}" '$2 == ref {print $1; found=1} END {if (!found) exit 1}'
}

download_with_retries() {
    local url="$1"
    local output="$2"
    curl -fL \
        --connect-timeout 15 \
        --retry 3 \
        --retry-delay 5 \
        --speed-limit 1024 \
        --speed-time 60 \
        -o "$output" \
        "$url"
}

cleanup() {
    if [[ -n "${BUILD_CONTAINER:-}" ]]; then
        docker rm "$BUILD_CONTAINER" >/dev/null 2>&1 || true
        BUILD_CONTAINER=""
    fi
    if [[ -n "${BUILD_IMAGE:-}" ]]; then
        docker image rm "$BUILD_IMAGE" >/dev/null 2>&1 || true
        BUILD_IMAGE=""
    fi
    if [[ -n "${UPGRADE_TMPDIR:-}" && -d "$UPGRADE_TMPDIR" ]]; then
        rm -rf "$UPGRADE_TMPDIR"
        UPGRADE_TMPDIR=""
    fi
}

prune_backups() {
    local index=0 record backup

    while IFS= read -r record; do
        backup="${record#* }"
        ((index += 1))
        if (( index > BACKUP_RETENTION )); then
            rm -f -- "$backup"
        fi
    done < <(find "$INSTALL_DIR" -maxdepth 1 -type f \
        -name 'sub2api.backup.*' -printf '%T@ %p\n' | sort -nr)
}

write_state() {
    local upstream_version="$1"
    local upstream_commit="$2"
    local patch_head="$3"
    local patch_sha256="$4"
    local state_tmp="${STATE_FILE}.tmp.$$"

    {
        printf 'upstream_repo=%s\n' "$UPSTREAM_REPO"
        printf 'upstream_version=%s\n' "$upstream_version"
        printf 'upstream_commit=%s\n' "$upstream_commit"
        printf 'feature_repo=%s\n' "$FEATURE_REPO"
        printf 'feature_ref=%s\n' "$FEATURE_REF"
        printf 'feature_marker=%s\n' "$FEATURE_MARKER"
        printf 'feature_patch_head=%s\n' "$patch_head"
        printf 'feature_patch_sha256=%s\n' "$patch_sha256"
        printf 'updater_id=%s\n' "$UPDATER_ID"
        printf 'updated_at=%s\n' "$(date -Is)"
    } > "$state_tmp"
    chown "$SERVICE_USER:$SERVICE_GROUP" "$state_tmp"
    chmod 0640 "$state_tmp"
    mv -f "$state_tmp" "$STATE_FILE"
}

state_value() {
    local key="$1"
    [[ -r "$STATE_FILE" ]] || return 0
    sed -nE "s/^${key}=//p" "$STATE_FILE" | head -1
}

is_feature_version() {
    [[ "$1" == *"${FEATURE_MARKER}"* ]]
}

source_commit_for_tag() {
    local tag="$1"
    local ref="refs/tags/${tag}"
    git ls-remote "$UPSTREAM_GIT" "$ref" "${ref}^{}" 2>/dev/null \
        | awk -v ref="$ref" '$2 == ref"^{}" {print $1; found=1} END {if (!found) exit 1}' \
        || git ls-remote "$UPSTREAM_GIT" "$ref" 2>/dev/null \
            | awk -v ref="$ref" '$2 == ref {print $1; found=1} END {if (!found) exit 1}'
}

sync_self() {
    local patch_head="$1"
    shift
    local running_path candidate source_url

    running_path="$(readlink -f "$0")"
    if [[ "$running_path" != "$SELF_PATH" \
        || "${SUB2API_UPDATER_SYNCED_HEAD:-}" == "$patch_head" ]]; then
        return 0
    fi

    candidate="$(mktemp /tmp/sub2api-auto-upgrade.self.XXXXXX)"
    source_url="https://raw.githubusercontent.com/${FEATURE_REPO}/${patch_head}/${FEATURE_SCRIPT_PATH}"
    if ! download_with_retries "$source_url" "$candidate" >/dev/null 2>&1; then
        rm -f "$candidate"
        log "warn: unable to refresh updater from feature commit ${patch_head}; continuing with installed updater"
        return 0
    fi
    if ! grep -q '^UPDATER_ID="sub2api-ticket292-updater-' "$candidate" \
        || ! bash -n "$candidate"; then
        rm -f "$candidate"
        log "warn: rejected invalid updater from feature commit ${patch_head}; continuing with installed updater"
        return 0
    fi
    if cmp -s "$candidate" "$SELF_PATH"; then
        rm -f "$candidate"
        return 0
    fi

    install -o root -g root -m 0755 "$candidate" "$SELF_PATH"
    rm -f "$candidate"
    log "updater: installed canonical script from feature commit ${patch_head}"
    export SUB2API_UPDATER_SYNCED_HEAD="$patch_head"
    exec 9>&-
    exec "$SELF_PATH" "$@"
}

download_feature_patch() {
    local patch_head="$1"
    local output="$2"
    local patch_url

    # Qualify the commit with the fork owner so newly pushed commits resolve
    # before GitHub's cross-repository SHA index catches up.
    patch_url="https://github.com/${UPSTREAM_REPO}/compare/${UPSTREAM_REF}...${FEATURE_REPO%%/*}:${patch_head}.diff"
    log "download: feature patch ${FEATURE_REPO}@${patch_head}"
    download_with_retries "$patch_url" "$output" 2>&1 | tee -a "$LOG_FILE"

    if [[ ! -s "$output" ]] \
        || ! grep -q '^diff --git a/backend/internal/service/openai_codex_ticket.go b/backend/internal/service/openai_codex_ticket.go$' "$output"; then
        log "fail: downloaded feature diff is empty or missing the Codex ticket implementation"
        return 1
    fi
}

prepare_patched_source() {
    local latest="$1"
    local version_num="$2"
    local patch_head="$3"
    local archive source_url source_dir patch_file

    UPGRADE_TMPDIR="$(mktemp -d /tmp/sub2api-feature-upgrade.XXXXXX)"
    archive="sub2api-source-${version_num}.tar.gz"
    source_url="${UPSTREAM_SOURCE_BASE}/${latest}.tar.gz"
    source_dir="${UPGRADE_TMPDIR}/source"
    patch_file="${UPGRADE_TMPDIR}/openai-codex-turn-state-ticket.diff"

    download_feature_patch "$patch_head" "$patch_file"
    PREPARED_PATCH_SHA256="$(sha256sum "$patch_file" | awk '{print $1}')"

    log "download: upstream source ${latest}"
    download_with_retries "$source_url" "${UPGRADE_TMPDIR}/${archive}" \
        2>&1 | tee -a "$LOG_FILE"
    mkdir -p "$source_dir"
    tar xzf "${UPGRADE_TMPDIR}/${archive}" -C "$source_dir" --strip-components=1

    log "verify: applying remote feature patch ${patch_head}"
    if ! (cd "$source_dir" && git apply --check "${PATCH_EXCLUDE_ARGS[@]}" "$patch_file") \
        2>&1 | tee -a "$LOG_FILE"; then
        log "fail: feature patch ${patch_head} does not apply to upstream ${latest}; keeping current binary"
        return 1
    fi
    (cd "$source_dir" && git apply "${PATCH_EXCLUDE_ARGS[@]}" "$patch_file")

    PREPARED_PATCH="$patch_file"
}

prepare_builder_context() {
    local source_dir="$1"
    local pnpm_archive="${source_dir}/.codex-pnpm.tgz"
    local dockerfile="${source_dir}/Dockerfile"
    local dockerfile_tmp="${source_dir}/Dockerfile.tmp.$$"

    log "download: pnpm ${PNPM_VERSION} build dependency"
    download_with_retries \
        "https://registry.npmjs.org/pnpm/-/pnpm-${PNPM_VERSION}.tgz" \
        "$pnpm_archive" 2>&1 | tee -a "$LOG_FILE"
    printf '%s  %s\n' "$PNPM_SHA512" "$pnpm_archive" | sha512sum -c - 2>&1 | tee -a "$LOG_FILE"

    if ! grep -q 'corepack enable && corepack prepare pnpm@9 --activate' "$dockerfile"; then
        log "fail: upstream Dockerfile no longer has the expected pnpm bootstrap step"
        return 1
    fi

    awk '
        BEGIN { added_copy = 0; replaced_bootstrap = 0 }
        /^WORKDIR \/app\/frontend$/ && !added_copy {
            print
            print "COPY .codex-pnpm.tgz /tmp/pnpm.tgz"
            added_copy = 1
            next
        }
        /RUN corepack enable && corepack prepare pnpm@9 --activate/ {
            print "RUN npm install --global /tmp/pnpm.tgz"
            replaced_bootstrap = 1
            next
        }
        { print }
        END {
            if (!added_copy || !replaced_bootstrap) exit 1
        }
    ' "$dockerfile" > "$dockerfile_tmp"
    mv -f "$dockerfile_tmp" "$dockerfile"
}

build_feature_binary() {
    local latest="$1"
    local version_num="$2"
    local patch_head="$3"
    local source_dir image_name artifact attempt

    prepare_patched_source "$latest" "$version_num" "$patch_head"
    source_dir="${UPGRADE_TMPDIR}/source"
    image_name="sub2api-feature-build:${version_num}-$$"
    BUILD_IMAGE="$image_name"
    prepare_builder_context "$source_dir"

    for attempt in $(seq 1 "$BUILD_RETRIES"); do
        log "build: patched upstream source using Docker backend-builder stage (attempt ${attempt}/${BUILD_RETRIES})"
        if docker build --network=host --pull --target backend-builder \
            --build-arg "VERSION=${version_num}-${FEATURE_MARKER}" \
            --build-arg "COMMIT=${patch_head}" \
            --build-arg "DATE=$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
            --tag "$image_name" "$source_dir" \
            2>&1 | tee -a "$LOG_FILE"; then
            break
        fi
        if (( attempt == BUILD_RETRIES )); then
            log "fail: Docker build failed after ${BUILD_RETRIES} attempts"
            return 1
        fi
        log "warn: Docker build attempt ${attempt} failed; retrying in ${BUILD_RETRY_DELAY}s"
        sleep "$BUILD_RETRY_DELAY"
    done

    artifact="${UPGRADE_TMPDIR}/sub2api"
    BUILD_CONTAINER="sub2api-feature-build-$$"
    docker create --name "$BUILD_CONTAINER" "$image_name" >/dev/null
    docker cp "$BUILD_CONTAINER:/app/sub2api" "$artifact"
    docker rm "$BUILD_CONTAINER" >/dev/null
    BUILD_CONTAINER=""

    if ! file "$artifact" | grep -q 'ELF 64-bit.*x86-64'; then
        log "fail: Docker build did not produce an x86-64 ELF binary"
        return 1
    fi
    if ! "$artifact" --version 2>&1 | grep -q "$FEATURE_MARKER"; then
        log "fail: built binary does not contain feature version marker"
        return 1
    fi

    BUILT_ARTIFACT="$artifact"
}

wait_for_service() {
    local attempt
    for attempt in $(seq 1 30); do
        if systemctl is-active --quiet "$SERVICE_NAME" \
            && curl -fsS --connect-timeout 3 http://127.0.0.1:8080/health >/dev/null 2>&1; then
            return 0
        fi
        sleep 1
    done
    return 1
}

install_binary() {
    local artifact="$1"
    local current="$2"
    local latest="$3"
    local upstream_commit="$4"
    local patch_head="$5"
    local patch_sha256="$6"
    local patch_source="$7"
    local backup_file new_version

    backup_file="${INSTALL_DIR}/sub2api.backup.${current:-unknown}.$(date +%Y%m%d%H%M%S)"
    log "backup: $backup_file"
    cp -p "$BINARY" "$backup_file"
    cp -p "$BINARY" "${INSTALL_DIR}/sub2api.backup"

    if systemctl is-active --quiet "$SERVICE_NAME"; then
        log "service: stopping $SERVICE_NAME"
        systemctl stop "$SERVICE_NAME"
    fi

    log "install: replacing binary with patched upstream build"
    install -o "$SERVICE_USER" -g "$SERVICE_GROUP" -m 0755 "$artifact" "$BINARY"

    log "service: starting $SERVICE_NAME"
    if ! systemctl start "$SERVICE_NAME" || ! wait_for_service; then
        log "fail: service failed to start after patched upgrade; rolling back"
        install -o "$SERVICE_USER" -g "$SERVICE_GROUP" -m 0755 "$backup_file" "$BINARY"
        systemctl start "$SERVICE_NAME" || true
        return 1
    fi

    install -o root -g root -m 0644 "$patch_source" "$PATCH_FILE"
    new_version="$(current_version)"
    write_state "$latest" "$upstream_commit" "$patch_head" "$patch_sha256"
    log "upgrade: finished current=${current:-unknown} new=${new_version:-unknown} upstream=${latest} feature=${patch_head}"
    log "backup: keeping latest $BACKUP_RETENTION versioned backups"
    prune_backups
}

perform_upgrade() {
    local latest="$1"
    local current="$2"
    local patch_head="$3"
    local version_num upstream_commit artifact

    version_num="$(normalize_version "$latest")"
    upstream_commit="$(source_commit_for_tag "$latest" || true)"
    if [[ -z "$upstream_commit" ]]; then
        log "fail: unable to resolve upstream commit for ${latest}"
        return 1
    fi
    build_feature_binary "$latest" "$version_num" "$patch_head"
    artifact="$BUILT_ARTIFACT"
    install_binary "$artifact" "$current" "$latest" "$upstream_commit" \
        "$patch_head" "$PREPARED_PATCH_SHA256" "$PREPARED_PATCH"
}

check_compatibility() {
    local latest="$1"
    local patch_head="$2"
    local version_num

    version_num="$(normalize_version "$latest")"
    prepare_patched_source "$latest" "$version_num" "$patch_head"
    log "check: compatible upstream=${latest} feature=${patch_head} patch_sha256=${PREPARED_PATCH_SHA256}"
}

usage() {
    printf 'Usage: %s [--check-only]\n' "$0"
}

main() {
    local check_only=false
    if (( $# > 1 )); then
        usage >&2
        exit 2
    fi
    if (( $# == 1 )); then
        if [[ "$1" != "--check-only" ]]; then
            usage >&2
            exit 2
        fi
        check_only=true
    fi

    if [[ ! -x "$BINARY" ]]; then
        log "skip: Sub2API binary not found or not executable at $BINARY"
        exit 0
    fi

    local current latest patch_head recorded_upstream recorded_patch_head
    current="$(current_version)"
    latest="$(latest_version || true)"
    patch_head="$(feature_head || true)"

    if [[ -z "$latest" ]]; then
        log "skip: failed to resolve latest upstream version"
        exit 0
    fi
    if [[ ! "$patch_head" =~ ^[0-9a-f]{40}$ ]]; then
        log "skip: failed to resolve feature branch ${FEATURE_REPO}@${FEATURE_REF}"
        exit 0
    fi

    sync_self "$patch_head" "$@"

    if [[ "$check_only" == true ]]; then
        check_compatibility "$latest" "$patch_head"
        exit 0
    fi

    recorded_upstream="$(state_value upstream_version)"
    recorded_patch_head="$(state_value feature_patch_head)"
    log "current=${current:-unknown} latest=${latest} recorded_upstream=${recorded_upstream:-none} remote_feature=${patch_head} recorded_feature=${recorded_patch_head:-none}"

    if is_feature_version "$current" \
        && [[ "$(normalize_version "$recorded_upstream")" == "$(normalize_version "$latest")" ]] \
        && [[ "$recorded_patch_head" == "$patch_head" ]]; then
        log "skip: feature build already tracks upstream ${latest} and feature ${patch_head}"
        exit 0
    fi

    log "upgrade: building remote feature ${patch_head} on upstream ${latest}"
    perform_upgrade "$latest" "$current" "$patch_head"
}

trap cleanup EXIT

(
    flock -n 9 || {
        log "skip: another auto-upgrade is already running"
        exit 0
    }
    main "$@"
) 9>"$LOCK_FILE"
