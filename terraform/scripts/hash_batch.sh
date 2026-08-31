#!/bin/bash
#
# Computes a hash of a Docker build context's relevant source files.
#
# Drop-in replacement for gfw-terraform-modules' container_registry module
# scripts/hash.sh, wired up via that module's `hash_script` variable
# override (not invoked directly -- see hash_arm64.sh/hash_amd64.sh).
#
# Two real bugs fixed relative to the vendored hash.sh:
#
#  1. It hashes the WHOLE repo root, filtered only by an ignore file --
#     meaning a commit anywhere outside terraform/ invalidates the hash and
#     triggers a full rebuild of the Batch images even when nothing in
#     batch/ changed (a ~15 minute tippecanoe-from-source rebuild, for e.g.
#     an unrelated app/ or tests/ change). This hashes just $DOCKER_PATH.
#
#  2. Its own $DOCKER_PATH-scoped ignore-file *existence* check doesn't
#     match what it actually *reads* -- that's an unqualified
#     `.dockerignore`, always relative to $ROOT_DIR regardless of
#     $DOCKER_PATH. So e.g. batch/.dockerignore's content was never
#     actually used; the repo root's .dockerignore silently was, for
#     every image. This reads the same, correctly $DOCKER_PATH-scoped
#     file consistently.
#
# It also doesn't reproduce the vendored script's fragile
# printf+find-regex+manual-dot-escaping approach, which (among other
# things) word-splits the ignore file's content on whitespace -- shredding
# multi-word comment lines into several bogus patterns. This uses `find`
# directly with one -path exclusion per ignore-file line instead, which
# also means normal glob wildcards (*, ?, [...]) in a .dockerignore just
# work, the same as find -path already supports natively.
#
# Usage: ./hash_batch.sh ROOT_DIR DOCKER_PATH [EXTRA_HASH_INPUT]
#
# EXTRA_HASH_INPUT, if given, is folded into the final hash -- used by
# hash_arm64.sh/hash_amd64.sh so the hash (and therefore whether a rebuild
# triggers) also depends on target architecture, not just file content. A
# pure content hash can't tell "nothing changed" apart from "var.architecture
# flipped with no code changes" -- both need a rebuild, but only the latter
# would otherwise go undetected, silently leaving a stale, wrong-architecture
# image in ECR under the same tag.

set -euo pipefail

ROOT_DIR=${1:-.}
DOCKER_PATH=${2:-.}
EXTRA_HASH_INPUT=${3:-}

SCAN_DIR="${ROOT_DIR%/}/${DOCKER_PATH}"

cd "$SCAN_DIR"

find_args=(. -type f -not -name "*.pyc")

if [ -f ".dockerignore" ]; then
  while IFS= read -r pattern || [ -n "$pattern" ]; do
    # Blank lines and comments, matching standard .dockerignore syntax.
    [ -z "$pattern" ] && continue
    case "$pattern" in
      \#*) continue ;;
    esac
    # Normalize a leading slash (Docker treats patterns as already relative
    # to the build context root either way) so it lines up with find's own
    # "./" prefixed output.
    pattern="${pattern#/}"
    find_args+=(-not -path "./${pattern}" -not -path "./${pattern}/*")
  done < ".dockerignore"
fi

# Sorted so the result is stable regardless of filesystem traversal order.
file_hashes="$(find "${find_args[@]}" -exec md5sum {} \; | sort)"

hash="$(printf '%s%s' "$file_hashes" "$EXTRA_HASH_INPUT" | md5sum | cut -d' ' -f1)"

printf '{ "hash": "%s" }\n' "$hash"
