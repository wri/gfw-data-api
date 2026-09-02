#!/bin/bash
# Thin wrapper around hash_batch.sh that folds "arm64" into the hash. Used
# as the hash_script when var.architecture selects arm64 (see
# terraform/main.tf), so a bare architecture toggle -- no code changes --
# still invalidates the hash and triggers a rebuild.
set -euo pipefail
exec "$(dirname "${BASH_SOURCE[0]}")/hash_batch.sh" "$1" "$2" "arm64"
