#!/bin/bash
# Thin wrapper around hash_batch.sh that folds "amd64" into the hash. Used
# as the hash_script when var.architecture selects x86_64 (see
# terraform/main.tf), so a bare architecture toggle -- no code changes --
# still invalidates the hash and triggers a rebuild.
set -euo pipefail
exec "$(dirname "${BASH_SOURCE[0]}")/hash_batch.sh" "$1" "$2" "amd64"
