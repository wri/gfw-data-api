#!/bin/bash
#
# Thin wrapper around buildx_push.sh that fixes BUILD_PLATFORMS to
# linux/arm64. Used as the push_script when var.architecture = "arm64"
# (see terraform/main.tf). See buildx_push.sh for why this is a separate
# wrapper file rather than a CI env var.

set -euo pipefail
exec env BUILD_PLATFORMS="linux/arm64" "$(dirname "${BASH_SOURCE[0]}")/buildx_push.sh" "$@"
