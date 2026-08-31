#!/bin/bash

set -euo pipefail
exec env BUILD_PLATFORMS="linux/amd64" "$(dirname "${BASH_SOURCE[0]}")/buildx_push.sh" "$@"
