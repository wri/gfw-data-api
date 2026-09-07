#!/bin/bash
#
# Builds a Docker image with buildx and pushes it to an AWS ECR repository.
#
# Drop-in replacement for gfw-terraform-modules' container_registry module
# scripts/push.sh (same argument signature), wired up via that module's
# `push_script` variable override.
#
# Not usually invoked directly -- terraform/main.tf points push_script at
# one of the two thin wrappers below (buildx_push_arm64.sh /
# buildx_push_amd64.sh), which set BUILD_PLATFORMS before delegating here.
# That's a separate wrapper file rather than a CI env var because this
# script runs inside the pinned globalforestwatch/terraform container
# (invoked via a local-exec provisioner), which only forwards the specific
# env vars terraform/docker/docker-compose.yml lists.
#
# Usage (identical positional args to the vendored push.sh):
#
#   ./buildx_push.sh ROOT_DIR REPOSITORY_URL TAG DOCKER_PATH DOCKER_FILE
#
# Env vars:
#   BUILD_PLATFORMS - platform to build for. Defaults to "linux/arm64" if
#                      invoked directly rather than through a wrapper.
#
# Also pushes/pulls a registry-backed BuildKit cache (a separate tag,
# "<repo>:buildcache-<arch>", in the same ECR repo the image itself goes
# to) via --cache-from/--cache-to. This is what actually speeds up a
# genuine rebuild (e.g. real batch/ changes -- see terraform/scripts/
# hash_batch.sh for what triggers a rebuild at all): layers unaffected by
# the change, like universal_batch.dockerfile's from-source tippecanoe
# compile, get reused instead of rebuilt every time. mode=max caches
# intermediate layers too, not just the final image, since that compile
# step specifically is the whole reason this exists. A missing cache ref
# (e.g. the very first build) is handled gracefully by buildx itself, no
# special-casing needed here. Expect an extra "buildcache-<arch>" tag to
# show up in each ECR repo alongside the real image tags -- that's this,
# working as intended, not something to clean up.

set -euo pipefail

ROOT_DIR=${1:-.}
REPOSITORY_URL=$2
TAG=${3:-latest}
DOCKER_PATH="${4:-.}"
DOCKER_FILE="$DOCKER_PATH/${5:-Dockerfile}"
PLATFORMS="${BUILD_PLATFORMS:-linux/arm64}"
BUILDER_NAME="gfw-multiarch-builder"

pushd "$ROOT_DIR" > /dev/null

REGION="$(echo "$REPOSITORY_URL" | cut -d. -f4)"
# The general ECR URL - the REPOSITORY_URL with the repo-name removed.
ECR_URL="$(echo "$REPOSITORY_URL" | cut -d/ -f1)"

if ! docker buildx version > /dev/null 2>&1; then
  echo "ERROR: 'docker buildx' is not available in this environment." >&2
  echo "terraform apply runs inside the globalforestwatch/terraform image with" >&2
  echo "/var/run/docker.sock bind-mounted, so this script talks to the host" >&2
  echo "Docker daemon -- but the 'docker' CLI invoked here (inside that" >&2
  echo "container) needs the buildx CLI plugin installed to drive it. Add the" >&2
  echo "docker-buildx-plugin package to that image, or install the plugin" >&2
  echo "(https://github.com/docker/buildx#installing) before running terraform apply." >&2
  exit 1
fi

if ! command -v flock > /dev/null 2>&1; then
  echo "ERROR: 'flock' is not available in this environment (needed to" >&2
  echo "safely serialize buildx builder creation across the concurrent" >&2
  echo "Terraform null_resources that invoke this script). It's part of" >&2
  echo "util-linux, normally preinstalled on Ubuntu-based images -- add the" >&2
  echo "util-linux package to the orchestration image if it's missing." >&2
  exit 1
fi

aws ecr get-login-password --region "$REGION" | docker login --username AWS \
          --password-stdin "${ECR_URL}"

# Reuse a persistent docker-container builder (required for --push with
# multiple platforms; the default "docker" driver can't export multi-arch
# manifests) across invocations instead of creating a new one every time.
BUILDER_LOCK="/tmp/gfw-multiarch-builder.lock"
(
  flock -x 200
  if ! docker buildx inspect "$BUILDER_NAME" > /dev/null 2>&1; then
    docker buildx create --name "$BUILDER_NAME" --driver docker-container --use
  else
    docker buildx use "$BUILDER_NAME"
  fi
  docker buildx inspect --bootstrap
) 200>"$BUILDER_LOCK"

# Derive an architecture-specific cache tag from the platform being built.
# arm64 and amd64 builds have fundamentally different layer sets, so each
# gets its own cache tag -- mixing them under one tag wouldn't make sense
# and could waste cache space on layers that can never be reused across
# architectures. This assumes PLATFORMS is a single "linux/ARCH" value,
# which is how this project always invokes it (via the wrapper scripts);
# a genuine multi-platform value here isn't a case this project uses.
CACHE_ARCH="${PLATFORMS#linux/}"
CACHE_REF="${REPOSITORY_URL}:buildcache-${CACHE_ARCH}"

docker buildx build \
  --platform "$PLATFORMS" \
  -t "$REPOSITORY_URL":"$TAG" \
  -f "$DOCKER_FILE" \
  --cache-from "type=registry,ref=${CACHE_REF}" \
  --cache-to "type=registry,ref=${CACHE_REF},mode=max" \
  --push \
  .

popd > /dev/null
