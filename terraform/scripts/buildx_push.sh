#!/bin/bash
#
# Builds a Docker image with buildx and pushes it to an AWS ECR repository.
#
# Drop-in replacement for gfw-terraform-modules' container_registry module
# scripts/push.sh (same argument signature), wired up via that module's
# `push_script` variable override. Used for every image this project builds
# -- the two Batch images and the FastAPI/ECS app image -- all following
# var.architecture (see terraform/main.tf).
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
# CI normally runs on a GitHub Actions runner whose native architecture
# already matches BUILD_PLATFORMS (see the workflow's runs-on), so this is
# usually a native build with no emulation. Building for a foreign platform
# still works via buildx, but needs QEMU registered on the host
# (docker/setup-qemu-action) first.

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

aws ecr get-login-password --region "$REGION" | docker login --username AWS \
          --password-stdin "${ECR_URL}"

# Reuse a persistent docker-container builder (required for --push with
# multiple platforms; the default "docker" driver can't export multi-arch
# manifests) across invocations instead of creating a new one every time.
if ! docker buildx inspect "$BUILDER_NAME" > /dev/null 2>&1; then
  docker buildx create --name "$BUILDER_NAME" --driver docker-container --use
else
  docker buildx use "$BUILDER_NAME"
fi
docker buildx inspect --bootstrap

docker buildx build \
  --platform "$PLATFORMS" \
  -t "$REPOSITORY_URL":"$TAG" \
  -f "$DOCKER_FILE" \
  --push \
  .

popd > /dev/null
