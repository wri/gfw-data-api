#!/bin/bash
#
# Builds a Docker image with buildx and pushes it to an AWS ECR repository.
#
# Drop-in replacement for gfw-terraform-modules' container_registry module
# scripts/push.sh (same argument signature), wired up via that module's
# `push_script` variable override. Used only for the Batch images
# (universal_batch.dockerfile, pixetl.dockerfile) for now -- the FastAPI
# app image (ECS/Fargate) still uses the vendored single-arch push.sh until
# that migration happens separately.
#
# All Batch compute environments now run on ARM64 (Graviton) -- see
# terraform/modules/compute_environment_arm -- so this defaults to building
# arm64 only rather than a multi-arch amd64+arm64 manifest; there's no more
# x86_64 Batch capacity left to pull an amd64 layer. Both of this project's
# batch base images (ghcr.io/osgeo/gdal:ubuntu-full-* and
# globalforestwatch/pixetl:*) already publish arm64 variants, and
# batch/uv.lock already resolves aarch64 wheels for its pinned deps, so no
# Dockerfile changes were needed -- only how the image gets built.
#
# Usage (identical positional args to the vendored push.sh):
#
#   ./buildx_push.sh ROOT_DIR REPOSITORY_URL TAG DOCKER_PATH DOCKER_FILE
#
# Env vars:
#   BUILD_PLATFORMS - comma separated list of platforms to build for.
#                      Defaults to "linux/arm64". Pass e.g.
#                      "linux/amd64,linux/arm64" to build a multi-arch
#                      manifest again (useful if x86_64 Batch capacity is
#                      ever reintroduced).
#
# Requires buildx to be available and, when *emulating* a foreign
# architecture (e.g. building arm64 on an x86_64 GitHub Actions runner),
# QEMU to be registered -- see docker/setup-qemu-action in
# .github/workflows/terraform_build.yaml. Building on a native arm64 runner
# (e.g. GitHub's ubuntu-24.04-arm) avoids emulation entirely.

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
