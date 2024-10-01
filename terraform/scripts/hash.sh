#!/bin/bash
# 
# Calculates hash of Docker image source contents
#
# Must be identical to the script that is used by the
# gfw-terraform-modules:terraform/modules/container_registry Terraform module.
#
# Usage:
#
# $ ./hash.sh .
#

set -e

pushd () {
    command pushd "$@" > /dev/null
}

popd () {
    command popd "$@" > /dev/null
}

ROOT_DIR=${1:-.}
DOCKER_PATH=${2:-.}
IGNORE="${DOCKER_PATH}/.dockerignore"

pushd "$ROOT_DIR"

# Hash all source files of the Docker image
if [ -f "$IGNORE" ]; then
    # We don't want to compute hashes for files listed in .dockerignore
    # to match regex pattern we need to escape leading .
    a=$(printf "! -regex ^./%s.* " `< .dockerignore`)
    b=${a//\/.//\\\.}

    file_hashes="$(
       find . -type f $b -exec md5sum {} \;
  )"
else
  # Exclude Python cache files, dot files
  file_hashes="$(
        find . -type f -not -name '*.pyc' -not -path './.**' -exec md5sum {} \;
  )"
fi

popd

hash="$(echo "$file_hashes" | md5sum | cut -d' ' -f1)"

echo '{ "hash": "'"$hash"'" }'
