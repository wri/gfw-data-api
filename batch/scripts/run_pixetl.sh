#!/bin/bash

set -e

# requires arguments
# -d | --dataset
# -v | --version
# -j | --json

# optional arguments
# --subset
# --overwrite

ME=$(basename "$0")
. get_arguments.sh "$@"

# in get_arguments.sh we call pushd to jump into the batchID subfolder
# pixETL expects /tmp as workdir and will make attempt to create subfolder itself
popd

echo "Build Raster Tile Set and upload to S3"

# Build an array of arguments to pass to pixetl
ARG_ARRAY=("--dataset" "${DATASET}" "--version" "${VERSION}")

if [ -n "${OVERWRITE}" ]; then
  ARG_ARRAY+=("--overwrite")
fi

if [ -n "${SUBSET}" ]; then
  ARG_ARRAY+=("--subset")
  ARG_ARRAY+=("${SUBSET}")
fi

ARG_ARRAY+=("${JSON}")

export LD_PRELOAD=/usr/local/app/libleak.so

# Run pixetl with the array of arguments
pixetl "${ARG_ARRAY[@]}"