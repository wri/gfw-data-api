#!/bin/bash

set -e

# requires arguments
# -d | --dataset
# -v | --version
# -I | --implementation
# --target_bucket
# --zoom_level

# and positional arguments
# asset_prefix

ME=$(basename "$0")
. get_arguments.sh "$@"

echo "Generate raster tile cache with GDAL2Tiles and upload to target S3 bucket"

raster_tile_cache.py -d "${DATASET}" -v "${VERSION}" -I "${IMPLEMENTATION}" \
  --target_bucket "${TARGET_BUCKET}" --zoom_level "${ZOOM_LEVEL}" "${POSITIONAL[@]}"