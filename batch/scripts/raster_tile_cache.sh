#!/bin/bash

set -e

# requires arguments
# -d | --dataset
# -v | --version
# -I | --implementation
# --skip
# --target_bucket
# --zoom_level

# and positional arguments
# asset_prefix

ME=$(basename "$0")
. get_arguments.sh "$@"

ARG_ARRAY=("--dataset" "${DATASET}"
           "--version" "${VERSION}"
           "--implementation" "${IMPLEMENTATION}"
           "--target-bucket" "${TARGET_BUCKET}"
           "--zoom-level" "${ZOOM_LEVEL}")

if [ -n "${SKIP}" ]; then
  ARG_ARRAY+=("--skip_empty_tiles")
fi

echo "Generate raster tile cache with GDAL2Tiles and upload to target S3 bucket"

raster_tile_cache.py  "${ARG_ARRAY[@]}" "${POSITIONAL[@]}"
