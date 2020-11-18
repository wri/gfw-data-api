#!/bin/bash

set -e

# requires arguments
# -d | --dataset
# -v | --version

# and positional arguments
# asset_uri
# tile_cache_uri

ME=$(basename "$0")
. get_arguments.sh "$@"

echo "Generate raster tile cache with GDAL2Tiles and upload to S3"

raster_tile_cache.py -d "${DATASET}" -v "${VERSION}" "${POSITIONAL[@]}"