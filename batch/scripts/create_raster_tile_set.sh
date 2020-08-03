#!/bin/bash

set -e

# requires arguments
# -d | --dataset
# -v | --version
# -j | --json  # FIXME: Use -l? Or -s?
# FIXME: Should we enable overwrite categorically? Does it matter?
# FIXME: Don't forget subset!

ME=$(basename "$0")
. get_arguments.sh "$@"

echo "Build Raster Tile Set"
pixetl --dataset "${DATASET}" --version "${VERSION}" "${JSON}"

echo "Upload tiles to S3"
echo "NOT IMPLEMENTED YET"
#tileputty tilecache --bucket "${TILE_CACHE}" --dataset "${DATASET}" --version "${VERSION}" --implementation "${IMPLEMENTATION}"
