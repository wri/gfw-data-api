#!/bin/bash

set -e

# requires arguments
# -d | --dataset
# -v | --version
# -j | --json  # FIXME: Use -l? Or -s?
# --subset

ME=$(basename "$0")
. get_arguments.sh "$@"

# FIXME: Add subset to command line if $SUBSET is set

echo "Build Raster Tile Set and upload to S3"
pixetl --dataset "${DATASET}" --version "${VERSION}" "${JSON}"
