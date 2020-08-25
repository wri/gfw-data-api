#!/bin/bash

set -e

# requires arguments
# -d | --dataset
# -v | --version
# -j | --json  # FIXME: Use -l? Or -s?
# --subset

ME=$(basename "$0")
. get_arguments.sh "$@"

# in get_arguments.sh we call pushd to jump into the batchID subfolder
# pixETL expects /tmp as workdir and will make attempt to create subfolder itself
popd

echo "Build Raster Tile Set and upload to S3"
if [ -z "${SUBSET}" ]; then
  pixetl --dataset "${DATASET}" --version "${VERSION}" "${JSON}"
else
  pixetl --dataset "${DATASET}" --version "${VERSION}" --subset "${SUBSET}" "${JSON}"
fi