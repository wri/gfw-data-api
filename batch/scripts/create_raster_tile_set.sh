#!/bin/bash

set -e

# requires arguments
# -d | --dataset
# -v | --version
# -j | --json
# --subset

ME=$(basename "$0")
. get_arguments.sh "$@"

# in get_arguments.sh we call pushd to jump into the batchID subfolder
# pixETL expects /tmp as workdir and will make attempt to create subfolder itself
popd

echo "Build Raster Tile Set and upload to S3"
if [ -n "${OVERWRITE}" ]; then
  OVERWRITE_ARG="--overwrite"
fi

if [ -n "${SUBSET}" ]; then
  SUBSET_ARG=(--subset "${SUBSET}")
fi

# Leave ${OVERWRITE_ARG} un-quoted in the following line, as quoting it seems
# to break things when $OVERWRITE is undefined. Perhaps it is interpreted as
# an argument consisting of a null string?
pixetl --dataset "${DATASET}" --version "${VERSION}" ${OVERWRITE_ARG} "${SUBSET_ARG[@]}" "${JSON}"
