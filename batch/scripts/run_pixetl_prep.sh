#!/bin/bash

set -e

# requires arguments
# -b | --bucket
# --prefix
# -d | --dataset
# -v | --version

# optional arguments
# --identifier
# --provider
# --overwrite

ME=$(basename "$0")
. get_arguments.sh "$@"

echo "Fetch remote GeoTIFFs headers to generate tiles.geojson"

# Build an array of arguments to pass to pixetl_prep
ARG_ARRAY=("${BUCKET}" "${PREFIX}" "--dataset" "${DATASET}" "--version" "${VERSION}")

if [ -n "${PROVIDER}" ]; then
  ARG_ARRAY+=("--provider" "${PROVIDER}")
fi

if [ -n "${IDENTIFIER}" ]; then
  ARG_ARRAY+=("--identifier" "${IDENTIFIER}")
fi

if [ -n "${OVERWRITE}" ]; then
  ARG_ARRAY+=("--ignore_existing_tiles" "True")
else
  ARG_ARRAY+=("--ignore_existing_tiles" "False")
fi


# Run pixetl_prep with the array of arguments
pixetl_prep "${ARG_ARRAY[@]}"