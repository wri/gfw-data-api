#!/bin/bash

set -e

# requires arguments
# -s | --source
# -d | --dataset
# -v | --version

# optional arguments
# --overwrite
# --prefix

ME=$(basename "$0")
. get_arguments.sh "$@"

echo "Fetch remote GeoTIFFs headers to generate tiles.geojson"

# Build an array of arguments to pass to pixetl_prep
ARG_ARRAY=$SRC
ARG_ARRAY+=("--dataset" "${DATASET}" "--version" "${VERSION}")


if [ -n "${PREFIX}" ]; then
  ARG_ARRAY+=("--prefix" "${PREFIX}")
fi

if [ -z "${OVERWRITE}" ]; then
  ARG_ARRAY+=("--merge_existing")
fi

# Run pixetl_prep with the array of arguments
. /usr/local/app/.venv/bin/activate && pipenv run pixetl_prep "${ARG_ARRAY[@]}"