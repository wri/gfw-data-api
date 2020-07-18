#!/bin/bash

set -e

# requires arguments
# -d | --dataset
# -v | --version
# -s | --source
# -Z | --min_zoom
# -z | --max_zoom
# -t | --tile_strategy
ME=$(basename "$0")
. get_arguments.sh "$@"

# Set TILE_STRATEGY
case ${TILE_STRATEGY} in
discontinuous) # Discontinuous polygon features
  STRATEGY=drop-densest-as-needed
  ;;
continuous) # Continuous polygon features
  STRATEGY=coalesce-densest-as-needed
  ;;
*)
  echo "Invalid Tile Cache option -${TILE_STRATEGY}"
  exit 1
  ;;
esac

echo "Fetch NDJSON data from Data Lake ${SRC} -> ${DATASET}"
aws s3 cp "${SRC}" "${DATASET}"

echo "Build Tile Cache"
# shellcheck disable=SC2086
tippecanoe -Z"${MIN_ZOOM}" -z"${MAX_ZOOM}" -e tilecache --"${STRATEGY}" --extend-zooms-if-still-dropping -P -n "${DATASET}" "${DATASET}"

echo "Upload tiles to S3"
tileputty tilecache --bucket "${TILE_CACHE}" --dataset "${DATASET}" --version "${VERSION}" --implementation "${IMPLEMENTATION}"
