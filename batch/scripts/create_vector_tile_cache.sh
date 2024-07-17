#!/bin/bash

set -e

# requires arguments
# -d | --dataset
# -v | --version
# -s | --source
# -Z | --min_zoom
# -z | --max_zoom
# -t | --tile_strategy
# -I | --implementation
ME=$(basename "$0")
. get_arguments.sh "$@"

# Set TILE_STRATEGY
case ${TILE_STRATEGY} in
discontinuous) # Discontinuous polygon features
  STRATEGY=("--drop-densest-as-needed" "--extend-zooms-if-still-dropping")
  ;;
continuous) # Continuous polygon features
  STRATEGY=("--coalesce-densest-as-needed" "--extend-zooms-if-still-dropping")
  ;;
keep_all) # never drop or coalesce feature, ignore size and feature count
  STRATEGY=("-r1")
  ;;
*)
  echo "Invalid Tile Cache option -${TILE_STRATEGY}"
  exit 1
  ;;
esac

echo "Fetch NDJSON data from Data Lake ${SRC} -> 'data.ndjson'"
aws s3 cp "${SRC}" 'data.ndjson' --no-progress

FINAL_DATA='data.ndjson'

if [ -n "$WHERE_FIELD" ]; then
  FINAL_DATA='filtered_data.ndjson'
  echo "Perform Filtering"
  ogr2ogr "${FINAL_DATA}" 'data.ndjson' -where "${WHERE_FIELD} IN (${WHERE_VALUES})"
fi

echo "Build Tile Cache"
tippecanoe -Z"${MIN_ZOOM}" -z"${MAX_ZOOM}" -e tilecache "${STRATEGY[@]}" -P -n "${DATASET}" "${FINAL_DATA}" --preserve-input-order

echo "Upload tiles to S3"
tileputty tilecache --bucket "${TILE_CACHE}" --dataset "${DATASET}" --version "${VERSION}" --implementation "${IMPLEMENTATION}" --cores "${NUM_PROCESSES}"