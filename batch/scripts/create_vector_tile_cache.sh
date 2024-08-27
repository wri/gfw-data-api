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

# optional arguments
# --filter

ME=$(basename "$0")
. get_arguments.sh "$@"


NDJSON_FILE="${DATASET}.json"

# Build an array of arguments to pass to tippecanoe
TIPPE_ARG_ARRAY=(
  "-e" "tilecache"
  "-Z${MIN_ZOOM}"
  "-z${MAX_ZOOM}"
  "--preserve-input-order"
  "-P"
  "-n" "${DATASET}"
  "-l" "${DATASET}"
)

case ${TILE_STRATEGY} in
discontinuous) # Discontinuous polygon features
  TIPPE_ARG_ARRAY+=("--drop-densest-as-needed" "--extend-zooms-if-still-dropping")
  ;;
continuous) # Continuous polygon features
  TIPPE_ARG_ARRAY+=("--coalesce-densest-as-needed" "--extend-zooms-if-still-dropping")
  ;;
keep_all) # never drop or coalesce feature, ignore size and feature count
  TIPPE_ARG_ARRAY+=("-r1")
  ;;
*)
  echo "Invalid Tile Cache option -${TILE_STRATEGY}"
  exit 1
  ;;
esac

if [ -n "${FILTER}" ]; then
  echo "${FILTER}" > feature_filter.txt
  TIPPE_ARG_ARRAY+=("-J" "feature_filter.txt")
fi

TIPPE_ARG_ARRAY+=("${NDJSON_FILE}")

echo "Fetching NDJSON file from the Data Lake: ${SRC} -> ${NDJSON_FILE}..."
aws s3 cp "${SRC}" "${NDJSON_FILE}" --no-progress

echo "Building Tile Cache with Tippecanoe..."
tippecanoe "${TIPPE_ARG_ARRAY[@]}"

echo "Uploading tiles to S3 with TilePutty..."
tileputty tilecache --bucket "${TILE_CACHE}" --dataset "${DATASET}" --version "${VERSION}" --implementation "${IMPLEMENTATION}" --cores "${NUM_PROCESSES}"