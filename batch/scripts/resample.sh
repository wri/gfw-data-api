#!/bin/bash

set -e

# requires arguments
# -d | --dataset
# -v | --version
# -s | --source
# -r | --resampling_method)
# --zoom_level
# -T | --target

ME=$(basename "$0")
. get_arguments.sh "$@"

echo "Reproject to WM and resample"

# Build an array of arguments to pass to resample.py
ARG_ARRAY=("--dataset" "${DATASET}" "--version" "${VERSION}")

ARG_ARRAY+=("--source-uri" "${SRC}")

ARG_ARRAY+=("--resampling-method" "${RESAMPLE}")

ARG_ARRAY+=("--target-zoom" "${ZOOM_LEVEL}")

ARG_ARRAY+=("--target-prefix" "${TARGET}")

# Run resample.py with the array of arguments
. /usr/local/app/.venv/bin/activate && pipenv run resample.py "${ARG_ARRAY[@]}"