#!/bin/bash

set -e

# requires arguments
# -s | --source
# -r | --resampling_method)
# --zoom_level
# -T | --target

ME=$(basename "$0")
. get_arguments.sh "$@"

echo "Reproject to WM and resample"

# Build an array of arguments to pass to resample.py
ARG_ARRAY=("--source-uri" "${SRC}")

ARG_ARRAY+=("--resampling-method" "${RESAMPLE}")

ARG_ARRAY+=("--target-zoom" "${ZOOM_LEVEL}")

ARG_ARRAY+=("--target-prefix" "${TARGET}")

# Run resample.py with the array of arguments
resample.py "${ARG_ARRAY[@]}"