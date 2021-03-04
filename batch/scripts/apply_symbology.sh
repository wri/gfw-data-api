#!/bin/bash

set -e

# requires arguments
# -d | --dataset
# -v | --version
# -j | --json
# -n | --no_data
# -s | --source
# -T | --target

ME=$(basename "$0")
. get_arguments.sh "$@"

echo "Apply symbology and upload RGB asset to S3"

# Build an array of arguments to pass to apply_symbology.py
ARG_ARRAY=("--dataset" "${DATASET}" "--version" "${VERSION}")

ARG_ARRAY+=("--symbology" "${JSON}")

ARG_ARRAY+=("--no-data" "${NO_DATA}")

ARG_ARRAY+=("--source-uri" "${SRC}")

ARG_ARRAY+=("--target-prefix" "${TARGET}")

# Run apply_symbology.py with the array of arguments
apply_symbology.py "${ARG_ARRAY[@]}"