#!/bin/bash

set -e

# requires arguments
# -d | --dataset
# -v | --version

# and positional arguments
# date_conf_uri
# intensity_uri
# destination_uri

ME=$(basename "$0")
. get_arguments.sh "$@"

echo "Merge date_conf and intensity assets and upload to S3"

merge_intensity.py -d "${DATASET}" -v "${VERSION}" "${POSITIONAL[@]}"