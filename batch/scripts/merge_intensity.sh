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

# in get_arguments.sh we call pushd to jump into the batchID subfolder
# pixETL expects /tmp as workdir and will make attempt to create subfolder itself
#popd

echo "Merge date_conf and intensity assets and upload to S3"

# Run pixetl with the array of arguments
merge_intensity.py -d "${DATASET}" -v "${VERSION}" "${POSITIONAL[@]}"