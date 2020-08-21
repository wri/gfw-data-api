#!/bin/bash

set -e

# requires arguments
# -d | --dataset
# -v | --version
# -j | --json  # FIXME: Use -l? Or -s?
# --subset

ME=$(basename "$0")
. get_arguments.sh "$@"

# If in testing env, emulate bootstrapping performed in prod. env by supplying /tmp/READY
if [ "$ENV" = "test" ]; then
  touch $JOB_ID/READY
  export AWS_S3_ENDPOINT=http://motoserver:5000
  export AWS_HTTPS=NO
  export AWS_VIRTUAL_HOSTING=NO
fi

echo "Build Raster Tile Set and upload to S3"
if [ -z "${SUBSET}" ]; then
  pixetl --dataset "${DATASET}" --version "${VERSION}" "${JSON}"
else
  pixetl --dataset "${DATASET}" --version "${VERSION}" --subset "${SUBSET}" "${JSON}"
fi