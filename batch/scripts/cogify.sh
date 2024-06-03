#!/bin/bash

set -e

# requires arguments
# -d | --dataset
# -v | --version
# -s | --source
# --target_bucket
# --target_prefix

ME=$(basename "$0")
. get_arguments.sh "$@"

set -x
# download all GeoTiff files
aws s3 cp --recursive --exclude "*" --include "*.tif" "${SRC}" .

# combine to one big COG
gdalwarp *.tif "${DATASET}_${VERSION}.tif" -r "${RESAMPLE}" -t_srs "${SRID}" -of COG -co BLOCKSIZE="${BLOCKSIZE}" -co NUM_THREADS=ALL_CPUS

# upload to data lake
aws s3 cp "${DATASET}_${VERSION}.tif" "s3://${TARGET_BUCKET}/${TARGET_PREFIX}"
set +x

