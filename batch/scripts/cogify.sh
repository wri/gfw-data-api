#!/bin/bash

set -e

# requires arguments
# -d | --dataset
# -v | --version
# -s | --source
# --target_bucket

ME=$(basename "$0")
. get_arguments.sh "$@"

# download all GeoTiff files
aws s3 cp --recursive --exclude "*" --include "*.tif" "${SRC}" .

# get list of file names
sources=()
for source in *.tif; do
  sources+=("$source")
done

# combine to one big COG
gdalwarp "${sources[@]}" "${DATASET}_${VERSION}.tif" -r "${RESAMPLE}" -t_srs "${EPSG}" -of COG -co BLOCKSIZE="${BLOCKSIZE}" -co NUM_THREADS=ALL_CPUS

# upload to data lake
aws s3 cp "${DATASET}_${VERSION}.tif" "s3://${TARGET_BUCKET}/${DATASET}/${VERSION}/epsg-${EPSG}/cog/${DATASET}_${VERSION}.tif"

