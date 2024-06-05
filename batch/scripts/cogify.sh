#!/bin/bash

set -e

# requires arguments
# -s | --source
# --target_bucket
# --target_prefix
# --block_size
# -r | --resample

ME=$(basename "$0")
. get_arguments.sh "$@"

set -x
# download all GeoTiff files
aws s3 cp --recursive --exclude "*" --include "*.tif" "${SRC}" .

set GDAL_NUM_THREADS=ALL_CPUS

# create VRT of input files so we can use gdal_translate
if [ ! -f "merged.vrt" ]; then
  gdalbuildvrt merged.vrt *.tif
fi

# merge all rasters into one huge raster using COG block size
if [ ! -f "merged.tif" ]; then
  gdal_translate -of GTiff -co BLOCKSIZE="${BLOCKSIZE}" -co COMPRESS=LZW -co NUM_THREADS=ALL_CPUS merged.vrt merged.tif
fi

# create overviews in raster
if ! gdalinfo "merged.tif" | grep -q "Overviews"; then
  gdaladdo merged.tif -r "${RESAMPLE}"
fi

# convert to COG using existing overviews, this adds some additional layout optimizations
if [ ! -f "cog.tif" ]; then
  gdal_translate merged.tif cog.tif -of COG -co BLOCKSIZE="${BLOCK_SIZE}" -co NUM_THREADS=ALL_CPUS
fi

# upload to data lake
aws s3 cp cog.tif "s3://${TARGET_BUCKET}/${TARGET_PREFIX}"
set +x

