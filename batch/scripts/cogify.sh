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
aws s3 cp --recursive --exclude "*" --include "*.tif" "${SRC}" /tmp

set GDAL_NUM_THREADS=ALL_CPUS

# create VRT of input files so we can use gdal_translate
if [ ! -f "/tmp/merged.vrt" ]; then
  gdalbuildvrt /tmp/merged.vrt /tmp/*.tif
fi

# merge all rasters into one huge raster using COG block size
if [ ! -f "/tmp/merged.tif" ]; then
  gdal_translate -of GTiff -co TILED=YES -co BLOCKXSIZE="${BLOCK_SIZE}" -co BLOCKYSIZE="${BLOCK_SIZE}" -co COMPRESS=DEFLATE -co BIGTIFF=IF_SAFER -co NUM_THREADS=ALL_CPUS /tmp/merged.vrt /tmp/merged.tif
fi

# create overviews in raster
if ! gdalinfo "/tmp/merged.tif" | grep -q "Overviews"; then
  gdaladdo /tmp/merged.tif -r "${RESAMPLE}"
fi

# convert to COG using existing overviews, this adds some additional layout optimizations
if [ ! -f "/tmp/cog.tif" ]; then
  gdal_translate /tmp/merged.tif /tmp/cog.tif -of COG -co COMPRESS=DEFLATE -co BLOCKSIZE="${BLOCK_SIZE}" -co BIGTIFF=IF_SAFER -co NUM_THREADS=ALL_CPUS
fi

# upload to data lake
aws s3 cp /tmp/cog.tif "s3://${TARGET_BUCKET}/${TARGET_PREFIX}"
set +x

