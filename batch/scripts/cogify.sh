#!/bin/bash

set -e

# requires arguments
# -s | --source
# -T | --target
# --block_size
# -r | --resample
# -G | --export_to_gee
# -d | --dataset
# -I | --implementation

ME=$(basename "$0")
. get_arguments.sh "$@"

set -x
# download all GeoTiff files
aws s3 cp --recursive --exclude "*" --include "*.tif" "${SRC}" .

# create VRT of input files so we can use gdal_translate
if [ ! -f "merged.vrt" ]; then
  gdalbuildvrt merged.vrt *.tif
fi

# merge all rasters into one huge raster using COG block size
if [ ! -f "merged.tif" ]; then
  gdal_translate -of GTiff -co TILED=YES -co BLOCKXSIZE="${BLOCK_SIZE}" -co BLOCKYSIZE="${BLOCK_SIZE}" -co COMPRESS=DEFLATE -co BIGTIFF=IF_SAFER -co NUM_THREADS=ALL_CPUS --config GDAL_CACHEMAX 70% --config GDAL_NUM_THREADS ALL_CPUS merged.vrt merged.tif
fi

# create overviews in raster
if ! gdalinfo "merged.tif" | grep -q "Overviews"; then
  gdaladdo merged.tif -r "${RESAMPLE}" --config GDAL_NUM_THREADS ALL_CPUS --config GDAL_CACHEMAX 70%
fi

# convert to COG using existing overviews, this adds some additional layout optimizations
if [ ! -f "cog.tif" ]; then
  gdal_translate merged.tif cog.tif -of COG -co COMPRESS=DEFLATE -co BLOCKSIZE="${BLOCK_SIZE}" -co BIGTIFF=IF_SAFER -co NUM_THREADS=ALL_CPUS --config GDAL_CACHEMAX 70% --config GDAL_NUM_THREADS ALL_CPUS
fi

# upload to data lake
aws s3 cp cog.tif "${TARGET}"

if [ -z "$EXPORT_TO_GEE" ]; then
  python export_to_gee.py --dataset "${DATASET}" --implementation "${IMPLEMENTATION}"
fi

set +x

