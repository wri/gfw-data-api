#!/bin/bash

set -e

# requires arguments
# --block_size
# -r | --resample
# -G | --export_to_gee
# -I | --implementation
# -t | --target
# --prefix

ME=$(basename "$0")
. get_arguments.sh "$@"

set -x
# download all GeoTiff files

if [[ $(aws s3 ls "${PREFIX}/merged.tif") ]]; then
  aws s3 cp "${PREFIX}/merged.tif" merged.tif
else
  aws s3 cp --recursive --exclude "*" --include "*.tif" "${SRC}" .

  # create VRT of input files so we can use gdal_translate
  gdalbuildvrt merged.vrt *.tif

  # merge all rasters into one huge raster using COG block size
  gdal_translate -of GTiff -co TILED=YES -co BLOCKXSIZE="${BLOCK_SIZE}" -co BLOCKYSIZE="${BLOCK_SIZE}" -co COMPRESS=DEFLATE -co BIGTIFF=IF_SAFER -co NUM_THREADS=ALL_CPUS --config GDAL_CACHEMAX 70% --config GDAL_NUM_THREADS ALL_CPUS merged.vrt merged.tif
  aws s3 cp merged.tif "${PREFIX}/merged.tif"
fi

if [[ $(aws s3 ls "${PREFIX}/merged.tif.ovr") ]]; then
  aws s3 cp "${PREFIX}/merged.tif.ovr" merged.tif.ovr
else
  # generate overviews externally
  gdaladdo merged.tif -r "${RESAMPLE}" -ro --config GDAL_NUM_THREADS ALL_CPUS --config GDAL_CACHEMAX 70% --config COMPRESS_OVERVIEW DEFLATE
  aws s3 cp merged.tif.ovr "${PREFIX}/merged.tif.ovr"
fi

# convert to COG using existing overviews, this adds some additional layout optimizations
gdal_translate merged.tif cog.tif -of COG -co COMPRESS=DEFLATE -co BLOCKSIZE="${BLOCK_SIZE}" -co BIGTIFF=IF_SAFER -co NUM_THREADS=ALL_CPUS -co OVERVIEWS=FORCE_USE_EXISTING --config GDAL_CACHEMAX 70% --config GDAL_NUM_THREADS ALL_CPUS

# upload to data lake
aws s3 cp cog.tif "${TARGET}"

# delete intermediate file
aws s3 rm "${PREFIX}/merged.tif"
aws s3 rm "${PREFIX}/merged.tif.ovr"

if [ -n "$EXPORT_TO_GEE" ]; then
  export_to_gee.py --dataset "${DATASET}" --implementation "${IMPLEMENTATION}"
fi

set +x