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

# If ${SRC} is a *.geojson file, then copy the files specified in the geojson file.
if ! aws s3 ls  "${PREFIX}/${IMPLEMENTATION}_merged.tif"; then
    if [[ "$SRC" == *.geojson ]]; then
	files=$(aws s3 cp ${SRC} - | jq -r '.features[].properties.name | split("/") | last' | tr '\n' ' ')
	# Get the path to the folder by removing the last component (e.g. '/tiles.geojson').
	path=${SRC%/*}
	for f in $files; do
	    aws s3 cp $path/$f .
	done
    else
	# Else copy all TIF files in the specified folder.
	aws s3 cp --recursive --exclude "*" --include "*.tif" "${SRC}" .
    fi
fi

if [[ ${BLOCK_SIZE} -ge 2048 ]]; then
  # For blocksize 2048 and greater, generate overviews internally.
  # create VRT of input files so we can use gdal_translate
  gdalbuildvrt "${IMPLEMENTATION}_merged.vrt" *.tif

  gdal_translate -of COG -co COMPRESS=DEFLATE -co PREDICTOR=2 -co BLOCKSIZE="${BLOCK_SIZE}" -co BIGTIFF=IF_SAFER -co NUM_THREADS=ALL_CPUS -co OVERVIEWS=AUTO -r "${RESAMPLE}" --config COMPRESS_OVERVIEW DEFLATE -co SPARSE_OK=TRUE --config GDAL_CACHEMAX 70% --config GDAL_NUM_THREADS ALL_CPUS "${IMPLEMENTATION}_merged.vrt" "${IMPLEMENTATION}.tif"
else
  if [[ $(aws s3 ls "${PREFIX}/${IMPLEMENTATION}_merged.tif") ]]; then
    aws s3 cp "${PREFIX}/${IMPLEMENTATION}_merged.tif" "${IMPLEMENTATION}_merged.tif"
  else
    # create VRT of input files so we can use gdal_translate
    gdalbuildvrt "${IMPLEMENTATION}_merged.vrt" *.tif

    # merge all rasters into one huge raster using COG block size
    gdal_translate -of GTiff -co TILED=YES -co BLOCKXSIZE="${BLOCK_SIZE}" -co BLOCKYSIZE="${BLOCK_SIZE}" -co COMPRESS=DEFLATE -co BIGTIFF=IF_SAFER -co NUM_THREADS=ALL_CPUS --config GDAL_CACHEMAX 70% --config GDAL_NUM_THREADS ALL_CPUS "${IMPLEMENTATION}_merged.vrt" "${IMPLEMENTATION}_merged.tif"
    aws s3 cp "${IMPLEMENTATION}_merged.tif" "${PREFIX}/${IMPLEMENTATION}_merged.tif"
  fi

  if [[ $(aws s3 ls "${PREFIX}/${IMPLEMENTATION}_merged.tif.ovr") ]]; then
    aws s3 cp "${PREFIX}/${IMPLEMENTATION}_merged.tif.ovr" "${IMPLEMENTATION}_merged.tif.ovr"
  else
    # generate overviews externally
    gdaladdo "${IMPLEMENTATION}_merged.tif" -r "${RESAMPLE}" -ro --config GDAL_NUM_THREADS ALL_CPUS --config GDAL_CACHEMAX 70% --config COMPRESS_OVERVIEW DEFLATE --config PREDICTOR_OVERVIEW 2 --config ZLEVEL_OVERVIEW 1
    aws s3 cp "${IMPLEMENTATION}_merged.tif.ovr" "${PREFIX}/${IMPLEMENTATION}_merged.tif.ovr"
  fi

  # convert to COG using existing overviews, this adds some additional layout optimizations
  gdal_translate "${IMPLEMENTATION}_merged.tif" "${IMPLEMENTATION}.tif" -of COG -co COMPRESS=DEFLATE -co BLOCKSIZE="${BLOCK_SIZE}" -co BIGTIFF=IF_SAFER -co NUM_THREADS=ALL_CPUS -co OVERVIEWS=FORCE_USE_EXISTING -co SPARSE_OK=TRUE --config GDAL_CACHEMAX 70% --config GDAL_NUM_THREADS ALL_CPUS
fi

# upload to data lake
aws s3 cp "${IMPLEMENTATION}.tif" "${TARGET}"

# delete intermediate file
aws s3 rm "${PREFIX}/${IMPLEMENTATION}_merged.tif"
aws s3 rm "${PREFIX}/${IMPLEMENTATION}_merged.tif.ovr"

if [ -n "$EXPORT_TO_GEE" ]; then
  export_to_gee.py --dataset "${DATASET}" --implementation "${IMPLEMENTATION}"
fi

set +x
