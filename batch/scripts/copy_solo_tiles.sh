#!/bin/bash

set -e

# requires arguments
# -s | --source
# -T | --target

# copy_solo_tiles.sh --source source_uri --target target_uri
# where target/source are URIs like
# "s3://gfw-data-lake/umd_glad_dist_alerts/v20251018/raster/epsg-4326/10/100000/resample10m/geotiff/{tile_id}.tif"

# Copy all tiles in the source S3 folder that don't exist in the target folder, and
# update tiles.geojson and extent.geojson. Requires that the target raster has every
# tile that is in the source raster, and fails if this is not true. Also creates
# overlap.geojson (tiles originally in target) and nonoverlap.geojson (tiles in
# source that were not in target)

ME=$(basename "$0")
. get_arguments.sh "$@"

# Remove s3:// start of the uris.
spath="${SRC#s3://}"
tpath="${TARGET#s3://}"

# Separate into path components
IFS='/' read -r -a scomponents <<< "$spath"
IFS='/' read -r -a tcomponents <<< "$tpath"

# The uris should end in either geotiff/tiles.geojson or geotiff/{tile_id}.tif
if [[ ${#scomponents[@]} -ne 10 || ${scomponents[8]} -ne "geotiff" ]]; then
    echo "Error: bad format for source $SRC"
    exit 1
fi
if [[ ${#tcomponents[@]} -ne 10 || ${tcomponents[8]} -ne "geotiff" ]]; then
    echo "Error: bad format for target $TARGET"
    exit 1
fi

# Get dataset/version and pixel_meaning for both source and target
sversion="${scomponents[1]}/${scomponents[2]}"
smeaning="${scomponents[7]}"
tversion="${tcomponents[1]}/${tcomponents[2]}"
tmeaning="${tcomponents[7]}"

# Same as SRC and TARGET, but with geotiff/{...} removed
source="s3://${scomponents[0]}/$sversion/${scomponents[3]}/${scomponents[4]}/${scomponents[5]}/${scomponents[6]}/${scomponents[7]}"
target="s3://${tcomponents[0]}/$tversion/${tcomponents[3]}/${tcomponents[4]}/${tcomponents[5]}/${tcomponents[6]}/${tcomponents[7]}"

# Get the set of tiles in each raster
stiles=$(aws s3 ls $source/geotiff/ | grep '.*.tif$' | cut -c32-)
ttiles=$(aws s3 ls $target/geotiff/ | grep '.*.tif$' | cut -c32-)

uniqSource=$(comm -13 <(echo "$ttiles" | tr ' ' '\n') <(echo "$stiles" | tr ' ' '\n'))
uniqTarget=$(comm -23 <(echo "$ttiles" | tr ' ' '\n') <(echo "$stiles" | tr ' ' '\n'))

if [ ! -z "$uniqTarget" ]; then 
   echo "There are tiles in the target that are not in the source, so failing:"
   echo $uniqTarget
   exit 1
fi

if [ -z "$uniqSource" ]; then 
   echo "There are no unique tiles in source to copy"
   exit 1
fi

l=($uniqSource)
len=${#l[@]}
echo Copying $len tiles

j=1
for f in $uniqSource; do
  echo "Copying $j/$len tile"
  ((j++))
  aws s3 cp $source/geotiff/$f $target/geotiff/$f
  aws s3 cp $source/gdal-geotiff/$f $target/gdal-geotiff/$f
done

echo Copying extent.geojson
aws s3 cp $source/geotiff/extent.geojson $target/geotiff/extent.geojson
aws s3 cp $source/gdal-geotiff/extent.geojson $target/gdal-geotiff/extent.geojson

# overlap.geojson are the original tiles in the target.
aws s3 cp $target/geotiff/tiles.geojson $target/geotiff/overlap.geojson
aws s3 cp $target/gdal-geotiff/tiles.geojson $target/gdal-geotiff/overlap.geojson

geotiff_geojson=$(aws s3 cp $source/geotiff/tiles.geojson - | sed "s#/$sversion/#/$tversion/#g; s#/$smeaning/#/$tmeaning/#g")
gdalgeotiff_geojson=$(aws s3 cp $source/gdal-geotiff/tiles.geojson - | sed "s#/$sversion/#/$tversion/#g; s#/$smeaning/#/$tmeaning/#g")

# Save the full updated tiles.geojson to target
echo Copying tiles.geojson
echo "$geotiff_geojson" | aws s3 cp - $target/geotiff/tiles.geojson
echo "$gdalgeotiff_geojson" | aws s3 cp - $target/gdal-geotiff/tiles.geojson

# Filter for only the new tiles and save as nonoverlap.geojson
echo Creating nonoverlap.geojson

# Convert the space-separated uniqSource string into a JSON array for jq
json_list=$(echo "$uniqSource" | jq -R . | jq -s .)

echo "$geotiff_geojson" | jq --argjson list "$json_list" '
  .features |= map(
    select(.properties.name | split("/") | last as $fname | $list | index($fname))
  )
' | aws s3 cp - $target/geotiff/nonoverlap.geojson

echo "$gdalgeotiff_geojson" | jq --argjson list "$json_list" '
  .features |= map(
    select(.properties.name | split("/") | last as $fname | $list | index($fname))
  )
' | aws s3 cp - $target/gdal-geotiff/nonoverlap.geojson


# Special case code for creating overlap/nonoverlap COGs for the intensity raster as well.
# Pre-copy overlap.geojson, nonoverlap.geojson to $target[:-1]/intensity/geotiff
# and gdal-geotiff.
intensity="s3://${tcomponents[0]}/$tversion/${tcomponents[3]}/${tcomponents[4]}/${tcomponents[5]}/${tcomponents[6]}/intensity"
aws s3 cp $target/geotiff/overlap.geojson $intensity/geotiff/
aws s3 cp $target/geotiff/nonoverlap.geojson $intensity/geotiff/
aws s3 cp $target/gdal-geotiff/overlap.geojson $intensity/gdal-geotiff/
aws s3 cp $target/gdal-geotiff/nonoverlap.geojson $intensity/gdal-geotiff/
