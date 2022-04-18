#!/bin/bash

set -e

# requires arguments
# -d | --dataset
# -v | --version
# --lat
# --lng
ME=$(basename "$0")
. get_arguments.sh "$@"

# Update GFW columns
echo "PSQL: UPDATE \"$DATASET\".\"$SOURCE_VERSION\". Set GFW attributes"
psql -c "UPDATE \"$DATASET\".\"$SOURCE_VERSION\" SET ${GEOMETRY_NAME}_wm = ST_Transform(ST_Force2D($GEOMETRY_NAME), 3857),
                                      gfw_area__ha = ST_Area($GEOMETRY_NAME::geography)/10000,
                                      gfw_geostore_id = md5(ST_asgeojson($GEOMETRY_NAME))::uuid,
                                      gfw_geojson = ST_asGeojson($GEOMETRY_NAME),
                                      gfw_version = '$VERSION',
                                      gfw_bbox = ARRAY[
                                          ST_XMin(ST_Envelope($GEOMETRY_NAME)::geometry),
                                          ST_YMin(ST_Envelope($GEOMETRY_NAME)::geometry),
                                          ST_XMax(ST_Envelope($GEOMETRY_NAME)::geometry),
                                          ST_YMax(ST_Envelope($GEOMETRY_NAME)::geometry)]::NUMERIC[]
                                      WHERE gfw_version IS null;"

