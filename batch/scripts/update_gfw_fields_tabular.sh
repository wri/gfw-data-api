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
echo echo "PSQL: ALTER TABLE \"$DATASET\".\"$SOURCE_VERSION\". Add version column."
psql -c "UPDATE \"$DATASET\".\"$SOURCE_VERSION\" SET gfw_version = '$VERSION' WHERE gfw_version IS null;"

if [ -n "${LNG}" ] && [ -n "${LAT}" ]; then
  echo "PSQL: UPDATE \"$DATASET\".\"$SOURCE_VERSION\". Update Point columns"
  psql -c "UPDATE \"$DATASET\".\"$SOURCE_VERSION\" SET ${GEOMETRY_NAME} = ST_SetSRID(ST_MakePoint($LNG, $LAT),4326),
                          ${GEOMETRY_NAME}_wm = ST_Transform(ST_SetSRID(ST_MakePoint($LNG, $LAT),4326), 3857)
                          WHERE ${GEOMETRY_NAME} IS null OR ${GEOMETRY_NAME}_wm IS null;"
fi
