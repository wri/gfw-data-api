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
echo "PSQL: UPDATE \"$DATASET\".\"$VERSION\". Update Point columns"
psql -c "UPDATE \"$DATASET\".\"$VERSION\" SET ${GEOMETRY_NAME} = ST_SetSRID(ST_MakePoint($LNG, $LAT),4326),
                        ${GEOMETRY_NAME}_wm = ST_Transform(ST_SetSRID(ST_MakePoint($LNG, $LAT),4326), 3857)
                        WHERE ${GEOMETRY_NAME} = null OR ${GEOMETRY_NAME}_wm = null;"
