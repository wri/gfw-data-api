#!/bin/bash

set -e

# requires arguments
# -d | --dataset
# -v | --version
# --lat
# --lng
ME=$(basename "$0")
. get_arguments.sh "$@"

# Add GFW specific layers
echo "PSQL: ALTER TABLE \"$DATASET\".\"$VERSION\". Add Point columns"
psql -c "ALTER TABLE \"$DATASET\".\"$VERSION\" ADD COLUMN ${GEOMETRY_NAME} geometry(Point,4326);
         ALTER TABLE \"$DATASET\".\"$VERSION\" ADD COLUMN ${GEOMETRY_NAME}_wm geometry(Point,3857);"

# Update GFW columns
echo "PSQL: UPDATE \"$DATASET\".\"$VERSION\". Update Point columns"
psql -c "UPDATE \"$DATASET\".\"$VERSION\" SET ${GEOMETRY_NAME} = ST_SetSRID(ST_MakePoint($LNG, $LAT),4326),
                        ${GEOMETRY_NAME}_wm = ST_Transform(ST_SetSRID(ST_MakePoint($LNG, $LAT),4326), 3857);"
