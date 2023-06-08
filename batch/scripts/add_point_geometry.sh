#!/bin/bash

set -e

# requires arguments
# -d | --dataset
# -v | --version
# --lat
# --lng

# optional arguments
# -g | --geometry_name (get_arguments.sh specifies default)

ME=$(basename "$0")
. get_arguments.sh "$@"

# Add GFW specific layers
echo "PSQL: ALTER TABLE \"$DATASET\".\"$VERSION\". Add Point columns"
psql -c "ALTER TABLE \"$DATASET\".\"$VERSION\" ADD COLUMN ${GEOMETRY_NAME} geometry(Point,4326);
         ALTER TABLE \"$DATASET\".\"$VERSION\" ADD COLUMN ${GEOMETRY_NAME}_wm geometry(Point,3857);"