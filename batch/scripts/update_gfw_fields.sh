#!/bin/bash

set -e

# requires arguments
# -d | --dataset
# -v | --version

# optional arguments
# -g | --geometry_name (get_arguments.sh specifies default)

ME=$(basename "$0")
. get_arguments.sh "$@"

# Transform to web mercator (WM) in two steps to isolate the more involved
# one for polygons that overflow WM lat bounds of -85/85 degrees

# Reproject all polygons within WM bounds
psql -c "
  UPDATE
    \"$DATASET\".\"$VERSION\"
  SET
    ${GEOMETRY_NAME}_wm = ST_Transform(ST_Force2D($GEOMETRY_NAME), 3857)
  WHERE
    ST_Within($GEOMETRY_NAME, ST_MakeEnvelope(-180, -85, 180, 85, 4326));"

# For all polygons outside of WM bounds, clip then reproject to WM
psql -c "
  UPDATE
    \"$DATASET\".\"$VERSION\"
  SET
    ${GEOMETRY_NAME}_wm = ST_Transform(ST_Force2D(ST_Buffer(ST_Intersection($GEOMETRY_NAME, ST_MakeEnvelope(-180, -85, 180, 85, 4326)), 0)), 3857)
  WHERE
    NOT ST_Within($GEOMETRY_NAME, ST_MakeEnvelope(-180, -85, 180, 85, 4326));"