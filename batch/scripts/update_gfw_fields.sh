#!/bin/bash

set -e

# requires arguments
# -d | --dataset
# -v | --version

# optional arguments
# -g | --geometry_name (get_arguments.sh specifies default)

ME=$(basename "$0")
. get_arguments.sh "$@"

# Set GFW fields where needed (new records)
echo "PSQL: UPDATE \"$DATASET\".\"$VERSION\". Set GFW attributes"
psql -c "UPDATE \"$DATASET\".\"$VERSION\" SET
  gfw_area__ha = ST_Area($GEOMETRY_NAME::geography)/10000,
  gfw_geostore_id = md5(ST_asgeojson($GEOMETRY_NAME))::uuid,
  gfw_geojson = ST_asGeojson($GEOMETRY_NAME),
  gfw_bbox = ARRAY[
      ST_XMin(ST_Envelope($GEOMETRY_NAME)::geometry),
      ST_YMin(ST_Envelope($GEOMETRY_NAME)::geometry),
      ST_XMax(ST_Envelope($GEOMETRY_NAME)::geometry),
      ST_YMax(ST_Envelope($GEOMETRY_NAME)::geometry)]::NUMERIC[];"
#  WHERE NOT gfw_geostore_id;"

# splitting the transform to web mercator (WM) in two steps for performance
# to isolate the slower/involved one for polygons that overflow WM lat bounds of -85/85 deg
# transform to WM ensuring polygons outside WM bounds are clipped.
psql -c "UPDATE \"$DATASET\".\"$VERSION\" SET ${GEOMETRY_NAME}_wm = ST_Transform(ST_Force2D($GEOMETRY_NAME), 3857)
                                    WHERE ST_Within($GEOMETRY_NAME, ST_MakeEnvelope(-180, -85, 180, 85, 4326));"

# transform to web mercator (WM) ensuring polygons outside WM bounds are clipped.
psql -c "UPDATE \"$DATASET\".\"$VERSION\" SET ${GEOMETRY_NAME}_wm = ST_Transform(ST_Force2D(ST_Buffer(ST_Intersection($GEOMETRY_NAME, ST_MakeEnvelope(-180, -85, 180, 85, 4326)), 0)), 3857)
                                    WHERE NOT ST_Within($GEOMETRY_NAME, ST_MakeEnvelope(-180, -85, 180, 85, 4326));"

# Set gfw_geostore_id not NULL to be compliant with GEOSTORE
echo "PSQL: ALTER TABLE \"$DATASET\".\"$VERSION\". ALTER COLUMN gfw_geostore_id SET NOT NULL"
psql -c "ALTER TABLE \"$DATASET\".\"$VERSION\" ALTER COLUMN gfw_geostore_id SET NOT NULL;"