#!/bin/bash

set -e

# requires arguments
# -d | --dataset
# -v | --version
# -s | --source
# -l | --source_layer
# -f | --local_file
# -X | --zipped

# optional arguments
# -g | --geometry_name (get_arguments.sh specifies default)
# -i | --fid_name (get_arguments.sh specifies default)

ME=$(basename "$0")
. get_arguments.sh "$@"

set -u

echo "AWSCLI: COPY DATA FROM $SRC TO $LOCAL_FILE"
aws s3 cp "$SRC" "$LOCAL_FILE"

# use virtual GDAL vsizip wrapper for ZIP files
# TODO: [GTC-661] Allow for a more flexible file structure inside the ZIP file
# the current implementation assumes that the file sits at the root level of
# the zip file and can't be in a sub directory.
if [ "${ZIPPED}" == "True" ]; then
  LOCAL_FILE="/vsizip/${LOCAL_FILE}"
fi

TEMP_TABLE="temp_table"

# Add GFW-specific columns to the new table
TABLE_MISSING_COLUMNS=$TEMP_TABLE

# GEOMETRY_TYPE_SQL is defined by sourcing _get_geometry_type_sql.sh
# It contains the SQL snippet we'll pass to the psql client command
. _get_geometry_type_sql.sh

# Get the geometry type of the new table
GEOMETRY_TYPE=$(psql -X -A -t -c "${GEOMETRY_TYPE_SQL}")

# ADD_GFW_FIELDS_SQL is defined by sourcing _add_gfw_fields_sql.sh
# It contains the SQL snippet we'll pass to ogr2ogr
. _add_gfw_fields_sql.sh

# FILL_GFW_FIELDS_SQL is defined by sourcing _fill_gfw_fields_sql.sh
# It contains a SQL snippet we'll pass to ogr2ogr
. _fill_gfw_fields_sql.sh

COPY_FROM_TEMP_SQL="INSERT INTO \"$DATASET\".\"$VERSION\" SELECT * FROM $TEMP_TABLE"

echo "OGR2OGR: Import \"${DATASET}\".\"${VERSION}\" from ${LOCAL_FILE} ${SRC_LAYER}"
ogr2ogr -f "PostgreSQL" PG:"password=$PGPASSWORD host=$PGHOST port=$PGPORT dbname=$PGDATABASE user=$PGUSER" \
  "$LOCAL_FILE" "$SRC_LAYER" \
  -doo CLOSING_STATEMENTS="$ADD_GFW_FIELDS_SQL; $FILL_GFW_FIELDS_SQL; $COPY_FROM_TEMP_SQL;" \
  -lco GEOMETRY_NAME="$GEOMETRY_NAME" -lco SPATIAL_INDEX=NONE -lco FID="$FID_NAME" \
  -lco TEMPORARY=ON \
  -nlt PROMOTE_TO_MULTI \
  -nln $TEMP_TABLE \
  -t_srs EPSG:4326 \
  --config PG_USE_COPY YES \
  -makevalid -update