#!/bin/bash

set -e

# requires arguments
# -d | --dataset
# -v | --version
# -s | --source

# optional arguments
# -g | --geometry_name (get_arguments.sh specifies default)

ME=$(basename "$0")
. get_arguments.sh "$@"

set -u

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

for uri in "${SRC[@]}"; do
  # convert to vsis3 protocol for ogr
  VSIS3_URI=$(sed 's/s3:\//\/vsis3/g' <<< "$uri")

  # since CSV has no inherent concept of CRS, just manually set source CRS (s_srs) to EPSG:4326
  ogr2ogr -f "PostgreSQL" PG:"password=$PGPASSWORD host=$PGHOST port=$PGPORT dbname=$PGDATABASE user=$PGUSER" \
    "$VSIS3_URI" \
    -oo GEOM_POSSIBLE_NAMES="$GEOMETRY_NAME" -oo KEEP_GEOM_COLUMNS=NO \
    -doo CLOSING_STATEMENTS="$ADD_GFW_FIELDS_SQL; $FILL_GFW_FIELDS_SQL; $COPY_FROM_TEMP_SQL;" \
    -lco TEMPORARY=ON \
    -nlt PROMOTE_TO_MULTI \
    -nln $TEMP_TABLE \
    -t_srs EPSG:4326 \
    -s_srs EPSG:4326 \
    --config PG_USE_COPY YES \
    -makevalid -update
done