#!/bin/bash

set -e

# requires arguments
# -d | --dataset
# -v | --version
# -s | --source
# -m | --field_map

# optional arguments
# -g | --geometry_name (get_arguments.sh specifies default)
# -i | --fid_name (get_arguments.sh specifies default)

ME=$(basename "$0")
. get_arguments.sh "$@"

set -u

# I think Postgres temporary tables are such that concurrent jobs won't
# interfere with each other, but make the temp table name unique just
# in case.
UUID=$(python -c 'import uuid; print(uuid.uuid4(), end="")' | sed s/-//g)
TEMP_TABLE="temp_${UUID}"

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

# Credit to this answer for the next SQL query: https://dba.stackexchange.com/a/115315
ALL_NON_SERIAL_COLUMNS_SQL="SELECT string_agg(quote_ident(attname), ', ' ORDER BY attnum)
  FROM   pg_attribute
  WHERE  attrelid = '\"$DATASET\".\"$VERSION\"'::regclass
  AND    NOT attisdropped         -- no dropped (dead) columns
  AND    attnum > 0               -- no system columns
  AND    attname <> '$FID_NAME';  -- case sensitive!"

ALL_NON_SERIAL_COLUMNS=$(psql -X -A -t -c "$ALL_NON_SERIAL_COLUMNS_SQL")

COPY_FROM_TEMP_SQL="INSERT INTO \"$DATASET\".\"$VERSION\"($ALL_NON_SERIAL_COLUMNS) SELECT $ALL_NON_SERIAL_COLUMNS FROM $TEMP_TABLE"

LCO_ARGS=(-lco TEMPORARY=ON -lco GEOMETRY_NAME="$GEOMETRY_NAME" -lco SPATIAL_INDEX=NONE -lco FID="$FID_NAME")


if [[ -n "${FIELD_MAP:-}" ]]; then
  echo "OGR2OGR: Overriding table schema with that specified in creation options"
  COLUMN_TYPES=""
  for row in $(echo "${FIELD_MAP}" | jq -r '.[] | @base64'); do
    _jq() {
      echo "${row}" | base64 --decode | jq -r "${1}"
    }
    FIELD_NAME=$(_jq '.name')
    FIELD_TYPE=$(_jq '.data_type')

    COLUMN_TYPES+="$FIELD_NAME=$FIELD_TYPE,"
  done

  LCO_ARGS+=(-lco COLUMN_TYPES=${COLUMN_TYPES%?})
fi


for uri in "${SRC[@]}"; do
  # convert to vsis3 protocol for ogr
  VSIS3_URI=$(sed 's/s3:\//\/vsis3/g' <<< "$uri")

  # since CSV has no inherent concept of CRS, just manually set source CRS (s_srs) to EPSG:4326
  ogr2ogr -f "PostgreSQL" PG:"password=$PGPASSWORD host=$PGHOST port=$PGPORT dbname=$PGDATABASE user=$PGUSER" \
    "$VSIS3_URI" \
    -oo GEOM_POSSIBLE_NAMES="$GEOMETRY_NAME" -oo KEEP_GEOM_COLUMNS=NO \
    -doo CLOSING_STATEMENTS="$ADD_GFW_FIELDS_SQL; $FILL_GFW_FIELDS_SQL; $COPY_FROM_TEMP_SQL;" \
    "${LCO_ARGS[@]}" \
    -nlt PROMOTE_TO_MULTI \
    -nln $TEMP_TABLE \
    -t_srs EPSG:4326 \
    -s_srs EPSG:4326 \
    --config PG_USE_COPY YES \
    -makevalid -update
done