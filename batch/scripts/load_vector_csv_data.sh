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


TEMP_TABLE="temp_table"

GEOMETRY_TYPE_SQL="
  SELECT type
  FROM geometry_columns
  WHERE f_table_schema = '${DATASET}'
    AND f_table_name = '${VERSION}'
    AND f_geometry_column = '${GEOMETRY_NAME}';"

GEOMETRY_TYPE=$(psql -X -A -t -c "${GEOMETRY_TYPE_SQL}")

ADD_GFW_FIELDS_SQL="
  ALTER TABLE $TEMP_TABLE ADD COLUMN ${GEOMETRY_NAME}_wm geometry(${GEOMETRY_TYPE},3857);
  ALTER TABLE $TEMP_TABLE ALTER COLUMN ${GEOMETRY_NAME}_wm SET STORAGE EXTERNAL;
  ALTER TABLE $TEMP_TABLE ADD COLUMN gfw_area__ha NUMERIC;
  ALTER TABLE $TEMP_TABLE ADD COLUMN gfw_geostore_id UUID;
  ALTER TABLE $TEMP_TABLE ADD COLUMN gfw_geojson TEXT COLLATE pg_catalog.\"default\";
  ALTER TABLE $TEMP_TABLE ADD COLUMN gfw_bbox NUMERIC[];
  ALTER TABLE $TEMP_TABLE ADD COLUMN created_on timestamp without time zone DEFAULT now();
  ALTER TABLE $TEMP_TABLE ADD COLUMN updated_on timestamp without time zone DEFAULT now();"

ENRICH_SQL="
  UPDATE
    $TEMP_TABLE
  SET
    gfw_area__ha = ST_Area($GEOMETRY_NAME::geography)/10000,
    gfw_geostore_id = md5(ST_asgeojson($GEOMETRY_NAME))::uuid,
    gfw_geojson = ST_asGeojson($GEOMETRY_NAME),
    gfw_bbox = ARRAY[
      ST_XMin(ST_Envelope($GEOMETRY_NAME)::geometry),
      ST_YMin(ST_Envelope($GEOMETRY_NAME)::geometry),
      ST_XMax(ST_Envelope($GEOMETRY_NAME)::geometry),
      ST_YMax(ST_Envelope($GEOMETRY_NAME)::geometry)
    ]::NUMERIC[]"

COPY_FROM_TEMP_SQL="INSERT INTO \"$DATASET\".\"$VERSION\" SELECT * FROM $TEMP_TABLE"


for uri in "${SRC[@]}"; do
  # convert to vsis3 protocol for ogr
  VSIS3_URI=$(sed 's/s3:\//\/vsis3/g' <<< "$uri")

  # since CSV has no inherent concept of CRS, just manually set source CRS (s_srs) to EPSG:4326
  ogr2ogr -f "PostgreSQL" PG:"password=$PGPASSWORD host=$PGHOST port=$PGPORT dbname=$PGDATABASE user=$PGUSER" \
    "$VSIS3_URI" \
    -oo GEOM_POSSIBLE_NAMES="$GEOMETRY_NAME" -oo KEEP_GEOM_COLUMNS=NO \
    -doo CLOSING_STATEMENTS="$ADD_GFW_FIELDS_SQL; $ENRICH_SQL; $COPY_FROM_TEMP_SQL;" \
    -lco TEMPORARY=ON \
    -nlt PROMOTE_TO_MULTI \
    -nln $TEMP_TABLE \
    -t_srs EPSG:4326 \
    -s_srs EPSG:4326 \
    --config PG_USE_COPY YES \
    -makevalid -update
done