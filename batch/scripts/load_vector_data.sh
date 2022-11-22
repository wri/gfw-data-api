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

ME=$(basename "$0")
. get_arguments.sh "$@"

echo "AWSCLI: COPY DATA FROM $SRC TO $LOCAL_FILE"
aws s3 cp "$SRC" "$LOCAL_FILE"

# use virtual GDAL vsizip wrapper for ZIP files
# TODO: [GTC-661] Allow for a more flexible file structure inside the ZIP file
# the current implementation assumes that the file sits at the root level of
# the zip file and can't be in a sub directory.
if [ "${ZIPPED}" == "True" ]; then
  LOCAL_FILE="/vsizip/${LOCAL_FILE}"
fi

TEMP_TABLE=temp_table

#CREATE_TEMP_TABLE_SQL="
#  CREATE TEMPORARY TABLE $TEMP_TABLE (LIKE \"$DATASET\".\"$VERSION\" INCLUDING ALL) ON COMMIT DROP;
#  ALTER TABLE $TEMP_TABLE ALTER COLUMN gfw_geostore_id DROP NOT NULL
#"
CREATE_TEMP_TABLE_SQL="
  CREATE TEMPORARY TABLE $TEMP_TABLE (LIKE \"$DATASET\".\"$VERSION\") ON COMMIT DROP;
  ALTER TABLE $TEMP_TABLE ALTER COLUMN gfw_geostore_id DROP NOT NULL
"
#CREATE_TEMP_TABLE_SQL="
#  CREATE TEMPORARY TABLE $TEMP_TABLE AS \"$DATASET\".\"$VERSION\" WITH NO DATA;
#  ALTER TABLE $TEMP_TABLE ALTER COLUMN gfw_geostore_id DROP NOT NULL
#"

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

echo "OGR2OGR: Import \"${DATASET}\".\"${VERSION}\" from ${LOCAL_FILE} ${SRC_LAYER}"
ogr2ogr -f "PostgreSQL" PG:"password=$PGPASSWORD host=$PGHOST port=$PGPORT dbname=$PGDATABASE user=$PGUSER" \
    "$LOCAL_FILE" "$SRC_LAYER" \
    -doo PRELUDE_STATEMENTS="BEGIN; $CREATE_TEMP_TABLE_SQL;" \
    -doo CLOSING_STATEMENTS="$ENRICH_SQL; $COPY_FROM_TEMP_SQL; COMMIT;" \
    -nlt PROMOTE_TO_MULTI \
    -nln $TEMP_TABLE \
    -t_srs EPSG:4326 \
    --config PG_USE_COPY YES \
    -update -append -makevalid