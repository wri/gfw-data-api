#!/bin/bash

set -e

# requires arguments
# -d | --dataset
# -v | --version
# -s | --source
# -l | --source_layer
# -f | --local_file
# -m | --field_map
# -X | --zipped

# optional arguments
# -g | --geometry_name (get_arguments.sh specifies default)
# -i | --fid_name (get_arguments.sh specifies default)

ME=$(basename "$0")
. get_arguments.sh "$@"

set -u

# TODO: Downloading the whole file for this step might be unnecessary
# See https://gfw.atlassian.net/browse/GTC-2233
echo "AWSCLI: COPY DATA FROM $SRC TO $LOCAL_FILE"
aws s3 cp "$SRC" "$LOCAL_FILE" --no-progress

# use virtual GDAL vsizip wrapper for ZIP files
# TODO: [GTC-661] Allow for a more flexible file structure inside the ZIP file
#  the current implementation assumes that the file sits at the root level of the zip file
#  and can't be in sub directory.
if [ "${ZIPPED}" == "True" ]; then
  LOCAL_FILE="/vsizip/${LOCAL_FILE}"
fi

args=(-f "PostgreSQL" PG:"password=$PGPASSWORD host=$PGHOST port=$PGPORT dbname=$PGDATABASE user=$PGUSER" \
     "$LOCAL_FILE" "$SRC_LAYER" \
     -nlt PROMOTE_TO_MULTI -nln "$DATASET.$VERSION" \
     -lco GEOMETRY_NAME="$GEOMETRY_NAME" -lco SPATIAL_INDEX=NONE -lco FID="$FID_NAME" \
     -t_srs EPSG:4326 -limit 0 -dim xy)

# If source is CSV, there's no SRS information so add it manually
if [[ "$SRC" == *".csv" ]]; then
  echo "OGR2OGR: Set SRS for CSV source"
  args+=(-s_srs EPSG:4326 -oo GEOM_POSSIBLE_NAMES="$GEOMETRY_NAME" -oo KEEP_GEOM_COLUMNS=NO)
fi

if [[ -n "${FIELD_MAP:-}" ]]; then
  echo "OGR2OGR: Override table schema with that specified in creation options"
  COLUMN_TYPES=""
  for row in $(echo "${FIELD_MAP}" | jq -r '.[] | @base64'); do
      _jq() {
       echo "${row}" | base64 --decode | jq -r "${1}"
      }
     FIELD_NAME=$(_jq '.name')
     FIELD_TYPE=$(_jq '.data_type')

     COLUMN_TYPES+="$FIELD_NAME=$FIELD_TYPE,"
  done

  args+=(-lco COLUMN_TYPES=${COLUMN_TYPES%?})
fi

echo "OGR2OGR: Create table schema for \"${DATASET}\".\"${VERSION}\" from ${LOCAL_FILE} ${SRC_LAYER}"
ogr2ogr "${args[@]}"

# Set storage to external for faster querying
# http://blog.cleverelephant.ca/2018/09/postgis-external-storage.html
echo "PSQL: ALTER TABLE. Set storage external"
psql -c "ALTER TABLE \"$DATASET\".\"$VERSION\" ALTER COLUMN $GEOMETRY_NAME SET STORAGE EXTERNAL;"

echo "DONE"
set +u
