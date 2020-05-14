#!/bin/bash

set -e

# requires arguments
# -d | --dataset
# -v | --version
. get_arguments.sh "@"


# Add GFW specific layers
echo "PSQL: ALTER TABLE $DATASET.$VERSION. Add GFW columns"
psql -c "ALTER TABLE $DATASET.$VERSION ADD COLUMN ${GEOMETRY_NAME}_wm geometry(MultiPolygon,3857);
         ALTER TABLE $DATASET.$VERSION ALTER COLUMN ${GEOMETRY_NAME}_wm SET STORAGE EXTERNAL;
         ALTER TABLE $DATASET.$VERSION ADD COLUMN gfw_area__ha NUMERIC;
         ALTER TABLE $DATASET.$VERSION ADD COLUMN gfw_geostore_id UUID;
         ALTER TABLE $DATASET.$VERSION ADD COLUMN gfw_geojson TEXT;
         ALTER TABLE $DATASET.$VERSION ADD COLUMN gfw_bbox BOX2D;"

# Update GFW columns
echo "PSQL: UPDATE $DATASET.$VERSION. Set GFW attributes"
psql -c "UPDATE $DATASET.$VERSION SET ${GEOMETRY_NAME}_wm = ST_Transform($GEOMETRY_NAME, 3857),
                                      gfw_area__ha = ST_area($GEOMETRY_NAME::geography)/10000,
                                      gfw_geostore_id = md5(ST_asgeojson($GEOMETRY_NAME))::uuid,
                                      gfw_geojson = ST_asgeojson($GEOMETRY_NAME),
                                      gfw_bbox = box2d($GEOMETRY_NAME);"