#!/bin/bash
set -u

FILL_GFW_FIELDS_SQL="
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