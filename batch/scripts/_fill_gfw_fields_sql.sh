#!/bin/bash
set -u

# This script is meant to be sourced by another shell script, and all it
# does is compose a SQL snippet and set a variable to it. Note that it
# requires the environment variables used below to be set, and exits with
# an error if one is not (thanks to the set -u).

FILL_GFW_FIELDS_SQL="
  UPDATE
    $TABLE_MISSING_COLUMNS
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