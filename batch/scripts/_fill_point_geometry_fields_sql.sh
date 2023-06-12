#!/bin/bash
set -u

# This script is meant to be sourced by another shell script, and all it
# does is compose a SQL snippet and assign it to a variable. Note that it
# requires the environment variables used below to be set, and exits with
# an error if one is not (thanks to the set -u).

FILL_POINT_GEOMETRY_FIELDS_SQL="
  UPDATE
    \"$TEMP_TABLE\"
  SET
    ${GEOMETRY_NAME} = ST_SetSRID(ST_MakePoint($LNG, $LAT),4326),
    ${GEOMETRY_NAME}_wm = ST_Transform(ST_SetSRID(ST_MakePoint($LNG, $LAT),4326), 3857)
  WHERE
    ${GEOMETRY_NAME} IS null OR ${GEOMETRY_NAME}_wm IS null;"