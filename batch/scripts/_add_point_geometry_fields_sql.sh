#!/bin/bash
set -u

# This script is meant to be sourced by another shell script, and all it
# does is compose a SQL snippet and assign it to a variable. Note that it
# requires the environment variables used below to be set, and exits with
# an error if one is not (thanks to the set -u).

ADD_POINT_GEOMETRY_FIELDS_SQL="
  ALTER TABLE
    \"$TEMP_TABLE\"
  ADD COLUMN
    ${GEOMETRY_NAME} geometry(Point,4326);

  ALTER TABLE
    \"$TEMP_TABLE\"
  ADD COLUMN
    ${GEOMETRY_NAME}_wm geometry(Point,3857);"