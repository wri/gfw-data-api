#!/bin/bash
set -u

# This script is meant to be sourced by another shell script, and all it
# does is compose a SQL snippet and set a variable to it. Note that it
# requires the environment variables used below to be set, and exits with
# an error if one is not (thanks to the set -u).

GEOMETRY_TYPE_SQL="
  SELECT type
  FROM geometry_columns
  WHERE f_table_schema = '${DATASET}'
    AND f_table_name = '${VERSION}'
    AND f_geometry_column = '${GEOMETRY_NAME}';"