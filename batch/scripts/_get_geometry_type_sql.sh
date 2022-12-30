#!/bin/bash
set -u

GEOMETRY_TYPE_SQL="
  SELECT type
  FROM geometry_columns
  WHERE f_table_schema = '${DATASET}'
    AND f_table_name = '${VERSION}'
    AND f_geometry_column = '${GEOMETRY_NAME}';"