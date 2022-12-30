#!/bin/bash
set -u

# This script is meant to be sourced by another shell script, and all it
# does is compose a SQL snippet and set a variable to it. Note that it
# requires the environment variables used below to be set, and exits with
# an error if one is not (thanks to the set -u).

ADD_GFW_FIELDS_SQL="
  ALTER TABLE ${TABLE_MISSING_COLUMNS} ADD COLUMN ${GEOMETRY_NAME}_wm geometry(${GEOMETRY_TYPE},3857);
  ALTER TABLE ${TABLE_MISSING_COLUMNS} ALTER COLUMN ${GEOMETRY_NAME}_wm SET STORAGE EXTERNAL;
  ALTER TABLE ${TABLE_MISSING_COLUMNS} ADD COLUMN gfw_area__ha NUMERIC;
  ALTER TABLE ${TABLE_MISSING_COLUMNS} ADD COLUMN gfw_geostore_id UUID;
  ALTER TABLE ${TABLE_MISSING_COLUMNS} ADD COLUMN gfw_geojson TEXT COLLATE pg_catalog.\"default\";
  ALTER TABLE ${TABLE_MISSING_COLUMNS} ADD COLUMN gfw_bbox NUMERIC[];
  ALTER TABLE ${TABLE_MISSING_COLUMNS} ADD COLUMN created_on timestamp without time zone DEFAULT now();
  ALTER TABLE ${TABLE_MISSING_COLUMNS} ADD COLUMN updated_on timestamp without time zone DEFAULT now();"