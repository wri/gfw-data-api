#!/bin/bash
set -u

ADD_GFW_FIELDS_SQL="
  ALTER TABLE ${TABLE_MISSING_COLUMNS} ADD COLUMN ${GEOMETRY_NAME}_wm geometry(${GEOMETRY_TYPE},3857);
  ALTER TABLE ${TABLE_MISSING_COLUMNS} ALTER COLUMN ${GEOMETRY_NAME}_wm SET STORAGE EXTERNAL;
  ALTER TABLE ${TABLE_MISSING_COLUMNS} ADD COLUMN gfw_area__ha NUMERIC;
  ALTER TABLE ${TABLE_MISSING_COLUMNS} ADD COLUMN gfw_geostore_id UUID;
  ALTER TABLE ${TABLE_MISSING_COLUMNS} ADD COLUMN gfw_geojson TEXT COLLATE pg_catalog.\"default\";
  ALTER TABLE ${TABLE_MISSING_COLUMNS} ADD COLUMN gfw_bbox NUMERIC[];
  ALTER TABLE ${TABLE_MISSING_COLUMNS} ADD COLUMN created_on timestamp without time zone DEFAULT now();
  ALTER TABLE ${TABLE_MISSING_COLUMNS} ADD COLUMN updated_on timestamp without time zone DEFAULT now();"