#!/bin/bash

set -e

echo "OGR2OGR: Export ${DATASET}.${VERSION} to ${DST} using driver ${DRIVER}"
ogr2ogr -f "${DRIVER}" "${DST}" \
        PG:"password=$PGPASSWORD host=$PGHOST port=$PGPORT dbname=$PGDATABASE user=$PGUSER" \
        -sql "select ${COLUMNS} from ${DATASET}.${VERSION}"
