#!/bin/bash

#set -e

# requires arguments
# -d | --dataset
# -v | --version
# -s | --source
. get_arguments.sh "$@"

# Fetch first 100 rows from input table, analyse and create table create statement
# shellcheck disable=SC2086
aws s3 cp ${SRC} - | head -100 | csvsql -i postgresql --no-constraints --tables "$DATASET"."$VERSION" > create_table.sql

# The `head` command will cause a broken pipe error for `aws s3 cp`, this is expected and can be ignored
# We hence only set -e here
set -e

# TODO: Make sure ENV variables are correctly set for authentication
#  https://www.postgresql.org/docs/11/libpq-envars.html
psql -f create_table.sql

DELIMITER=$(python3 ../python/check_csv.py "${SRC}")
aws s3 cp "${SRC}" - | psql -c "COPY \"$DATASET\".\"$VERSION\" FROM STDIN WITH DELIMITER $DELIMITER CSV HEADER"

