#!/bin/bash

#set -e

# requires arguments
# -d | --dataset
# -v | --version
# -s | --source
# -p | --partition_type
# -c | --column_name
. get_arguments.sh "$@"

# Fetch first 100 rows from input table, analyse and create table create statement
aws s3 cp ${SRC} - | head -100 | csvsql -i postgresql --no-constraints --tables "$VERSION" > create_table.sql

# The `head` command will cause a broken pipe error for `aws s3 cp`, this is expected and can be ignored
# We hence only set -e here
set -e

# csvsql sets the quotes for schema and table wrong. It is saver to set the schema separately
sed -i "1s/^/SET SCHEMA '$DATASET';\n/" create_table.sql

# Make sure that table is create with partition if set
if [[ -n "${PARTITION_TYPE}" ]]; then
  sed -i "" "s/$);/) PARTITION BY $PARTITION_TYPE ($COLUMN_NAME);/g" create_table.sql
fi

psql -f create_table.sql

