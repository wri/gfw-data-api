#!/bin/bash

set -e

# requires arguments
# -d | --dataset
# -v | --version
# -s | --source
# -p | --partition_type
# -c | --column_name
# -m | --field_map
ME=$(basename "$0")
. get_arguments.sh "$@"

# The `head` command will cause a broken pipe error for `aws s3 cp`, this is expected and can be ignored
# We hence we have to temporarily use set +e here
set +e
# Fetch first 100 rows from input table, analyse and create table create statement
aws s3 cp "${SRC}" - | head -100 | csvsql -i postgresql --no-constraints --tables "$VERSION" -q \" > create_table.sql

set -e

# csvsql sets the quotes for schema and table wrong. It is saver to set the schema separately
sed -i "1s/^/SET SCHEMA '$DATASET';\n/" create_table.sql

psql -c "CREATE TABLE \"$DATASET\".\"$VERSION\" (LIKE \"$DATASET\".\"$PARENT_VERSION}\")
          PARTITION BY LIST(version)"
psql -c "ALTER TABLE ONLY \"$DATASET\".\"$VERSION\" ALTER COLUMN version SET DEFAULT \"$VERSION\""
psql -c "CREATE TABLE \"$DATASET\".\"_$VERSION\" (LIKE \"$DATASET\".\"$VERSION\")
          PARTITION OF \"$DATASET\".\"$VERSION\" FOR VALUES IN \"$VERSION\""
psql -c "ALTER TABLE \"$DATASET\".\"$VERSION\" ATTACH PARTITION \"$DATASET\".\"$PARENT_VERSION}\"
          FOR VALUES IN \"$PARENT_VERSION\""

cat create_table.sql

psql -f create_table.sql

