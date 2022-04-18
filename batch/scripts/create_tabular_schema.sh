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

# csvsql sets the quotes for schema and table wrong. It is safer to set the schema separately
sed -i "1s/^/SET SCHEMA '$DATASET';\n/" create_table.sql

# update field types
# This expects a JSON List like this '[{"field_name":"name1", "field_type":"type1"},{"field_name":"name2", "field_type":"type2"}]'
# It will export the different key value pairs as ENV variables so that we can reference them within the for loop
# We will then update rows within our create_table.sql file using the new field type
# https://starkandwayne.com/blog/bash-for-loop-over-json-array-using-jq/
if [[ -n "${FIELD_MAP}" ]]; then
  for row in $(echo "${FIELD_MAP}" | jq -r '.[] | @base64'); do
      _jq() {
       echo "${row}" | base64 --decode | jq -r "${1}"
      }
     FIELD_NAME=$(_jq '.field_name')
     FIELD_TYPE=$(_jq '.field_type')

     # field names might be in double quotes
     # make sure there is no comma after the last field
     sed -i "s/^\t${FIELD_NAME} .*$/\t${FIELD_NAME} ${FIELD_TYPE},/" create_table.sql
     sed -i "s/^\t\"${FIELD_NAME}\" .*$/\t\"${FIELD_NAME}\" ${FIELD_TYPE},/" create_table.sql
     sed -i 'x; ${s/,//;p;x}; 1d' create_table.sql
  done
fi

# Make sure that the table is created with partition if set
if [[ -n "${PARTITION_TYPE}" ]]; then
  echo "ADD PARTITION"
  sed -i "s/);$/) PARTITION BY $PARTITION_TYPE ($COLUMN_NAME);/g" create_table.sql
fi

cat create_table.sql

psql -f create_table.sql
