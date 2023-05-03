#!/bin/bash

set -e

# requires arguments
# -d | --dataset
# -v | --version
# -s | --source
# -p | --partition_type
# -c | --column_name
# -m | --field_map
# -u | --unique_constraint

ME=$(basename "$0")
. get_arguments.sh "$@"


if [[ -n "${UNIQUE_CONSTRAINT_COLUMN_NAMES}" ]]; then
  UNIQUE_CONSTRAINT="--unique-constraint ${UNIQUE_CONSTRAINT_COLUMN_NAMES}"
fi

# Fetch first 100 lines from CSV, analyze and create table create statement
# The `head` command will cause a broken pipe error for `aws s3 cp`: This is
# expected and can be ignored, so temporarily use set +e here. HOWEVER we
# still want to know if csvsql had a problem, so check its exit status
set +e
aws s3 cp "${SRC}" - | head -100 | csvsql -i postgresql --no-constraints $UNIQUE_CONSTRAINT --tables "$VERSION" -q \" > create_table.sql
CSVSQL_EXIT_CODE="${PIPESTATUS[2]}"  # Grab the exit code of the csvsql command
set -e

if [ $CSVSQL_EXIT_CODE -ne 0 ]; then
  echo "csvsql exited with code ${CSVSQL_EXIT_CODE}"
  exit $CSVSQL_EXIT_CODE
fi

# csvsql sets the quotes for schema and table wrong. It is safer to set the schema separately
sed -i "1s/^/SET SCHEMA '$DATASET';\n/" create_table.sql

# If the user has specified a field map, override csvsql's inferred field
# types with those specified. It is expected that $FIELD_MAP contains (after
# sourcing get_arguments.sh above) a JSON List of the form
# '[{"name":"name1", "data_type":"type1"},{"name":"name2", "data_type":"type2"}]'
# https://starkandwayne.com/blog/bash-for-loop-over-json-array-using-jq/
if [[ -n "${FIELD_MAP}" ]]; then
  for row in $(echo "${FIELD_MAP}" | jq -r '.[] | @base64'); do
    _jq() {
      echo "${row}" | base64 --decode | jq -r "${1}"
    }
    FIELD_NAME=$(_jq '.name')
    FIELD_TYPE=$(_jq '.data_type')

    # Override the field types, whether naked or in double quotes
    sed -i "s/^\t${FIELD_NAME} .*$/\t${FIELD_NAME} ${FIELD_TYPE},/" create_table.sql
    sed -i "s/^\t\"${FIELD_NAME}\" .*$/\t\"${FIELD_NAME}\" ${FIELD_TYPE},/" create_table.sql
  done
fi

# Make sure that the table is created with partition if set
if [[ -n "${PARTITION_TYPE}" ]]; then
  echo "ADD PARTITION"
  sed -i "s/);$/) PARTITION BY $PARTITION_TYPE ($COLUMN_NAME);/g" create_table.sql
fi

cat create_table.sql

psql -v ON_ERROR_STOP=1 -f create_table.sql