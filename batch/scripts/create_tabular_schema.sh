#!/bin/bash

set -e

# requires arguments
# -d | --dataset
# -v | --version
# -s | --source
# -D | --delimiter

# optional arguments
# -p | --partition_type
# -c | --column_name
# -m | --field_map
# -u | --unique_constraint

ME=$(basename "$0")
. get_arguments.sh "$@"


if [[ -n "${UNIQUE_CONSTRAINT_COLUMN_NAMES}" ]]; then
  UNIQUE_CONSTRAINT="--unique-constraint ${UNIQUE_CONSTRAINT_COLUMN_NAMES}"
fi

# Unescape TAB character
if [ "$DELIMITER" == "\t" ]; then
  DELIMITER=$(echo -e "\t")
fi

# Initialize flag to track if we've found a valid source
FOUND_VALID_SOURCE=0

# Loop through each source URI in the SRC array.
# It is possible that the first source URI with have only a header
# and no data. When that happens, a schema cannot be inferred by csvsql.
# Looping over several source URIs lessens the chance of this rare occurrence
# happening.
for SRC_URI in "${SRC[@]}"; do
  echo "Trying source: $SRC_URI"

  # Fetch first 100 lines from CSV, analyze and create table create statement
  # The `head` command will cause a broken pipe error for `aws s3 cp`: This is
  # expected and can be ignored, so temporarily use set +e here. HOWEVER we
  # still want to know if csvsql had a problem, so check its exit status
  set +e
  aws s3 cp "${SRC_URI}" - | head -100 | csvsql -d "$DELIMITER" -i postgresql --no-constraints $UNIQUE_CONSTRAINT --tables "$VERSION" -q \" > create_table.sql
  CSVSQL_EXIT_CODE="${PIPESTATUS[2]}"  # Grab the exit code of the csvsql command
  set -e

  if [ $CSVSQL_EXIT_CODE -ne 0 ]; then
    echo "csvsql exited with code ${CSVSQL_EXIT_CODE}"
    exit $CSVSQL_EXIT_CODE
  fi

   # Check if create_table.sql is not empty
   if [ -s "create_table.sql" ]; then
     FOUND_VALID_SOURCE=1
     break  # Break out of the loop since we found a valid source
   else
     echo "create_table.sql was empty for source ${SRC_URI}, trying next source..."
   fi
done

if [ $FOUND_VALID_SOURCE -eq 0 ]; then
  echo "Error: No source URI succeeded in producing a valid table definition."
  exit 1
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

# After enforcing the field map above, the last field will now have a trailing
# comma before a parenthesis . Remove it.
# This is difficult to do with sed because the comma and parenthesis are on
# different lines, and sed likes to work with a line at a time. So first turn
# all tabs into spaces, turn all newlines into spaces, and then squish all
# consecutive spaces into one. Finally remove the comma(s).
cat create_table.sql |tr "\t" " " | tr "\n" " "| tr -s " " | sed "s/, )/)/" > create_table_squeezed.sql

echo "Resulting create_table_squeezed.sql:"
cat create_table_squeezed.sql
echo "end of create_table_squeezed.sql"

psql -v ON_ERROR_STOP=1 -f create_table_squeezed.sql