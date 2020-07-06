#!/bin/bash

set -e

# requires arguments
# -d | --dataset
# -v | --version
# -s | --source
# -D | --delimiter
# -fn | --field_names
ME=$(basename "$0")
. get_arguments.sh "$@"


# Unescape TAB character
if [ "$DELIMITER" == "\t" ]; then
  DELIMITER=$(echo -e "\t")
fi

for uri in "${SRC[@]}"; do
  if [ -z "$FIELD_NAMES" ]; then
    aws s3 cp "${uri}" - | psql -c "COPY \"$DATASET\".\"$VERSION\" FROM STDIN WITH (FORMAT CSV, DELIMITER '$DELIMITER', HEADER)"
  else
    aws s3 cp "${uri}" - | psql -c "COPY \"$DATASET\".\"$VERSION\" $FIELD_NAMES FROM STDIN WITH (FORMAT CSV, DELIMITER '$DELIMITER', HEADER)"
  fi
done
