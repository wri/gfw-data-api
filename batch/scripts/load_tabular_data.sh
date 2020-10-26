#!/bin/bash

set -e

# requires arguments
# -d | --dataset
# -v | --version
# -s | --source
# -D | --delimiter
ME=$(basename "$0")
. get_arguments.sh "$@"


# Unescape TAB character
if [ "$DELIMITER" == "\t" ]; then
  DELIMITER=$(echo -e "\t")
fi

for uri in "${SRC[@]}"; do
  set +e
  FIELDS=$(aws s3 cp "${uri}" - | head -1)
  set -e

  FIELDS=$(sed -e $'s/\\t/,/g' -e 's/^/"/;s/$/"/' -e 's/,/","/g' -e 's/\r$//' <<< "$FIELDS")
  FIELDS="($FIELDS)"

  aws s3 cp "${uri}" - | sed -e 's/\r$//' | psql -c "COPY \"$DATASET\".\"$VERSION\" $FIELDS FROM STDIN WITH (FORMAT CSV, DELIMITER '$DELIMITER', HEADER)"
done
