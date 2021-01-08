#!/bin/bash

# Use Python json.dumps to escape special characters for JSON payload
json_escape () {
    python -c 'import json,sys; print(json.dumps(sys.stdin.read())[1:-1])'
}

# SERVICE_ACCOUNT_TOKEN, STATUS_URL are put in the env from WRITER_SECRETS
# AWS_BATCH_JOB_ID is put in the env by AWS Batch/moto
HEADERS="Authorization: Bearer $SERVICE_ACCOUNT_TOKEN"
URL=${STATUS_URL}/${AWS_BATCH_JOB_ID}

OUTPUT_FILE="/tmp/${AWS_BATCH_JOB_ID}_output.txt"

# Execute command, save the exit code and output (stdout AND stderr)
"$@" 2>&1 | tee $OUTPUT_FILE
EXIT_CODE="${PIPESTATUS[0]}"

echo COMMAND EXIT CODE: $EXIT_CODE

# Escape all quotes inside command to not break JSON payload
ESC_COMMAND=$(echo -n "$*" | json_escape)

# Also make sure we don't reveal any sensitive information
# But we still want to know if the var was set
ESC_OUTPUT="$(cat $OUTPUT_FILE \
  | sed 's/^AWS_SECRET_ACCESS_KEY.*$/AWS_SECRET_ACCESS_KEY=\*\*\*/' \
  | sed 's/^AWS_ACCESS_KEY_ID.*$/AWS_ACCESS_KEY_ID=\*\*\*/' \
  | sed 's/^PGPASSWORD.*$/PGPASSWORD=\*\*\*/' \
  | sed 's/^PGUSER.*$/PGUSER=\*\*\*/' \
  | sed 's/^PGDATABASE.*$/PGDATABASE=\*\*\*/' \
  | sed 's/^PGHOST.*$/PGHOST=\*\*\*/' \
  | sed 's/^SERVICE_ACCOUNT_TOKEN.*$/SERVICE_ACCOUNT_TOKEN=\*\*\*/' \
  | sed 's/^GPG_KEY.*$/GPG_KEY=\*\*\*/' \
  | tail -c 1000 \
  | json_escape
)"

if [ "$EXIT_CODE" -eq 0 ]; then
    STATUS="success"
    MESSAGE="Successfully ran command [ $ESC_COMMAND ]"
    DETAIL=""
else
    STATUS="failed"
    MESSAGE="Command [ $ESC_COMMAND ] encountered errors"
    DETAIL=$ESC_OUTPUT # crop output length to be able to send using CURL
fi

AFTER=$(date '+%Y-%m-%d %H:%M:%S')

generate_payload()
{
  cat <<EOF
{
  "change_log": [{
    "date_time": "$AFTER",
    "status": "$STATUS",
    "message": "$MESSAGE",
    "detail": "$DETAIL"
  }]
}
EOF
}

echo "$(generate_payload)"

curl -s -X PATCH -H "${HEADERS}" -d "$(generate_payload)" "${URL}"


# Try to clean up to free space for potential other batch jobs on the same node
set +e
pushd /tmp
WORK_DIR="/tmp/$AWS_BATCH_JOB_ID"
rm -R "$WORK_DIR"
rm "$OUTPUT_FILE"
set -e


if [ "$EXIT_CODE" -eq 0 ]; then
    exit 0
else
    exit 1
fi