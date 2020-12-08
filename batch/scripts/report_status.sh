#!/bin/bash

# Use Python json.dumps to escape special characters for JSON payload
json_escape () {
    printf '%s' "$1" | python -c 'import json,sys; print(json.dumps(sys.stdin.read())[1:-1])'
}

# SERVICE_ACCOUNT_TOKEN, STATUS_URL are put in the env from WRITER_SECRETS
# AWS_BATCH_JOB_ID is put in the env by AWS Batch/moto
HEADERS="Authorization: Bearer $SERVICE_ACCOUNT_TOKEN"
URL=${STATUS_URL}/${AWS_BATCH_JOB_ID}
echo "URL: $URL"

# Execute command, save the exit code and output (stdout AND stderr)
"$@" 2>&1 | tee output.txt
EXIT_CODE="${PIPESTATUS[0]}"

echo COMMAND EXIT CODE: $EXIT_CODE

cat output.txt | grep -i error
GREP_EXIT_CODE=$?

echo GREP EXIT CODE: $GREP_EXIT_CODE

# escape all quotes inside command to not break JSON payload
# Also make sure we don't reveal any sensitive information
# But we still want to know if the var was set
ESC_COMMAND=$(json_escape "$*")

cat output.txt \
  | sed 's/^AWS_SECRET_ACCESS_KEY.*$/AWS_SECRET_ACCESS_KEY=\*\*\*/' \
  | sed 's/^AWS_ACCESS_KEY_ID.*$/AWS_ACCESS_KEY_ID=\*\*\*/' \
  | sed 's/^PGPASSWORD.*$/PGPASSWORD=\*\*\*/' \
  | sed 's/^PGUSER.*$/PGUSER=\*\*\*/' \
  | sed 's/^PGDATABASE.*$/PGDATABASE=\*\*\*/' \
  | sed 's/^PGHOST.*$/PGHOST=\*\*\*/' \
  | sed 's/^SERVICE_ACCOUNT_TOKEN.*$/SERVICE_ACCOUNT_TOKEN=\*\*\*/' \
  | sed 's/^GPG_KEY.*$/GPG_KEY=\*\*\*/' \
  | tail -c 1000 \
  > filtered_output.txt

ESC_OUTPUT="$(cat filtered_output.txt | python -c 'import json,sys; print(json.dumps(sys.stdin.read())[1:-1])')"

if [ $EXIT_CODE -eq 0 ] && [ $GREP_EXIT_CODE -ne 0 ]; then
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
set -e


if [ $EXIT_CODE -eq 0 ] && [ $GREP_EXIT_CODE -ne 0 ]; then
    exit 0
else
    exit 1
fi