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
OUTPUT=$("$@" 2>&1)
EXIT_CODE=$?

echo COMMAND EXIT CODE: $EXIT_CODE
echo OUTPUT: "$OUTPUT"

echo "$OUTPUT" | grep -i error
GREP_EXIT_CODE=$?

echo GREP EXIT CODE: $GREP_EXIT_CODE

# escape all quotes inside command to not break JSON payload
# Also make sure we don't reveal any sensitive information
# But we still want to know if the var was set
ESC_COMMAND=$(json_escape "$*")

ESC_OUTPUT=$(echo "$OUTPUT" | sed 's/^AWS_SECRET_ACCESS_KEY.*$/AWS_SECRET_ACCESS_KEY=\*\*\*/') # pragma: allowlist secret
ESC_OUTPUT=$(echo "$ESC_OUTPUT" | sed 's/^AWS_ACCESS_KEY_ID.*$/AWS_ACCESS_KEY_ID=\*\*\*/')
ESC_OUTPUT=$(echo "$ESC_OUTPUT" | sed 's/^PGPASSWORD.*$/PGPASSWORD=\*\*\*/')  # pragma: allowlist secret
ESC_OUTPUT=$(echo "$ESC_OUTPUT" | sed 's/^PGUSER.*$/PGUSER=\*\*\*/')
ESC_OUTPUT=$(echo "$ESC_OUTPUT" | sed 's/^PGDATABASE.*$/PGDATABASE=\*\*\*/')
ESC_OUTPUT=$(echo "$ESC_OUTPUT" | sed 's/^PGHOST.*$/PGHOST=\*\*\*/')
ESC_OUTPUT=$(echo "$ESC_OUTPUT" | sed 's/^SERVICE_ACCOUNT_TOKEN.*$/SERVICE_ACCOUNT_TOKEN=\*\*\*/')
ESC_OUTPUT=$(echo "$ESC_OUTPUT" | sed 's/^GPG_KEY.*$/GPG_KEY=\*\*\*/')
ESC_OUTPUT=$(json_escape "$ESC_OUTPUT")

if [ $EXIT_CODE -eq 0 ] && [ $GREP_EXIT_CODE -ne 0 ]; then
    STATUS="success"
    MESSAGE="Successfully ran command [ $ESC_COMMAND ]"
    DETAIL=""
else
    STATUS="failed"
    MESSAGE="Command [ $ESC_COMMAND ] encountered errors"
    DETAIL="$ESC_OUTPUT"
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

echo PAYLOAD: "$(generate_payload)"

curl -s -X PATCH -H "${HEADERS}" -d "$(generate_payload)" "${URL}"

if [ $EXIT_CODE -eq 0 ] && [ $GREP_EXIT_CODE -ne 0 ]; then
    exit 0
else
    exit 1
fi