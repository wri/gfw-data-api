#!/bin/bash

# Use Python json.dumps to escape special characters for JSON payload
json_escape () {
  python -c 'import json,sys; print(json.dumps(sys.stdin.read())[1:-1])'
}

# SERVICE_ACCOUNT_TOKEN, STATUS_URL are put in the env from WRITER_SECRETS
# AWS_BATCH_JOB_ID is put in the env by AWS Batch/moto
AUTH_HEADER="Authorization: Bearer $SERVICE_ACCOUNT_TOKEN"
URL=${STATUS_URL}/${AWS_BATCH_JOB_ID}

OUTPUT_FILE="/tmp/${AWS_BATCH_JOB_ID}_output.txt"

# Source the virtualenv with all our Python packages
. "${VENV_DIR}"/bin/activate

# If this is not the first attempt, and previous attempts failed due to OOM,
# reduce the NUM_PROCESSES value (thus increasing memory per process)
if [[ -n $AWS_BATCH_JOB_ATTEMPT ]] && [[ $AWS_BATCH_JOB_ATTEMPT -gt 1 ]]; then
  export NUM_PROCESSES=$(adjust_num_processes.py)
fi

# Execute command, save the exit code and output (stdout AND stderr)
"$@" 2>&1 | tee $OUTPUT_FILE
EXIT_CODE="${PIPESTATUS[0]}"

echo COMMAND EXIT CODE: $EXIT_CODE

# Escape all quotes inside command to not break JSON payload
ESC_COMMAND=$(echo -n "$*" | json_escape)

# Also make sure we don't reveal any sensitive information
# But we still want to know if the var was set
# Crop output length to be able to send using CURL
sed -i 's/^AWS_SECRET_ACCESS_KEY.*$/AWS_SECRET_ACCESS_KEY=\*\*\*/g' "$OUTPUT_FILE" # pragma: allowlist secret
sed -i 's/^PGPASSWORD.*$/PGPASSWORD=\*\*\*/g' "$OUTPUT_FILE"  # pragma: allowlist secret
ESC_OUTPUT="$(cat $OUTPUT_FILE \
  | sed 's/^AWS_ACCESS_KEY_ID.*$/AWS_ACCESS_KEY_ID=\*\*\*/g' \
  | sed 's/^PGUSER.*$/PGUSER=\*\*\*/g' \
  | sed 's/^PGDATABASE.*$/PGDATABASE=\*\*\*/g' \
  | sed 's/^PGHOST.*$/PGHOST=\*\*\*/g' \
  | sed 's/^SERVICE_ACCOUNT_TOKEN.*$/SERVICE_ACCOUNT_TOKEN=\*\*\*/g' \
  | sed 's/^GPG_KEY.*$/GPG_KEY=\*\*\*/g' \
  | tail -c 1000 \
  | json_escape
)"

# If a process was killed involuntarily, we probably ran out of memory.
# Exit with code 137 to trigger the retry logic. Better luck next time!
if [ "$EXIT_CODE" -eq 137 ]; then
    exit 137
elif [ "$EXIT_CODE" -eq 0 ]; then
    STATUS="success"
    MESSAGE="Successfully ran command [ $ESC_COMMAND ]"
    DETAIL=""
else
    STATUS="failed"
    MESSAGE="Command [ $ESC_COMMAND ] encountered errors"
    DETAIL=$ESC_OUTPUT
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

set -x

echo "$(generate_payload)"

CTYPE_HEADER="Content-Type:application/json"
RESPONSE=$(curl -s -X PATCH -H "${AUTH_HEADER}" -H "${CTYPE_HEADER}" -d "$(generate_payload)" "${URL}")

echo $RESPONSE

# Try to clean up free space for potential other batch jobs on the same node
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