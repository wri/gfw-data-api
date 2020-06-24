#!/bin/bash
COMMAND="$@"

# ADMIN_TOKEN, STATUS_URL are put in the env from WRITER_SECRETS
# AWS_BATCH_JOB_ID is put in the env by AWS Batch/moto
HEADERS="Authorization: Bearer $ADMIN_TOKEN"
URL=${STATUS_URL}/${AWS_BATCH_JOB_ID}
echo "URL: $URL"

# Execute command, save the exit code and output (stdout AND stderr)
OUTPUT=$($COMMAND 2>&1)
EXIT_CODE=$?

echo COMMAND EXIT CODE: $EXIT_CODE
echo OUTPUT: $OUTPUT

echo $OUTPUT | grep -i error
GREP_EXIT_CODE=$?

echo GREP EXIT CODE: $GREP_EXIT_CODE

if [ $EXIT_CODE -eq 0 ] && [ $GREP_EXIT_CODE -ne 0 ]; then
    STATUS="success"
    MESSAGE="Successfully ran command [ $COMMAND ]"
    DETAIL="None"
else
    STATUS="failure"
    MESSAGE="Command [ $COMMAND ] encountered errors"
    DETAIL="None" # Would be nice to attach properly escaped output here
fi

AFTER=`date '+%Y-%m-%d %H:%M:%S'`

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

curl -s -X PUT -H "${HEADERS}" -d "$(generate_payload)" "${URL}"

if [ $EXIT_CODE -eq 0 ] && [ $GREP_EXIT_CODE -ne 0 ]; then
    exit 0
else
    exit 1
fi