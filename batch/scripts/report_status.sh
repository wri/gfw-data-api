#!/bin/bash

# ADMIN_TOKEN, STATUS_URL are put in the env from WRITER_SECRETS
# AWS_BATCH_JOB_ID is put in the env by AWS Batch/moto
HEADERS="Authorization: Bearer $ADMIN_TOKEN"
URL=${STATUS_URL}/${AWS_BATCH_JOB_ID}

echo "URL: $URL"

BEFORE=`date '+%Y-%m-%d %H:%M:%S'`

COMMAND="$@"
echo Command: $COMMAND

# Execute command, save exit code
eval "$COMMAND"
EXIT_CODE=$?

AFTER=`date '+%Y-%m-%d %H:%M:%S'`


if [[ $EXIT_CODE -eq 0 ]]
then
    STATUS="complete"
    MESSAGE="Successfully ran [ $COMMAND ]"
    DETAIL="blah"
else
    STATUS="error"
    MESSAGE="Error: Command [ $COMMAND ] returned exit code $EXIT_CODE"
    DETAIL="bad blah"
fi

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

curl -s -X PUT -d "$(generate_payload)" "${URL}"
#curl -s -X PUT -H "$HEADERS" -d "$(generate_payload)" "$URL"

sleep 2

exit $EXIT_STATUS
