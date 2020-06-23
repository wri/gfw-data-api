#!/bin/bash

# ADMIN_TOKEN, STATUS_URL are put in the env from WRITER_SECRETS
# AWS_BATCH_JOB_ID is put in the env by AWS Batch/moto
HEADERS="Authorization: Bearer $ADMIN_TOKEN"
URL=${STATUS_URL}/${AWS_BATCH_JOB_ID}

echo "URL: $URL"

BEFORE=`date '+%Y-%m-%d %H:%M:%S'`

# Execute command, save the exit code and output
COMMAND="$@"
echo "Command: $COMMAND"
$COMMAND 2>&1 >> ~/output.log
EXIT_CODE=$?
echo EXIT CODE: $EXIT_CODE

AFTER=`date '+%Y-%m-%d %H:%M:%S'`


if [[ $EXIT_CODE -eq 0 ]]
then
    STATUS="success"
    MESSAGE="Successfully ran command [ $COMMAND ]"
    DETAIL="None"
else
    STATUS="failure"
    MESSAGE="Command [ $COMMAND ] returned exit code [ $EXIT_CODE ]"
    DETAIL="None" # `tail -n100 ~/output.log`
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

echo PAYLOAD: "$(generate_payload)"

curl -s -X PUT -H "${HEADERS}" -d "$(generate_payload)" "${URL}"

sleep 2

exit $EXIT_STATUS
