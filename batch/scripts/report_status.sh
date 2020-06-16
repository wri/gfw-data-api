#!/bin/bash
HEADERS="Authorization: Bearer $ADMIN_TOKEN"
URL="$STATUS_URL/$AWS_BATCH_JOB_ID"

BEFORE=`date '+%Y-%m-%d %H:%M:%S'`

COMMAND="$@"
echo Command: $COMMAND

# Execute command, save exit code
eval "$COMMAND"
EXIT_STATUS=$?

AFTER=`date '+%Y-%m-%d %H:%M:%S'`


if [[ $EXIT_STATUS -eq 0 ]]
then
    STATUS="complete"
    MESSAGE="Successfully ran [ $COMMAND ]"
    DETAIL="blah"
else
    STATUS="error"
    MESSAGE="Error: Command [ $@ ] returned $EXIT_STATUS"
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

curl -X PUT -H "$HEADERS" -d "$(generate_payload)" "$URL"
return $EXIT_STATUS
