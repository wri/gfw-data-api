def lambda_handler(event, context):
    policy = {
        "principalId": "x-api-key",
        "policyDocument": {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Action": "execute-api:Invoke",
                    "Effect": "Allow",
                    "Resource": event["methodArn"],
                }
            ],
        },
        "usageIdentifierKey": event["headers"].get("x-api-key", None)
        or event["queryStringParameters"].get("x-api-key", ""),
    }

    return policy
