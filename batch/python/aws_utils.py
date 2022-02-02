import os
from typing import Tuple

import boto3

AWS_REGION = os.environ.get("AWS_REGION")
AWS_ENDPOINT_URL = os.environ.get("ENDPOINT_URL")  # For boto


def get_s3_client(aws_region=AWS_REGION, endpoint_url=AWS_ENDPOINT_URL):
    return boto3.client("s3", region_name=aws_region, endpoint_url=endpoint_url)


def get_s3_path_parts(s3url) -> Tuple[str, str]:
    """Splits an S3 URL into bucket and key."""
    just_path = s3url.split("s3://")[1]
    bucket = just_path.split("/")[0]
    key = "/".join(just_path.split("/")[1:])
    return bucket, key


def exists_in_s3(target_bucket, target_key):
    """Returns whether or not target_key exists in target_bucket."""
    s3_client = get_s3_client()
    response = s3_client.list_objects_v2(
        Bucket=target_bucket,
        Prefix=target_key,
    )
    for obj in response.get("Contents", []):
        if obj["Key"] == target_key:
            return obj["Size"] > 0
