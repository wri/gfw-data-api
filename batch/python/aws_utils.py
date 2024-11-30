import os
from typing import List, Sequence, Tuple, Dict, Any

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


def get_aws_files(
    bucket: str, prefix: str, extensions: Sequence[str] = (".tif",)
) -> List[str]:
    """Get all matching files in S3."""
    files: List[str] = list()

    s3_client = get_s3_client()
    paginator = s3_client.get_paginator("list_objects_v2")

    print("get_aws_files")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        try:
            contents = page["Contents"]
        except KeyError:
            break

        for obj in contents:
            key = str(obj["Key"])
            if any(key.endswith(ext) for ext in extensions):
                files.append(f"/vsis3/{bucket}/{key}")

    print("done get_aws_files")
    return files


def upload_s3(path: str, bucket: str, dst: str) -> Dict[str, Any]:
    s3_client = get_s3_client()
    return s3_client.upload_file(path, bucket, dst)
