from typing import Tuple
from urllib.parse import urlparse

from app.settings.globals import AWS_REGION


S3_CLIENT = None
BATCH_CLIENT = None


def split_s3_path(s3_path: str) -> Tuple[str, str]:
    o = urlparse(s3_path, allow_fragments=False)
    return o.netloc, o.path.lstrip("/")


def get_s3_client():
    import boto3

    global S3_CLIENT
    if S3_CLIENT is None:
        S3_CLIENT = boto3.client("batch", region_name=AWS_REGION)
    return S3_CLIENT


def get_batch_client():
    import boto3

    global BATCH_CLIENT
    if BATCH_CLIENT is None:
        BATCH_CLIENT = boto3.client("batch", region_name=AWS_REGION)
    return BATCH_CLIENT
