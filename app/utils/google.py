import json
from functools import lru_cache
from typing import List, Optional, Sequence, Dict

from google.cloud.storage import Client
from google.oauth2 import service_account
from limiter import Limiter

from .aws import get_secret_client
from ..settings.globals import AWS_GCS_KEY_SECRET_ARN, S3_ENTRYPOINT_URL, AWS_REGION


# Don't hit the GCS API with more than 10 requests per second
limit_api_calls = Limiter(rate=10, capacity=10, consume=1)


@lru_cache(maxsize=1)
def get_gcs_service_account_auth_info() -> Dict[str, str]:
    secret_client = get_secret_client()
    response = secret_client.get_secret_value(SecretId=AWS_GCS_KEY_SECRET_ARN)
    return json.loads(response["SecretString"])


# @limit_api_calls
def get_prefix_objects(bucket: str, prefix: str, limit: Optional[int] = None) -> List[str]:
    """Get ALL object names under a bucket and prefix in GCS."""

    auth_info = get_gcs_service_account_auth_info()
    scopes = [
        "https://www.googleapis.com/auth/devstorage.read_only",
        "https://www.googleapis.com/auth/cloud-platform.read-only",
    ]

    account_info = {
        "scopes": scopes,
        **auth_info
    }

    service_account_info = service_account.Credentials.from_service_account_info(
        account_info
    )
    client = Client(project=None, credentials=service_account_info)

    blobs = client.list_blobs(bucket, prefix=prefix, max_results=limit)
    return [blob.name for blob in blobs]


def get_gs_files(
    bucket: str,
    prefix: str,
    limit: Optional[int] = None,
    exit_after_max: Optional[int] = None,
    extensions: Sequence[str] = tuple()
) -> List[str]:
    """Get matching object names under a bucket and prefix in GCS."""

    matches: List[str] = list()
    num_matches: int = 0

    for blob_name in get_prefix_objects(bucket, prefix, limit):
        if not extensions or any(blob_name.endswith(ext) for ext in extensions):
            matches.append(f"/vsigs/{bucket}/{blob_name}")
            num_matches += 1
            if exit_after_max and num_matches >= exit_after_max:
                return matches

    return matches
