import json
from typing import Dict, List, Optional, Sequence

import aioboto3
import aiogoogle
from aiogoogle.auth.creds import ServiceAccountCreds
from async_lru import alru_cache
from limiter import Limiter

from ..settings.globals import AWS_GCS_KEY_SECRET_ARN, AWS_REGION, S3_ENTRYPOINT_URL


# Don't hit the GCS API with more than 10 requests per second
limit_api_calls = Limiter(rate=10, capacity=10, consume=1)


@alru_cache(maxsize=1)
async def get_gcs_service_account_key() -> Dict[str, str]:
    session = aioboto3.Session()
    async with session.client(
        "secretsmanager", region_name=AWS_REGION, endpoint_url=S3_ENTRYPOINT_URL
    ) as secrets_client:
        response = await secrets_client.get_secret_value(SecretId=AWS_GCS_KEY_SECRET_ARN)
        return json.loads(response["SecretString"])


@limit_api_calls
async def get_prefix_objects(bucket: str, prefix: str) -> List[str]:
    """Get ALL object names under a bucket and prefix in GCS."""

    service_account_info = await get_gcs_service_account_key()

    creds = ServiceAccountCreds(
        scopes=[
            "https://www.googleapis.com/auth/devstorage.read_only",
            "https://www.googleapis.com/auth/cloud-platform.read-only",
        ],
        **service_account_info
    )

    async with aiogoogle.Aiogoogle(service_account_creds=creds) as aiogoogle_api:
        storage = await aiogoogle_api.discover('storage', 'v1')
        response: aiogoogle.models.Response = await aiogoogle_api.as_service_account(
            storage.objects.list(bucket=bucket, prefix=prefix),
            full_res=True
        )
    results = response.json.get("items", [])

    return [blob["name"] for blob in results]


async def get_matching_gs_files(
    bucket: str,
    prefix: str,
    limit: Optional[int] = None,  # Ignored for this function! :(
    exit_after_max: Optional[int] = None,
    extensions: Sequence[str] = tuple(),
) -> List[str]:
    """Get matching object names under a bucket and prefix in GCS."""

    # NOTE: We can limit the number of results per page in list_gs_objects
    # but not the total results returned from GCS. So I'm afraid the limit
    # arg to this function is a lie :(

    matches: List[str] = list()
    num_matches: int = 0

    for blob_name in await get_prefix_objects(bucket, prefix):
        if not extensions or any(blob_name.endswith(ext) for ext in extensions):
            matches.append(f"/vsigs/{bucket}/{blob_name}")
            num_matches += 1
            if exit_after_max and num_matches >= exit_after_max:
                return matches

    return matches
