from typing import Any, Dict, List, Optional, Sequence

import boto3
import httpx
from httpx_auth import AWS4Auth

from ..settings.globals import (
    AWS_REGION,
    AWS_SECRETSMANAGER_URL,
    LAMBDA_ENTRYPOINT_URL,
    S3_ENTRYPOINT_URL,
)


def client_constructor(service: str, entrypoint_url=None):
    """Using closure design for a client constructor This way we only need to
    create the client once in central location and it will be easier to
    mock."""
    service_client = None

    def client():
        nonlocal service_client
        if service_client is None:
            service_client = boto3.client(
                service, region_name=AWS_REGION, endpoint_url=entrypoint_url
            )
        return service_client

    return client


get_batch_client = client_constructor("batch")
get_cloudfront_client = client_constructor("cloudfront")
get_ecs_client = client_constructor("ecs")
get_lambda_client = client_constructor("lambda")
get_api_gateway_client = client_constructor("apigateway")
get_s3_client = client_constructor("s3", S3_ENTRYPOINT_URL)
get_secret_client = client_constructor("secretsmanager", AWS_SECRETSMANAGER_URL)


async def invoke_lambda(
    lambda_name: str, payload: Dict[str, Any], timeout: int = 55
) -> httpx.Response:

    auth = _aws_auth("lambda")
    headers = {"X-Amz-Invocation-Type": "RequestResponse"}

    async with httpx.AsyncClient() as client:
        response: httpx.Response = await client.post(
            f"{LAMBDA_ENTRYPOINT_URL}/2015-03-31/functions/{lambda_name}/invocations",
            json=payload,
            auth=auth,
            timeout=timeout,
            headers=headers,
        )

    return response


async def head_s3(bucket: str, key: str) -> bool:
    auth = _aws_auth("s3")

    if S3_ENTRYPOINT_URL:
        s3_entrypoint_url = f"{S3_ENTRYPOINT_URL}/{bucket}"
    else:
        s3_entrypoint_url = f"https://{bucket}.s3.amazonaws.com"

    async with httpx.AsyncClient() as client:
        response: httpx.Response = await client.head(
            f"{s3_entrypoint_url}/{key}", auth=auth
        )

    return response.status_code == 200


def get_aws_files(
    bucket: str,
    prefix: str,
    limit: Optional[int] = None,
    exit_after_max: Optional[int] = None,
    extensions: Sequence[str] = tuple(),
) -> List[str]:
    """Get all matching files in S3."""

    matches: List[str] = list()
    num_matches: int = 0

    s3_client = get_s3_client()
    paginator = s3_client.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(
        Bucket=bucket, Prefix=prefix, PaginationConfig={"MaxItems": limit}
    )

    try:
        for page in page_iterator:
            try:
                contents = page["Contents"]
            except KeyError:
                break

            for obj in contents:
                key = str(obj["Key"])
                if not extensions or any(key.endswith(ext) for ext in extensions):
                    matches.append(f"/vsis3/{bucket}/{key}")
                    num_matches += 1
                    if exit_after_max and num_matches >= exit_after_max:
                        break
            if exit_after_max and num_matches >= exit_after_max:
                break

    except s3_client.exceptions.NoSuchBucket:
        matches = list()

    return matches


def _aws_auth(service: str) -> AWS4Auth:
    session = boto3.Session()
    cred = session.get_credentials()

    return AWS4Auth(
        access_id=cred.access_key,
        secret_key=cred.secret_key,
        security_token=cred.token,
        region=AWS_REGION,
        service=service,
    )
