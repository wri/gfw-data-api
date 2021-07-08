from typing import Any, Dict

import boto3
import httpx
from httpx_auth import AWS4Auth

from ..settings.globals import AWS_REGION, LAMBDA_ENTRYPOINT_URL, S3_ENTRYPOINT_URL
from .path import split_s3_path


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


get_s3_client = client_constructor("s3", S3_ENTRYPOINT_URL)
get_batch_client = client_constructor("batch")
get_cloudfront_client = client_constructor("cloudfront")
get_ecs_client = client_constructor("ecs")
get_lambda_client = client_constructor("lambda")


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


async def head_s3(s3_path: str) -> bool:
    auth = _aws_auth("s3")
    bucket, key = split_s3_path(s3_path)

    async with httpx.AsyncClient() as client:
        response: httpx.Response = await client.head(
            f"https://{bucket}.s3.amazonaws.com/{key}", auth=auth
        )

    return response.status_code == 200


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
